use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::executor::QueryExecutor;
use crate::sql::transform_bq_to_duckdb;

use super::registry::TableRegistry;
use super::types::{DagRunResult, TableDef, TableKind};

pub struct DagExecutor;

impl DagExecutor {
    pub fn new() -> Self {
        Self
    }

    pub async fn run(
        &self,
        registry: &TableRegistry,
        targets: &[String],
        executor: Arc<dyn QueryExecutor>,
        schema_name: &str,
    ) -> Result<DagRunResult> {
        let tables_to_run = self.get_required_tables(registry, targets)?;

        if tables_to_run.is_empty() {
            return Ok(DagRunResult {
                executed_tables: vec![],
            });
        }

        self.validate_dependencies(registry, &tables_to_run)?;

        let cancel_token = CancellationToken::new();

        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

        for name in &tables_to_run {
            let deps = registry.get(name).unwrap().dependencies();
            let relevant_deps: Vec<_> = deps.iter().filter(|d| tables_to_run.contains(*d)).collect();
            in_degree.insert(name.clone(), relevant_deps.len());

            for dep in relevant_deps {
                dependents.entry(dep.clone()).or_default().push(name.clone());
            }
        }

        let execution_order = Arc::new(Mutex::new(Vec::new()));
        let in_degree = Arc::new(Mutex::new(in_degree));

        let (tx, mut rx) = mpsc::unbounded_channel::<std::result::Result<String, Error>>();

        let mut pending = tables_to_run.len();

        {
            let in_deg = in_degree.lock().await;
            for (name, &deg) in in_deg.iter() {
                if deg == 0 {
                    self.spawn_table_task(
                        name.clone(),
                        registry.get(name).unwrap().clone(),
                        executor.clone(),
                        schema_name.to_string(),
                        tx.clone(),
                        cancel_token.clone(),
                    );
                }
            }
        }

        while pending > 0 {
            let result = rx.recv().await.ok_or_else(|| {
                Error::Internal("Channel closed unexpectedly".to_string())
            })?;

            match result {
                Ok(completed_name) => {
                    pending -= 1;
                    execution_order.lock().await.push(completed_name.clone());

                    if let Some(deps) = dependents.get(&completed_name) {
                        for dep_name in deps {
                            let mut in_deg = in_degree.lock().await;
                            if let Some(count) = in_deg.get_mut(dep_name) {
                                *count -= 1;
                                if *count == 0 {
                                    self.spawn_table_task(
                                        dep_name.clone(),
                                        registry.get(dep_name).unwrap().clone(),
                                        executor.clone(),
                                        schema_name.to_string(),
                                        tx.clone(),
                                        cancel_token.clone(),
                                    );
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    cancel_token.cancel();
                    return Err(e);
                }
            }
        }

        let order = Arc::try_unwrap(execution_order)
            .map_err(|_| Error::Internal("Failed to unwrap execution order".to_string()))?
            .into_inner();

        Ok(DagRunResult {
            executed_tables: order,
        })
    }

    fn spawn_table_task(
        &self,
        name: String,
        def: TableDef,
        executor: Arc<dyn QueryExecutor>,
        schema_name: String,
        tx: mpsc::UnboundedSender<std::result::Result<String, Error>>,
        cancel_token: CancellationToken,
    ) {
        tokio::spawn(async move {
            if cancel_token.is_cancelled() {
                return;
            }

            let result = tokio::task::spawn_blocking(move || {
                execute_table(&*executor, &schema_name, &name, &def)
            })
            .await
            .map_err(|e| Error::Internal(format!("Task join error: {e}")))
            .and_then(|r| r);

            if !cancel_token.is_cancelled() {
                let _ = tx.send(result);
            }
        });
    }

    fn get_required_tables(
        &self,
        registry: &TableRegistry,
        targets: &[String],
    ) -> Result<HashSet<String>> {
        let mut required = HashSet::new();

        if targets.is_empty() {
            for def in registry.all() {
                required.insert(def.name.clone());
            }
        } else {
            let mut stack: Vec<String> = targets.to_vec();
            while let Some(name) = stack.pop() {
                if required.contains(&name) {
                    continue;
                }
                if let Some(def) = registry.get(&name) {
                    required.insert(name.clone());
                    for dep in def.dependencies() {
                        stack.push(dep);
                    }
                }
            }
        }

        Ok(required)
    }

    fn validate_dependencies(
        &self,
        registry: &TableRegistry,
        tables: &HashSet<String>,
    ) -> Result<()> {
        for name in tables {
            if let Some(def) = registry.get(name) {
                for dep in def.dependencies() {
                    if !tables.contains(&dep) && !registry.contains(&dep) {
                        return Err(Error::InvalidRequest(format!(
                            "Table '{}' depends on '{}' which is not registered",
                            name, dep
                        )));
                    }
                }
            }
        }

        self.detect_cycles(registry, tables)?;

        Ok(())
    }

    fn detect_cycles(&self, registry: &TableRegistry, tables: &HashSet<String>) -> Result<()> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut graph: HashMap<String, Vec<String>> = HashMap::new();

        for name in tables {
            in_degree.entry(name.clone()).or_insert(0);
            if let Some(def) = registry.get(name) {
                for dep in def.dependencies() {
                    if tables.contains(&dep) {
                        graph.entry(dep.clone()).or_default().push(name.clone());
                        *in_degree.entry(name.clone()).or_insert(0) += 1;
                    }
                }
            }
        }

        let mut queue: Vec<String> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(name, _)| name.clone())
            .collect();

        let mut visited = 0;

        while let Some(name) = queue.pop() {
            visited += 1;
            if let Some(dependents) = graph.get(&name) {
                for dep in dependents {
                    if let Some(deg) = in_degree.get_mut(dep) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push(dep.clone());
                        }
                    }
                }
            }
        }

        if visited != tables.len() {
            return Err(Error::InvalidRequest(
                "Cycle detected in DAG dependencies".to_string(),
            ));
        }

        Ok(())
    }
}

fn execute_table(
    executor: &dyn QueryExecutor,
    schema_name: &str,
    name: &str,
    def: &TableDef,
) -> Result<String> {
    match &def.kind {
        TableKind::Source { schema, rows } => {
            create_source_table(executor, schema_name, name, schema, rows)?;
        }
        TableKind::Derived { sql, .. } => {
            create_view(executor, schema_name, name, sql)?;
        }
    }
    Ok(name.to_string())
}

fn create_source_table(
    executor: &dyn QueryExecutor,
    schema_name: &str,
    name: &str,
    schema: &[super::types::ColumnDef],
    rows: &[Vec<Value>],
) -> Result<()> {
    let drop_sql = format!(r#"DROP TABLE IF EXISTS "{}"."{}" CASCADE"#, schema_name, name);
    executor.execute_statement(schema_name, &drop_sql)?;

    let columns: Vec<String> = schema
        .iter()
        .map(|col| format!("\"{}\" {}", col.name, bq_type_to_duckdb(&col.column_type)))
        .collect();

    let create_sql = format!(
        r#"CREATE TABLE "{}"."{}" ({})"#,
        schema_name,
        name,
        columns.join(", ")
    );
    executor.execute_statement(schema_name, &create_sql)?;

    if !rows.is_empty() {
        executor.bulk_insert_rows(schema_name, name, rows)?;
    }

    Ok(())
}

fn create_view(
    executor: &dyn QueryExecutor,
    schema_name: &str,
    name: &str,
    sql: &str,
) -> Result<()> {
    let transformed_sql = transform_bq_to_duckdb(sql, schema_name)?;

    let drop_sql = format!(r#"DROP VIEW IF EXISTS "{}"."{}" CASCADE"#, schema_name, name);
    executor.execute_statement(schema_name, &drop_sql)?;

    let view_sql = format!(
        r#"CREATE VIEW "{}"."{}" AS {}"#,
        schema_name, name, transformed_sql
    );
    executor.execute_statement(schema_name, &view_sql)?;

    Ok(())
}

fn bq_type_to_duckdb(bq_type: &str) -> &str {
    match bq_type.to_uppercase().as_str() {
        "STRING" => "VARCHAR",
        "INT64" | "INTEGER" => "BIGINT",
        "FLOAT64" | "FLOAT" => "DOUBLE",
        "BOOL" | "BOOLEAN" => "BOOLEAN",
        "BYTES" => "BLOB",
        "DATE" => "DATE",
        "DATETIME" => "TIMESTAMP",
        "TIME" => "TIME",
        "TIMESTAMP" => "TIMESTAMPTZ",
        "NUMERIC" | "BIGNUMERIC" => "DECIMAL",
        "GEOGRAPHY" => "VARCHAR",
        "JSON" => "JSON",
        _ => "VARCHAR",
    }
}

impl Default for DagExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::types::ColumnDef;
    use crate::executor::DuckDbExecutor;
    use serde_json::json;

    fn create_test_executor() -> Arc<dyn QueryExecutor> {
        Arc::new(DuckDbExecutor::new().unwrap())
    }

    #[tokio::test]
    async fn test_simple_dag() {
        let executor = create_test_executor();
        let schema_name = "test_dag_1";
        executor.create_schema(schema_name).unwrap();

        let mut registry = TableRegistry::new();

        registry.register_source(
            "source".to_string(),
            vec![
                ColumnDef { name: "id".to_string(), column_type: "INT64".to_string() },
                ColumnDef { name: "value".to_string(), column_type: "STRING".to_string() },
            ],
            vec![
                vec![json!(1), json!("a")],
                vec![json!(2), json!("b")],
            ],
        ).unwrap();

        registry.register_derived(
            "derived".to_string(),
            "SELECT id, UPPER(value) as upper_value FROM source".to_string(),
        ).unwrap();

        let dag_executor = DagExecutor::new();
        let result = dag_executor.run(&registry, &["derived".to_string()], executor.clone(), schema_name).await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.executed_tables.len(), 2);

        executor.drop_schema(schema_name).unwrap();
    }

    #[tokio::test]
    async fn test_cycle_detection() {
        let mut registry = TableRegistry::new();

        registry.register_derived("a".to_string(), "SELECT * FROM b".to_string()).unwrap();
        registry.register_derived("b".to_string(), "SELECT * FROM c".to_string()).unwrap();
        registry.register_derived("c".to_string(), "SELECT * FROM a".to_string()).unwrap();

        let executor = create_test_executor();
        let dag_executor = DagExecutor::new();

        let result = dag_executor.run(&registry, &[], executor, "test").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cycle"));
    }

    #[tokio::test]
    async fn test_parallel_independent_tables() {
        let executor = create_test_executor();
        let schema_name = "test_dag_parallel";
        executor.create_schema(schema_name).unwrap();

        let mut registry = TableRegistry::new();

        for i in 1..=5 {
            registry.register_source(
                format!("source_{}", i),
                vec![ColumnDef { name: "x".to_string(), column_type: "INT64".to_string() }],
                vec![vec![json!(i)]],
            ).unwrap();
        }

        let dag_executor = DagExecutor::new();
        let result = dag_executor.run(&registry, &[], executor.clone(), schema_name).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap().executed_tables.len(), 5);

        executor.drop_schema(schema_name).unwrap();
    }
}
