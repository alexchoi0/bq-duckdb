use std::sync::Arc;

use serde_json::{json, Value};
use uuid::Uuid;

use crate::dag::{ColumnDef as DagColumnDef, DagExecutor, TableKind};
use crate::error::{Error, Result};
use crate::session::SessionManager;
use crate::sql::transform_bq_to_duckdb;

use super::types::*;

pub struct RpcMethods {
    session_manager: Arc<SessionManager>,
}

impl RpcMethods {
    pub fn new(session_manager: Arc<SessionManager>) -> Self {
        Self { session_manager }
    }

    pub async fn dispatch(&self, method: &str, params: Value) -> Result<Value> {
        match method {
            "bq.ping" => self.ping(params).await,
            "bq.createSession" => self.create_session(params).await,
            "bq.destroySession" => self.destroy_session(params).await,
            "bq.query" => self.query(params).await,
            "bq.createTable" => self.create_table(params).await,
            "bq.insert" => self.insert(params).await,
            "bq.registerDag" => self.register_dag(params).await,
            "bq.runDag" => self.run_dag(params).await,
            "bq.getDag" => self.get_dag(params).await,
            "bq.clearDag" => self.clear_dag(params).await,
            _ => Err(Error::InvalidRequest(format!("Unknown method: {}", method))),
        }
    }

    async fn ping(&self, _params: Value) -> Result<Value> {
        Ok(json!(PingResult { message: "pong".to_string() }))
    }

    async fn create_session(&self, _params: Value) -> Result<Value> {
        let session_manager = Arc::clone(&self.session_manager);

        let session_id = tokio::task::spawn_blocking(move || session_manager.create_session())
            .await
            .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(json!(CreateSessionResult {
            session_id: session_id.to_string(),
        }))
    }

    async fn destroy_session(&self, params: Value) -> Result<Value> {
        let p: DestroySessionParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let session_manager = Arc::clone(&self.session_manager);

        tokio::task::spawn_blocking(move || session_manager.destroy_session(session_id))
            .await
            .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(json!(DestroySessionResult { success: true }))
    }

    async fn query(&self, params: Value) -> Result<Value> {
        let p: QueryParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let schema_name = self.session_manager.get_schema_name(session_id)?;
        let transformed_sql = transform_bq_to_duckdb(&p.sql, &schema_name)?;

        let session_manager = Arc::clone(&self.session_manager);

        let result = tokio::task::spawn_blocking(move || {
            session_manager
                .executor()
                .execute_query(&schema_name, &transformed_sql)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(result.to_bq_response())
    }

    async fn create_table(&self, params: Value) -> Result<Value> {
        let p: CreateTableParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let schema_name = self.session_manager.get_schema_name(session_id)?;

        let columns: Vec<String> = p
            .schema
            .iter()
            .map(|col| format!("\"{}\" {}", col.name, bq_type_to_duckdb(&col.column_type)))
            .collect();

        let sql = format!(
            "CREATE TABLE \"{}\" ({})",
            p.table_name,
            columns.join(", ")
        );

        let session_manager = Arc::clone(&self.session_manager);

        tokio::task::spawn_blocking(move || {
            session_manager.executor().execute_statement(&schema_name, &sql)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(json!(CreateTableResult { success: true }))
    }

    async fn insert(&self, params: Value) -> Result<Value> {
        let p: InsertParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let schema_name = self.session_manager.get_schema_name(session_id)?;

        if p.rows.is_empty() {
            return Ok(json!(InsertResult { inserted_rows: 0 }));
        }

        let values: Vec<String> = p
            .rows
            .iter()
            .filter_map(|row| {
                if let Value::Array(arr) = row {
                    let vals: Vec<String> = arr.iter().map(json_to_sql_value).collect();
                    Some(format!("({})", vals.join(", ")))
                } else if let Value::Object(obj) = row {
                    let vals: Vec<String> = obj.values().map(json_to_sql_value).collect();
                    Some(format!("({})", vals.join(", ")))
                } else {
                    None
                }
            })
            .collect();

        let sql = format!(
            "INSERT INTO \"{}\" VALUES {}",
            p.table_name,
            values.join(", ")
        );

        let session_manager = Arc::clone(&self.session_manager);
        let inserted = tokio::task::spawn_blocking(move || {
            session_manager.executor().execute_statement(&schema_name, &sql)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(json!(InsertResult {
            inserted_rows: inserted,
        }))
    }

    async fn register_dag(&self, params: Value) -> Result<Value> {
        let p: RegisterDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let mut results = Vec::new();

        for table_def in p.tables {
            if let Some(sql) = table_def.sql {
                let def = self.session_manager.with_registry_mut(session_id, |registry| {
                    registry.register_derived(table_def.name.clone(), sql)
                })??;

                results.push(RegisterDagTableResult {
                    name: def.name.clone(),
                    dependencies: def.dependencies(),
                });
            } else if let (Some(schema), Some(rows)) = (table_def.schema, table_def.rows) {
                let dag_schema: Vec<DagColumnDef> = schema
                    .into_iter()
                    .map(|c| DagColumnDef {
                        name: c.name,
                        column_type: c.column_type,
                    })
                    .collect();

                let rows_data: Vec<Vec<Value>> = rows
                    .into_iter()
                    .filter_map(|row| {
                        if let Value::Array(arr) = row {
                            Some(arr)
                        } else {
                            None
                        }
                    })
                    .collect();

                let def = self.session_manager.with_registry_mut(session_id, |registry| {
                    registry.register_source(table_def.name.clone(), dag_schema, rows_data)
                })??;

                results.push(RegisterDagTableResult {
                    name: def.name.clone(),
                    dependencies: def.dependencies(),
                });
            } else {
                return Err(Error::InvalidRequest(
                    "Table must have either sql or schema+rows".to_string(),
                ));
            }
        }

        let schema_name = self.session_manager.get_schema_name(session_id)?;
        let executor = self.session_manager.executor();
        self.session_manager.with_registry(session_id, |registry| {
            registry.validate_dependencies(executor, &schema_name)
        })??;

        Ok(json!(RegisterDagResult {
            success: true,
            tables: results,
        }))
    }

    async fn run_dag(&self, params: Value) -> Result<Value> {
        let p: RunDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let schema_name = self.session_manager.get_schema_name(session_id)?;
        let executor = self.session_manager.arc_executor();

        let registry_snapshot = self.session_manager.with_registry(session_id, |registry| {
            registry.all().into_iter().cloned().collect::<Vec<_>>()
        })?;

        let mut temp_registry = crate::dag::TableRegistry::new();
        for def in registry_snapshot {
            match &def.kind {
                TableKind::Derived { sql, .. } => {
                    temp_registry.register_derived(def.name.clone(), sql.clone())?;
                }
                TableKind::Source { schema, rows } => {
                    temp_registry.register_source(def.name.clone(), schema.clone(), rows.clone())?;
                }
            }
        }

        let dag_executor = DagExecutor::new();
        let targets = p.table_names.unwrap_or_default();
        let result = dag_executor
            .run(&temp_registry, &targets, executor, &schema_name)
            .await?;

        Ok(json!(RunDagResult {
            success: true,
            executed_tables: result.executed_tables,
        }))
    }

    async fn get_dag(&self, params: Value) -> Result<Value> {
        let p: GetDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let tables = self.session_manager.with_registry(session_id, |registry| {
            registry
                .all()
                .iter()
                .map(|def| {
                    let (sql, is_source) = match &def.kind {
                        TableKind::Source { .. } => (None, true),
                        TableKind::Derived { sql, .. } => (Some(sql.clone()), false),
                    };
                    GetDagTableInfo {
                        name: def.name.clone(),
                        sql,
                        is_source,
                        dependencies: def.dependencies(),
                    }
                })
                .collect::<Vec<_>>()
        })?;

        Ok(json!(GetDagResult { tables }))
    }

    async fn clear_dag(&self, params: Value) -> Result<Value> {
        let p: ClearDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        self.session_manager.with_registry_mut(session_id, |registry| {
            registry.clear();
        })?;

        Ok(json!(ClearDagResult { success: true }))
    }
}

fn parse_uuid(s: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|_| Error::InvalidRequest(format!("Invalid session ID: {}", s)))
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

fn json_to_sql_value(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_sql_value).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Object(obj) => {
            let fields: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("'{}': {}", k, json_to_sql_value(v)))
                .collect();
            format!("{{{}}}", fields.join(", "))
        }
    }
}
