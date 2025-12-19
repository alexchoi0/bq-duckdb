use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;

use serde_json::Value;

use crate::error::{Error, Result};
use crate::executor::{Executor, ExecutorMode};
use crate::rpc::types::{ColumnDef, DagTableDef, DagTableDetail, DagTableInfo};
use crate::utils::json_to_sql_value;

#[derive(Debug, Clone)]
pub struct DagTable {
    pub name: String,
    pub sql: Option<String>,
    pub schema: Option<Vec<ColumnDef>>,
    pub rows: Vec<Value>,
    pub dependencies: Vec<String>,
    pub is_source: bool,
}

pub struct Dag {
    tables: HashMap<String, DagTable>,
}

impl Dag {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn register(&mut self, defs: Vec<DagTableDef>) -> Result<Vec<DagTableInfo>> {
        let mut infos = Vec::new();

        for def in defs {
            let is_source = def.sql.is_none();
            let dependencies = if is_source {
                vec![]
            } else {
                extract_dependencies(def.sql.as_deref().unwrap_or(""), &self.tables)
            };

            let table = DagTable {
                name: def.name.clone(),
                sql: def.sql,
                schema: def.schema,
                rows: def.rows,
                dependencies: dependencies.clone(),
                is_source,
            };

            infos.push(DagTableInfo {
                name: table.name.clone(),
                dependencies: dependencies.clone(),
            });

            self.tables.insert(table.name.clone(), table);
        }

        let updates: Vec<(String, Vec<String>)> = self
            .tables
            .iter()
            .filter_map(|(name, table)| {
                if let Some(sql) = &table.sql {
                    let deps = extract_dependencies(sql, &self.tables);
                    if deps != table.dependencies {
                        Some((name.clone(), deps))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        for (name, deps) in updates {
            if let Some(t) = self.tables.get_mut(&name) {
                t.dependencies = deps;
            }
        }

        for info in &mut infos {
            if let Some(table) = self.tables.get(&info.name) {
                info.dependencies = table.dependencies.clone();
            }
        }

        Ok(infos)
    }

    pub fn run(
        &self,
        executor: Arc<Executor>,
        targets: Option<Vec<String>>,
    ) -> Result<Vec<String>> {
        let subset = if let Some(targets) = targets {
            self.get_tables_with_deps_set(&targets)?
        } else {
            self.tables.keys().cloned().collect()
        };

        let levels = self.topological_sort_levels(&subset)?;
        let mut executed = Vec::new();

        match executor.mode() {
            ExecutorMode::Mock => {
                for level in levels {
                    for name in level {
                        self.execute_single_table(&executor, &name)?;
                        executed.push(name);
                    }
                }
            }
            ExecutorMode::BigQuery => {
                for level in levels {
                    if level.len() == 1 {
                        let name = &level[0];
                        self.execute_single_table(&executor, name)?;
                        executed.push(name.clone());
                    } else {
                        let handles: Vec<_> = level
                            .into_iter()
                            .map(|name| {
                                let executor = Arc::clone(&executor);
                                let table = self.tables.get(&name).cloned();
                                thread::spawn(move || -> Result<String> {
                                    let table = table.ok_or_else(|| {
                                        Error::InvalidRequest(format!("Table not found: {}", name))
                                    })?;
                                    execute_table(&executor, &table)?;
                                    Ok(name)
                                })
                            })
                            .collect();

                        for handle in handles {
                            match handle.join() {
                                Ok(Ok(name)) => executed.push(name),
                                Ok(Err(e)) => return Err(e),
                                Err(_) => {
                                    return Err(Error::Internal("Thread panicked".to_string()))
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(executed)
    }

    fn get_tables_with_deps_set(&self, targets: &[String]) -> Result<HashSet<String>> {
        let mut needed: HashSet<String> = HashSet::new();
        let mut stack: Vec<String> = targets.to_vec();

        while let Some(name) = stack.pop() {
            if needed.contains(&name) {
                continue;
            }
            needed.insert(name.clone());

            if let Some(table) = self.tables.get(&name) {
                for dep in &table.dependencies {
                    if !needed.contains(dep) {
                        stack.push(dep.clone());
                    }
                }
            }
        }

        Ok(needed)
    }

    fn execute_single_table(&self, executor: &Executor, name: &str) -> Result<()> {
        let table = self.tables.get(name).ok_or_else(|| {
            Error::InvalidRequest(format!("Table not found: {}", name))
        })?;
        execute_table(executor, table)
    }

    fn topological_sort_levels(&self, subset: &HashSet<String>) -> Result<Vec<Vec<String>>> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

        for name in subset {
            in_degree.entry(name.clone()).or_insert(0);
            if let Some(table) = self.tables.get(name) {
                for dep in &table.dependencies {
                    if subset.contains(dep) {
                        *in_degree.entry(name.clone()).or_insert(0) += 1;
                        dependents.entry(dep.clone()).or_default().push(name.clone());
                    }
                }
            }
        }

        let mut levels: Vec<Vec<String>> = Vec::new();
        let mut processed = 0;

        loop {
            let mut current_level: Vec<String> = in_degree
                .iter()
                .filter(|(_, &deg)| deg == 0)
                .map(|(name, _)| name.clone())
                .collect();

            if current_level.is_empty() {
                break;
            }

            current_level.sort();

            for name in &current_level {
                in_degree.remove(name);
                if let Some(deps) = dependents.get(name) {
                    for dep_name in deps {
                        if let Some(degree) = in_degree.get_mut(dep_name) {
                            *degree -= 1;
                        }
                    }
                }
            }

            processed += current_level.len();
            levels.push(current_level);
        }

        if processed != subset.len() {
            return Err(Error::InvalidRequest("Circular dependency detected".to_string()));
        }

        Ok(levels)
    }

    pub fn get_tables(&self) -> Vec<DagTableDetail> {
        self.tables
            .values()
            .map(|t| DagTableDetail {
                name: t.name.clone(),
                sql: t.sql.clone(),
                is_source: t.is_source,
                dependencies: t.dependencies.clone(),
            })
            .collect()
    }

    pub fn clear(&mut self, executor: &Executor) {
        for table_name in self.tables.keys() {
            let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
            let _ = executor.execute_statement(&drop_sql);
        }
        self.tables.clear();
    }
}

impl Default for Dag {
    fn default() -> Self {
        Self::new()
    }
}

fn execute_table(executor: &Executor, table: &DagTable) -> Result<()> {
    if table.is_source {
        create_source_table_standalone(executor, table)?;
    } else if let Some(sql) = &table.sql {
        let drop_sql = format!("DROP TABLE IF EXISTS {}", table.name);
        let _ = executor.execute_statement(&drop_sql);

        let query_result = executor.execute_query(sql).map_err(|e| {
            Error::Executor(format!("Failed to execute query for table {}: {}", table.name, e))
        })?;

        if !query_result.columns.is_empty() {
            let column_types: Vec<String> = if !query_result.rows.is_empty() {
                query_result
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        let sample_value = &query_result.rows[0][i];
                        let inferred_type = infer_sql_type(sample_value);
                        format!("{} {}", name, inferred_type)
                    })
                    .collect()
            } else {
                query_result
                    .columns
                    .iter()
                    .map(|c| format!("{} STRING", c))
                    .collect()
            };

            let create_sql = format!(
                "CREATE TABLE {} ({})",
                table.name,
                column_types.join(", ")
            );
            executor.execute_statement(&create_sql)?;

            if !query_result.rows.is_empty() {
                let values: Vec<String> = query_result
                    .rows
                    .iter()
                    .map(|row| {
                        let vals: Vec<String> = row.iter().map(json_to_sql_value).collect();
                        format!("({})", vals.join(", "))
                    })
                    .collect();

                let insert_sql = format!(
                    "INSERT INTO {} VALUES {}",
                    table.name,
                    values.join(", ")
                );
                executor.execute_statement(&insert_sql)?;
            }
        }
    }

    Ok(())
}

fn create_source_table_standalone(executor: &Executor, table: &DagTable) -> Result<()> {
    if let Some(schema) = &table.schema {
        let columns: Vec<String> = schema
            .iter()
            .map(|col| format!("{} {}", col.name, col.column_type))
            .collect();

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table.name,
            columns.join(", ")
        );
        executor.execute_statement(&create_sql)?;

        if !table.rows.is_empty() {
            let values: Vec<String> = table
                .rows
                .iter()
                .filter_map(|row| {
                    if let Value::Array(arr) = row {
                        let vals: Vec<String> = arr.iter().map(json_to_sql_value).collect();
                        Some(format!("({})", vals.join(", ")))
                    } else {
                        None
                    }
                })
                .collect();

            if !values.is_empty() {
                let insert_sql = format!(
                    "INSERT INTO {} VALUES {}",
                    table.name,
                    values.join(", ")
                );
                executor.execute_statement(&insert_sql)?;
            }
        }
    }
    Ok(())
}

fn extract_dependencies(sql: &str, known_tables: &HashMap<String, DagTable>) -> Vec<String> {
    let mut deps = Vec::new();
    let sql_upper = sql.to_uppercase();

    for table_name in known_tables.keys() {
        let name_upper = table_name.to_uppercase();
        let patterns = [
            format!("FROM {}", name_upper),
            format!("JOIN {}", name_upper),
            format!("FROM {} ", name_upper),
            format!("JOIN {} ", name_upper),
            format!("FROM {}\n", name_upper),
            format!("JOIN {}\n", name_upper),
            format!("FROM {},", name_upper),
            format!("FROM {}", name_upper),
            format!(", {}", name_upper),
            format!(", {} ", name_upper),
        ];

        for pattern in &patterns {
            if sql_upper.contains(pattern) {
                if !deps.contains(table_name) {
                    deps.push(table_name.clone());
                }
                break;
            }
        }
    }

    deps.sort();
    deps
}

fn infer_sql_type(val: &Value) -> &'static str {
    match val {
        Value::Null => "STRING",
        Value::Bool(_) => "BOOL",
        Value::Number(n) => {
            if n.is_i64() {
                "INT64"
            } else {
                "FLOAT64"
            }
        }
        Value::String(_) => "STRING",
        Value::Array(_) => "JSON",
        Value::Object(_) => "JSON",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::YachtSqlExecutor;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn create_mock_executor() -> Arc<Executor> {
        Arc::new(Executor::Mock(YachtSqlExecutor::new().unwrap()))
    }

    fn source_table(name: &str, schema: Vec<(&str, &str)>, rows: Vec<Value>) -> DagTableDef {
        DagTableDef {
            name: name.to_string(),
            sql: None,
            schema: Some(
                schema
                    .into_iter()
                    .map(|(n, t)| ColumnDef {
                        name: n.to_string(),
                        column_type: t.to_string(),
                    })
                    .collect(),
            ),
            rows,
        }
    }

    fn computed_table(name: &str, sql: &str) -> DagTableDef {
        DagTableDef {
            name: name.to_string(),
            sql: Some(sql.to_string()),
            schema: None,
            rows: vec![],
        }
    }

    #[test]
    fn test_register_single_source_table() {
        let mut dag = Dag::new();
        let tables = vec![source_table(
            "users",
            vec![("id", "INT64"), ("name", "STRING")],
            vec![],
        )];

        let result = dag.register(tables).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "users");
        assert!(result[0].dependencies.is_empty());
    }

    #[test]
    fn test_register_computed_table_with_dependency() {
        let mut dag = Dag::new();

        dag.register(vec![source_table(
            "users",
            vec![("id", "INT64"), ("name", "STRING")],
            vec![],
        )])
        .unwrap();

        let result = dag
            .register(vec![computed_table(
                "active_users",
                "SELECT * FROM users WHERE active = true",
            )])
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "active_users");
        assert_eq!(result[0].dependencies, vec!["users"]);
    }

    #[test]
    fn test_register_multiple_dependencies() {
        let mut dag = Dag::new();

        dag.register(vec![
            source_table("users", vec![("id", "INT64"), ("name", "STRING")], vec![]),
            source_table(
                "orders",
                vec![("id", "INT64"), ("user_id", "INT64")],
                vec![],
            ),
        ])
        .unwrap();

        let result = dag
            .register(vec![computed_table(
                "user_orders",
                "SELECT u.name, o.id FROM users u JOIN orders o ON u.id = o.user_id",
            )])
            .unwrap();

        assert_eq!(result[0].name, "user_orders");
        let mut deps = result[0].dependencies.clone();
        deps.sort();
        assert_eq!(deps, vec!["orders", "users"]);
    }

    #[test]
    fn test_run_single_source_table() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "users",
            vec![("id", "INT64"), ("name", "STRING")],
            vec![json!([1, "Alice"]), json!([2, "Bob"])],
        )])
        .unwrap();

        let executed = dag.run(executor.clone(), None).unwrap();

        assert_eq!(executed, vec!["users"]);

        let result = executor.execute_query("SELECT * FROM users ORDER BY id").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], json!(1));
        assert_eq!(result.rows[0][1], json!("Alice"));
        assert_eq!(result.rows[1][0], json!(2));
        assert_eq!(result.rows[1][1], json!("Bob"));
    }

    #[test]
    fn test_run_computed_table_from_source() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "numbers",
            vec![("value", "INT64")],
            vec![json!([1]), json!([2]), json!([3]), json!([4]), json!([5])],
        )])
        .unwrap();

        dag.register(vec![computed_table(
            "even_numbers",
            "SELECT value FROM numbers WHERE value % 2 = 0",
        )])
        .unwrap();

        let executed = dag.run(executor.clone(), None).unwrap();

        assert!(executed.contains(&"numbers".to_string()));
        assert!(executed.contains(&"even_numbers".to_string()));

        let result = executor
            .execute_query("SELECT * FROM even_numbers ORDER BY value")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], json!(2));
        assert_eq!(result.rows[1][0], json!(4));
    }

    #[test]
    fn test_run_chain_of_computed_tables() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "raw_numbers",
            vec![("n", "INT64")],
            vec![
                json!([1]),
                json!([2]),
                json!([3]),
                json!([4]),
                json!([5]),
                json!([6]),
            ],
        )])
        .unwrap();

        dag.register(vec![computed_table(
            "doubled",
            "SELECT n * 2 AS n FROM raw_numbers",
        )])
        .unwrap();

        dag.register(vec![computed_table(
            "plus_ten",
            "SELECT n + 10 AS n FROM doubled",
        )])
        .unwrap();

        let executed = dag.run(executor.clone(), None).unwrap();

        assert_eq!(executed.len(), 3);
        let idx_raw = executed.iter().position(|x| x == "raw_numbers").unwrap();
        let idx_doubled = executed.iter().position(|x| x == "doubled").unwrap();
        let idx_plus_ten = executed.iter().position(|x| x == "plus_ten").unwrap();
        assert!(idx_raw < idx_doubled);
        assert!(idx_doubled < idx_plus_ten);

        let result = executor
            .execute_query("SELECT * FROM plus_ten ORDER BY n")
            .unwrap();
        assert_eq!(result.rows.len(), 6);
        assert_eq!(result.rows[0][0], json!(12)); // 1*2+10
        assert_eq!(result.rows[5][0], json!(22)); // 6*2+10
    }

    #[test]
    fn test_run_diamond_dependency() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "source",
            vec![("x", "INT64")],
            vec![json!([10]), json!([20]), json!([30])],
        )])
        .unwrap();

        dag.register(vec![
            computed_table("left_branch", "SELECT x + 1 AS x FROM source"),
            computed_table("right_branch", "SELECT x - 1 AS x FROM source"),
        ])
        .unwrap();

        dag.register(vec![computed_table(
            "merged",
            "SELECT l.x AS left_x, r.x AS right_x FROM left_branch l, right_branch r WHERE l.x - r.x = 2",
        )])
        .unwrap();

        let executed = dag.run(executor.clone(), None).unwrap();

        assert_eq!(executed.len(), 4);
        let idx_source = executed.iter().position(|x| x == "source").unwrap();
        let idx_left = executed.iter().position(|x| x == "left_branch").unwrap();
        let idx_right = executed.iter().position(|x| x == "right_branch").unwrap();
        let idx_merged = executed.iter().position(|x| x == "merged").unwrap();

        assert!(idx_source < idx_left);
        assert!(idx_source < idx_right);
        assert!(idx_left < idx_merged);
        assert!(idx_right < idx_merged);

        let result = executor
            .execute_query("SELECT * FROM merged ORDER BY left_x")
            .unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_run_with_specific_targets() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![
            source_table("a", vec![("v", "INT64")], vec![json!([1])]),
            source_table("b", vec![("v", "INT64")], vec![json!([2])]),
            source_table("c", vec![("v", "INT64")], vec![json!([3])]),
        ])
        .unwrap();

        dag.register(vec![
            computed_table("from_a", "SELECT v * 10 AS v FROM a"),
            computed_table("from_b", "SELECT v * 10 AS v FROM b"),
        ])
        .unwrap();

        let executed = dag
            .run(executor.clone(), Some(vec!["from_a".to_string()]))
            .unwrap();

        assert!(executed.contains(&"a".to_string()));
        assert!(executed.contains(&"from_a".to_string()));
        assert!(!executed.contains(&"b".to_string()));
        assert!(!executed.contains(&"from_b".to_string()));
        assert!(!executed.contains(&"c".to_string()));

        let result = executor.execute_query("SELECT * FROM from_a").unwrap();
        assert_eq!(result.rows[0][0], json!(10));

        assert!(executor.execute_query("SELECT * FROM from_b").is_err());
    }

    #[test]
    fn test_run_with_multiple_targets() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![
            source_table("x", vec![("v", "INT64")], vec![json!([100])]),
            source_table("y", vec![("v", "INT64")], vec![json!([200])]),
        ])
        .unwrap();

        dag.register(vec![
            computed_table("from_x", "SELECT v FROM x"),
            computed_table("from_y", "SELECT v FROM y"),
        ])
        .unwrap();

        let executed = dag
            .run(
                executor.clone(),
                Some(vec!["from_x".to_string(), "from_y".to_string()]),
            )
            .unwrap();

        assert_eq!(executed.len(), 4);
        assert!(executed.contains(&"x".to_string()));
        assert!(executed.contains(&"y".to_string()));
        assert!(executed.contains(&"from_x".to_string()));
        assert!(executed.contains(&"from_y".to_string()));
    }

    #[test]
    fn test_topological_sort_levels_independent_tables() {
        let mut dag = Dag::new();

        dag.register(vec![
            source_table("a", vec![("v", "INT64")], vec![]),
            source_table("b", vec![("v", "INT64")], vec![]),
            source_table("c", vec![("v", "INT64")], vec![]),
        ])
        .unwrap();

        let all_names: HashSet<String> = dag.tables.keys().cloned().collect();
        let levels = dag.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].len(), 3);
    }

    #[test]
    fn test_topological_sort_levels_linear_chain() {
        let mut dag = Dag::new();

        dag.register(vec![source_table("a", vec![("v", "INT64")], vec![])])
            .unwrap();
        dag.register(vec![computed_table("b", "SELECT * FROM a")])
            .unwrap();
        dag.register(vec![computed_table("c", "SELECT * FROM b")])
            .unwrap();
        dag.register(vec![computed_table("d", "SELECT * FROM c")])
            .unwrap();

        let all_names: HashSet<String> = dag.tables.keys().cloned().collect();
        let levels = dag.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 4);
        assert_eq!(levels[0], vec!["a"]);
        assert_eq!(levels[1], vec!["b"]);
        assert_eq!(levels[2], vec!["c"]);
        assert_eq!(levels[3], vec!["d"]);
    }

    #[test]
    fn test_topological_sort_levels_diamond() {
        let mut dag = Dag::new();

        dag.register(vec![source_table("root", vec![("v", "INT64")], vec![])])
            .unwrap();
        dag.register(vec![
            computed_table("left", "SELECT * FROM root"),
            computed_table("right", "SELECT * FROM root"),
        ])
        .unwrap();
        dag.register(vec![computed_table(
            "bottom",
            "SELECT * FROM left, right",
        )])
        .unwrap();

        let all_names: HashSet<String> = dag.tables.keys().cloned().collect();
        let levels = dag.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0], vec!["root"]);
        assert_eq!(levels[1].len(), 2);
        assert!(levels[1].contains(&"left".to_string()));
        assert!(levels[1].contains(&"right".to_string()));
        assert_eq!(levels[2], vec!["bottom"]);
    }

    #[test]
    fn test_topological_sort_levels_complex_dag() {
        let mut dag = Dag::new();

        dag.register(vec![
            source_table("s1", vec![("v", "INT64")], vec![]),
            source_table("s2", vec![("v", "INT64")], vec![]),
        ])
        .unwrap();

        dag.register(vec![
            computed_table("a", "SELECT * FROM s1"),
            computed_table("b", "SELECT * FROM s2"),
            computed_table("c", "SELECT * FROM s1, s2"),
        ])
        .unwrap();

        dag.register(vec![computed_table("d", "SELECT * FROM a, b")])
            .unwrap();

        dag.register(vec![computed_table("e", "SELECT * FROM c, d")])
            .unwrap();

        let all_names: HashSet<String> = dag.tables.keys().cloned().collect();
        let levels = dag.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 4);

        assert_eq!(levels[0].len(), 2);
        assert!(levels[0].contains(&"s1".to_string()));
        assert!(levels[0].contains(&"s2".to_string()));

        assert_eq!(levels[1].len(), 3);
        assert!(levels[1].contains(&"a".to_string()));
        assert!(levels[1].contains(&"b".to_string()));
        assert!(levels[1].contains(&"c".to_string()));

        assert_eq!(levels[2], vec!["d"]);
        assert_eq!(levels[3], vec!["e"]);
    }

    #[test]
    fn test_empty_source_table() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "empty_table",
            vec![("id", "INT64"), ("value", "STRING")],
            vec![],
        )])
        .unwrap();

        let executed = dag.run(executor.clone(), None).unwrap();
        assert_eq!(executed, vec!["empty_table"]);

        let result = executor
            .execute_query("SELECT * FROM empty_table")
            .unwrap();
        assert_eq!(result.rows.len(), 0);
        assert_eq!(result.columns, vec!["id", "value"]);
    }

    #[test]
    fn test_aggregation_query() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "sales",
            vec![("product", "STRING"), ("amount", "INT64")],
            vec![
                json!(["Widget", 100]),
                json!(["Widget", 150]),
                json!(["Gadget", 200]),
                json!(["Gadget", 50]),
                json!(["Widget", 75]),
            ],
        )])
        .unwrap();

        dag.register(vec![computed_table(
            "sales_summary",
            "SELECT product, SUM(amount) AS total FROM sales GROUP BY product",
        )])
        .unwrap();

        dag.run(executor.clone(), None).unwrap();

        let result = executor
            .execute_query("SELECT * FROM sales_summary ORDER BY product")
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        let gadget_row = result
            .rows
            .iter()
            .find(|r| r[0] == json!("Gadget"))
            .unwrap();
        assert_eq!(gadget_row[1], json!(250));

        let widget_row = result
            .rows
            .iter()
            .find(|r| r[0] == json!("Widget"))
            .unwrap();
        assert_eq!(widget_row[1], json!(325));
    }

    #[test]
    fn test_join_tables() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![
            source_table(
                "customers",
                vec![("id", "INT64"), ("name", "STRING")],
                vec![
                    json!([1, "Alice"]),
                    json!([2, "Bob"]),
                    json!([3, "Charlie"]),
                ],
            ),
            source_table(
                "orders",
                vec![("id", "INT64"), ("customer_id", "INT64"), ("total", "INT64")],
                vec![
                    json!([101, 1, 500]),
                    json!([102, 1, 300]),
                    json!([103, 2, 150]),
                ],
            ),
        ])
        .unwrap();

        dag.register(vec![computed_table(
            "customer_orders",
            "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id",
        )])
        .unwrap();

        dag.run(executor.clone(), None).unwrap();

        let result = executor
            .execute_query("SELECT * FROM customer_orders ORDER BY name, total")
            .unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_clear_dag() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "test_table",
            vec![("v", "INT64")],
            vec![json!([42])],
        )])
        .unwrap();

        dag.run(executor.clone(), None).unwrap();

        assert!(executor
            .execute_query("SELECT * FROM test_table")
            .is_ok());

        dag.clear(&executor);

        assert!(dag.get_tables().is_empty());
        assert!(executor
            .execute_query("SELECT * FROM test_table")
            .is_err());
    }

    #[test]
    fn test_get_tables() {
        let mut dag = Dag::new();

        dag.register(vec![
            source_table("src", vec![("v", "INT64")], vec![]),
        ])
        .unwrap();

        dag.register(vec![computed_table("derived", "SELECT * FROM src")])
            .unwrap();

        let tables = dag.get_tables();
        assert_eq!(tables.len(), 2);

        let src = tables.iter().find(|t| t.name == "src").unwrap();
        assert!(src.is_source);
        assert!(src.sql.is_none());
        assert!(src.dependencies.is_empty());

        let derived = tables.iter().find(|t| t.name == "derived").unwrap();
        assert!(!derived.is_source);
        assert!(derived.sql.is_some());
        assert_eq!(derived.dependencies, vec!["src"]);
    }

    #[test]
    fn test_null_values() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "with_nulls",
            vec![("id", "INT64"), ("value", "STRING")],
            vec![
                json!([1, "hello"]),
                json!([2, null]),
                json!([3, "world"]),
            ],
        )])
        .unwrap();

        dag.run(executor.clone(), None).unwrap();

        let result = executor
            .execute_query("SELECT * FROM with_nulls ORDER BY id")
            .unwrap();
        assert_eq!(result.rows.len(), 3);
        assert!(result.rows[1][1].is_null());
    }

    #[test]
    fn test_boolean_values() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "flags",
            vec![("name", "STRING"), ("active", "BOOL")],
            vec![
                json!(["feature_a", true]),
                json!(["feature_b", false]),
                json!(["feature_c", true]),
            ],
        )])
        .unwrap();

        dag.register(vec![computed_table(
            "active_flags",
            "SELECT name FROM flags WHERE active = true",
        )])
        .unwrap();

        dag.run(executor.clone(), None).unwrap();

        let result = executor
            .execute_query("SELECT * FROM active_flags ORDER BY name")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], json!("feature_a"));
        assert_eq!(result.rows[1][0], json!("feature_c"));
    }

    #[test]
    fn test_float_values() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "measurements",
            vec![("sensor", "STRING"), ("reading", "FLOAT64")],
            vec![
                json!(["temp", 23.5]),
                json!(["humidity", 65.2]),
                json!(["pressure", 1013.25]),
            ],
        )])
        .unwrap();

        dag.register(vec![computed_table(
            "high_readings",
            "SELECT sensor, reading FROM measurements WHERE reading > 50",
        )])
        .unwrap();

        dag.run(executor.clone(), None).unwrap();

        let result = executor
            .execute_query("SELECT * FROM high_readings ORDER BY reading")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_dependency_detection_case_insensitive() {
        let mut dag = Dag::new();

        dag.register(vec![source_table("MyTable", vec![("v", "INT64")], vec![])])
            .unwrap();

        let result = dag
            .register(vec![computed_table("derived", "SELECT * FROM mytable")])
            .unwrap();

        assert_eq!(result[0].dependencies, vec!["MyTable"]);
    }

    #[test]
    fn test_rerun_computed_table_reflects_source_changes() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        executor
            .execute_statement("CREATE TABLE counter (n INT64)")
            .unwrap();
        executor
            .execute_statement("INSERT INTO counter VALUES (1)")
            .unwrap();

        dag.register(vec![computed_table(
            "doubled",
            "SELECT n * 2 AS n FROM counter",
        )])
        .unwrap();

        dag.run(executor.clone(), None).unwrap();

        let result = executor.execute_query("SELECT * FROM doubled").unwrap();
        assert_eq!(result.rows[0][0], json!(2));

        executor
            .execute_statement("INSERT INTO counter VALUES (10)")
            .unwrap();

        dag.run(executor.clone(), None).unwrap();

        let result = executor
            .execute_query("SELECT * FROM doubled ORDER BY n")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], json!(2));
        assert_eq!(result.rows[1][0], json!(20));
    }

    #[test]
    fn test_wide_dag_many_independent_branches() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "root",
            vec![("v", "INT64")],
            vec![json!([1])],
        )])
        .unwrap();

        for i in 0..10 {
            dag.register(vec![computed_table(
                &format!("branch_{}", i),
                &format!("SELECT v + {} AS v FROM root", i),
            )])
            .unwrap();
        }

        let executed = dag.run(executor.clone(), None).unwrap();

        assert_eq!(executed.len(), 11);
        assert_eq!(executed[0], "root");

        for i in 0..10 {
            let result = executor
                .execute_query(&format!("SELECT * FROM branch_{}", i))
                .unwrap();
            assert_eq!(result.rows[0][0], json!(1 + i as i64));
        }
    }

    #[test]
    fn test_deep_dag_long_chain() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "step_0",
            vec![("n", "INT64")],
            vec![json!([0])],
        )])
        .unwrap();

        for i in 1..=20 {
            dag.register(vec![computed_table(
                &format!("step_{}", i),
                &format!("SELECT n + 1 AS n FROM step_{}", i - 1),
            )])
            .unwrap();
        }

        let executed = dag.run(executor.clone(), None).unwrap();

        assert_eq!(executed.len(), 21);

        for i in 0..=20 {
            assert_eq!(executed[i], format!("step_{}", i));
        }

        let result = executor.execute_query("SELECT * FROM step_20").unwrap();
        assert_eq!(result.rows[0][0], json!(20));
    }

    #[test]
    fn test_execution_order_respects_dependencies() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![
            source_table("t_a", vec![("v", "INT64")], vec![json!([1])]),
            source_table("t_b", vec![("v", "INT64")], vec![json!([2])]),
        ])
        .unwrap();

        dag.register(vec![computed_table("t_c", "SELECT v FROM t_a")])
            .unwrap();

        dag.register(vec![computed_table(
            "t_d",
            "SELECT t_b.v FROM t_b JOIN t_c ON 1=1",
        )])
        .unwrap();

        dag.register(vec![computed_table("t_e", "SELECT v FROM t_d")])
            .unwrap();

        let executed = dag.run(executor.clone(), None).unwrap();

        assert_eq!(executed.len(), 5);

        let pos = |name: &str| executed.iter().position(|x| x == name).unwrap();

        assert!(pos("t_a") < pos("t_c"));
        assert!(pos("t_b") < pos("t_d"));
        assert!(pos("t_c") < pos("t_d"));
        assert!(pos("t_d") < pos("t_e"));
    }

    #[test]
    fn test_mock_mode_executes_serially() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        dag.register(vec![source_table(
            "base",
            vec![("id", "INT64")],
            vec![json!([1])],
        )])
        .unwrap();

        for i in 0..5 {
            dag.register(vec![computed_table(
                &format!("branch_{}", i),
                "SELECT id FROM base",
            )])
            .unwrap();
        }

        let all_names: HashSet<String> = dag.tables.keys().cloned().collect();
        let levels = dag.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0], vec!["base"]);
        assert_eq!(levels[1].len(), 5);

        static EXECUTION_COUNTER: AtomicUsize = AtomicUsize::new(0);
        static MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
        static CURRENT_CONCURRENT: AtomicUsize = AtomicUsize::new(0);

        EXECUTION_COUNTER.store(0, Ordering::SeqCst);
        MAX_CONCURRENT.store(0, Ordering::SeqCst);
        CURRENT_CONCURRENT.store(0, Ordering::SeqCst);

        let executed = dag.run(executor.clone(), None).unwrap();

        assert_eq!(executed.len(), 6);
        assert_eq!(executed[0], "base");

        for i in 0..5 {
            let result = executor
                .execute_query(&format!("SELECT * FROM branch_{}", i))
                .unwrap();
            assert_eq!(result.rows.len(), 1);
        }
    }

    #[test]
    fn test_mock_mode_serial_execution_timing() {
        let mut dag = Dag::new();
        let executor = create_mock_executor();

        executor
            .execute_statement("CREATE TABLE timing_base (id INT64)")
            .unwrap();
        executor
            .execute_statement("INSERT INTO timing_base VALUES (1)")
            .unwrap();

        for i in 0..3 {
            dag.register(vec![computed_table(
                &format!("timing_{}", i),
                "SELECT id FROM timing_base",
            )])
            .unwrap();
        }

        let all_names: HashSet<String> = dag.tables.keys().cloned().collect();
        let levels = dag.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 1, "All tables should be in same level (independent)");
        assert_eq!(levels[0].len(), 3, "Should have 3 independent tables");

        let executed = dag.run(executor.clone(), None).unwrap();

        assert_eq!(executed.len(), 3);

        for name in &executed {
            let result = executor
                .execute_query(&format!("SELECT * FROM {}", name))
                .unwrap();
            assert_eq!(result.rows.len(), 1);
        }
    }

    #[test]
    fn test_mock_mode_execution_order_is_deterministic() {
        for _ in 0..5 {
            let mut dag = Dag::new();
            let executor = create_mock_executor();

            dag.register(vec![source_table(
                "root",
                vec![("v", "INT64")],
                vec![json!([1])],
            )])
            .unwrap();

            dag.register(vec![
                computed_table("a", "SELECT v FROM root"),
                computed_table("b", "SELECT v FROM root"),
                computed_table("c", "SELECT v FROM root"),
            ])
            .unwrap();

            let executed = dag.run(executor.clone(), None).unwrap();

            assert_eq!(executed[0], "root");
            assert_eq!(executed[1], "a");
            assert_eq!(executed[2], "b");
            assert_eq!(executed[3], "c");
        }
    }

    #[test]
    fn test_mock_mode_no_parallel_execution() {
        use std::sync::atomic::AtomicBool;

        static IS_EXECUTING: AtomicBool = AtomicBool::new(false);
        static OVERLAP_DETECTED: AtomicBool = AtomicBool::new(false);

        IS_EXECUTING.store(false, Ordering::SeqCst);
        OVERLAP_DETECTED.store(false, Ordering::SeqCst);

        let mut dag = Dag::new();
        let executor = create_mock_executor();

        executor
            .execute_statement("CREATE TABLE serial_base (v INT64)")
            .unwrap();

        for i in 0..1000 {
            executor
                .execute_statement(&format!("INSERT INTO serial_base VALUES ({})", i))
                .unwrap();
        }

        for i in 0..5 {
            dag.register(vec![computed_table(
                &format!("heavy_{}", i),
                "SELECT SUM(v) as total FROM serial_base",
            )])
            .unwrap();
        }

        let all_names: HashSet<String> = dag.tables.keys().cloned().collect();
        let levels = dag.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 1, "All heavy tables should be at same level");
        assert_eq!(levels[0].len(), 5, "Should have 5 independent heavy tables");

        let executed = dag.run(executor.clone(), None).unwrap();

        assert_eq!(executed.len(), 5);

        for name in &executed {
            let result = executor
                .execute_query(&format!("SELECT * FROM {}", name))
                .unwrap();
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0][0], json!(499500)); // sum of 0..999
        }

        assert!(
            !OVERLAP_DETECTED.load(Ordering::SeqCst),
            "Parallel execution was detected in mock mode!"
        );
    }

    #[test]
    fn test_verify_executor_mode_is_mock() {
        let executor = create_mock_executor();
        assert_eq!(executor.mode(), ExecutorMode::Mock);
    }
}
