use std::collections::{HashMap, HashSet};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::executor::Executor;
use crate::rpc::types::{ColumnDef, DagTableDef, DagTableDetail, DagTableInfo};

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
        executor: &Executor,
        targets: Option<Vec<String>>,
    ) -> Result<Vec<String>> {
        let tables_to_run = if let Some(targets) = targets {
            self.get_tables_with_deps(&targets)?
        } else {
            self.get_execution_order()?
        };

        let mut executed = Vec::new();

        for name in tables_to_run {
            let table = self.tables.get(&name).ok_or_else(|| {
                Error::InvalidRequest(format!("Table not found: {}", name))
            })?;

            if table.is_source {
                self.create_source_table(executor, table)?;
            } else if let Some(sql) = &table.sql {
                let drop_sql = format!("DROP TABLE IF EXISTS {}", table.name);
                let _ = executor.execute_statement(&drop_sql);

                let query_result = executor.execute_query(sql).map_err(|e| {
                    let dep_data = self.get_dependency_data(executor, &table.dependencies);
                    Error::Executor(format!("{}\n\nDependency data:\n{}", e, dep_data))
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

            executed.push(name);
        }

        Ok(executed)
    }

    fn create_source_table(&self, executor: &Executor, table: &DagTable) -> Result<()> {
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

    fn get_tables_with_deps(&self, targets: &[String]) -> Result<Vec<String>> {
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

        self.topological_sort(&needed)
    }

    fn get_execution_order(&self) -> Result<Vec<String>> {
        let all_names: HashSet<String> = self.tables.keys().cloned().collect();
        self.topological_sort(&all_names)
    }

    fn topological_sort(&self, subset: &HashSet<String>) -> Result<Vec<String>> {
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

        let mut queue: Vec<String> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(name, _)| name.clone())
            .collect();
        queue.sort();

        let mut result = Vec::new();

        while let Some(name) = queue.pop() {
            result.push(name.clone());

            if let Some(deps) = dependents.get(&name) {
                for dep_name in deps {
                    if let Some(degree) = in_degree.get_mut(dep_name) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push(dep_name.clone());
                            queue.sort();
                        }
                    }
                }
            }
        }

        if result.len() != subset.len() {
            return Err(Error::InvalidRequest("Circular dependency detected".to_string()));
        }

        Ok(result)
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

    fn get_dependency_data(&self, executor: &Executor, dependencies: &[String]) -> String {
        let mut result = Vec::new();

        for dep_name in dependencies {
            let query = format!("SELECT * FROM {}", dep_name);
            match executor.execute_query(&query) {
                Ok(query_result) => {
                    let mut table_str = format!("{}:\n", dep_name);
                    if query_result.rows.is_empty() {
                        table_str.push_str("  (empty)\n");
                    } else {
                        table_str.push_str(&format!("  columns: {}\n", query_result.columns.join(", ")));
                        for row in &query_result.rows {
                            let row_vals: Vec<String> = row.iter().map(|v| v.to_string()).collect();
                            table_str.push_str(&format!("  - {}\n", row_vals.join(", ")));
                        }
                    }
                    result.push(table_str);
                }
                Err(e) => {
                    result.push(format!("{}: <failed to query: {}>\n", dep_name, e));
                }
            }
        }

        if result.is_empty() {
            "(no dependencies)".to_string()
        } else {
            result.join("\n")
        }
    }
}

impl Default for Dag {
    fn default() -> Self {
        Self::new()
    }
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
