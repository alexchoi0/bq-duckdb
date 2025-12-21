use std::collections::{HashMap, HashSet};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use parking_lot::Mutex;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::executor::{Executor, ExecutorMode};
use crate::rpc::types::{ColumnDef, DagTableDef, DagTableDetail, DagTableInfo};
use crate::utils::json_to_sql_value;

#[derive(Debug, Clone)]
pub struct PipelineTable {
    pub name: String,
    pub sql: Option<String>,
    pub schema: Option<Vec<ColumnDef>>,
    pub rows: Vec<Value>,
    pub dependencies: Vec<String>,
    pub is_source: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PipelineResult {
    pub succeeded: Vec<String>,
    pub failed: Vec<TableError>,
    pub skipped: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableError {
    pub table: String,
    pub error: String,
}

impl PipelineResult {
    pub fn all_succeeded(&self) -> bool {
        self.failed.is_empty() && self.skipped.is_empty()
    }
}

#[derive(Clone)]
pub struct Pipeline {
    tables: HashMap<String, PipelineTable>,
}

const DEFAULT_MAX_CONCURRENCY: usize = 8;

struct StreamState {
    pending_deps: HashMap<String, HashSet<String>>,
    completed: HashSet<String>,
    blocked: HashSet<String>,
    in_flight: HashSet<String>,
    max_concurrency: usize,
}

impl StreamState {
    fn is_pending(&self, name: &str) -> bool {
        !self.completed.contains(name)
            && !self.blocked.contains(name)
            && !self.in_flight.contains(name)
    }

    fn is_ready(&self, name: &str) -> bool {
        self.pending_deps
            .get(name)
            .map(|deps| deps.is_empty())
            .unwrap_or(false)
            && self.is_pending(name)
    }

    fn mark_completed(&mut self, name: &str) {
        self.completed.insert(name.to_string());
        for deps in self.pending_deps.values_mut() {
            deps.remove(name);
        }
    }

    fn mark_blocked(&mut self, name: &str) {
        self.blocked.insert(name.to_string());
    }

    fn mark_in_flight(&mut self, name: &str) {
        self.in_flight.insert(name.to_string());
    }

    fn finish_in_flight(&mut self, name: &str) {
        self.in_flight.remove(name);
    }

    fn ready_tables(&self) -> Vec<String> {
        let available_slots = self.max_concurrency.saturating_sub(self.in_flight.len());
        if available_slots == 0 {
            return vec![];
        }

        self.pending_deps
            .keys()
            .filter(|name| self.is_ready(name))
            .take(available_slots)
            .cloned()
            .collect()
    }
}

impl Pipeline {
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

            let table = PipelineTable {
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
    ) -> Result<PipelineResult> {
        let subset = if let Some(targets) = targets {
            self.get_tables_with_deps_set(&targets)?
        } else {
            self.tables.keys().cloned().collect()
        };

        self.run_subset(executor, subset)
    }

    pub fn retry_failed(
        &self,
        executor: Arc<Executor>,
        previous_result: &PipelineResult,
    ) -> Result<PipelineResult> {
        let subset: HashSet<String> = previous_result
            .failed
            .iter()
            .map(|e| e.table.clone())
            .chain(previous_result.skipped.iter().cloned())
            .collect();

        self.run_subset(executor, subset)
    }

    fn run_subset(
        &self,
        executor: Arc<Executor>,
        tables: HashSet<String>,
    ) -> Result<PipelineResult> {
        if tables.is_empty() {
            return Ok(PipelineResult::default());
        }

        match executor.mode() {
            ExecutorMode::Mock => {
                let levels = self.topological_sort_levels(&tables)?;
                #[allow(clippy::needless_borrow)]
                self.run_in_serial(&executor, levels)
            }
            ExecutorMode::BigQuery => self.run_via_streaming(executor, tables),
        }
    }

    fn run_in_serial(
        &self,
        executor: &Executor,
        levels: Vec<Vec<String>>,
    ) -> Result<PipelineResult> {
        let mut result = PipelineResult::default();
        let mut blocked_tables: HashSet<String> = HashSet::new();

        for level in levels {
            for name in level {
                if self.should_skip(&name, &blocked_tables) {
                    blocked_tables.insert(name.clone());
                    result.skipped.push(name);
                    continue;
                }

                match self.execute_single_table(executor, &name) {
                    Ok(()) => result.succeeded.push(name),
                    Err(e) => {
                        blocked_tables.insert(name.clone());
                        result.failed.push(TableError {
                            table: name,
                            error: e.to_string(),
                        });
                    }
                }
            }
        }

        Ok(result)
    }

    fn run_via_streaming(
        &self,
        executor: Arc<Executor>,
        subset: HashSet<String>,
    ) -> Result<PipelineResult> {
        let all_tables: Vec<String> = subset.into_iter().collect();
        let total_count = all_tables.len();

        let pending_deps: HashMap<String, HashSet<String>> = all_tables
            .iter()
            .map(|name| {
                let deps: HashSet<String> = self
                    .tables
                    .get(name)
                    .map(|t| t.dependencies.iter().cloned().collect())
                    .unwrap_or_default();
                let relevant_deps: HashSet<String> = deps
                    .into_iter()
                    .filter(|d| all_tables.contains(d))
                    .collect();
                (name.clone(), relevant_deps)
            })
            .collect();

        let max_concurrency = std::env::var("BQ_MAX_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MAX_CONCURRENCY);

        let state = Arc::new(Mutex::new(StreamState {
            pending_deps,
            completed: HashSet::new(),
            blocked: HashSet::new(),
            in_flight: HashSet::new(),
            max_concurrency,
        }));

        let (tx, rx) = mpsc::channel::<(String, Result<()>)>();

        self.spawn_ready_tables(&executor, &state, &tx);

        let mut result = PipelineResult::default();
        let mut processed = 0;

        while processed < total_count {
            let (name, outcome) = match rx.recv() {
                Ok(msg) => msg,
                Err(_) => break,
            };

            processed += 1;

            {
                let mut s = state.lock();
                s.finish_in_flight(&name);
                match &outcome {
                    Ok(()) => s.mark_completed(&name),
                    Err(_) => s.mark_blocked(&name),
                }
            }

            match outcome {
                Ok(()) => result.succeeded.push(name),
                Err(e) => {
                    result.failed.push(TableError {
                        table: name,
                        error: e.to_string(),
                    });
                }
            }

            self.spawn_ready_tables(&executor, &state, &tx);

            {
                let mut s = state.lock();
                let newly_skipped: Vec<String> = s
                    .pending_deps
                    .keys()
                    .filter(|name| s.is_pending(name))
                    .filter(|name| self.should_skip(name, &s.blocked))
                    .cloned()
                    .collect();

                for name in newly_skipped {
                    s.mark_blocked(&name);
                    result.skipped.push(name);
                    processed += 1;
                }
            }
        }

        Ok(result)
    }

    fn spawn_ready_tables(
        &self,
        executor: &Arc<Executor>,
        state: &Arc<Mutex<StreamState>>,
        tx: &mpsc::Sender<(String, Result<()>)>,
    ) {
        let ready: Vec<String> = {
            let s = state.lock();
            s.ready_tables()
        };

        for name in ready {
            {
                let mut s = state.lock();
                if !s.is_pending(&name) {
                    continue;
                }
                s.mark_in_flight(&name);
            }

            let executor = Arc::clone(executor);
            let table = self.tables.get(&name).cloned();
            let tx = tx.clone();

            thread::spawn(move || {
                let res = if let Some(table) = table {
                    execute_table(&executor, &table)
                } else {
                    Err(Error::InvalidRequest(format!("Table not found: {}", name)))
                };
                let _ = tx.send((name, res));
            });
        }
    }

    fn should_skip(&self, table_name: &str, blocked_tables: &HashSet<String>) -> bool {
        if let Some(table) = self.tables.get(table_name) {
            for dep in &table.dependencies {
                if blocked_tables.contains(dep) {
                    return true;
                }
            }
        }
        false
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
        let table = self
            .tables
            .get(name)
            .ok_or_else(|| Error::InvalidRequest(format!("Table not found: {}", name)))?;
        execute_table(executor, table)
    }

    fn topological_sort_levels(&self, queries: &HashSet<String>) -> Result<Vec<Vec<String>>> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

        for query in queries {
            in_degree.entry(query.clone()).or_insert(0);
            if let Some(table) = self.tables.get(query) {
                for dep in &table.dependencies {
                    if queries.contains(dep) {
                        *in_degree.entry(query.clone()).or_insert(0) += 1;
                        dependents
                            .entry(dep.clone())
                            .or_default()
                            .push(query.clone());
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

        if processed != queries.len() {
            return Err(Error::InvalidRequest(
                "Circular dependency detected".to_string(),
            ));
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
        let handle = tokio::runtime::Handle::current();
        for table_name in self.tables.keys() {
            let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
            let _ = handle.block_on(executor.execute_statement(&drop_sql));
        }
        self.tables.clear();
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

fn execute_table(executor: &Executor, table: &PipelineTable) -> Result<()> {
    let handle = tokio::runtime::Handle::current();

    if table.is_source {
        create_source_table_standalone(executor, table)?;
    } else if let Some(sql) = &table.sql {
        let drop_sql = format!("DROP TABLE IF EXISTS {}", table.name);
        let _ = handle.block_on(executor.execute_statement(&drop_sql));

        let query_result = handle.block_on(executor.execute_query(sql)).map_err(|e| {
            Error::Executor(format!(
                "Failed to execute query for table {}: {}",
                table.name, e
            ))
        })?;

        if !query_result.columns.is_empty() {
            let column_types: Vec<String> = query_result
                .columns
                .iter()
                .map(|col| format!("{} {}", col.name, col.data_type))
                .collect();

            let create_sql = format!("CREATE TABLE {} ({})", table.name, column_types.join(", "));
            handle.block_on(executor.execute_statement(&create_sql))?;

            if !query_result.rows.is_empty() {
                let values: Vec<String> = query_result
                    .rows
                    .iter()
                    .map(|row| {
                        let vals: Vec<String> = row.iter().map(json_to_sql_value).collect();
                        format!("({})", vals.join(", "))
                    })
                    .collect();

                let insert_sql = format!("INSERT INTO {} VALUES {}", table.name, values.join(", "));
                handle.block_on(executor.execute_statement(&insert_sql))?;
            }
        }
    }

    Ok(())
}

fn create_source_table_standalone(executor: &Executor, table: &PipelineTable) -> Result<()> {
    let handle = tokio::runtime::Handle::current();

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
        handle.block_on(executor.execute_statement(&create_sql))?;

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
                let insert_sql = format!("INSERT INTO {} VALUES {}", table.name, values.join(", "));
                handle.block_on(executor.execute_statement(&insert_sql))?;
            }
        }
    }
    Ok(())
}

fn extract_dependencies(sql: &str, known_tables: &HashMap<String, PipelineTable>) -> Vec<String> {
    let cte_names = extract_cte_names(sql);
    let mut deps = Vec::new();
    let sql_upper = sql.to_uppercase();

    for table_name in known_tables.keys() {
        let name_upper = table_name.to_uppercase();

        if cte_names.contains(&name_upper) {
            continue;
        }

        if is_table_referenced(&sql_upper, &name_upper) && !deps.contains(table_name) {
            deps.push(table_name.clone());
        }
    }

    deps.sort();
    deps
}

fn extract_cte_names(sql: &str) -> HashSet<String> {
    let mut cte_names = HashSet::new();
    let sql_upper = sql.to_uppercase();

    let Some(with_pos) = sql_upper.find("WITH ") else {
        return cte_names;
    };

    let mut sql_after_with = &sql_upper[with_pos + 5..];
    let trimmed = sql_after_with.trim_start();
    if let Some(stripped) = trimmed.strip_prefix("RECURSIVE ") {
        sql_after_with = stripped;
    }

    let mut in_parens = 0;
    let mut current_pos = 0;
    let mut looking_for_name = true;
    let chars: Vec<char> = sql_after_with.chars().collect();

    while current_pos < chars.len() {
        let ch = chars[current_pos];

        if looking_for_name {
            while current_pos < chars.len() && chars[current_pos].is_whitespace() {
                current_pos += 1;
            }
            if current_pos >= chars.len() {
                break;
            }

            let start = current_pos;
            while current_pos < chars.len()
                && (chars[current_pos].is_alphanumeric() || chars[current_pos] == '_')
            {
                current_pos += 1;
            }

            if current_pos > start {
                let name: String = chars[start..current_pos].iter().collect();
                cte_names.insert(name);
            }

            looking_for_name = false;
            continue;
        }

        match ch {
            '(' => in_parens += 1,
            ')' => in_parens -= 1,
            ',' if in_parens == 0 => {
                looking_for_name = true;
            }
            _ => {}
        }

        if in_parens == 0 {
            let remaining: String = chars[current_pos..].iter().collect();
            let trimmed = remaining.trim_start();

            if trimmed.starts_with("SELECT")
                || trimmed.starts_with("INSERT")
                || trimmed.starts_with("UPDATE")
                || trimmed.starts_with("DELETE")
            {
                break;
            }
        }

        current_pos += 1;
    }

    cte_names
}

fn is_table_referenced(sql_upper: &str, table_name_upper: &str) -> bool {
    let patterns = [
        format!("FROM {}", table_name_upper),
        format!("JOIN {}", table_name_upper),
        format!(", {}", table_name_upper),
    ];

    for pattern in &patterns {
        if let Some(pos) = sql_upper.find(pattern.as_str()) {
            let after_pos = pos + pattern.len();
            if after_pos >= sql_upper.len() {
                return true;
            }
            let next_char = sql_upper.chars().nth(after_pos).unwrap_or(' ');
            if !next_char.is_alphanumeric() && next_char != '_' {
                return true;
            }
        }
    }

    let qualified_patterns = [
        format!("FROM {}.{}", table_name_upper, ""),
        format!("JOIN {}.{}", table_name_upper, ""),
    ];

    for pattern in &qualified_patterns {
        let base = pattern.trim_end_matches('.');
        if sql_upper.contains(&format!("{}.", base)) {
            return false;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{QueryResult, YachtSqlExecutor};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::runtime::Runtime;

    fn create_mock_executor() -> Arc<Executor> {
        Arc::new(Executor::Mock(YachtSqlExecutor::new()))
    }

    fn test_runtime() -> Runtime {
        Runtime::new().unwrap()
    }

    trait ExecutorTestExt {
        fn execute_query_sync(&self, sql: &str) -> Result<QueryResult>;
        fn execute_statement_sync(&self, sql: &str) -> Result<u64>;
    }

    impl ExecutorTestExt for Arc<Executor> {
        fn execute_query_sync(&self, sql: &str) -> Result<QueryResult> {
            let rt = test_runtime();
            rt.block_on(self.execute_query(sql))
        }

        fn execute_statement_sync(&self, sql: &str) -> Result<u64> {
            let rt = test_runtime();
            rt.block_on(self.execute_statement(sql))
        }
    }

    trait PipelineTestExt {
        fn run_sync(
            &self,
            executor: Arc<Executor>,
            targets: Option<Vec<String>>,
        ) -> Result<PipelineResult>;
        fn retry_failed_sync(
            &self,
            executor: Arc<Executor>,
            prev_result: &PipelineResult,
        ) -> Result<PipelineResult>;
        fn clear_sync(&mut self, executor: &Arc<Executor>);
    }

    impl PipelineTestExt for Pipeline {
        fn run_sync(
            &self,
            executor: Arc<Executor>,
            targets: Option<Vec<String>>,
        ) -> Result<PipelineResult> {
            let rt = test_runtime();
            let pipeline = self.clone();
            rt.block_on(async move {
                tokio::task::spawn_blocking(move || pipeline.run(executor, targets))
                    .await
                    .map_err(|e| Error::Internal(e.to_string()))?
            })
        }

        fn retry_failed_sync(
            &self,
            executor: Arc<Executor>,
            prev_result: &PipelineResult,
        ) -> Result<PipelineResult> {
            let rt = test_runtime();
            let pipeline = self.clone();
            let prev = prev_result.clone();
            rt.block_on(async move {
                tokio::task::spawn_blocking(move || pipeline.retry_failed(executor, &prev))
                    .await
                    .map_err(|e| Error::Internal(e.to_string()))?
            })
        }

        fn clear_sync(&mut self, executor: &Arc<Executor>) {
            let rt = test_runtime();
            let exec = executor.clone();
            let tables: Vec<String> = self.tables.keys().cloned().collect();
            rt.block_on(async move {
                tokio::task::spawn_blocking(move || {
                    let handle = tokio::runtime::Handle::current();
                    for table_name in tables {
                        let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
                        let _ = handle.block_on(exec.execute_statement(&drop_sql));
                    }
                })
                .await
                .unwrap()
            });
            self.tables.clear();
        }
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
        let mut pipeline = Pipeline::new();
        let tables = vec![source_table(
            "users",
            vec![("id", "INT64"), ("name", "STRING")],
            vec![],
        )];

        let result = pipeline.register(tables).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "users");
        assert!(result[0].dependencies.is_empty());
    }

    #[test]
    fn test_register_computed_table_with_dependency() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![source_table(
                "users",
                vec![("id", "INT64"), ("name", "STRING")],
                vec![],
            )])
            .unwrap();

        let result = pipeline
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
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![
                source_table("users", vec![("id", "INT64"), ("name", "STRING")], vec![]),
                source_table(
                    "orders",
                    vec![("id", "INT64"), ("user_id", "INT64")],
                    vec![],
                ),
            ])
            .unwrap();

        let result = pipeline
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
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "users",
                vec![("id", "INT64"), ("name", "STRING")],
                vec![json!([1, "Alice"]), json!([2, "Bob"])],
            )])
            .unwrap();

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert!(result.all_succeeded());
        assert_eq!(result.succeeded, vec!["users"]);

        let query = executor
            .execute_query_sync("SELECT * FROM users ORDER BY id")
            .unwrap();
        assert_eq!(query.rows.len(), 2);
        assert_eq!(query.rows[0][0], json!(1));
        assert_eq!(query.rows[0][1], json!("Alice"));
        assert_eq!(query.rows[1][0], json!(2));
        assert_eq!(query.rows[1][1], json!("Bob"));
    }

    #[test]
    fn test_run_computed_table_from_source() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "numbers",
                vec![("value", "INT64")],
                vec![json!([1]), json!([2]), json!([3]), json!([4]), json!([5])],
            )])
            .unwrap();

        pipeline
            .register(vec![computed_table(
                "even_numbers",
                "SELECT value FROM numbers WHERE value % 2 = 0",
            )])
            .unwrap();

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert!(result.succeeded.contains(&"numbers".to_string()));
        assert!(result.succeeded.contains(&"even_numbers".to_string()));

        let result = executor
            .execute_query_sync("SELECT * FROM even_numbers ORDER BY value")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], json!(2));
        assert_eq!(result.rows[1][0], json!(4));
    }

    #[test]
    fn test_run_chain_of_computed_tables() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
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

        pipeline
            .register(vec![computed_table(
                "doubled",
                "SELECT n * 2 AS n FROM raw_numbers",
            )])
            .unwrap();

        pipeline
            .register(vec![computed_table(
                "plus_ten",
                "SELECT n + 10 AS n FROM doubled",
            )])
            .unwrap();

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert_eq!(result.succeeded.len(), 3);
        let idx_raw = result
            .succeeded
            .iter()
            .position(|x| x == "raw_numbers")
            .unwrap();
        let idx_doubled = result
            .succeeded
            .iter()
            .position(|x| x == "doubled")
            .unwrap();
        let idx_plus_ten = result
            .succeeded
            .iter()
            .position(|x| x == "plus_ten")
            .unwrap();
        assert!(idx_raw < idx_doubled);
        assert!(idx_doubled < idx_plus_ten);

        let result = executor
            .execute_query_sync("SELECT * FROM plus_ten ORDER BY n")
            .unwrap();
        assert_eq!(result.rows.len(), 6);
        assert_eq!(result.rows[0][0], json!(12)); // 1*2+10
        assert_eq!(result.rows[5][0], json!(22)); // 6*2+10
    }

    #[test]
    fn test_run_diamond_dependency() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "source",
                vec![("x", "INT64")],
                vec![json!([10]), json!([20]), json!([30])],
            )])
            .unwrap();

        pipeline
            .register(vec![
                computed_table("left_branch", "SELECT x + 1 AS x FROM source"),
                computed_table("right_branch", "SELECT x - 1 AS x FROM source"),
            ])
            .unwrap();

        pipeline.register(vec![computed_table(
            "merged",
            "SELECT l.x AS left_x, r.x AS right_x FROM left_branch l, right_branch r WHERE l.x - r.x = 2",
        )])
        .unwrap();

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert_eq!(result.succeeded.len(), 4);
        let idx_source = result.succeeded.iter().position(|x| x == "source").unwrap();
        let idx_left = result
            .succeeded
            .iter()
            .position(|x| x == "left_branch")
            .unwrap();
        let idx_right = result
            .succeeded
            .iter()
            .position(|x| x == "right_branch")
            .unwrap();
        let idx_merged = result.succeeded.iter().position(|x| x == "merged").unwrap();

        assert!(idx_source < idx_left);
        assert!(idx_source < idx_right);
        assert!(idx_left < idx_merged);
        assert!(idx_right < idx_merged);

        let result = executor
            .execute_query_sync("SELECT * FROM merged ORDER BY left_x")
            .unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_run_with_specific_targets() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![
                source_table("a", vec![("v", "INT64")], vec![json!([1])]),
                source_table("b", vec![("v", "INT64")], vec![json!([2])]),
                source_table("c", vec![("v", "INT64")], vec![json!([3])]),
            ])
            .unwrap();

        pipeline
            .register(vec![
                computed_table("from_a", "SELECT v * 10 AS v FROM a"),
                computed_table("from_b", "SELECT v * 10 AS v FROM b"),
            ])
            .unwrap();

        let result = pipeline
            .run_sync(executor.clone(), Some(vec!["from_a".to_string()]))
            .unwrap();

        assert!(result.succeeded.contains(&"a".to_string()));
        assert!(result.succeeded.contains(&"from_a".to_string()));
        assert!(!result.succeeded.contains(&"b".to_string()));
        assert!(!result.succeeded.contains(&"from_b".to_string()));
        assert!(!result.succeeded.contains(&"c".to_string()));

        let result = executor.execute_query_sync("SELECT * FROM from_a").unwrap();
        assert_eq!(result.rows[0][0], json!(10));

        assert!(executor.execute_query_sync("SELECT * FROM from_b").is_err());
    }

    #[test]
    fn test_run_with_multiple_targets() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![
                source_table("x", vec![("v", "INT64")], vec![json!([100])]),
                source_table("y", vec![("v", "INT64")], vec![json!([200])]),
            ])
            .unwrap();

        pipeline
            .register(vec![
                computed_table("from_x", "SELECT v FROM x"),
                computed_table("from_y", "SELECT v FROM y"),
            ])
            .unwrap();

        let result = pipeline
            .run_sync(
                executor.clone(),
                Some(vec!["from_x".to_string(), "from_y".to_string()]),
            )
            .unwrap();

        assert_eq!(result.succeeded.len(), 4);
        assert!(result.succeeded.contains(&"x".to_string()));
        assert!(result.succeeded.contains(&"y".to_string()));
        assert!(result.succeeded.contains(&"from_x".to_string()));
        assert!(result.succeeded.contains(&"from_y".to_string()));
    }

    #[test]
    fn test_topological_sort_levels_independent_tables() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![
                source_table("a", vec![("v", "INT64")], vec![]),
                source_table("b", vec![("v", "INT64")], vec![]),
                source_table("c", vec![("v", "INT64")], vec![]),
            ])
            .unwrap();

        let all_names: HashSet<String> = pipeline.tables.keys().cloned().collect();
        let levels = pipeline.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].len(), 3);
    }

    #[test]
    fn test_topological_sort_levels_linear_chain() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![source_table("a", vec![("v", "INT64")], vec![])])
            .unwrap();
        pipeline
            .register(vec![computed_table("b", "SELECT * FROM a")])
            .unwrap();
        pipeline
            .register(vec![computed_table("c", "SELECT * FROM b")])
            .unwrap();
        pipeline
            .register(vec![computed_table("d", "SELECT * FROM c")])
            .unwrap();

        let all_names: HashSet<String> = pipeline.tables.keys().cloned().collect();
        let levels = pipeline.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 4);
        assert_eq!(levels[0], vec!["a"]);
        assert_eq!(levels[1], vec!["b"]);
        assert_eq!(levels[2], vec!["c"]);
        assert_eq!(levels[3], vec!["d"]);
    }

    #[test]
    fn test_topological_sort_levels_diamond() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![source_table("root", vec![("v", "INT64")], vec![])])
            .unwrap();
        pipeline
            .register(vec![
                computed_table("left", "SELECT * FROM root"),
                computed_table("right", "SELECT * FROM root"),
            ])
            .unwrap();
        pipeline
            .register(vec![computed_table("bottom", "SELECT * FROM left, right")])
            .unwrap();

        let all_names: HashSet<String> = pipeline.tables.keys().cloned().collect();
        let levels = pipeline.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0], vec!["root"]);
        assert_eq!(levels[1].len(), 2);
        assert!(levels[1].contains(&"left".to_string()));
        assert!(levels[1].contains(&"right".to_string()));
        assert_eq!(levels[2], vec!["bottom"]);
    }

    #[test]
    fn test_topological_sort_levels_complex_dag() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![
                source_table("s1", vec![("v", "INT64")], vec![]),
                source_table("s2", vec![("v", "INT64")], vec![]),
            ])
            .unwrap();

        pipeline
            .register(vec![
                computed_table("a", "SELECT * FROM s1"),
                computed_table("b", "SELECT * FROM s2"),
                computed_table("c", "SELECT * FROM s1, s2"),
            ])
            .unwrap();

        pipeline
            .register(vec![computed_table("d", "SELECT * FROM a, b")])
            .unwrap();

        pipeline
            .register(vec![computed_table("e", "SELECT * FROM c, d")])
            .unwrap();

        let all_names: HashSet<String> = pipeline.tables.keys().cloned().collect();
        let levels = pipeline.topological_sort_levels(&all_names).unwrap();

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
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "empty_table",
                vec![("id", "INT64"), ("value", "STRING")],
                vec![],
            )])
            .unwrap();

        let result = pipeline.run_sync(executor.clone(), None).unwrap();
        assert_eq!(result.succeeded, vec!["empty_table"]);

        let result = executor
            .execute_query_sync("SELECT * FROM empty_table")
            .unwrap();
        assert_eq!(result.rows.len(), 0);
        let column_names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(column_names, vec!["id", "value"]);
    }

    #[test]
    #[ignore = "requires SUM/GROUP BY aggregates which are not yet implemented in concurrent executor"]
    fn test_aggregation_query() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
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

        pipeline
            .register(vec![computed_table(
                "sales_summary",
                "SELECT product, SUM(amount) AS total FROM sales GROUP BY product",
            )])
            .unwrap();

        pipeline.run_sync(executor.clone(), None).unwrap();

        let result = executor
            .execute_query_sync("SELECT * FROM sales_summary ORDER BY product")
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
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![
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
                    vec![
                        ("id", "INT64"),
                        ("customer_id", "INT64"),
                        ("total", "INT64"),
                    ],
                    vec![
                        json!([101, 1, 500]),
                        json!([102, 1, 300]),
                        json!([103, 2, 150]),
                    ],
                ),
            ])
            .unwrap();

        pipeline
            .register(vec![computed_table(
                "customer_orders",
                "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id",
            )])
            .unwrap();

        pipeline.run_sync(executor.clone(), None).unwrap();

        let result = executor
            .execute_query_sync("SELECT * FROM customer_orders ORDER BY name, total")
            .unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_clear_dag() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "test_table",
                vec![("v", "INT64")],
                vec![json!([42])],
            )])
            .unwrap();

        pipeline.run_sync(executor.clone(), None).unwrap();

        assert!(executor
            .execute_query_sync("SELECT * FROM test_table")
            .is_ok());

        pipeline.clear_sync(&executor);

        assert!(pipeline.get_tables().is_empty());
        assert!(executor
            .execute_query_sync("SELECT * FROM test_table")
            .is_err());
    }

    #[test]
    fn test_get_tables() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![source_table("src", vec![("v", "INT64")], vec![])])
            .unwrap();

        pipeline
            .register(vec![computed_table("derived", "SELECT * FROM src")])
            .unwrap();

        let tables = pipeline.get_tables();
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
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "with_nulls",
                vec![("id", "INT64"), ("value", "STRING")],
                vec![json!([1, "hello"]), json!([2, null]), json!([3, "world"])],
            )])
            .unwrap();

        pipeline.run_sync(executor.clone(), None).unwrap();

        let result = executor
            .execute_query_sync("SELECT * FROM with_nulls ORDER BY id")
            .unwrap();
        assert_eq!(result.rows.len(), 3);
        assert!(result.rows[1][1].is_null());
    }

    #[test]
    fn test_boolean_values() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "flags",
                vec![("name", "STRING"), ("active", "BOOL")],
                vec![
                    json!(["feature_a", true]),
                    json!(["feature_b", false]),
                    json!(["feature_c", true]),
                ],
            )])
            .unwrap();

        pipeline
            .register(vec![computed_table(
                "active_flags",
                "SELECT name FROM flags WHERE active = true",
            )])
            .unwrap();

        pipeline.run_sync(executor.clone(), None).unwrap();

        let result = executor
            .execute_query_sync("SELECT * FROM active_flags ORDER BY name")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], json!("feature_a"));
        assert_eq!(result.rows[1][0], json!("feature_c"));
    }

    #[test]
    fn test_float_values() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "measurements",
                vec![("sensor", "STRING"), ("reading", "FLOAT64")],
                vec![
                    json!(["temp", 23.5]),
                    json!(["humidity", 65.2]),
                    json!(["pressure", 1013.25]),
                ],
            )])
            .unwrap();

        pipeline
            .register(vec![computed_table(
                "high_readings",
                "SELECT sensor, reading FROM measurements WHERE reading > 50",
            )])
            .unwrap();

        pipeline.run_sync(executor.clone(), None).unwrap();

        let result = executor
            .execute_query_sync("SELECT * FROM high_readings ORDER BY reading")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_dependency_detection_case_insensitive() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![source_table("MyTable", vec![("v", "INT64")], vec![])])
            .unwrap();

        let result = pipeline
            .register(vec![computed_table("derived", "SELECT * FROM mytable")])
            .unwrap();

        assert_eq!(result[0].dependencies, vec!["MyTable"]);
    }

    #[test]
    fn test_rerun_computed_table_reflects_source_changes() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        executor
            .execute_statement_sync("CREATE TABLE counter (n INT64)")
            .unwrap();
        executor
            .execute_statement_sync("INSERT INTO counter VALUES (1)")
            .unwrap();

        pipeline
            .register(vec![computed_table(
                "doubled",
                "SELECT n * 2 AS n FROM counter",
            )])
            .unwrap();

        pipeline.run_sync(executor.clone(), None).unwrap();

        let result = executor
            .execute_query_sync("SELECT * FROM doubled")
            .unwrap();
        assert_eq!(result.rows[0][0], json!(2));

        executor
            .execute_statement_sync("INSERT INTO counter VALUES (10)")
            .unwrap();

        pipeline.run_sync(executor.clone(), None).unwrap();

        let result = executor
            .execute_query_sync("SELECT * FROM doubled ORDER BY n")
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], json!(2));
        assert_eq!(result.rows[1][0], json!(20));
    }

    #[test]
    fn test_wide_dag_many_independent_branches() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "root",
                vec![("v", "INT64")],
                vec![json!([1])],
            )])
            .unwrap();

        for i in 0..10 {
            pipeline
                .register(vec![computed_table(
                    &format!("branch_{}", i),
                    &format!("SELECT v + {} AS v FROM root", i),
                )])
                .unwrap();
        }

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert_eq!(result.succeeded.len(), 11);
        assert_eq!(result.succeeded[0], "root");

        for i in 0..10 {
            let query = executor
                .execute_query_sync(&format!("SELECT * FROM branch_{}", i))
                .unwrap();
            assert_eq!(query.rows[0][0], json!(1 + i as i64));
        }
    }

    #[test]
    fn test_deep_dag_long_chain() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "step_0",
                vec![("n", "INT64")],
                vec![json!([0])],
            )])
            .unwrap();

        for i in 1..=20 {
            pipeline
                .register(vec![computed_table(
                    &format!("step_{}", i),
                    &format!("SELECT n + 1 AS n FROM step_{}", i - 1),
                )])
                .unwrap();
        }

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert_eq!(result.succeeded.len(), 21);

        for i in 0..=20 {
            assert_eq!(result.succeeded[i], format!("step_{}", i));
        }

        let query = executor
            .execute_query_sync("SELECT * FROM step_20")
            .unwrap();
        assert_eq!(query.rows[0][0], json!(20));
    }

    #[test]
    fn test_execution_order_respects_dependencies() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![
                source_table("t_a", vec![("v", "INT64")], vec![json!([1])]),
                source_table("t_b", vec![("v", "INT64")], vec![json!([2])]),
            ])
            .unwrap();

        pipeline
            .register(vec![computed_table("t_c", "SELECT v FROM t_a")])
            .unwrap();

        pipeline
            .register(vec![computed_table(
                "t_d",
                "SELECT t_b.v FROM t_b JOIN t_c ON 1=1",
            )])
            .unwrap();

        pipeline
            .register(vec![computed_table("t_e", "SELECT v FROM t_d")])
            .unwrap();

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert_eq!(result.succeeded.len(), 5);

        let pos = |name: &str| result.succeeded.iter().position(|x| x == name).unwrap();

        assert!(pos("t_a") < pos("t_c"));
        assert!(pos("t_b") < pos("t_d"));
        assert!(pos("t_c") < pos("t_d"));
        assert!(pos("t_d") < pos("t_e"));
    }

    #[test]
    fn test_mock_mode_executes_serially() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "base",
                vec![("id", "INT64")],
                vec![json!([1])],
            )])
            .unwrap();

        for i in 0..5 {
            pipeline
                .register(vec![computed_table(
                    &format!("branch_{}", i),
                    "SELECT id FROM base",
                )])
                .unwrap();
        }

        let all_names: HashSet<String> = pipeline.tables.keys().cloned().collect();
        let levels = pipeline.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0], vec!["base"]);
        assert_eq!(levels[1].len(), 5);

        static EXECUTION_COUNTER: AtomicUsize = AtomicUsize::new(0);
        static MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
        static CURRENT_CONCURRENT: AtomicUsize = AtomicUsize::new(0);

        EXECUTION_COUNTER.store(0, Ordering::SeqCst);
        MAX_CONCURRENT.store(0, Ordering::SeqCst);
        CURRENT_CONCURRENT.store(0, Ordering::SeqCst);

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert_eq!(result.succeeded.len(), 6);
        assert_eq!(result.succeeded[0], "base");

        for i in 0..5 {
            let query = executor
                .execute_query_sync(&format!("SELECT * FROM branch_{}", i))
                .unwrap();
            assert_eq!(query.rows.len(), 1);
        }
    }

    #[test]
    fn test_mock_mode_serial_execution_timing() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        executor
            .execute_statement_sync("CREATE TABLE timing_base (id INT64)")
            .unwrap();
        executor
            .execute_statement_sync("INSERT INTO timing_base VALUES (1)")
            .unwrap();

        for i in 0..3 {
            pipeline
                .register(vec![computed_table(
                    &format!("timing_{}", i),
                    "SELECT id FROM timing_base",
                )])
                .unwrap();
        }

        let all_names: HashSet<String> = pipeline.tables.keys().cloned().collect();
        let levels = pipeline.topological_sort_levels(&all_names).unwrap();

        assert_eq!(
            levels.len(),
            1,
            "All tables should be in same level (independent)"
        );
        assert_eq!(levels[0].len(), 3, "Should have 3 independent tables");

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert_eq!(result.succeeded.len(), 3);

        for name in &result.succeeded {
            let query = executor
                .execute_query_sync(&format!("SELECT * FROM {}", name))
                .unwrap();
            assert_eq!(query.rows.len(), 1);
        }
    }

    #[test]
    fn test_mock_mode_execution_order_is_deterministic() {
        for _ in 0..5 {
            let mut pipeline = Pipeline::new();
            let executor = create_mock_executor();

            pipeline
                .register(vec![source_table(
                    "root",
                    vec![("v", "INT64")],
                    vec![json!([1])],
                )])
                .unwrap();

            pipeline
                .register(vec![
                    computed_table("a", "SELECT v FROM root"),
                    computed_table("b", "SELECT v FROM root"),
                    computed_table("c", "SELECT v FROM root"),
                ])
                .unwrap();

            let result = pipeline.run_sync(executor.clone(), None).unwrap();

            assert_eq!(result.succeeded[0], "root");
            assert_eq!(result.succeeded[1], "a");
            assert_eq!(result.succeeded[2], "b");
            assert_eq!(result.succeeded[3], "c");
        }
    }

    #[test]
    #[ignore = "requires SUM aggregate which is not yet implemented in concurrent executor"]
    fn test_mock_mode_no_parallel_execution() {
        use std::sync::atomic::AtomicBool;

        static IS_EXECUTING: AtomicBool = AtomicBool::new(false);
        static OVERLAP_DETECTED: AtomicBool = AtomicBool::new(false);

        IS_EXECUTING.store(false, Ordering::SeqCst);
        OVERLAP_DETECTED.store(false, Ordering::SeqCst);

        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        executor
            .execute_statement_sync("CREATE TABLE serial_base (v INT64)")
            .unwrap();

        for i in 0..1000 {
            executor
                .execute_statement_sync(&format!("INSERT INTO serial_base VALUES ({})", i))
                .unwrap();
        }

        for i in 0..5 {
            pipeline
                .register(vec![computed_table(
                    &format!("heavy_{}", i),
                    "SELECT SUM(v) as total FROM serial_base",
                )])
                .unwrap();
        }

        let all_names: HashSet<String> = pipeline.tables.keys().cloned().collect();
        let levels = pipeline.topological_sort_levels(&all_names).unwrap();

        assert_eq!(levels.len(), 1, "All heavy tables should be at same level");
        assert_eq!(levels[0].len(), 5, "Should have 5 independent heavy tables");

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert_eq!(result.succeeded.len(), 5);

        for name in &result.succeeded {
            let query = executor
                .execute_query_sync(&format!("SELECT * FROM {}", name))
                .unwrap();
            assert_eq!(query.rows.len(), 1);
            assert_eq!(query.rows[0][0], json!(499500)); // sum of 0..999
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

    #[test]
    fn test_failed_table_tracked() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![computed_table(
                "bad_query",
                "SELECT * FROM nonexistent_table",
            )])
            .unwrap();

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert!(!result.all_succeeded());
        assert!(result.succeeded.is_empty());
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].table, "bad_query");
        assert!(result.failed[0].error.contains("nonexistent"));
    }

    #[test]
    fn test_downstream_tables_skipped_on_failure() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![computed_table(
                "failing_source",
                "SELECT * FROM nonexistent_table",
            )])
            .unwrap();

        pipeline
            .register(vec![computed_table(
                "dependent_a",
                "SELECT * FROM failing_source",
            )])
            .unwrap();

        pipeline
            .register(vec![computed_table(
                "dependent_b",
                "SELECT * FROM dependent_a",
            )])
            .unwrap();

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert!(!result.all_succeeded());
        assert!(result.succeeded.is_empty());
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].table, "failing_source");
        assert_eq!(result.skipped.len(), 2);
        assert!(result.skipped.contains(&"dependent_a".to_string()));
        assert!(result.skipped.contains(&"dependent_b".to_string()));
    }

    #[test]
    fn test_partial_success_with_independent_tables() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        executor
            .execute_statement_sync("CREATE TABLE good_data (v INT64)")
            .unwrap();
        executor
            .execute_statement_sync("INSERT INTO good_data VALUES (42)")
            .unwrap();

        pipeline
            .register(vec![
                computed_table("good_table", "SELECT v FROM good_data"),
                computed_table("bad_table", "SELECT * FROM nonexistent"),
            ])
            .unwrap();

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert!(!result.all_succeeded());
        assert_eq!(result.succeeded.len(), 1);
        assert!(result.succeeded.contains(&"good_table".to_string()));
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].table, "bad_table");
        assert!(result.skipped.is_empty());
    }

    #[test]
    fn test_retry_failed_tables() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![computed_table(
                "needs_setup",
                "SELECT v FROM setup_table",
            )])
            .unwrap();

        pipeline
            .register(vec![computed_table(
                "downstream",
                "SELECT v * 2 AS v FROM needs_setup",
            )])
            .unwrap();

        let first_result = pipeline.run_sync(executor.clone(), None).unwrap();
        assert!(!first_result.all_succeeded());
        assert_eq!(first_result.failed.len(), 1);
        assert_eq!(first_result.failed[0].table, "needs_setup");
        assert_eq!(first_result.skipped.len(), 1);
        assert_eq!(first_result.skipped[0], "downstream");

        executor
            .execute_statement_sync("CREATE TABLE setup_table (v INT64)")
            .unwrap();
        executor
            .execute_statement_sync("INSERT INTO setup_table VALUES (100)")
            .unwrap();

        let retry_result = pipeline
            .retry_failed_sync(executor.clone(), &first_result)
            .unwrap();

        assert!(retry_result.all_succeeded());
        assert_eq!(retry_result.succeeded.len(), 2);
        assert!(retry_result.succeeded.contains(&"needs_setup".to_string()));
        assert!(retry_result.succeeded.contains(&"downstream".to_string()));
        assert!(retry_result.failed.is_empty());
        assert!(retry_result.skipped.is_empty());

        let query = executor
            .execute_query_sync("SELECT * FROM downstream")
            .unwrap();
        assert_eq!(query.rows[0][0], json!(200));
    }

    #[test]
    fn test_retry_preserves_successful_tables() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        executor
            .execute_statement_sync("CREATE TABLE source_a (v INT64)")
            .unwrap();
        executor
            .execute_statement_sync("INSERT INTO source_a VALUES (10)")
            .unwrap();

        pipeline
            .register(vec![
                computed_table("from_a", "SELECT v FROM source_a"),
                computed_table("from_b", "SELECT v FROM source_b"),
            ])
            .unwrap();

        let first_result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert_eq!(first_result.succeeded.len(), 1);
        assert!(first_result.succeeded.contains(&"from_a".to_string()));
        assert_eq!(first_result.failed.len(), 1);
        assert_eq!(first_result.failed[0].table, "from_b");

        executor
            .execute_statement_sync("CREATE TABLE source_b (v INT64)")
            .unwrap();
        executor
            .execute_statement_sync("INSERT INTO source_b VALUES (20)")
            .unwrap();

        let retry_result = pipeline
            .retry_failed_sync(executor.clone(), &first_result)
            .unwrap();

        assert!(retry_result.all_succeeded());
        assert_eq!(retry_result.succeeded.len(), 1);
        assert!(retry_result.succeeded.contains(&"from_b".to_string()));
    }

    #[test]
    fn test_diamond_dependency_with_one_branch_failing() {
        let mut pipeline = Pipeline::new();
        let executor = create_mock_executor();

        pipeline
            .register(vec![source_table(
                "root",
                vec![("v", "INT64")],
                vec![json!([1])],
            )])
            .unwrap();

        executor
            .execute_statement_sync("CREATE TABLE external_good (v INT64)")
            .unwrap();
        executor
            .execute_statement_sync("INSERT INTO external_good VALUES (5)")
            .unwrap();

        pipeline
            .register(vec![
                computed_table("left_good", "SELECT v FROM external_good"),
                computed_table("right_bad", "SELECT v FROM nonexistent"),
            ])
            .unwrap();

        pipeline
            .register(vec![computed_table(
                "merged",
                "SELECT l.v + r.v AS v FROM left_good l, right_bad r",
            )])
            .unwrap();

        let result = pipeline.run_sync(executor.clone(), None).unwrap();

        assert_eq!(result.succeeded.len(), 2);
        assert!(result.succeeded.contains(&"root".to_string()));
        assert!(result.succeeded.contains(&"left_good".to_string()));
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].table, "right_bad");
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped.contains(&"merged".to_string()));
    }

    #[test]
    fn test_cte_not_detected_as_dependency() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![source_table(
                "real_table",
                vec![("v", "INT64")],
                vec![],
            )])
            .unwrap();

        pipeline
            .register(vec![source_table(
                "cte_alias",
                vec![("v", "INT64")],
                vec![],
            )])
            .unwrap();

        let result = pipeline
            .register(vec![computed_table(
                "derived",
                "WITH cte_alias AS (SELECT v FROM real_table) SELECT * FROM cte_alias",
            )])
            .unwrap();

        assert_eq!(result[0].dependencies, vec!["real_table"]);
        assert!(!result[0].dependencies.contains(&"cte_alias".to_string()));
    }

    #[test]
    fn test_multiple_ctes_not_detected_as_dependencies() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![source_table("base", vec![("v", "INT64")], vec![])])
            .unwrap();

        pipeline
            .register(vec![
                source_table("first", vec![("v", "INT64")], vec![]),
                source_table("second", vec![("v", "INT64")], vec![]),
            ])
            .unwrap();

        let result = pipeline
            .register(vec![computed_table(
                "derived",
                "WITH first AS (SELECT 1 AS v), second AS (SELECT 2 AS v) SELECT * FROM first, second, base",
            )])
            .unwrap();

        assert_eq!(result[0].dependencies, vec!["base"]);
    }

    #[test]
    fn test_table_name_prefix_not_matched() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![source_table("user", vec![("id", "INT64")], vec![])])
            .unwrap();

        pipeline
            .register(vec![source_table("users", vec![("id", "INT64")], vec![])])
            .unwrap();

        let result = pipeline
            .register(vec![computed_table("derived", "SELECT * FROM users")])
            .unwrap();

        assert_eq!(result[0].dependencies, vec!["users"]);
        assert!(!result[0].dependencies.contains(&"user".to_string()));
    }

    #[test]
    fn test_cte_with_recursive() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![source_table("nums", vec![("n", "INT64")], vec![])])
            .unwrap();

        pipeline
            .register(vec![source_table("seq", vec![("n", "INT64")], vec![])])
            .unwrap();

        let result = pipeline
            .register(vec![computed_table(
                "derived",
                "WITH RECURSIVE seq AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM seq WHERE n < 10) SELECT * FROM seq, nums",
            )])
            .unwrap();

        assert_eq!(result[0].dependencies, vec!["nums"]);
        assert!(!result[0].dependencies.contains(&"seq".to_string()));
    }

    #[test]
    fn test_subquery_alias_not_detected() {
        let mut pipeline = Pipeline::new();

        pipeline
            .register(vec![source_table(
                "real_table",
                vec![("v", "INT64")],
                vec![],
            )])
            .unwrap();

        let result = pipeline
            .register(vec![computed_table(
                "derived",
                "SELECT * FROM (SELECT v FROM real_table) AS sub",
            )])
            .unwrap();

        assert_eq!(result[0].dependencies, vec!["real_table"]);
    }

    #[test]
    fn test_extract_cte_names_function() {
        let ctes =
            extract_cte_names("WITH foo AS (SELECT 1), bar AS (SELECT 2) SELECT * FROM foo, bar");
        assert!(ctes.contains("FOO"));
        assert!(ctes.contains("BAR"));
        assert_eq!(ctes.len(), 2);
    }

    #[test]
    fn test_extract_cte_names_nested_parens() {
        let ctes = extract_cte_names(
            "WITH complex AS (SELECT COUNT(*) FROM (SELECT 1)) SELECT * FROM complex",
        );
        assert!(ctes.contains("COMPLEX"));
        assert_eq!(ctes.len(), 1);
    }

    #[test]
    fn test_no_cte() {
        let ctes = extract_cte_names("SELECT * FROM users");
        assert!(ctes.is_empty());
    }
}
