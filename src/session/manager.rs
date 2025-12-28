use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::executor::{BigQueryExecutor, Executor, ExecutorMode, QueryResult, YachtSqlExecutor};
use crate::rpc::types::{DagTableDef, DagTableDetail, DagTableInfo};

use super::pipeline::{Pipeline, PipelineResult, TableError};

pub struct SessionManager {
    sessions: RwLock<HashMap<Uuid, Session>>,
    mode: ExecutorMode,
}

struct Session {
    executor: Arc<Executor>,
    pipeline: Pipeline,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            mode: ExecutorMode::Mock,
        }
    }

    pub fn with_mode(mode: ExecutorMode) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            mode,
        }
    }

    pub async fn create_session(&self) -> Result<Uuid> {
        let session_id = Uuid::new_v4();
        let executor = match self.mode {
            ExecutorMode::Mock => Executor::Mock(YachtSqlExecutor::new()),
            ExecutorMode::BigQuery => Executor::BigQuery(BigQueryExecutor::new().await?),
        };
        let pipeline = Pipeline::new();

        let session = Session {
            executor: Arc::new(executor),
            pipeline,
        };

        self.sessions.write().insert(session_id, session);

        Ok(session_id)
    }

    pub fn destroy_session(&self, session_id: Uuid) -> Result<()> {
        self.sessions
            .write()
            .remove(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;

        Ok(())
    }

    pub async fn execute_query(&self, session_id: Uuid, sql: &str) -> Result<QueryResult> {
        let executor = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            Arc::clone(&session.executor)
        };
        executor.execute_query(sql).await
    }

    pub async fn execute_statement(&self, session_id: Uuid, sql: &str) -> Result<u64> {
        let executor = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            Arc::clone(&session.executor)
        };
        executor.execute_statement(sql).await
    }

    pub fn register_dag(
        &self,
        session_id: Uuid,
        tables: Vec<DagTableDef>,
    ) -> Result<Vec<DagTableInfo>> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.pipeline.register(tables)
    }

    pub fn run_dag(
        &self,
        session_id: Uuid,
        targets: Option<Vec<String>>,
        retry_count: u32,
    ) -> Result<PipelineResult> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;

        let mut result = session
            .pipeline
            .run(Arc::clone(&session.executor), targets)?;

        for _ in 0..retry_count {
            if result.all_succeeded() {
                break;
            }

            let retry_result = session
                .pipeline
                .retry_failed(Arc::clone(&session.executor), &result)?;

            result.succeeded.extend(retry_result.succeeded);
            result.failed = retry_result.failed;
            result.skipped = retry_result.skipped;
        }

        Ok(result)
    }

    pub fn retry_dag(
        &self,
        session_id: Uuid,
        failed_tables: Vec<String>,
        skipped_tables: Vec<String>,
    ) -> Result<PipelineResult> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;

        let previous_result = PipelineResult {
            succeeded: vec![],
            failed: failed_tables
                .into_iter()
                .map(|t| TableError {
                    table: t,
                    error: String::new(),
                })
                .collect(),
            skipped: skipped_tables,
        };

        session
            .pipeline
            .retry_failed(Arc::clone(&session.executor), &previous_result)
    }

    pub fn get_dag(&self, session_id: Uuid) -> Result<Vec<DagTableDetail>> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        Ok(session.pipeline.get_tables())
    }

    pub fn clear_dag(&self, session_id: Uuid) -> Result<()> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.pipeline.clear(&session.executor);
        Ok(())
    }

    pub async fn load_parquet(
        &self,
        session_id: Uuid,
        table_name: &str,
        path: &str,
        schema: &[crate::rpc::types::ColumnDef],
    ) -> Result<u64> {
        let executor = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            Arc::clone(&session.executor)
        };
        executor.load_parquet(table_name, path, schema).await
    }

    pub async fn list_tables(&self, session_id: Uuid) -> Result<Vec<(String, u64)>> {
        let executor = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            Arc::clone(&session.executor)
        };
        executor.list_tables().await
    }

    pub async fn describe_table(
        &self,
        session_id: Uuid,
        table_name: &str,
    ) -> Result<(Vec<(String, String)>, u64)> {
        let executor = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            Arc::clone(&session.executor)
        };
        executor.describe_table(table_name).await
    }

    pub fn set_default_project(&self, session_id: Uuid, project: Option<String>) -> Result<()> {
        let executor = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            Arc::clone(&session.executor)
        };
        executor.set_default_project(project)
    }

    pub fn get_default_project(&self, session_id: Uuid) -> Result<Option<String>> {
        let executor = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            Arc::clone(&session.executor)
        };
        executor.get_default_project()
    }

    pub fn get_projects(&self, session_id: Uuid) -> Result<Vec<String>> {
        let executor = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            Arc::clone(&session.executor)
        };
        executor.get_projects()
    }

    pub fn get_datasets(&self, session_id: Uuid, project: &str) -> Result<Vec<String>> {
        let executor = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            Arc::clone(&session.executor)
        };
        executor.get_datasets(project)
    }

    pub fn get_tables_in_dataset(
        &self,
        session_id: Uuid,
        project: &str,
        dataset: &str,
    ) -> Result<Vec<String>> {
        let executor = {
            let sessions = self.sessions.read();
            let session = sessions
                .get(&session_id)
                .ok_or(Error::SessionNotFound(session_id))?;
            Arc::clone(&session.executor)
        };
        executor.get_tables_in_dataset(project, dataset)
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn run_blocking_test<F, T>(f: F) -> Result<T>
    where
        F: FnOnce() -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(f)
            .await
            .map_err(|e| Error::Internal(format!("Task join error: {e}")))?
    }

    #[tokio::test]
    async fn test_create_session() {
        let manager = SessionManager::new();
        let session_id = manager.create_session().await.unwrap();

        let result = manager.execute_query(session_id, "SELECT 1 AS x").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_multiple_sessions() {
        let manager = SessionManager::new();

        let s1 = manager.create_session().await.unwrap();
        let s2 = manager.create_session().await.unwrap();
        let s3 = manager.create_session().await.unwrap();

        assert_ne!(s1, s2);
        assert_ne!(s2, s3);
    }

    #[tokio::test]
    async fn test_destroy_session() {
        let manager = SessionManager::new();
        let session_id = manager.create_session().await.unwrap();

        assert!(manager.execute_query(session_id, "SELECT 1").await.is_ok());
        manager.destroy_session(session_id).unwrap();
        assert!(manager.execute_query(session_id, "SELECT 1").await.is_err());
    }

    #[tokio::test]
    async fn test_destroy_nonexistent_session() {
        let manager = SessionManager::new();
        let fake_id = Uuid::new_v4();

        let result = manager.destroy_session(fake_id);
        assert!(result.is_err());
    }

    #[tokio::test]
    #[ignore = "requires COUNT aggregate which is not yet implemented in concurrent executor"]
    async fn test_session_isolation() {
        let manager = SessionManager::new();

        let s1 = manager.create_session().await.unwrap();
        let s2 = manager.create_session().await.unwrap();

        manager
            .execute_statement(s1, "CREATE TABLE users (id INT64, name STRING)")
            .await
            .unwrap();

        manager
            .execute_statement(s1, "INSERT INTO users VALUES (1, 'Alice')")
            .await
            .unwrap();

        manager
            .execute_statement(s2, "CREATE TABLE users (id INT64, name STRING)")
            .await
            .unwrap();

        manager
            .execute_statement(s2, "INSERT INTO users VALUES (2, 'Bob'), (3, 'Charlie')")
            .await
            .unwrap();

        let result1 = manager
            .execute_query(s1, "SELECT COUNT(*) FROM users")
            .await
            .unwrap();

        let result2 = manager
            .execute_query(s2, "SELECT COUNT(*) FROM users")
            .await
            .unwrap();

        assert_eq!(result1.rows.len(), 1);
        assert_eq!(result2.rows.len(), 1);

        let count1: i64 = result1.rows[0][0].as_i64().unwrap();
        let count2: i64 = result2.rows[0][0].as_i64().unwrap();

        assert_eq!(count1, 1);
        assert_eq!(count2, 2);
    }

    #[tokio::test]
    async fn test_load_parquet() {
        use arrow::array::{
            ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
            TimestampMicrosecondArray,
        };
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        use tempfile::NamedTempFile;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("created_date", DataType::Date32, true),
            Field::new(
                "updated_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let name_array: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None]));
        let score_array: ArrayRef =
            Arc::new(Float64Array::from(vec![Some(95.5), Some(87.3), Some(92.1)]));
        let active_array: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
        ]));
        let date_array: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(19000),
            Some(19001),
            Some(19002),
        ]));
        let timestamp_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1640000000000000i64),
            Some(1640000001000000i64),
            Some(1640000002000000i64),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                id_array,
                name_array,
                score_array,
                active_array,
                date_array,
                timestamp_array,
            ],
        )
        .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        {
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let manager = SessionManager::new();
        let session_id = manager.create_session().await.unwrap();

        let bq_schema = vec![
            crate::rpc::types::ColumnDef {
                name: "id".to_string(),
                column_type: "INT64".to_string(),
            },
            crate::rpc::types::ColumnDef {
                name: "name".to_string(),
                column_type: "STRING".to_string(),
            },
            crate::rpc::types::ColumnDef {
                name: "score".to_string(),
                column_type: "FLOAT64".to_string(),
            },
            crate::rpc::types::ColumnDef {
                name: "active".to_string(),
                column_type: "BOOL".to_string(),
            },
            crate::rpc::types::ColumnDef {
                name: "created_date".to_string(),
                column_type: "DATE".to_string(),
            },
            crate::rpc::types::ColumnDef {
                name: "updated_at".to_string(),
                column_type: "TIMESTAMP".to_string(),
            },
        ];

        let result = manager
            .load_parquet(session_id, "test_data", &path, &bq_schema)
            .await;
        println!("Load result: {:?}", result);
        assert!(result.is_ok(), "Failed to load parquet: {:?}", result.err());
        let row_count = result.unwrap();
        println!("Loaded {} rows", row_count);

        let query_result = manager
            .execute_query(session_id, "SELECT * FROM test_data ORDER BY id")
            .await;

        println!("Query result: {:?}", query_result);
        let query_result = query_result.unwrap();

        assert_eq!(query_result.rows.len(), 3);
        assert_eq!(query_result.columns.len(), 6);

        assert_eq!(query_result.rows[0][0].as_i64().unwrap(), 1);
        assert_eq!(query_result.rows[0][1].as_str().unwrap(), "Alice");
        assert_eq!(query_result.rows[0][2].as_f64().unwrap(), 95.5);
        assert_eq!(query_result.rows[0][3].as_bool().unwrap(), true);

        assert_eq!(query_result.rows[1][0].as_i64().unwrap(), 2);
        assert_eq!(query_result.rows[1][1].as_str().unwrap(), "Bob");

        assert_eq!(query_result.rows[2][0].as_i64().unwrap(), 3);
        assert!(query_result.rows[2][1].is_null());

        println!("Columns: {:?}", query_result.columns);
        for (i, row) in query_result.rows.iter().enumerate() {
            println!("Row {}: {:?}", i, row);
        }
    }

    #[tokio::test]
    #[ignore = "requires SUM/COUNT aggregates which are not yet implemented in concurrent executor"]
    async fn test_multiple_sessions_run_dags_in_parallel() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;

        let manager = Arc::new(SessionManager::new());
        let num_sessions = 4;

        let mut session_ids = Vec::new();
        for _ in 0..num_sessions {
            let session_id = manager.create_session().await.unwrap();

            manager
                .execute_statement(session_id, "CREATE TABLE base (v INT64)")
                .await
                .unwrap();
            for i in 0..100 {
                manager
                    .execute_statement(session_id, &format!("INSERT INTO base VALUES ({})", i))
                    .await
                    .unwrap();
            }

            let tables = vec![
                crate::rpc::types::DagTableDef {
                    name: "sum_table".to_string(),
                    sql: Some("SELECT SUM(v) AS total FROM base".to_string()),
                    schema: None,
                    rows: vec![],
                },
                crate::rpc::types::DagTableDef {
                    name: "count_table".to_string(),
                    sql: Some("SELECT COUNT(*) AS cnt FROM base".to_string()),
                    schema: None,
                    rows: vec![],
                },
            ];
            manager.register_dag(session_id, tables).unwrap();

            session_ids.push(session_id);
        }

        static CONCURRENT_EXECUTIONS: AtomicUsize = AtomicUsize::new(0);
        static MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);

        CONCURRENT_EXECUTIONS.store(0, Ordering::SeqCst);
        MAX_CONCURRENT.store(0, Ordering::SeqCst);

        let start = Instant::now();

        let handles: Vec<_> = session_ids
            .into_iter()
            .map(|session_id| {
                let manager = Arc::clone(&manager);
                std::thread::spawn(move || {
                    let prev = CONCURRENT_EXECUTIONS.fetch_add(1, Ordering::SeqCst);
                    let current = prev + 1;

                    loop {
                        let max = MAX_CONCURRENT.load(Ordering::SeqCst);
                        if current <= max {
                            break;
                        }
                        if MAX_CONCURRENT
                            .compare_exchange(max, current, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            break;
                        }
                    }

                    let result = manager.run_dag(session_id, None, 0);

                    CONCURRENT_EXECUTIONS.fetch_sub(1, Ordering::SeqCst);

                    (session_id, result)
                })
            })
            .collect();

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.join().unwrap());
        }

        let elapsed = start.elapsed();

        for (session_id, result) in &results {
            let dag_result = result.as_ref().expect("DAG should execute successfully");
            assert_eq!(
                dag_result.succeeded.len(),
                2,
                "Each session should execute 2 tables"
            );

            let sum_result = manager
                .execute_query(*session_id, "SELECT * FROM sum_table")
                .await
                .unwrap();
            assert_eq!(sum_result.rows[0][0].as_i64().unwrap(), 4950);

            let count_result = manager
                .execute_query(*session_id, "SELECT * FROM count_table")
                .await
                .unwrap();
            assert_eq!(count_result.rows[0][0].as_i64().unwrap(), 100);
        }

        let max_concurrent = MAX_CONCURRENT.load(Ordering::SeqCst);
        assert!(
            max_concurrent >= 2,
            "Expected at least 2 sessions to run concurrently, but max was {}",
            max_concurrent
        );

        println!(
            "Parallel session test: {} sessions, max {} concurrent, took {:?}",
            num_sessions, max_concurrent, elapsed
        );
    }

    #[tokio::test]
    #[ignore = "requires runtime context in spawned threads - needs refactoring for async executor"]
    async fn test_sessions_are_isolated_during_parallel_dag_execution() {
        let manager = Arc::new(SessionManager::new());

        let s1 = manager.create_session().await.unwrap();
        let s2 = manager.create_session().await.unwrap();

        manager
            .execute_statement(s1, "CREATE TABLE data (v INT64)")
            .await
            .unwrap();
        manager
            .execute_statement(s1, "INSERT INTO data VALUES (100)")
            .await
            .unwrap();

        manager
            .execute_statement(s2, "CREATE TABLE data (v INT64)")
            .await
            .unwrap();
        manager
            .execute_statement(s2, "INSERT INTO data VALUES (200)")
            .await
            .unwrap();

        manager
            .register_dag(
                s1,
                vec![crate::rpc::types::DagTableDef {
                    name: "result".to_string(),
                    sql: Some("SELECT v * 2 AS doubled FROM data".to_string()),
                    schema: None,
                    rows: vec![],
                }],
            )
            .unwrap();

        manager
            .register_dag(
                s2,
                vec![crate::rpc::types::DagTableDef {
                    name: "result".to_string(),
                    sql: Some("SELECT v * 3 AS tripled FROM data".to_string()),
                    schema: None,
                    rows: vec![],
                }],
            )
            .unwrap();

        let manager1 = Arc::clone(&manager);
        let manager2 = Arc::clone(&manager);

        let handle1 = std::thread::spawn(move || manager1.run_dag(s1, None, 0));
        let handle2 = std::thread::spawn(move || manager2.run_dag(s2, None, 0));

        let result1 = handle1.join().unwrap().unwrap();
        let result2 = handle2.join().unwrap().unwrap();

        assert_eq!(result1.succeeded, vec!["result"]);
        assert_eq!(result2.succeeded, vec!["result"]);

        let query1 = manager
            .execute_query(s1, "SELECT * FROM result")
            .await
            .unwrap();
        let query2 = manager
            .execute_query(s2, "SELECT * FROM result")
            .await
            .unwrap();

        assert_eq!(query1.rows[0][0].as_i64().unwrap(), 200);
        assert_eq!(query2.rows[0][0].as_i64().unwrap(), 600);
    }

    #[tokio::test]
    #[ignore = "requires runtime context in spawned threads - needs refactoring for async executor"]
    async fn test_many_sessions_parallel_with_complex_dags() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let manager = Arc::new(SessionManager::new());
        let num_sessions = 8;

        static COMPLETED_SESSIONS: AtomicUsize = AtomicUsize::new(0);
        COMPLETED_SESSIONS.store(0, Ordering::SeqCst);

        let mut session_ids = Vec::new();
        for i in 0..num_sessions {
            let session_id = manager.create_session().await.unwrap();

            manager
                .execute_statement(session_id, "CREATE TABLE source (n INT64)")
                .await
                .unwrap();
            manager
                .execute_statement(
                    session_id,
                    &format!("INSERT INTO source VALUES ({})", i + 1),
                )
                .await
                .unwrap();

            let tables = vec![
                crate::rpc::types::DagTableDef {
                    name: "step1".to_string(),
                    sql: Some("SELECT n * 2 AS n FROM source".to_string()),
                    schema: None,
                    rows: vec![],
                },
                crate::rpc::types::DagTableDef {
                    name: "step2".to_string(),
                    sql: Some("SELECT n + 10 AS n FROM step1".to_string()),
                    schema: None,
                    rows: vec![],
                },
                crate::rpc::types::DagTableDef {
                    name: "step3".to_string(),
                    sql: Some("SELECT n * n AS n FROM step2".to_string()),
                    schema: None,
                    rows: vec![],
                },
            ];
            manager.register_dag(session_id, tables).unwrap();

            session_ids.push((session_id, i + 1));
        }

        let handles: Vec<_> = session_ids
            .into_iter()
            .map(|(session_id, base_value)| {
                let manager = Arc::clone(&manager);
                std::thread::spawn(move || {
                    let result = manager.run_dag(session_id, None, 0);
                    COMPLETED_SESSIONS.fetch_add(1, Ordering::SeqCst);
                    (session_id, base_value, result)
                })
            })
            .collect();

        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.join().unwrap());
        }

        assert_eq!(
            COMPLETED_SESSIONS.load(Ordering::SeqCst),
            num_sessions,
            "All sessions should complete"
        );

        for (session_id, base_value, result) in results {
            let dag_result = result.unwrap();
            assert_eq!(dag_result.succeeded.len(), 3);

            assert_eq!(dag_result.succeeded[0], "step1");
            assert_eq!(dag_result.succeeded[1], "step2");
            assert_eq!(dag_result.succeeded[2], "step3");

            let final_result = manager
                .execute_query(session_id, "SELECT * FROM step3")
                .await
                .unwrap();

            let expected = {
                let step1 = base_value * 2;
                let step2 = step1 + 10;
                let step3 = step2 * step2;
                step3
            };

            assert_eq!(
                final_result.rows[0][0].as_i64().unwrap(),
                expected as i64,
                "Session with base {} should have final result {}",
                base_value,
                expected
            );
        }
    }

    #[tokio::test]
    async fn test_dag_execution_order_within_session_is_serial() {
        let manager = Arc::new(SessionManager::new());

        let session_id = manager.create_session().await.unwrap();

        manager
            .execute_statement(session_id, "CREATE TABLE root (v INT64)")
            .await
            .unwrap();
        manager
            .execute_statement(session_id, "INSERT INTO root VALUES (1)")
            .await
            .unwrap();

        let tables: Vec<crate::rpc::types::DagTableDef> = (0..5)
            .map(|i| crate::rpc::types::DagTableDef {
                name: format!("branch_{}", i),
                sql: Some(format!("SELECT v + {} AS v FROM root", i)),
                schema: None,
                rows: vec![],
            })
            .collect();

        let infos = manager.register_dag(session_id, tables).unwrap();

        for info in &infos {
            assert!(
                info.dependencies.is_empty() || info.dependencies == vec!["root"],
                "All branches should only depend on root (external table)"
            );
        }

        let m = Arc::clone(&manager);
        let dag_result = run_blocking_test(move || m.run_dag(session_id, None, 0))
            .await
            .unwrap();

        assert_eq!(dag_result.succeeded.len(), 5);

        let mut sorted_executed = dag_result.succeeded.clone();
        sorted_executed.sort();
        assert_eq!(
            dag_result.succeeded, sorted_executed,
            "In mock mode, tables at same level should execute in alphabetical order"
        );

        for i in 0..5 {
            let result = manager
                .execute_query(session_id, &format!("SELECT * FROM branch_{}", i))
                .await
                .unwrap();
            assert_eq!(result.rows[0][0].as_i64().unwrap(), 1 + i as i64);
        }
    }
}
