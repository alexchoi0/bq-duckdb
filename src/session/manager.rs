use std::collections::HashMap;

use parking_lot::RwLock;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::executor::{BigQueryExecutor, Executor, ExecutorMode, QueryResult, YachtSqlExecutor};
use crate::rpc::types::{DagTableDef, DagTableDetail, DagTableInfo};

use super::dag::Dag;

pub struct SessionManager {
    sessions: RwLock<HashMap<Uuid, Session>>,
    mode: ExecutorMode,
}

struct Session {
    executor: Executor,
    dag: Dag,
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
            ExecutorMode::Mock => Executor::Mock(YachtSqlExecutor::new()?),
            ExecutorMode::BigQuery => Executor::BigQuery(BigQueryExecutor::new().await?),
        };
        let dag = Dag::new();

        let session = Session { executor, dag };

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

    pub fn execute_query(&self, session_id: Uuid, sql: &str) -> Result<QueryResult> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.executor.execute_query(sql)
    }

    pub fn execute_statement(&self, session_id: Uuid, sql: &str) -> Result<u64> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.executor.execute_statement(sql)
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
        session.dag.register(tables)
    }

    pub fn run_dag(
        &self,
        session_id: Uuid,
        targets: Option<Vec<String>>,
    ) -> Result<Vec<String>> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.dag.run(&session.executor, targets)
    }

    pub fn get_dag(&self, session_id: Uuid) -> Result<Vec<DagTableDetail>> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        Ok(session.dag.get_tables())
    }

    pub fn clear_dag(&self, session_id: Uuid) -> Result<()> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.dag.clear(&session.executor);
        Ok(())
    }

    pub fn load_parquet(
        &self,
        session_id: Uuid,
        table_name: &str,
        path: &str,
        schema: &[crate::rpc::types::ColumnDef],
    ) -> Result<u64> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        session.executor.load_parquet(table_name, path, schema)
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

    #[tokio::test]
    async fn test_create_session() {
        let manager = SessionManager::new();
        let session_id = manager.create_session().await.unwrap();

        let result = manager.execute_query(session_id, "SELECT 1 AS x");
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

        assert!(manager.execute_query(session_id, "SELECT 1").is_ok());
        manager.destroy_session(session_id).unwrap();
        assert!(manager.execute_query(session_id, "SELECT 1").is_err());
    }

    #[tokio::test]
    async fn test_destroy_nonexistent_session() {
        let manager = SessionManager::new();
        let fake_id = Uuid::new_v4();

        let result = manager.destroy_session(fake_id);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_session_isolation() {
        let manager = SessionManager::new();

        let s1 = manager.create_session().await.unwrap();
        let s2 = manager.create_session().await.unwrap();

        manager
            .execute_statement(s1, "CREATE TABLE users (id INT64, name STRING)")
            .unwrap();

        manager
            .execute_statement(s1, "INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();

        manager
            .execute_statement(s2, "CREATE TABLE users (id INT64, name STRING)")
            .unwrap();

        manager
            .execute_statement(s2, "INSERT INTO users VALUES (2, 'Bob'), (3, 'Charlie')")
            .unwrap();

        let result1 = manager
            .execute_query(s1, "SELECT COUNT(*) FROM users")
            .unwrap();

        let result2 = manager
            .execute_query(s2, "SELECT COUNT(*) FROM users")
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
            ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
            Date32Array, TimestampMicrosecondArray,
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
            Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]));

        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let name_array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Alice"),
            Some("Bob"),
            None,
        ]));
        let score_array: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(95.5),
            Some(87.3),
            Some(92.1),
        ]));
        let active_array: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
        ]));
        let date_array: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(19000), // days since epoch
            Some(19001),
            Some(19002),
        ]));
        let timestamp_array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1640000000000000i64), // microseconds since epoch
            Some(1640000001000000i64),
            Some(1640000002000000i64),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![id_array, name_array, score_array, active_array, date_array, timestamp_array],
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
            crate::rpc::types::ColumnDef { name: "id".to_string(), column_type: "INT64".to_string() },
            crate::rpc::types::ColumnDef { name: "name".to_string(), column_type: "STRING".to_string() },
            crate::rpc::types::ColumnDef { name: "score".to_string(), column_type: "FLOAT64".to_string() },
            crate::rpc::types::ColumnDef { name: "active".to_string(), column_type: "BOOL".to_string() },
            crate::rpc::types::ColumnDef { name: "created_date".to_string(), column_type: "DATE".to_string() },
            crate::rpc::types::ColumnDef { name: "updated_at".to_string(), column_type: "TIMESTAMP".to_string() },
        ];

        let result = manager.load_parquet(session_id, "test_data", &path, &bq_schema);
        println!("Load result: {:?}", result);
        assert!(result.is_ok(), "Failed to load parquet: {:?}", result.err());
        let row_count = result.unwrap();
        println!("Loaded {} rows", row_count);

        let query_result = manager
            .execute_query(session_id, "SELECT * FROM test_data ORDER BY id");

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
}
