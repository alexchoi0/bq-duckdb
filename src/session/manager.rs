use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uuid::Uuid;

use crate::dag::TableRegistry;
use crate::error::{Error, Result};
use crate::executor::QueryExecutor;

pub struct SessionManager {
    sessions: RwLock<HashMap<Uuid, SessionMeta>>,
    executor: Arc<dyn QueryExecutor>,
}

struct SessionMeta {
    schema_name: String,
    table_registry: TableRegistry,
}

impl SessionManager {
    pub fn new(executor: Arc<dyn QueryExecutor>) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            executor,
        }
    }

    pub fn create_session(&self) -> Result<Uuid> {
        let session_id = Uuid::new_v4();
        let schema_name = format!("s_{}", session_id.simple());

        self.executor.create_schema(&schema_name)?;

        let meta = SessionMeta {
            schema_name,
            table_registry: TableRegistry::new(),
        };

        self.sessions.write().insert(session_id, meta);

        Ok(session_id)
    }

    pub fn destroy_session(&self, session_id: Uuid) -> Result<()> {
        let meta = self
            .sessions
            .write()
            .remove(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;

        self.executor.drop_schema(&meta.schema_name)?;

        Ok(())
    }

    pub fn get_schema_name(&self, session_id: Uuid) -> Result<String> {
        self.sessions
            .read()
            .get(&session_id)
            .map(|m| m.schema_name.clone())
            .ok_or(Error::SessionNotFound(session_id))
    }

    pub fn executor(&self) -> &dyn QueryExecutor {
        &*self.executor
    }

    pub fn arc_executor(&self) -> Arc<dyn QueryExecutor> {
        Arc::clone(&self.executor)
    }

    pub fn with_registry<F, R>(&self, session_id: Uuid, f: F) -> Result<R>
    where
        F: FnOnce(&TableRegistry) -> R,
    {
        let sessions = self.sessions.read();
        let meta = sessions
            .get(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        Ok(f(&meta.table_registry))
    }

    pub fn with_registry_mut<F, R>(&self, session_id: Uuid, f: F) -> Result<R>
    where
        F: FnOnce(&mut TableRegistry) -> R,
    {
        let mut sessions = self.sessions.write();
        let meta = sessions
            .get_mut(&session_id)
            .ok_or(Error::SessionNotFound(session_id))?;
        Ok(f(&mut meta.table_registry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::DuckDbExecutor;

    fn create_manager() -> SessionManager {
        let executor: Arc<dyn QueryExecutor> = Arc::new(DuckDbExecutor::new().unwrap());
        SessionManager::new(executor)
    }

    #[test]
    fn test_create_session() {
        let manager = create_manager();
        let session_id = manager.create_session().unwrap();

        assert!(manager.get_schema_name(session_id).is_ok());
    }

    #[test]
    fn test_create_multiple_sessions() {
        let manager = create_manager();

        let s1 = manager.create_session().unwrap();
        let s2 = manager.create_session().unwrap();
        let s3 = manager.create_session().unwrap();

        assert_ne!(s1, s2);
        assert_ne!(s2, s3);
        assert!(manager.get_schema_name(s3).is_ok());
    }

    #[test]
    fn test_destroy_session() {
        let manager = create_manager();
        let session_id = manager.create_session().unwrap();

        assert!(manager.get_schema_name(session_id).is_ok());
        manager.destroy_session(session_id).unwrap();
        assert!(manager.get_schema_name(session_id).is_err());
    }

    #[test]
    fn test_destroy_nonexistent_session() {
        let manager = create_manager();
        let fake_id = Uuid::new_v4();

        let result = manager.destroy_session(fake_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_schema_name() {
        let manager = create_manager();
        let session_id = manager.create_session().unwrap();

        let schema_name = manager.get_schema_name(session_id).unwrap();
        assert!(schema_name.starts_with("s_"));
        assert!(schema_name.contains(&session_id.simple().to_string()));
    }

    #[test]
    fn test_get_schema_name_nonexistent() {
        let manager = create_manager();
        let fake_id = Uuid::new_v4();

        let result = manager.get_schema_name(fake_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_session_isolation() {
        let manager = create_manager();

        let s1 = manager.create_session().unwrap();
        let s2 = manager.create_session().unwrap();

        let schema1 = manager.get_schema_name(s1).unwrap();
        let schema2 = manager.get_schema_name(s2).unwrap();

        assert_ne!(schema1, schema2);
    }

    #[test]
    fn test_execute_query_in_session() {
        let manager = create_manager();
        let session_id = manager.create_session().unwrap();
        let schema_name = manager.get_schema_name(session_id).unwrap();

        let result = manager
            .executor()
            .execute_statement(&schema_name, "CREATE TABLE test_table (id INTEGER, name VARCHAR)")
            .unwrap();

        assert_eq!(result, 0);

        let query_result = manager
            .executor()
            .execute_query(&schema_name, "SELECT * FROM test_table")
            .unwrap();

        assert_eq!(query_result.columns, vec!["id", "name"]);
        assert!(query_result.rows.is_empty());
    }

    #[test]
    fn test_session_data_isolation() {
        let manager = create_manager();

        let s1 = manager.create_session().unwrap();
        let s2 = manager.create_session().unwrap();

        let schema1 = manager.get_schema_name(s1).unwrap();
        let schema2 = manager.get_schema_name(s2).unwrap();

        manager
            .executor()
            .execute_statement(&schema1, "CREATE TABLE users (id INTEGER)")
            .unwrap();

        manager
            .executor()
            .execute_statement(&schema1, "INSERT INTO users VALUES (1)")
            .unwrap();

        manager
            .executor()
            .execute_statement(&schema2, "CREATE TABLE users (id INTEGER)")
            .unwrap();

        manager
            .executor()
            .execute_statement(&schema2, "INSERT INTO users VALUES (2), (3)")
            .unwrap();

        let result1 = manager
            .executor()
            .execute_query(&schema1, "SELECT COUNT(*) FROM users")
            .unwrap();

        let result2 = manager
            .executor()
            .execute_query(&schema2, "SELECT COUNT(*) FROM users")
            .unwrap();

        assert_eq!(result1.rows.len(), 1);
        assert_eq!(result2.rows.len(), 1);

        let count1: i64 = result1.rows[0][0].as_i64().unwrap();
        let count2: i64 = result2.rows[0][0].as_i64().unwrap();

        assert_eq!(count1, 1);
        assert_eq!(count2, 2);
    }
}
