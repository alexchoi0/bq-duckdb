use std::sync::Arc;

use serde_json::{json, Value};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::session::SessionManager;

use super::types::{
    ClearDagParams, ClearDagResult, CreateSessionResult, CreateTableParams, CreateTableResult,
    DestroySessionParams, DestroySessionResult, GetDagParams, GetDagResult, InsertParams,
    InsertResult, LoadParquetParams, LoadParquetResult, PingResult, QueryParams,
    RegisterDagParams, RegisterDagResult, RunDagParams, RunDagResult,
};

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
            "bq.loadParquet" => self.load_parquet(params).await,
            _ => Err(Error::InvalidRequest(format!("Unknown method: {}", method))),
        }
    }

    async fn ping(&self, _params: Value) -> Result<Value> {
        Ok(json!(PingResult { message: "pong".to_string() }))
    }

    async fn create_session(&self, _params: Value) -> Result<Value> {
        let session_id = self.session_manager.create_session().await?;

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
        let sql = p.sql;

        let session_manager = Arc::clone(&self.session_manager);

        let result = tokio::task::spawn_blocking(move || {
            session_manager.execute_query(session_id, &sql)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(result.to_bq_response())
    }

    async fn create_table(&self, params: Value) -> Result<Value> {
        let p: CreateTableParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let columns: Vec<String> = p
            .schema
            .iter()
            .map(|col| format!("{} {}", col.name, col.column_type))
            .collect();

        let sql = format!(
            "CREATE TABLE {} ({})",
            p.table_name,
            columns.join(", ")
        );

        let session_manager = Arc::clone(&self.session_manager);

        tokio::task::spawn_blocking(move || {
            session_manager.execute_statement(session_id, &sql)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(json!(CreateTableResult { success: true }))
    }

    async fn insert(&self, params: Value) -> Result<Value> {
        let p: InsertParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

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

        let row_count = values.len() as u64;

        let sql = format!(
            "INSERT INTO {} VALUES {}",
            p.table_name,
            values.join(", ")
        );

        let session_manager = Arc::clone(&self.session_manager);
        tokio::task::spawn_blocking(move || {
            session_manager.execute_statement(session_id, &sql)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(json!(InsertResult {
            inserted_rows: row_count,
        }))
    }

    async fn register_dag(&self, params: Value) -> Result<Value> {
        let p: RegisterDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        let tables = p.tables;

        let session_manager = Arc::clone(&self.session_manager);

        let table_infos = tokio::task::spawn_blocking(move || {
            session_manager.register_dag(session_id, tables)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(json!(RegisterDagResult {
            success: true,
            tables: table_infos,
        }))
    }

    async fn run_dag(&self, params: Value) -> Result<Value> {
        let p: RunDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        let targets = p.table_names;

        let session_manager = Arc::clone(&self.session_manager);

        let executed = tokio::task::spawn_blocking(move || {
            session_manager.run_dag(session_id, targets)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(json!(RunDagResult {
            success: true,
            executed_tables: executed,
        }))
    }

    async fn get_dag(&self, params: Value) -> Result<Value> {
        let p: GetDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let session_manager = Arc::clone(&self.session_manager);

        let tables = tokio::task::spawn_blocking(move || {
            session_manager.get_dag(session_id)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(json!(GetDagResult { tables }))
    }

    async fn clear_dag(&self, params: Value) -> Result<Value> {
        let p: ClearDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let session_manager = Arc::clone(&self.session_manager);

        tokio::task::spawn_blocking(move || {
            session_manager.clear_dag(session_id)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(json!(ClearDagResult { success: true }))
    }

    async fn load_parquet(&self, params: Value) -> Result<Value> {
        let p: LoadParquetParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        let table_name = p.table_name;
        let path = p.path;
        let schema = p.schema;

        let session_manager = Arc::clone(&self.session_manager);

        let row_count = tokio::task::spawn_blocking(move || {
            session_manager.load_parquet(session_id, &table_name, &path, &schema)
        })
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))??;

        Ok(json!(LoadParquetResult {
            success: true,
            row_count,
        }))
    }
}

fn parse_uuid(s: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|_| Error::InvalidRequest(format!("Invalid session ID: {}", s)))
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
