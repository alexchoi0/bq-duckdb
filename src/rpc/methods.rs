use std::sync::Arc;

use serde_json::{json, Value};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::session::SessionManager;
use crate::utils::json_to_sql_value;

use super::types::{
    ClearDagParams, ClearDagResult, ColumnDef, CreateSessionResult, CreateTableParams,
    CreateTableResult, DescribeTableParams, DescribeTableResult, DestroySessionParams,
    DestroySessionResult, GetDagParams, GetDagResult, InsertParams, InsertResult, ListTablesParams,
    ListTablesResult, LoadParquetParams, LoadParquetResult, PingResult, QueryParams,
    RegisterDagParams, RegisterDagResult, RetryDagParams, RunDagParams, RunDagResult,
    TableErrorInfo, TableInfo,
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
            "bq.retryDag" => self.retry_dag(params).await,
            "bq.getDag" => self.get_dag(params).await,
            "bq.clearDag" => self.clear_dag(params).await,
            "bq.loadParquet" => self.load_parquet(params).await,
            "bq.listTables" => self.list_tables(params).await,
            "bq.describeTable" => self.describe_table(params).await,
            _ => Err(Error::InvalidRequest(format!("Unknown method: {}", method))),
        }
    }

    async fn ping(&self, _params: Value) -> Result<Value> {
        Ok(json!(PingResult {
            message: "pong".to_string()
        }))
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

        self.session_manager.destroy_session(session_id)?;

        Ok(json!(DestroySessionResult { success: true }))
    }

    async fn query(&self, params: Value) -> Result<Value> {
        let p: QueryParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let result = self
            .session_manager
            .execute_query(session_id, &p.sql)
            .await?;

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

        let sql = format!("CREATE TABLE {} ({})", p.table_name, columns.join(", "));

        self.session_manager
            .execute_statement(session_id, &sql)
            .await?;

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
        let sql = format!("INSERT INTO {} VALUES {}", p.table_name, values.join(", "));

        self.session_manager
            .execute_statement(session_id, &sql)
            .await?;

        Ok(json!(InsertResult {
            inserted_rows: row_count,
        }))
    }

    async fn register_dag(&self, params: Value) -> Result<Value> {
        let p: RegisterDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let table_infos = self.session_manager.register_dag(session_id, p.tables)?;

        Ok(json!(RegisterDagResult {
            success: true,
            tables: table_infos,
        }))
    }

    async fn run_dag(&self, params: Value) -> Result<Value> {
        let p: RunDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        let targets = p.table_names;
        let retry_count = p.retry_count;
        let session_manager = Arc::clone(&self.session_manager);

        let result =
            run_blocking(move || session_manager.run_dag(session_id, targets, retry_count)).await?;

        Ok(json!(RunDagResult {
            success: result.all_succeeded(),
            succeeded_tables: result.succeeded,
            failed_tables: result
                .failed
                .into_iter()
                .map(|e| TableErrorInfo {
                    table: e.table,
                    error: e.error,
                })
                .collect(),
            skipped_tables: result.skipped,
        }))
    }

    async fn retry_dag(&self, params: Value) -> Result<Value> {
        let p: RetryDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;
        let failed_tables = p.failed_tables;
        let skipped_tables = p.skipped_tables;
        let session_manager = Arc::clone(&self.session_manager);

        let result = run_blocking(move || {
            session_manager.retry_dag(session_id, failed_tables, skipped_tables)
        })
        .await?;

        Ok(json!(RunDagResult {
            success: result.all_succeeded(),
            succeeded_tables: result.succeeded,
            failed_tables: result
                .failed
                .into_iter()
                .map(|e| TableErrorInfo {
                    table: e.table,
                    error: e.error,
                })
                .collect(),
            skipped_tables: result.skipped,
        }))
    }

    async fn get_dag(&self, params: Value) -> Result<Value> {
        let p: GetDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let tables = self.session_manager.get_dag(session_id)?;

        Ok(json!(GetDagResult { tables }))
    }

    async fn clear_dag(&self, params: Value) -> Result<Value> {
        let p: ClearDagParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        self.session_manager.clear_dag(session_id)?;

        Ok(json!(ClearDagResult { success: true }))
    }

    async fn load_parquet(&self, params: Value) -> Result<Value> {
        let p: LoadParquetParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let row_count = self
            .session_manager
            .load_parquet(session_id, &p.table_name, &p.path, &p.schema)
            .await?;

        Ok(json!(LoadParquetResult {
            success: true,
            row_count,
        }))
    }

    async fn list_tables(&self, params: Value) -> Result<Value> {
        let p: ListTablesParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let table_infos = self.session_manager.list_tables(session_id).await?;

        Ok(json!(ListTablesResult {
            tables: table_infos
                .into_iter()
                .map(|(name, row_count)| TableInfo { name, row_count })
                .collect(),
        }))
    }

    async fn describe_table(&self, params: Value) -> Result<Value> {
        let p: DescribeTableParams = serde_json::from_value(params)?;
        let session_id = parse_uuid(&p.session_id)?;

        let (schema, row_count) = self
            .session_manager
            .describe_table(session_id, &p.table_name)
            .await?;

        Ok(json!(DescribeTableResult {
            name: p.table_name,
            schema: schema
                .into_iter()
                .map(|(name, col_type)| ColumnDef {
                    name,
                    column_type: col_type,
                })
                .collect(),
            row_count,
        }))
    }
}

fn parse_uuid(s: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|_| Error::InvalidRequest(format!("Invalid session ID: {}", s)))
}

async fn run_blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Internal(format!("Task join error: {e}")))?
}
