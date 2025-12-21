use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct RpcResponse {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
    pub id: Value,
}

#[derive(Debug, Serialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcResponse {
    pub fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn error(id: Value, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code,
                message,
                data: None,
            }),
            id,
        }
    }

    pub fn parse_error() -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code: -32700,
                message: "Parse error".to_string(),
                data: None,
            }),
            id: Value::Null,
        }
    }

    pub fn invalid_request() -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code: -32600,
                message: "Invalid Request".to_string(),
                data: None,
            }),
            id: Value::Null,
        }
    }

    pub fn method_not_found(id: Value, method: &str) -> Self {
        Self {
            jsonrpc: "2.0",
            result: None,
            error: Some(RpcError {
                code: -32601,
                message: format!("Method not found: {}", method),
                data: None,
            }),
            id,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PingResult {
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct CreateSessionResult {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Deserialize)]
pub struct DestroySessionParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct DestroySessionResult {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub sql: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateTableParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
    pub schema: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, column_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: column_type.into(),
        }
    }

    pub fn int64(name: impl Into<String>) -> Self {
        Self::new(name, "INT64")
    }

    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, "STRING")
    }

    pub fn float64(name: impl Into<String>) -> Self {
        Self::new(name, "FLOAT64")
    }

    pub fn bool(name: impl Into<String>) -> Self {
        Self::new(name, "BOOLEAN")
    }

    pub fn date(name: impl Into<String>) -> Self {
        Self::new(name, "DATE")
    }

    pub fn timestamp(name: impl Into<String>) -> Self {
        Self::new(name, "TIMESTAMP")
    }

    pub fn numeric(name: impl Into<String>) -> Self {
        Self::new(name, "NUMERIC")
    }

    pub fn bytes(name: impl Into<String>) -> Self {
        Self::new(name, "BYTES")
    }
}

impl From<(String, String)> for ColumnDef {
    fn from((name, column_type): (String, String)) -> Self {
        Self { name, column_type }
    }
}

impl From<(&str, &str)> for ColumnDef {
    fn from((name, column_type): (&str, &str)) -> Self {
        Self {
            name: name.to_string(),
            column_type: column_type.to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct CreateTableResult {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct InsertParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
    pub rows: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct InsertResult {
    #[serde(rename = "insertedRows")]
    pub inserted_rows: u64,
}

#[derive(Debug, Deserialize)]
pub struct RegisterDagParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    pub tables: Vec<DagTableDef>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DagTableDef {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Vec<ColumnDef>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct RegisterDagResult {
    pub success: bool,
    pub tables: Vec<DagTableInfo>,
}

#[derive(Debug, Serialize)]
pub struct DagTableInfo {
    pub name: String,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct RunDagParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableNames")]
    pub table_names: Option<Vec<String>>,
    #[serde(rename = "retryCount", default)]
    pub retry_count: u32,
}

#[derive(Debug, Serialize)]
pub struct RunDagResult {
    pub success: bool,
    #[serde(rename = "succeededTables")]
    pub succeeded_tables: Vec<String>,
    #[serde(rename = "failedTables")]
    pub failed_tables: Vec<TableErrorInfo>,
    #[serde(rename = "skippedTables")]
    pub skipped_tables: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct TableErrorInfo {
    pub table: String,
    pub error: String,
}

#[derive(Debug, Deserialize)]
pub struct RetryDagParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "failedTables")]
    pub failed_tables: Vec<String>,
    #[serde(rename = "skippedTables")]
    pub skipped_tables: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct GetDagParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct GetDagResult {
    pub tables: Vec<DagTableDetail>,
}

#[derive(Debug, Serialize)]
pub struct DagTableDetail {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(rename = "isSource")]
    pub is_source: bool,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClearDagParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct ClearDagResult {
    pub success: bool,
}

#[derive(Debug, Deserialize)]
pub struct LoadParquetParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
    pub path: String,
    pub schema: Vec<ColumnDef>,
}

#[derive(Debug, Serialize)]
pub struct LoadParquetResult {
    pub success: bool,
    #[serde(rename = "rowCount")]
    pub row_count: u64,
}

#[derive(Debug, Deserialize)]
pub struct ListTablesParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
}

#[derive(Debug, Serialize)]
pub struct ListTablesResult {
    pub tables: Vec<TableInfo>,
}

#[derive(Debug, Serialize)]
pub struct TableInfo {
    pub name: String,
    #[serde(rename = "rowCount")]
    pub row_count: u64,
}

#[derive(Debug, Deserialize)]
pub struct DescribeTableParams {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
}

#[derive(Debug, Serialize)]
pub struct DescribeTableResult {
    pub name: String,
    pub schema: Vec<ColumnDef>,
    #[serde(rename = "rowCount")]
    pub row_count: u64,
}
