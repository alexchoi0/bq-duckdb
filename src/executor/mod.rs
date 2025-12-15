mod duckdb;
mod bigquery;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;

pub use self::duckdb::DuckDbExecutor;
pub use self::duckdb::QueryResult;
pub use self::bigquery::BigQueryExecutor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Mock,
    Interactive,
    Batch,
}

impl std::str::FromStr for ExecutionMode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mock" => Ok(ExecutionMode::Mock),
            "interactive" => Ok(ExecutionMode::Interactive),
            "batch" => Ok(ExecutionMode::Batch),
            _ => Err(format!("Invalid mode: {}. Use mock, interactive, or batch", s)),
        }
    }
}

#[async_trait]
pub trait QueryExecutor: Send + Sync {
    fn create_schema(&self, schema_name: &str) -> Result<()>;
    fn drop_schema(&self, schema_name: &str) -> Result<()>;
    fn execute_query(&self, schema_name: &str, sql: &str) -> Result<QueryResult>;
    fn execute_statement(&self, schema_name: &str, sql: &str) -> Result<u64>;
    fn table_exists(&self, schema_name: &str, table_name: &str) -> Result<bool>;
    fn bulk_insert_rows(&self, schema_name: &str, table_name: &str, rows: &[Vec<Value>]) -> Result<()>;
}

impl QueryExecutor for DuckDbExecutor {
    fn create_schema(&self, schema_name: &str) -> Result<()> {
        DuckDbExecutor::create_schema(self, schema_name)
    }

    fn drop_schema(&self, schema_name: &str) -> Result<()> {
        DuckDbExecutor::drop_schema(self, schema_name)
    }

    fn execute_query(&self, schema_name: &str, sql: &str) -> Result<QueryResult> {
        DuckDbExecutor::execute_query(self, schema_name, sql)
    }

    fn execute_statement(&self, schema_name: &str, sql: &str) -> Result<u64> {
        DuckDbExecutor::execute_statement(self, schema_name, sql)
    }

    fn table_exists(&self, schema_name: &str, table_name: &str) -> Result<bool> {
        DuckDbExecutor::table_exists(self, schema_name, table_name)
    }

    fn bulk_insert_rows(&self, schema_name: &str, table_name: &str, rows: &[Vec<Value>]) -> Result<()> {
        DuckDbExecutor::bulk_insert_rows(self, schema_name, table_name, rows)
    }
}
