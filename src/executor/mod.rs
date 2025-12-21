mod bigquery;
mod yachtsql;

pub use self::bigquery::BigQueryExecutor;
pub use self::yachtsql::QueryResult;
pub use self::yachtsql::YachtSqlExecutor;

use crate::error::Result;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutorMode {
    #[default]
    Mock,
    BigQuery,
}

pub enum Executor {
    Mock(YachtSqlExecutor),
    BigQuery(BigQueryExecutor),
}

impl Executor {
    pub fn mode(&self) -> ExecutorMode {
        match self {
            Executor::Mock(_) => ExecutorMode::Mock,
            Executor::BigQuery(_) => ExecutorMode::BigQuery,
        }
    }

    pub async fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        match self {
            Executor::Mock(e) => e.execute_query(sql).await,
            Executor::BigQuery(e) => e.execute_query(sql).await,
        }
    }

    pub async fn execute_statement(&self, sql: &str) -> Result<u64> {
        match self {
            Executor::Mock(e) => e.execute_statement(sql).await,
            Executor::BigQuery(e) => e.execute_statement(sql).await,
        }
    }

    pub async fn load_parquet(
        &self,
        table_name: &str,
        path: &str,
        schema: &[crate::rpc::types::ColumnDef],
    ) -> Result<u64> {
        match self {
            Executor::Mock(e) => e.load_parquet(table_name, path, schema).await,
            Executor::BigQuery(e) => e.load_parquet(table_name, path, schema).await,
        }
    }

    pub async fn list_tables(&self) -> Result<Vec<(String, u64)>> {
        match self {
            Executor::Mock(e) => e.list_tables().await,
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "list_tables not supported for BigQuery executor".to_string(),
            )),
        }
    }

    pub async fn describe_table(&self, table_name: &str) -> Result<(Vec<(String, String)>, u64)> {
        match self {
            Executor::Mock(e) => e.describe_table(table_name).await,
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "describe_table not supported for BigQuery executor".to_string(),
            )),
        }
    }
}
