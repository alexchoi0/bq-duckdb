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

    pub fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        match self {
            Executor::Mock(e) => e.execute_query(sql),
            Executor::BigQuery(e) => e.execute_query(sql),
        }
    }

    pub fn execute_statement(&self, sql: &str) -> Result<u64> {
        match self {
            Executor::Mock(e) => e.execute_statement(sql),
            Executor::BigQuery(e) => e.execute_statement(sql),
        }
    }

    pub fn load_parquet(
        &self,
        table_name: &str,
        path: &str,
        schema: &[crate::rpc::types::ColumnDef],
    ) -> Result<u64> {
        match self {
            Executor::Mock(e) => e.load_parquet(table_name, path, schema),
            Executor::BigQuery(_) => {
                Err(crate::error::Error::Executor(
                    "load_parquet not supported for BigQuery executor".to_string(),
                ))
            }
        }
    }
}
