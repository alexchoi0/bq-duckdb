mod bigquery;
mod yachtsql;

pub use self::bigquery::BigQueryExecutor;
pub use self::yachtsql::{ColumnInfo, QueryResult, YachtSqlExecutor};
pub use crate::rpc::types::ColumnDef;

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
    #[allow(dead_code)]
    pub fn mock() -> Result<Self> {
        Ok(Self::Mock(YachtSqlExecutor::new()))
    }

    #[allow(dead_code)]
    pub async fn bigquery() -> Result<Self> {
        Ok(Self::BigQuery(BigQueryExecutor::new().await?))
    }

    pub fn mode(&self) -> ExecutorMode {
        match self {
            Executor::Mock(_) => ExecutorMode::Mock,
            Executor::BigQuery(_) => ExecutorMode::BigQuery,
        }
    }

    #[allow(dead_code)]
    pub fn is_mock(&self) -> bool {
        matches!(self, Executor::Mock(_))
    }

    #[allow(dead_code)]
    pub async fn query(&self, sql: &str) -> Result<QueryResult> {
        match self {
            Executor::Mock(e) => e.execute_query(sql).await,
            Executor::BigQuery(e) => e.execute_query(sql).await,
        }
    }

    #[allow(dead_code)]
    pub async fn execute(&self, sql: &str) -> Result<u64> {
        match self {
            Executor::Mock(e) => e.execute_statement(sql).await,
            Executor::BigQuery(e) => e.execute_statement(sql).await,
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

    pub fn set_default_project(&self, project: Option<String>) -> Result<()> {
        match self {
            Executor::Mock(e) => {
                e.set_default_project(project);
                Ok(())
            }
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "set_default_project not supported for BigQuery executor".to_string(),
            )),
        }
    }

    pub fn get_default_project(&self) -> Result<Option<String>> {
        match self {
            Executor::Mock(e) => Ok(e.get_default_project()),
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "get_default_project not supported for BigQuery executor".to_string(),
            )),
        }
    }

    pub fn get_projects(&self) -> Result<Vec<String>> {
        match self {
            Executor::Mock(e) => Ok(e.get_projects()),
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "get_projects not supported for BigQuery executor".to_string(),
            )),
        }
    }

    pub fn get_datasets(&self, project: &str) -> Result<Vec<String>> {
        match self {
            Executor::Mock(e) => Ok(e.get_datasets(project)),
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "get_datasets not supported for BigQuery executor".to_string(),
            )),
        }
    }

    pub fn get_tables_in_dataset(&self, project: &str, dataset: &str) -> Result<Vec<String>> {
        match self {
            Executor::Mock(e) => Ok(e.get_tables_in_dataset(project, dataset)),
            Executor::BigQuery(_) => Err(crate::error::Error::Executor(
                "get_tables_in_dataset not supported for BigQuery executor".to_string(),
            )),
        }
    }
}
