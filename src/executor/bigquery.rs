use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::tabledata::list::Value as BqValue;
use serde_json::Value as JsonValue;
use tokio::runtime::Handle;

use crate::error::{Error, Result};

use super::QueryResult;

pub struct BigQueryExecutor {
    client: Client,
    project_id: String,
}

impl BigQueryExecutor {
    pub async fn new() -> Result<Self> {
        let (config, project_id) = ClientConfig::new_with_auth()
            .await
            .map_err(|e| Error::Executor(format!("Failed to authenticate: {}", e)))?;

        let project_id = project_id
            .ok_or_else(|| Error::Executor("No project_id in credentials".into()))?;

        let client = Client::new(config)
            .await
            .map_err(|e| Error::Executor(format!("Failed to create BigQuery client: {}", e)))?;

        Ok(Self { client, project_id })
    }

    pub fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let handle = Handle::current();
        handle.block_on(self.execute_query_async(sql))
    }

    async fn execute_query_async(&self, sql: &str) -> Result<QueryResult> {
        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            ..Default::default()
        };

        let response = self
            .client
            .job()
            .query(&self.project_id, &request)
            .await
            .map_err(|e| Error::Executor(format!("BigQuery query failed: {}\n\nSQL: {}", e, sql)))?;

        let columns: Vec<String> = response
            .schema
            .as_ref()
            .map(|s| {
                s.fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect()
            })
            .unwrap_or_default();

        let rows: Vec<Vec<JsonValue>> = response
            .rows
            .unwrap_or_default()
            .into_iter()
            .map(|tuple| {
                tuple.f.into_iter().map(|cell| bq_value_to_json(cell.v)).collect()
            })
            .collect();

        Ok(QueryResult { columns, rows })
    }

    pub fn execute_statement(&self, sql: &str) -> Result<u64> {
        let handle = Handle::current();
        handle.block_on(self.execute_statement_async(sql))
    }

    async fn execute_statement_async(&self, sql: &str) -> Result<u64> {
        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            ..Default::default()
        };

        let response = self
            .client
            .job()
            .query(&self.project_id, &request)
            .await
            .map_err(|e| Error::Executor(format!("BigQuery statement failed: {}\n\nSQL: {}", e, sql)))?;

        Ok(response.num_dml_affected_rows.unwrap_or(0) as u64)
    }
}

fn bq_value_to_json(value: BqValue) -> JsonValue {
    match value {
        BqValue::Null => JsonValue::Null,
        BqValue::String(s) => JsonValue::String(s),
        BqValue::Array(cells) => {
            JsonValue::Array(cells.into_iter().map(|c| bq_value_to_json(c.v)).collect())
        }
        BqValue::Struct(tuple) => {
            JsonValue::Array(tuple.f.into_iter().map(|c| bq_value_to_json(c.v)).collect())
        }
    }
}
