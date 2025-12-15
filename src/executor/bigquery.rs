use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::runtime::Runtime;

use crate::error::{Error, Result};
use crate::executor::{ExecutionMode, QueryExecutor, QueryResult};

pub struct BigQueryExecutor {
    client: Arc<tokio::sync::Mutex<Client>>,
    project_id: String,
    #[allow(dead_code)]
    mode: ExecutionMode,
    schemas: Mutex<HashMap<String, SchemaState>>,
    runtime: Runtime,
}

#[derive(Default)]
struct SchemaState {
    tables: HashMap<String, TableInfo>,
}

struct TableInfo {
    columns: Vec<String>,
}

impl BigQueryExecutor {
    pub fn new_sync(project_id: String, mode: ExecutionMode) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Internal(format!("Failed to create runtime: {}", e)))?;

        let client = runtime.block_on(async {
            Self::create_client().await
        })?;

        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
            project_id,
            mode,
            schemas: Mutex::new(HashMap::new()),
            runtime,
        })
    }

    async fn create_client() -> Result<Client> {
        if let Ok(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            return Client::from_service_account_key_file(&path)
                .await
                .map_err(|e| Error::Internal(format!("Failed to create client from key file: {}", e)));
        }

        let adc_path = Self::get_adc_path()?;
        if adc_path.exists() {
            let content = std::fs::read_to_string(&adc_path)
                .map_err(|e| Error::Internal(format!("Failed to read ADC file: {}", e)))?;

            let adc: serde_json::Value = serde_json::from_str(&content)
                .map_err(|e| Error::Internal(format!("Failed to parse ADC file: {}", e)))?;

            if let Some(cred_type) = adc.get("type").and_then(|v| v.as_str()) {
                match cred_type {
                    "authorized_user" => {
                        return Client::from_authorized_user_secret(adc_path.to_str().unwrap())
                            .await
                            .map_err(|e| Error::Internal(format!("Failed to create client from user credentials: {}", e)));
                    }
                    "service_account" => {
                        return Client::from_service_account_key_file(adc_path.to_str().unwrap())
                            .await
                            .map_err(|e| Error::Internal(format!("Failed to create client from service account: {}", e)));
                    }
                    _ => {}
                }
            }
        }

        Client::from_application_default_credentials()
            .await
            .map_err(|e| Error::Internal(format!("Failed to create BigQuery client: {}", e)))
    }

    fn get_adc_path() -> Result<PathBuf> {
        let home = std::env::var("HOME")
            .map_err(|_| Error::Internal("HOME environment variable not set".to_string()))?;
        Ok(PathBuf::from(home).join(".config/gcloud/application_default_credentials.json"))
    }

    pub async fn new(project_id: String, mode: ExecutionMode) -> Result<Self> {
        tokio::task::spawn_blocking(move || Self::new_sync(project_id, mode))
            .await
            .map_err(|e| Error::Internal(format!("Task join error: {}", e)))?
    }

    fn use_legacy_sql(&self) -> bool {
        false
    }
}

impl QueryExecutor for BigQueryExecutor {
    fn create_schema(&self, schema_name: &str) -> Result<()> {
        let mut schemas = self.schemas.lock();
        schemas.entry(schema_name.to_string()).or_default();
        Ok(())
    }

    fn drop_schema(&self, schema_name: &str) -> Result<()> {
        let mut schemas = self.schemas.lock();
        if let Some(state) = schemas.remove(schema_name) {
            let client = self.client.clone();
            let project_id = self.project_id.clone();
            let schema = schema_name.to_string();
            let tables: Vec<String> = state.tables.keys().cloned().collect();

            self.runtime.block_on(async {
                let c = client.lock().await;
                for table in tables {
                    let full_name = format!("{}.{}", schema, table);
                    let _ = c.table().delete(&project_id, &schema, &table).await;
                    tracing::debug!("Dropped BigQuery table: {}", full_name);
                }
            });
        }
        Ok(())
    }

    fn execute_query(&self, schema_name: &str, sql: &str) -> Result<QueryResult> {
        let client = self.client.clone();
        let project_id = self.project_id.clone();
        let query = sql.to_string();
        let use_legacy = self.use_legacy_sql();
        let dataset = schema_name.to_string();

        self.runtime.block_on(async {
            let c = client.lock().await;

            let mut request = QueryRequest::new(&query);
            request.use_legacy_sql = use_legacy;
            request.default_dataset = Some(gcp_bigquery_client::model::dataset_reference::DatasetReference {
                dataset_id: dataset,
                project_id: project_id.clone(),
            });

            let mut result_set = c
                .job()
                .query(&project_id, request)
                .await
                .map_err(|e| Error::Internal(format!("BigQuery query failed: {}", e)))?;

            let columns: Vec<String> = result_set
                .column_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect();

            let mut rows: Vec<Vec<Value>> = Vec::new();

            while result_set.next_row() {
                let mut row = Vec::with_capacity(columns.len());
                for col in &columns {
                    let value = result_set
                        .get_json_value_by_name(col)
                        .map_err(|e| Error::Internal(format!("Failed to get column {}: {}", col, e)))?
                        .unwrap_or(Value::Null);
                    row.push(value);
                }
                rows.push(row);
            }

            Ok(QueryResult { columns, rows })
        })
    }

    fn execute_statement(&self, schema_name: &str, sql: &str) -> Result<u64> {
        let result = self.execute_query(schema_name, sql)?;
        Ok(result.rows.len() as u64)
    }

    fn table_exists(&self, schema_name: &str, table_name: &str) -> Result<bool> {
        let schemas = self.schemas.lock();
        if let Some(state) = schemas.get(schema_name) {
            return Ok(state.tables.contains_key(table_name));
        }

        let client = self.client.clone();
        let project_id = self.project_id.clone();
        let dataset = schema_name.to_string();
        let table = table_name.to_string();

        self.runtime.block_on(async {
            let c = client.lock().await;
            match c.table().get(&project_id, &dataset, &table, None).await {
                Ok(_) => Ok(true),
                Err(_) => Ok(false),
            }
        })
    }

    fn bulk_insert_rows(
        &self,
        schema_name: &str,
        table_name: &str,
        rows: &[Vec<Value>],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let columns: Vec<String> = {
            let schemas = self.schemas.lock();
            schemas
                .get(schema_name)
                .and_then(|s| s.tables.get(table_name))
                .map(|t| t.columns.clone())
                .unwrap_or_default()
        };

        if columns.is_empty() {
            return Err(Error::Internal(format!(
                "Table {}.{} not found or has no columns",
                schema_name, table_name
            )));
        }

        let client = self.client.clone();
        let project_id = self.project_id.clone();
        let dataset = schema_name.to_string();
        let table = table_name.to_string();

        let insert_rows: Vec<_> = rows
            .iter()
            .map(|row| {
                let mut obj = serde_json::Map::new();
                for (i, val) in row.iter().enumerate() {
                    if let Some(col) = columns.get(i) {
                        obj.insert(col.clone(), val.clone());
                    }
                }
                obj
            })
            .collect();

        self.runtime.block_on(async {
            let c = client.lock().await;

            let mut request = TableDataInsertAllRequest::new();
            for row_data in insert_rows {
                request.add_row(None, row_data)
                    .map_err(|e| Error::Internal(format!("Failed to add row: {}", e)))?;
            }

            c.tabledata()
                .insert_all(&project_id, &dataset, &table, request)
                .await
                .map_err(|e| Error::Internal(format!("BigQuery insert failed: {}", e)))?;

            Ok(())
        })
    }
}
