use std::fs::File;

use arrow::array::Array;
use arrow::datatypes::DataType as ArrowDataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value as JsonValue};
use yachtsql::{AsyncQueryExecutor, DataType, Table, Value as YachtValue};

use crate::error::{Error, Result};

#[derive(Clone)]
pub struct YachtSqlExecutor {
    executor: AsyncQueryExecutor,
}

impl YachtSqlExecutor {
    pub fn new() -> Self {
        Self {
            executor: AsyncQueryExecutor::new(),
        }
    }

    pub async fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let result = self
            .executor
            .execute_sql(sql)
            .await
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, sql)))?;

        table_to_query_result(&result)
    }

    pub async fn execute_statement(&self, sql: &str) -> Result<u64> {
        let result = self
            .executor
            .execute_sql(sql)
            .await
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, sql)))?;

        Ok(result.row_count() as u64)
    }

    pub async fn load_parquet(
        &self,
        table_name: &str,
        path: &str,
        schema: &[crate::rpc::types::ColumnDef],
    ) -> Result<u64> {
        let file = File::open(path)
            .map_err(|e| Error::Executor(format!("Failed to open parquet file: {}", e)))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::Executor(format!("Failed to read parquet: {}", e)))?;

        let reader = builder
            .build()
            .map_err(|e| Error::Executor(format!("Failed to build parquet reader: {}", e)))?;

        let columns: Vec<String> = schema
            .iter()
            .map(|col| format!("{} {}", col.name, col.column_type))
            .collect();

        let create_sql = format!(
            "CREATE OR REPLACE TABLE {} ({})",
            table_name,
            columns.join(", ")
        );

        self.executor
            .execute_sql(&create_sql)
            .await
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, create_sql)))?;

        let mut total_rows = 0u64;

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| Error::Executor(format!("Failed to read batch: {}", e)))?;

            if batch.num_rows() == 0 {
                continue;
            }

            let mut all_values: Vec<Vec<String>> = Vec::new();
            for row_idx in 0..batch.num_rows() {
                let mut row_values = Vec::new();
                for (col_idx, col_schema) in schema.iter().enumerate().take(batch.num_columns()) {
                    let col = batch.column(col_idx);
                    let bq_type = &col_schema.column_type;
                    let value = arrow_value_to_sql(col.as_ref(), row_idx, bq_type);
                    row_values.push(value);
                }
                all_values.push(row_values);
            }

            let values_str: Vec<String> = all_values
                .iter()
                .map(|row| format!("({})", row.join(", ")))
                .collect();

            let insert_sql = format!(
                "INSERT INTO {} VALUES {}",
                table_name,
                values_str.join(", ")
            );

            self.executor
                .execute_sql(&insert_sql)
                .await
                .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, insert_sql)))?;

            total_rows += batch.num_rows() as u64;
        }

        Ok(total_rows)
    }

    pub async fn list_tables(&self) -> Result<Vec<(String, u64)>> {
        let result = self.execute_query(
            "SELECT table_name, table_rows FROM information_schema.tables WHERE table_schema = 'public'"
        ).await?;

        let tables: Vec<(String, u64)> = result
            .rows
            .into_iter()
            .map(|row| {
                let name = row[0].as_str().unwrap_or("").to_string();
                let row_count = row[1].as_u64().unwrap_or(0);
                (name, row_count)
            })
            .collect();

        Ok(tables)
    }

    pub async fn describe_table(&self, table_name: &str) -> Result<(Vec<(String, String)>, u64)> {
        let schema_sql = format!(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position",
            table_name
        );
        let schema_result = self.execute_query(&schema_sql).await?;

        let schema: Vec<(String, String)> = schema_result
            .rows
            .into_iter()
            .map(|row| {
                let name = row[0].as_str().unwrap_or("").to_string();
                let col_type = row[1].as_str().unwrap_or("STRING").to_string();
                (name, col_type)
            })
            .collect();

        let count_sql = format!("SELECT COUNT(*) FROM {}", table_name);
        let count_result = self.execute_query(&count_sql).await?;
        let row_count = count_result
            .rows
            .first()
            .and_then(|row| row.first())
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok((schema, row_count))
    }
}

impl Default for YachtSqlExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<JsonValue>>,
}

impl QueryResult {
    pub fn to_bq_response(&self) -> JsonValue {
        let schema_fields: Vec<JsonValue> = self
            .columns
            .iter()
            .map(|col| json!({ "name": col.name, "type": col.data_type }))
            .collect();

        let rows: Vec<JsonValue> = self
            .rows
            .iter()
            .map(|row| {
                let fields: Vec<JsonValue> = row.iter().map(|v| json!({ "v": v })).collect();
                json!({ "f": fields })
            })
            .collect();

        json!({
            "kind": "bigquery#queryResponse",
            "schema": { "fields": schema_fields },
            "rows": rows,
            "totalRows": self.rows.len().to_string(),
            "jobComplete": true
        })
    }
}

fn table_to_query_result(table: &Table) -> Result<QueryResult> {
    let schema = table.schema();
    let columns: Vec<ColumnInfo> = schema
        .fields()
        .iter()
        .map(|f| ColumnInfo {
            name: f.name.clone(),
            data_type: datatype_to_bq_type(&f.data_type),
        })
        .collect();

    let records = table
        .to_records()
        .map_err(|e| Error::Executor(e.to_string()))?;
    let rows: Vec<Vec<JsonValue>> = records
        .iter()
        .map(|record| record.values().iter().map(yacht_value_to_json).collect())
        .collect();

    Ok(QueryResult { columns, rows })
}

fn datatype_to_bq_type(dt: &DataType) -> String {
    match dt {
        DataType::Bool => "BOOLEAN".to_string(),
        DataType::Int64 => "INT64".to_string(),
        DataType::Float64 => "FLOAT64".to_string(),
        DataType::Numeric(_) | DataType::BigNumeric => "NUMERIC".to_string(),
        DataType::String => "STRING".to_string(),
        DataType::Bytes => "BYTES".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::DateTime => "DATETIME".to_string(),
        DataType::Time => "TIME".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
        DataType::Geography => "GEOGRAPHY".to_string(),
        DataType::Json => "JSON".to_string(),
        DataType::Struct(_) => "STRUCT".to_string(),
        DataType::Array(inner) => format!("ARRAY<{}>", datatype_to_bq_type(inner)),
        DataType::Interval => "INTERVAL".to_string(),
        DataType::Range(_) => "STRING".to_string(),
        DataType::Unknown => "STRING".to_string(),
    }
}

fn yacht_value_to_json(value: &YachtValue) -> JsonValue {
    match value {
        YachtValue::Null => JsonValue::Null,
        YachtValue::Bool(b) => JsonValue::Bool(*b),
        YachtValue::Int64(i) => json!(i),
        YachtValue::Float64(f) => json!(f.into_inner()),
        YachtValue::Numeric(d) => JsonValue::String(d.to_string()),
        YachtValue::String(s) => JsonValue::String(s.clone()),
        YachtValue::Bytes(b) => JsonValue::String(base64_encode(b)),
        YachtValue::Date(d) => JsonValue::String(d.to_string()),
        YachtValue::Time(t) => JsonValue::String(t.to_string()),
        YachtValue::DateTime(dt) => JsonValue::String(dt.to_string()),
        YachtValue::Timestamp(ts) => JsonValue::String(ts.to_string()),
        YachtValue::Json(j) => j.clone(),
        YachtValue::Array(arr) => {
            let items: Vec<JsonValue> = arr.iter().map(yacht_value_to_json).collect();
            JsonValue::Array(items)
        }
        YachtValue::Struct(fields) => {
            let obj: serde_json::Map<String, JsonValue> = fields
                .iter()
                .map(|(k, v)| (k.clone(), yacht_value_to_json(v)))
                .collect();
            JsonValue::Object(obj)
        }
        YachtValue::Geography(g) => JsonValue::String(g.clone()),
        YachtValue::Interval(i) => JsonValue::String(format!("{:?}", i)),
        YachtValue::Range(r) => JsonValue::String(format!("{:?}", r)),
        YachtValue::Default => JsonValue::Null,
    }
}

fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }
    result
}

fn arrow_value_to_sql(array: &dyn Array, row: usize, bq_type: &str) -> String {
    use arrow::array::*;

    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        ArrowDataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let val = arr.value(row);
            match bq_type.to_uppercase().as_str() {
                "DATE" => format!("DATE_FROM_UNIX_DATE({})", val),
                "TIMESTAMP" => format!("TIMESTAMP_MICROS({})", val),
                _ => val.to_string(),
            }
        }
        ArrowDataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(row).to_string()
        }
        ArrowDataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            format!("'{}'", arr.value(row).replace('\'', "''"))
        }
        ArrowDataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            format!("'{}'", arr.value(row).replace('\'', "''"))
        }
        ArrowDataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row);
            format!("DATE_FROM_UNIX_DATE({})", days)
        }
        ArrowDataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let ms = arr.value(row);
            let days = ms / (24 * 60 * 60 * 1000);
            format!("DATE_FROM_UNIX_DATE({})", days)
        }
        ArrowDataType::Timestamp(unit, _) => {
            let micros = match unit {
                arrow::datatypes::TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap();
                    arr.value(row) * 1_000_000
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    arr.value(row) * 1_000
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    arr.value(row)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    arr.value(row) / 1_000
                }
            };
            format!("TIMESTAMP_MICROS({})", micros)
        }
        _ => "NULL".to_string(),
    }
}
