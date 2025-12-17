use std::fs::File;

use arrow::array::Array;
use arrow::datatypes::DataType;
use parking_lot::Mutex;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value as JsonValue};
use yachtsql::{QueryExecutor as YachtExecutor, Value as YachtValue, Table};

use crate::error::{Error, Result};

pub struct YachtSqlExecutor {
    executor: Mutex<YachtExecutor>,
}

impl YachtSqlExecutor {
    pub fn new() -> Result<Self> {
        let executor = YachtExecutor::new();

        Ok(Self {
            executor: Mutex::new(executor),
        })
    }

    pub fn execute_query(&self, sql: &str) -> Result<QueryResult> {
        let mut executor = self.executor.lock();

        let result = executor
            .execute_sql(sql)
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, sql)))?;

        table_to_query_result(&result)
    }

    pub fn execute_statement(&self, sql: &str) -> Result<u64> {
        let mut executor = self.executor.lock();

        let result = executor
            .execute_sql(sql)
            .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, sql)))?;

        Ok(result.row_count() as u64)
    }

    pub fn load_parquet(
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

        let mut executor = self.executor.lock();
        executor
            .execute_sql(&create_sql)
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
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let bq_type = &schema[col_idx].column_type;
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

            executor
                .execute_sql(&insert_sql)
                .map_err(|e| Error::Executor(format!("{}\n\nSQL: {}", e, insert_sql)))?;

            total_rows += batch.num_rows() as u64;
        }

        Ok(total_rows)
    }
}

impl Default for YachtSqlExecutor {
    fn default() -> Self {
        Self::new().expect("Failed to create YachtSQL executor")
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<JsonValue>>,
}

impl QueryResult {
    pub fn to_bq_response(&self) -> JsonValue {
        let schema_fields: Vec<JsonValue> = self
            .columns
            .iter()
            .map(|name| json!({ "name": name, "type": "STRING" }))
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
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();

    let records = table.to_records().map_err(|e| Error::Executor(e.to_string()))?;
    let rows: Vec<Vec<JsonValue>> = records
        .iter()
        .map(|record| {
            record.values().iter().map(yacht_value_to_json).collect()
        })
        .collect();

    Ok(QueryResult { columns, rows })
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
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let val = arr.value(row);
            match bq_type.to_uppercase().as_str() {
                "DATE" => format!("DATE_FROM_UNIX_DATE({})", val),
                "TIMESTAMP" => format!("TIMESTAMP_MICROS({})", val),
                _ => val.to_string(),
            }
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(row).to_string()
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            format!("'{}'", arr.value(row).replace('\'', "''"))
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            format!("'{}'", arr.value(row).replace('\'', "''"))
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row);
            format!("DATE_FROM_UNIX_DATE({})", days)
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let ms = arr.value(row);
            let days = ms / (24 * 60 * 60 * 1000);
            format!("DATE_FROM_UNIX_DATE({})", days)
        }
        DataType::Timestamp(unit, _) => {
            let micros = match unit {
                arrow::datatypes::TimeUnit::Second => {
                    let arr = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                    arr.value(row) * 1_000_000
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                    arr.value(row) * 1_000
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    arr.value(row)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                    arr.value(row) / 1_000
                }
            };
            format!("TIMESTAMP_MICROS({})", micros)
        }
        _ => "NULL".to_string(),
    }
}
