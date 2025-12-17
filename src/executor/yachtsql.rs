use parking_lot::Mutex;
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
