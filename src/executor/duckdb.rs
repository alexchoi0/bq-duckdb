use std::sync::Arc;

use duckdb::{params, Connection, Config};
use parking_lot::Mutex;
use serde_json::{json, Value};

use crate::error::{Error, Result};

pub struct DuckDbExecutor {
    conn: Arc<Mutex<Connection>>,
}

impl DuckDbExecutor {
    pub fn new() -> Result<Self> {
        let config = Config::default()
            .threads(num_cpus::get() as i64)
            .map_err(|e| Error::Internal(format!("Failed to configure DuckDB: {e}")))?;

        let conn = Connection::open_in_memory_with_flags(config)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn create_schema(&self, schema_name: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute_batch(&format!(r#"CREATE SCHEMA IF NOT EXISTS "{schema_name}""#))?;
        Ok(())
    }

    pub fn drop_schema(&self, schema_name: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute_batch(&format!(r#"DROP SCHEMA IF EXISTS "{schema_name}" CASCADE"#))?;
        Ok(())
    }

    pub fn execute_query(&self, schema_name: &str, sql: &str) -> Result<QueryResult> {
        let conn = self.conn.lock();

        conn.execute_batch(&format!(r#"SET search_path = "{schema_name}""#))?;

        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(params![])?;

        let column_count = rows.as_ref().map(|r| r.column_count()).unwrap_or(0);
        let column_names: Vec<String> = (0..column_count)
            .map(|i| {
                rows.as_ref()
                    .and_then(|r| r.column_name(i).ok())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "?".to_string())
            })
            .collect();

        let mut rows_data: Vec<Vec<Value>> = Vec::new();

        while let Some(row) = rows.next()? {
            let mut row_values = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let value = row_value_to_json(row, i)?;
                row_values.push(value);
            }
            rows_data.push(row_values);
        }

        Ok(QueryResult {
            columns: column_names,
            rows: rows_data,
        })
    }

    pub fn execute_statement(&self, schema_name: &str, sql: &str) -> Result<u64> {
        let conn = self.conn.lock();
        conn.execute_batch(&format!(r#"SET search_path = "{schema_name}""#))?;
        let affected = conn.execute(sql, params![])?;
        Ok(affected as u64)
    }

    pub fn table_exists(&self, schema_name: &str, table_name: &str) -> Result<bool> {
        let conn = self.conn.lock();
        let sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        let mut stmt = conn.prepare(sql)?;
        let exists = stmt.exists(params![schema_name, table_name])?;
        Ok(exists)
    }

    pub fn bulk_insert_rows(
        &self,
        schema_name: &str,
        table_name: &str,
        rows: &[Vec<Value>],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let conn = self.conn.lock();
        conn.execute_batch(&format!(r#"SET search_path = "{schema_name}""#))?;

        let values: Vec<String> = rows
            .iter()
            .map(|row| {
                let vals: Vec<String> = row.iter().map(json_to_sql_literal).collect();
                format!("({})", vals.join(", "))
            })
            .collect();

        let sql = format!(
            r#"INSERT INTO "{}"."{}" VALUES {}"#,
            schema_name,
            table_name,
            values.join(", ")
        );

        conn.execute(&sql, params![])?;

        Ok(())
    }
}

impl Default for DuckDbExecutor {
    fn default() -> Self {
        Self::new().expect("Failed to create DuckDB executor")
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl QueryResult {
    pub fn to_bq_response(&self) -> Value {
        let schema_fields: Vec<Value> = self
            .columns
            .iter()
            .map(|name| json!({ "name": name, "type": "STRING" }))
            .collect();

        let rows: Vec<Value> = self
            .rows
            .iter()
            .map(|row| {
                let fields: Vec<Value> = row.iter().map(|v| json!({ "v": v })).collect();
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

fn row_value_to_json(row: &duckdb::Row, idx: usize) -> Result<Value> {
    use duckdb::types::ValueRef;

    let value_ref = row.get_ref(idx)?;

    let json_val = match value_ref {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Bool(b),
        ValueRef::TinyInt(i) => json!(i),
        ValueRef::SmallInt(i) => json!(i),
        ValueRef::Int(i) => json!(i),
        ValueRef::BigInt(i) => json!(i),
        ValueRef::HugeInt(i) => json!(i as i64),
        ValueRef::UTinyInt(i) => json!(i),
        ValueRef::USmallInt(i) => json!(i),
        ValueRef::UInt(i) => json!(i),
        ValueRef::UBigInt(i) => json!(i),
        ValueRef::Float(f) => json!(f),
        ValueRef::Double(f) => json!(f),
        ValueRef::Decimal(d) => json!(d.to_string()),
        ValueRef::Text(s) => {
            let text = std::str::from_utf8(s).unwrap_or("");
            Value::String(text.to_string())
        }
        ValueRef::Blob(b) => {
            Value::String(base64_encode(b))
        }
        ValueRef::Timestamp(unit, val) => {
            json!(format_timestamp(unit, val))
        }
        ValueRef::Date32(days) => {
            json!(format_date(days))
        }
        ValueRef::Time64(unit, val) => {
            json!(format_time(unit, val))
        }
        ValueRef::Interval { months, days, nanos } => {
            json!(format!("P{}M{}DT{}N", months, days, nanos))
        }
        ValueRef::List(list_type, idx) => {
            json!(format!("LIST<{:?}>@{}", list_type, idx))
        }
        ValueRef::Enum(val, idx) => {
            json!(format!("ENUM({:?})@{}", val, idx))
        }
        ValueRef::Struct(items, idx) => {
            json!(format!("STRUCT({} fields)@{}", items.num_columns(), idx))
        }
        ValueRef::Array(arr_type, idx) => {
            json!(format!("ARRAY<{:?}>@{}", arr_type, idx))
        }
        ValueRef::Map(key_type, val_type) => {
            json!(format!("MAP<{:?},{:?}>", key_type, val_type))
        }
        ValueRef::Union(members, idx) => {
            json!(format!("UNION({} members)@{}", members.len(), idx))
        }
    };

    Ok(json_val)
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

fn format_timestamp(unit: duckdb::types::TimeUnit, val: i64) -> String {
    let micros = match unit {
        duckdb::types::TimeUnit::Second => val * 1_000_000,
        duckdb::types::TimeUnit::Millisecond => val * 1_000,
        duckdb::types::TimeUnit::Microsecond => val,
        duckdb::types::TimeUnit::Nanosecond => val / 1_000,
    };
    let secs = micros / 1_000_000;
    let subsec_micros = (micros % 1_000_000).unsigned_abs();
    format!("{}.{:06}", secs, subsec_micros)
}

fn format_date(days: i32) -> String {
    let epoch = 719528; // days from year 0 to 1970-01-01
    let total_days = epoch + days as i64;

    let (year, month, day) = days_to_ymd(total_days);
    format!("{:04}-{:02}-{:02}", year, month, day)
}

fn days_to_ymd(days: i64) -> (i64, u32, u32) {
    let a = days + 32044;
    let b = (4 * a + 3) / 146097;
    let c = a - (146097 * b) / 4;
    let d = (4 * c + 3) / 1461;
    let e = c - (1461 * d) / 4;
    let m = (5 * e + 2) / 153;

    let day = (e - (153 * m + 2) / 5 + 1) as u32;
    let month = (m + 3 - 12 * (m / 10)) as u32;
    let year = 100 * b + d - 4800 + m / 10;

    (year, month, day)
}

fn format_time(unit: duckdb::types::TimeUnit, val: i64) -> String {
    let micros = match unit {
        duckdb::types::TimeUnit::Second => val * 1_000_000,
        duckdb::types::TimeUnit::Millisecond => val * 1_000,
        duckdb::types::TimeUnit::Microsecond => val,
        duckdb::types::TimeUnit::Nanosecond => val / 1_000,
    };

    let total_secs = micros / 1_000_000;
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    let subsec_micros = (micros % 1_000_000).unsigned_abs();

    format!("{:02}:{:02}:{:02}.{:06}", hours, minutes, seconds, subsec_micros)
}

fn json_to_sql_literal(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_sql_literal).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Object(obj) => {
            let fields: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("'{}': {}", k, json_to_sql_literal(v)))
                .collect();
            format!("{{{}}}", fields.join(", "))
        }
    }
}
