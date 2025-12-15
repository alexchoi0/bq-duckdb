use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
}

#[derive(Debug, Clone)]
pub enum TableKind {
    Derived {
        sql: String,
        dependencies: Vec<String>,
    },
    Source {
        schema: Vec<ColumnDef>,
        rows: Vec<Vec<Value>>,
    },
}

#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub kind: TableKind,
}

impl TableDef {
    pub fn dependencies(&self) -> Vec<String> {
        match &self.kind {
            TableKind::Derived { dependencies, .. } => dependencies.clone(),
            TableKind::Source { .. } => vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct DagRunResult {
    pub executed_tables: Vec<String>,
}
