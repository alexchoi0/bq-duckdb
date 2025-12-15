use std::collections::HashMap;

use serde_json::Value;

use crate::error::{Error, Result};
use crate::executor::DuckDbExecutor;

use super::parser::extract_dependencies;
use super::types::{ColumnDef, TableDef, TableKind};

#[derive(Debug, Default)]
pub struct TableRegistry {
    tables: HashMap<String, TableDef>,
}

impl TableRegistry {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn register_derived(&mut self, name: String, sql: String) -> Result<TableDef> {
        let dependencies = extract_dependencies(&sql)?;
        let def = TableDef {
            name: name.clone(),
            kind: TableKind::Derived { sql, dependencies },
        };
        self.tables.insert(name, def.clone());
        Ok(def)
    }

    pub fn register_source(
        &mut self,
        name: String,
        schema: Vec<ColumnDef>,
        rows: Vec<Vec<Value>>,
    ) -> Result<TableDef> {
        if let Some(existing) = self.tables.get_mut(&name) {
            if let TableKind::Source {
                rows: existing_rows,
                ..
            } = &mut existing.kind
            {
                existing_rows.extend(rows);
                return Ok(existing.clone());
            }
        }

        let def = TableDef {
            name: name.clone(),
            kind: TableKind::Source { schema, rows },
        };
        self.tables.insert(name, def.clone());
        Ok(def)
    }

    #[cfg(test)]
    pub fn unregister(&mut self, name: &str) -> Option<TableDef> {
        self.tables.remove(name)
    }

    pub fn get(&self, name: &str) -> Option<&TableDef> {
        self.tables.get(name)
    }

    pub fn all(&self) -> Vec<&TableDef> {
        self.tables.values().collect()
    }

    pub fn clear(&mut self) {
        self.tables.clear();
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    pub fn validate_dependencies(
        &self,
        executor: &DuckDbExecutor,
        schema_name: &str,
    ) -> Result<()> {
        for def in self.tables.values() {
            for dep in def.dependencies() {
                if !self.contains(&dep) && !executor.table_exists(schema_name, &dep)? {
                    return Err(Error::InvalidRequest(format!(
                        "Table '{}' depends on '{}' which is not in DAG or database",
                        def.name, dep
                    )));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_derived() {
        let mut registry = TableRegistry::new();
        let def = registry.register_derived(
            "report".to_string(),
            "SELECT * FROM users".to_string(),
        ).unwrap();

        assert_eq!(def.name, "report");
        assert_eq!(def.dependencies(), vec!["users"]);
    }

    #[test]
    fn test_register_source() {
        let mut registry = TableRegistry::new();
        let schema = vec![
            ColumnDef { name: "id".to_string(), column_type: "INT64".to_string() },
            ColumnDef { name: "name".to_string(), column_type: "STRING".to_string() },
        ];
        let rows = vec![
            vec![Value::from(1), Value::from("Alice")],
            vec![Value::from(2), Value::from("Bob")],
        ];
        let def = registry.register_source("users".to_string(), schema, rows).unwrap();

        assert_eq!(def.name, "users");
        assert!(matches!(def.kind, TableKind::Source { .. }));
        assert!(def.dependencies().is_empty());
    }

    #[test]
    fn test_get_and_contains() {
        let mut registry = TableRegistry::new();
        registry.register_derived("report".to_string(), "SELECT 1".to_string()).unwrap();

        assert!(registry.contains("report"));
        assert!(!registry.contains("nonexistent"));
        assert!(registry.get("report").is_some());
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn test_unregister() {
        let mut registry = TableRegistry::new();
        registry.register_derived("report".to_string(), "SELECT 1".to_string()).unwrap();

        assert!(registry.contains("report"));
        registry.unregister("report");
        assert!(!registry.contains("report"));
    }

    #[test]
    fn test_clear() {
        let mut registry = TableRegistry::new();
        registry.register_derived("a".to_string(), "SELECT 1".to_string()).unwrap();
        registry.register_derived("b".to_string(), "SELECT 2".to_string()).unwrap();

        assert_eq!(registry.len(), 2);
        registry.clear();
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
    }
}
