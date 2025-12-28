use std::path::Path;

use crate::error::{Error, Result};
use crate::rpc::types::ColumnDef;

#[derive(Debug, Clone)]
pub struct SqlFile {
    pub project: String,
    pub dataset: String,
    pub table: String,
    pub path: String,
    pub sql: String,
}

#[derive(Debug, Clone)]
pub struct ParquetFile {
    pub project: String,
    pub dataset: String,
    pub table: String,
    pub path: String,
    pub schema: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct DiscoveredFiles {
    pub sql_files: Vec<SqlFile>,
    pub parquet_files: Vec<ParquetFile>,
}

pub fn discover_files(root_path: &str) -> Result<DiscoveredFiles> {
    let root = Path::new(root_path);
    if !root.is_dir() {
        return Err(Error::Executor(format!(
            "Root path is not a directory: {}",
            root_path
        )));
    }

    let mut sql_files = Vec::new();
    let mut parquet_files = Vec::new();

    for project_entry in read_dir(root)? {
        let project_path = project_entry.path();
        if !project_path.is_dir() {
            continue;
        }
        let project_name = project_entry.file_name().to_string_lossy().to_string();

        for dataset_entry in read_dir(&project_path)? {
            let dataset_path = dataset_entry.path();
            if !dataset_path.is_dir() {
                continue;
            }
            let dataset_name = dataset_entry.file_name().to_string_lossy().to_string();

            for table_entry in read_dir(&dataset_path)? {
                let table_path = table_entry.path();
                if !table_path.is_file() {
                    continue;
                }
                let file_name = table_entry.file_name().to_string_lossy().to_string();

                if file_name.ends_with(".sql") {
                    let table_name = file_name.trim_end_matches(".sql").to_string();
                    let sql = read_file(&table_path)?;

                    sql_files.push(SqlFile {
                        project: project_name.clone(),
                        dataset: dataset_name.clone(),
                        table: table_name,
                        path: table_path.to_string_lossy().to_string(),
                        sql,
                    });
                } else if file_name.ends_with(".parquet") {
                    let table_name = file_name.trim_end_matches(".parquet").to_string();
                    let schema = load_schema(&dataset_path, &table_name)?;

                    parquet_files.push(ParquetFile {
                        project: project_name.clone(),
                        dataset: dataset_name.clone(),
                        table: table_name,
                        path: table_path.to_string_lossy().to_string(),
                        schema,
                    });
                }
            }
        }
    }

    Ok(DiscoveredFiles {
        sql_files,
        parquet_files,
    })
}

pub fn discover_sql_files(root_path: &str) -> Result<Vec<SqlFile>> {
    let discovered = discover_files(root_path)?;
    Ok(discovered.sql_files)
}

pub fn discover_parquet_files(root_path: &str) -> Result<Vec<ParquetFile>> {
    let discovered = discover_files(root_path)?;
    Ok(discovered.parquet_files)
}

fn read_dir(path: &Path) -> Result<Vec<std::fs::DirEntry>> {
    std::fs::read_dir(path)
        .map_err(|e| Error::Executor(format!("Failed to read directory: {}", e)))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Executor(format!("Failed to read entry: {}", e)))
}

fn read_file(path: &Path) -> Result<String> {
    std::fs::read_to_string(path)
        .map_err(|e| Error::Executor(format!("Failed to read file {}: {}", path.display(), e)))
}

fn load_schema(dataset_path: &Path, table_name: &str) -> Result<Vec<ColumnDef>> {
    let schema_path = dataset_path.join(format!("{}.schema.json", table_name));
    if !schema_path.exists() {
        return Err(Error::Executor(format!(
            "Schema file not found: {}",
            schema_path.display()
        )));
    }

    let schema_content = read_file(&schema_path)?;
    serde_json::from_str(&schema_content)
        .map_err(|e| Error::Executor(format!("Failed to parse schema: {}", e)))
}

impl SqlFile {
    pub fn full_table_name(&self) -> String {
        format!("{}.{}.{}", self.project, self.dataset, self.table)
    }
}

impl ParquetFile {
    pub fn full_table_name(&self) -> String {
        format!("{}.{}.{}", self.project, self.dataset, self.table)
    }
}
