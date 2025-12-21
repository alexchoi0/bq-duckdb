use std::fs;
use std::path::{Path, PathBuf};

use glob::glob;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct LoadedFile {
    pub name: String,
    pub content: String,
    pub path: PathBuf,
}

pub type SqlFile = LoadedFile;

pub struct FileLoader;

impl FileLoader {
    pub fn load_dir(path: impl AsRef<Path>, extension: &str) -> Result<Vec<LoadedFile>> {
        let pattern = path.as_ref().join(format!("**/*.{}", extension));
        let pattern_str = pattern.to_string_lossy();

        let files: Vec<PathBuf> = glob(&pattern_str)
            .map_err(|e| Error::Loader(format!("Invalid glob pattern: {}", e)))?
            .filter_map(|r| r.ok())
            .collect();

        files
            .into_iter()
            .map(|file_path| Self::load_file(&file_path))
            .collect()
    }

    pub fn load_file(path: impl AsRef<Path>) -> Result<LoadedFile> {
        let path = path.as_ref();

        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| Error::Loader(format!("Invalid filename: {}", path.display())))?
            .to_string();

        let content = fs::read_to_string(path)
            .map_err(|e| Error::Loader(format!("Failed to read {}: {}", path.display(), e)))?;

        Ok(LoadedFile {
            name,
            content,
            path: path.to_path_buf(),
        })
    }
}

pub struct SqlLoader;

impl SqlLoader {
    pub fn load_dir(path: impl AsRef<Path>) -> Result<Vec<SqlFile>> {
        FileLoader::load_dir(path, "sql")
    }

    pub fn load_file(path: impl AsRef<Path>) -> Result<SqlFile> {
        FileLoader::load_file(path)
    }
}
