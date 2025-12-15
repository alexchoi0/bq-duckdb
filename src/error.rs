use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("SQL parse error: {0}")]
    SqlParse(#[from] sqlparser::parser::ParserError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Session not found: {0}")]
    SessionNotFound(uuid::Uuid),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("SQL transformation error: {0}")]
    SqlTransform(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl Error {
    pub fn code(&self) -> i32 {
        match self {
            Error::DuckDb(_) => -32000,
            Error::SqlParse(_) => -32001,
            Error::Json(_) => -32700,
            Error::SessionNotFound(_) => -32002,
            Error::InvalidRequest(_) => -32600,
            Error::SqlTransform(_) => -32003,
            Error::Internal(_) => -32603,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
