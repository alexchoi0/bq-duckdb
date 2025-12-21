use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Executor error: {0}")]
    Executor(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Session not found: {0}")]
    SessionNotFound(uuid::Uuid),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Loader error: {0}")]
    Loader(String),
}

impl Error {
    pub fn code(&self) -> i32 {
        match self {
            Error::Executor(_) => -32000,
            Error::Json(_) => -32700,
            Error::SessionNotFound(_) => -32002,
            Error::InvalidRequest(_) => -32600,
            Error::Internal(_) => -32603,
            Error::Loader(_) => -32001,
        }
    }

    pub fn with_context(self, method: &str, session_id: Option<&str>) -> Self {
        let context = match session_id {
            Some(sid) => format!("[method={}, session={}]", method, sid),
            None => format!("[method={}]", method),
        };

        match self {
            Error::Executor(msg) => Error::Executor(format!("{} {}", context, msg)),
            Error::Internal(msg) => Error::Internal(format!("{} {}", context, msg)),
            Error::Loader(msg) => Error::Loader(format!("{} {}", context, msg)),
            other => other,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
