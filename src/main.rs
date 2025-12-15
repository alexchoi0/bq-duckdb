use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{State, WebSocketUpgrade},
    response::{Response, IntoResponse},
    routing::get,
    Json, Router,
};
use serde_json::json;
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

mod dag;
mod error;
mod executor;
mod rpc;
mod session;
mod sql;

use executor::DuckDbExecutor;
use rpc::{handle_websocket, RpcMethods};
use session::SessionManager;

#[derive(Clone)]
struct AppState {
    methods: Arc<RpcMethods>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let executor = Arc::new(DuckDbExecutor::new()?);
    let session_manager = Arc::new(SessionManager::new(executor));
    let methods = Arc::new(RpcMethods::new(session_manager));

    let state = AppState { methods };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/health", get(health_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    info!("Starting BQ-DuckDB emulator on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.max_message_size(usize::MAX)
        .on_upgrade(move |socket| handle_websocket(socket, state.methods))
}

async fn health_handler() -> impl IntoResponse {
    Json(json!({"status": "ok", "message": "pong"}))
}
