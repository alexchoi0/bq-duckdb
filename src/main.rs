use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{State, WebSocketUpgrade},
    response::{Response, IntoResponse},
    routing::get,
    Json, Router,
};
use clap::Parser;
use serde_json::json;
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

mod dag;
mod error;
mod executor;
mod rpc;
mod session;
mod sql;

use executor::{DuckDbExecutor, BigQueryExecutor, ExecutionMode, QueryExecutor};
use rpc::{handle_websocket, RpcMethods};
use session::SessionManager;

#[derive(Parser)]
#[command(name = "bq-runner")]
#[command(about = "BigQuery runner with mock, interactive, and batch modes")]
struct Args {
    #[arg(long, default_value = "mock")]
    mode: String,

    #[arg(long)]
    project: Option<String>,

    #[arg(long, default_value = "3000")]
    port: u16,
}

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

    let args = Args::parse();

    let mode: ExecutionMode = args.mode.parse().map_err(|e: String| anyhow::anyhow!(e))?;

    let executor: Arc<dyn QueryExecutor> = match mode {
        ExecutionMode::Mock => {
            info!("Starting in mock mode (DuckDB)");
            Arc::new(DuckDbExecutor::new()?)
        }
        ExecutionMode::Interactive | ExecutionMode::Batch => {
            let project = args.project.ok_or_else(|| {
                anyhow::anyhow!("--project is required for interactive/batch modes")
            })?;
            info!("Starting in {:?} mode with project: {}", mode, project);
            Arc::new(BigQueryExecutor::new(project, mode).await?)
        }
    };

    let session_manager = Arc::new(SessionManager::new(executor));
    let methods = Arc::new(RpcMethods::new(session_manager));

    let state = AppState { methods };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/health", get(health_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    info!("Starting bq-runner on {}", addr);

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
