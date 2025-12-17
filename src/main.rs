use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{State, WebSocketUpgrade},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use clap::{Parser, ValueEnum};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{error, info, Level};
use tracing_subscriber::EnvFilter;

mod error;
mod executor;
mod rpc;
mod session;

use executor::ExecutorMode;
use rpc::{handle_websocket, process_message, RpcMethods};
use session::SessionManager;

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum Backend {
    #[default]
    Mock,
    Bigquery,
}

impl From<Backend> for ExecutorMode {
    fn from(backend: Backend) -> Self {
        match backend {
            Backend::Mock => ExecutorMode::Mock,
            Backend::Bigquery => ExecutorMode::BigQuery,
        }
    }
}

#[derive(Parser)]
#[command(name = "bq-runner")]
#[command(about = "BigQuery runner with mock and real BigQuery modes")]
struct Args {
    #[arg(long, default_value = "3000")]
    port: u16,

    #[arg(long, help = "Use stdio transport (read JSON-RPC from stdin, write to stdout)")]
    stdio: bool,

    #[arg(long, value_enum, default_value = "mock", help = "Execution backend: mock (YachtSQL) or bigquery (real BigQuery)")]
    backend: Backend,
}

#[derive(Clone)]
struct AppState {
    methods: Arc<RpcMethods>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let executor_mode: ExecutorMode = args.backend.into();
    let session_manager = Arc::new(SessionManager::with_mode(executor_mode));
    let methods = Arc::new(RpcMethods::new(session_manager));

    if args.stdio {
        run_stdio_server(methods).await
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::builder()
                    .with_default_directive(Level::INFO.into())
                    .from_env_lossy(),
            )
            .init();

        match executor_mode {
            ExecutorMode::Mock => info!("Starting with mock backend (YachtSQL)"),
            ExecutorMode::BigQuery => info!("Starting with BigQuery backend"),
        }

        run_http_server(args.port, methods).await
    }
}

async fn run_stdio_server(methods: Arc<RpcMethods>) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = reader.next_line().await {
        if line.trim().is_empty() {
            continue;
        }

        let response = process_message(&line, &methods).await;

        match serde_json::to_string(&response) {
            Ok(response_text) => {
                if let Err(e) = stdout.write_all(response_text.as_bytes()).await {
                    error!("Failed to write response: {}", e);
                    break;
                }
                if let Err(e) = stdout.write_all(b"\n").await {
                    error!("Failed to write newline: {}", e);
                    break;
                }
                if let Err(e) = stdout.flush().await {
                    error!("Failed to flush stdout: {}", e);
                    break;
                }
            }
            Err(e) => {
                error!("Failed to serialize response: {}", e);
            }
        }
    }

    Ok(())
}

async fn run_http_server(port: u16, methods: Arc<RpcMethods>) -> anyhow::Result<()> {
    let state = AppState { methods };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/health", get(health_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
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
