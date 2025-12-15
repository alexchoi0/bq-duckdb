use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tracing::{error, info};

use super::methods::RpcMethods;
use super::types::{RpcRequest, RpcResponse};

pub async fn handle_websocket(socket: WebSocket, methods: Arc<RpcMethods>) {
    let (mut sender, mut receiver) = socket.split();

    while let Some(msg) = receiver.next().await {
        let msg = match msg {
            Ok(Message::Text(text)) => text,
            Ok(Message::Close(_)) => {
                info!("WebSocket closed by client");
                break;
            }
            Ok(_) => continue,
            Err(e) => {
                error!("WebSocket error: {}", e);
                break;
            }
        };

        let response = process_message(&msg, &methods).await;

        let response_text = match serde_json::to_string(&response) {
            Ok(text) => text,
            Err(e) => {
                error!("Failed to serialize response: {}", e);
                continue;
            }
        };

        if sender.send(Message::Text(response_text.into())).await.is_err() {
            error!("Failed to send response");
            break;
        }
    }
}

async fn process_message(msg: &str, methods: &RpcMethods) -> RpcResponse {
    let request: RpcRequest = match serde_json::from_str(msg) {
        Ok(req) => req,
        Err(_) => return RpcResponse::parse_error(),
    };

    if request.jsonrpc != "2.0" {
        return RpcResponse::invalid_request();
    }

    let id = request.id.clone().unwrap_or(Value::Null);

    match methods.dispatch(&request.method, request.params).await {
        Ok(result) => RpcResponse::success(id, result),
        Err(e) => {
            if matches!(e, crate::error::Error::InvalidRequest(ref msg) if msg.starts_with("Unknown method")) {
                RpcResponse::method_not_found(id, &request.method)
            } else {
                RpcResponse::error(id, e.code(), e.to_string())
            }
        }
    }
}
