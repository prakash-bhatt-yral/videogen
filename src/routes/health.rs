use axum::{extract::State, Json};
use serde_json::json;

use crate::{backend::HealthResponse, AppState};

/// Check backend health
#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Health status", body = HealthResponse),
    ),
    security(("bearer" = [])),
    tag = "System"
)]
pub async fn handle_health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let health = state.backend.health_check().await;
    let rmq = state.rabbitmq_status.read().await;
    let base = match health {
        Ok(h) => serde_json::to_value(h).unwrap_or_default(),
        Err(e) => json!({
            "status": "error",
            "backend": state.backend.name(),
            "error": e.to_string(),
        }),
    };
    let mut obj = base.as_object().cloned().unwrap_or_default();
    obj.insert(
        "rabbitmq".to_string(),
        json!({
            "enabled": rmq.enabled,
            "status": rmq.status,
            "queue": rmq.queue,
        }),
    );
    Json(serde_json::Value::Object(obj))
}

/// Service info
#[utoipa::path(
    get,
    path = "/",
    responses(
        (status = 200, description = "Service info"),
    ),
    tag = "System"
)]
pub async fn handle_root(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({
        "service": "videogen-worker",
        "version": env!("CARGO_PKG_VERSION"),
        "backend": state.backend.name(),
        "endpoints": ["/generate", "/result/{id}", "/upload/image", "/view", "/health", "/swagger-ui"],
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use std::sync::Arc;

    use crate::RabbitMqStatus;

    fn test_state_disabled() -> AppState {
        AppState {
            config: {
                let env = std::collections::HashMap::new();
                crate::config::AppConfig::from_env_map(&env).unwrap()
            },
            backend: Arc::new(crate::backend::comfyui::ComfyUIBackend::new(
                "127.0.0.1",
                18188,
            )),
            http_client: reqwest::Client::new(),
            rabbitmq_status: Arc::new(tokio::sync::RwLock::new(RabbitMqStatus {
                enabled: false,
                status: "disabled".to_string(),
                queue: "".to_string(),
            })),
        }
    }

    #[tokio::test]
    async fn health_includes_rabbitmq_status() {
        let state = test_state_disabled();
        let Json(body) = handle_health(State(state)).await;
        assert_eq!(body["rabbitmq"]["enabled"], false);
        assert_eq!(body["rabbitmq"]["status"], "disabled");
    }
}
