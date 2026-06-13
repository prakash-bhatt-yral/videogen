use std::sync::Arc;

use anyhow::Result;
use tracing::info;

mod auth;
mod backend;
mod cleanup;
mod completion;
mod config;
mod rabbitmq;
mod routes;
mod upload;
mod webhook;
mod worker;

use config::AppConfig;

// ─── RabbitMQ runtime status ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RabbitMqStatus {
    pub enabled: bool,
    pub status: String, // "connected", "disconnected", "disabled", "connecting"
    pub queue: String,
}

// ─── Application state ───────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub backend: Arc<dyn backend::VideoGenBackend>,
    pub http_client: reqwest::Client,
    pub rabbitmq_status: Arc<tokio::sync::RwLock<RabbitMqStatus>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Load config
    let config = AppConfig::from_env()?;

    // Initialize Sentry
    let _guard = config.sentry_dsn.as_ref().map(|dsn| {
        sentry::init((
            dsn.as_str(),
            sentry::ClientOptions {
                release: sentry::release_name!(),
                traces_sample_rate: 0.2,
                environment: Some(
                    std::env::var("SENTRY_ENVIRONMENT")
                        .unwrap_or_else(|_| "production".into())
                        .into(),
                ),
                ..Default::default()
            },
        ))
    });

    // Initialize tracing
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,videogen_worker=debug".into()),
        )
        .finish();

    if _guard.is_some() {
        use tracing_subscriber::layer::SubscriberExt;
        let subscriber = subscriber.with(sentry_tracing::layer());
        tracing::subscriber::set_global_default(subscriber)?;
    } else {
        tracing::subscriber::set_global_default(subscriber)?;
    }

    // Initialize backend
    let backend: Arc<dyn backend::VideoGenBackend> = match config.backend_type.as_str() {
        "comfyui" => {
            let comfyui =
                backend::comfyui::ComfyUIBackend::new(&config.comfyui_host, config.comfyui_port);
            info!(
                "Using ComfyUI backend at {}:{}",
                config.comfyui_host, config.comfyui_port
            );
            Arc::new(comfyui)
        }
        "stub" => {
            info!(
                "Using stub backend (output_dir={})",
                config.comfyui_output_dir
            );
            Arc::new(backend::stub::StubBackend::new(&config.comfyui_output_dir))
        }
        other => anyhow::bail!("Unknown backend type: {other}"),
    };

    // Shared RabbitMQ status for the health endpoint
    let rabbitmq_status = Arc::new(tokio::sync::RwLock::new(RabbitMqStatus {
        enabled: config.rabbitmq.enabled,
        status: if config.rabbitmq.enabled {
            "connecting".to_string()
        } else {
            "disabled".to_string()
        },
        queue: config.rabbitmq.queue.clone(),
    }));

    let state = AppState {
        config: config.clone(),
        backend: Arc::clone(&backend),
        http_client: reqwest::Client::new(),
        rabbitmq_status: Arc::clone(&rabbitmq_status),
    };

    // Wire RabbitMQ consumer when enabled
    if config.rabbitmq.enabled {
        let auth = config.completion_auth.as_ref().ok_or_else(|| {
            anyhow::anyhow!("VIDEOGEN_CALLBACK_SIGNING_KEY_ID required when rabbitmq enabled")
        })?;
        let hmac_key = crate::completion::CompletionHmacKey::from_str(&auth.key_id, &auth.secret)?;
        let completion_client: Arc<dyn crate::completion::CompletionClient> =
            Arc::new(crate::completion::HmacCompletionClient::new(hmac_key));

        // Build runtime worker backend
        let worker_backend: Arc<dyn crate::worker::WorkerBackend> =
            match config.backend_type.as_str() {
                "stub" => Arc::new(backend::stub::StubBackend::new(&config.comfyui_output_dir)),
                _ => Arc::new(backend::comfyui::ComfyUIBackend::new(
                    &config.comfyui_host,
                    config.comfyui_port,
                )),
            };

        // Build runtime uploader
        let worker_uploader: Arc<dyn crate::worker::VideoUploader> =
            Arc::new(crate::upload::RuntimeVideoUploader::new(
                config.upload.clone(),
                Arc::clone(&completion_client),
            ));

        let real_worker = Arc::new(crate::worker::RuntimeDeliveryWorker {
            backend: worker_backend,
            uploader: worker_uploader,
            client: Arc::clone(&completion_client),
        });

        // Spawn consumer
        let status_writer = Arc::clone(&rabbitmq_status);
        let consumer_config = config.rabbitmq.clone();

        tokio::spawn(async move {
            match crate::rabbitmq::consumer::spawn_consumer(&consumer_config, real_worker).await {
                Ok(handle) => {
                    status_writer.write().await.status = "connected".to_string();
                    handle.await.ok();
                    // Consumer loop exited — mark disconnected
                    status_writer.write().await.status = "disconnected".to_string();
                }
                Err(e) => {
                    tracing::error!(error = %e, "RabbitMQ consumer failed to start");
                    status_writer.write().await.status = "disconnected".to_string();
                }
            }
        });
    }

    // Build router
    let app = routes::build_router(state);

    // Spawn background cleanup task
    tokio::spawn(cleanup::start_cleanup_task(config.clone()));

    let addr = format!("0.0.0.0:{}", config.port);
    info!("Starting videogen-worker on {addr}");
    info!("Backend: {}", config.backend_type);
    info!(
        "Auth: {}",
        if config.auth_token.is_some() {
            "enabled"
        } else {
            "disabled"
        }
    );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
