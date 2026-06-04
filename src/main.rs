use std::sync::Arc;

use anyhow::Result;
use tracing::info;

mod auth;
mod backend;
mod cleanup;
mod completion;
mod config;
mod jobs;
mod rabbitmq;
mod recovery;
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
    pub job_store: Option<Arc<crate::jobs::JobStore>>,
    pub rabbitmq_status: Arc<tokio::sync::RwLock<RabbitMqStatus>>,
}

#[tokio::main]
async fn main() -> Result<()> {
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
        other => anyhow::bail!("Unknown backend type: {other}"),
    };

    // Open job store (only when RabbitMQ is enabled)
    let job_store: Option<Arc<crate::jobs::JobStore>> = if config.rabbitmq.enabled {
        let store = Arc::new(crate::jobs::JobStore::open(&config.worker_state.db_path).await?);
        Some(store)
    } else {
        None
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
        job_store: job_store.clone(),
        rabbitmq_status: Arc::clone(&rabbitmq_status),
    };

    // Wire RabbitMQ consumer + outbox runner when enabled
    if config.rabbitmq.enabled {
        let auth = config.completion_auth.as_ref().ok_or_else(|| {
            anyhow::anyhow!("PRAKASH_COMPLETION_HMAC_KEY_ID required when rabbitmq enabled")
        })?;
        let hmac_key =
            crate::completion::CompletionHmacKey::from_base64(&auth.key_id, &auth.secret_b64)?;
        let prakash_client: Arc<dyn crate::completion::PrakashCompletionClient> =
            Arc::new(crate::completion::HmacPrakashClient::new(hmac_key));

        let store = job_store
            .as_ref()
            .expect("job_store is Some when rabbitmq enabled")
            .clone();

        // Spawn outbox runner
        let outbox_store = Arc::clone(&store);
        let outbox_client = Arc::clone(&prakash_client);
        tokio::spawn(async move {
            crate::recovery::run_outbox_loop(outbox_store, outbox_client).await;
        });

        // Build runtime worker
        let worker_backend: Arc<dyn crate::worker::WorkerBackend> = {
            // Downcast is not possible on trait objects; instead we re-construct
            // ComfyUIBackend for the worker path (separate from the HTTP backend).
            // This is safe: both share no in-memory state that would cause conflicts.
            let w: Arc<dyn crate::worker::WorkerBackend> = Arc::new(
                backend::comfyui::ComfyUIBackend::new(&config.comfyui_host, config.comfyui_port),
            );
            w
        };

        let real_worker = Arc::new(crate::worker::RuntimeDeliveryWorker {
            store: Arc::clone(&store),
            backend: worker_backend,
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
