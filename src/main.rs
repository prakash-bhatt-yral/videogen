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
mod routes;
mod upload;
mod webhook;
mod worker;

use config::AppConfig;

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub backend: Arc<dyn backend::VideoGenBackend>,
    pub http_client: reqwest::Client,
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

    let state = AppState {
        config: config.clone(),
        backend,
        http_client: reqwest::Client::new(),
    };

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
