pub mod generate;
pub mod health;
pub mod upload;
pub mod view;

use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    auth,
    backend::{
        GenerateInput, GenerateRequest, GenerateResponse, HealthResponse, JobStatus, UploadResponse,
    },
    webhook::{OutputFile, WebhookConfig},
    AppState,
};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Videogen Worker API",
        version = "0.1.0",
        description = "Video generation worker with pluggable backend adapters. Currently supports ComfyUI.",
        contact(name = "dolr-ai"),
    ),
    paths(
        generate::handle_generate,
        generate::handle_get_result,
        upload::handle_upload_image,
        view::handle_view,
        health::handle_health,
        health::handle_root,
    ),
    components(schemas(
        GenerateRequest,
        GenerateInput,
        GenerateResponse,
        JobStatus,
        UploadResponse,
        HealthResponse,
        WebhookConfig,
        OutputFile,
    )),
    modifiers(&SecurityAddon),
    tags(
        (name = "Video Generation", description = "Submit and track video generation jobs"),
        (name = "Files", description = "Upload images and download output files"),
        (name = "System", description = "Health checks and service info"),
    )
)]
struct ApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer",
                utoipa::openapi::security::SecurityScheme::Http(
                    utoipa::openapi::security::Http::new(
                        utoipa::openapi::security::HttpAuthScheme::Bearer,
                    ),
                ),
            );
        }
    }
}

pub fn build_router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/generate", post(generate::handle_generate))
        .route("/result/{job_id}", get(generate::handle_get_result))
        .route("/upload/image", post(upload::handle_upload_image))
        .route("/view", get(view::handle_view))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth::auth_middleware,
        ));

    let public = Router::new()
        .route("/health", get(health::handle_health))
        .route("/", get(health::handle_root))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()));

    Router::new()
        .merge(protected)
        .merge(public)
        .with_state(state)
}
