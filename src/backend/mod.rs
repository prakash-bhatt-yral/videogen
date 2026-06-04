pub mod comfyui;
pub mod stub;

use anyhow::Result;
use axum::body::Bytes;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::webhook::{OutputFile, WebhookConfig};

/// Returned after ComfyUI accepts a workflow submission (before generation completes).
#[derive(Debug, Clone)]
pub struct AcceptedGeneration {
    pub request_id: String,
    pub prompt_id: String,
    pub client_id: String,
}

/// Returned after generation finishes — contains the raw output files.
#[derive(Debug, Clone)]
pub struct CompletedGeneration {
    pub prompt_id: String,
    pub outputs: Vec<crate::webhook::OutputFile>,
}

/// Request to generate a video
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GenerateRequest {
    pub input: GenerateInput,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GenerateInput {
    /// Unique request identifier
    pub request_id: String,
    /// ComfyUI workflow JSON
    pub workflow_json: serde_json::Value,
    /// Optional webhook to call on completion
    #[serde(default)]
    pub webhook: Option<WebhookConfig>,
}

/// Response after submitting a generation job
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GenerateResponse {
    /// Job identifier
    pub id: String,
    /// Current status (accepted, pending, processing, completed, failed)
    pub status: String,
    /// Optional status message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Job status response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobStatus {
    /// Job identifier
    pub id: String,
    /// Current status
    pub status: String,
    /// Error or info message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Output files (available when status=completed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Vec<OutputFile>>,
    /// Progress percentage (0-100)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<f64>,
}

/// Upload response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UploadResponse {
    /// Filename as stored by the backend
    pub name: String,
}

/// Health check response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    /// Overall status: healthy, degraded, unhealthy
    pub status: String,
    /// Backend adapter name
    pub backend: String,
    /// Backend-specific details
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// The core trait that all video generation backends must implement.
///
/// ComfyUI is the first adapter. Future adapters (e.g. LTX hosted API,
/// RunPod, local inference) implement this same trait.
#[async_trait::async_trait]
pub trait VideoGenBackend: Send + Sync {
    /// Submit a workflow for execution.
    /// Returns immediately with a job ID. Monitors in background and
    /// sends webhook on completion if configured.
    async fn generate(
        &self,
        request: GenerateRequest,
        http_client: &reqwest::Client,
    ) -> Result<GenerateResponse>;

    /// Get the status of a previously submitted job.
    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>>;

    /// Upload an image to the backend's input storage.
    /// Returns the filename/identifier to reference in workflows.
    async fn upload_image(
        &self,
        filename: &str,
        data: Bytes,
        content_type: &str,
    ) -> Result<UploadResponse>;

    /// Stream/proxy a file from the backend's output storage.
    async fn get_file(
        &self,
        filename: &str,
        subfolder: Option<&str>,
        file_type: Option<&str>,
    ) -> Result<(axum::http::HeaderMap, Bytes)>;

    /// Check if the backend is healthy and reachable.
    async fn health_check(&self) -> Result<HealthResponse>;

    /// Return the backend name (e.g. "comfyui", "ltx-api").
    fn name(&self) -> &str;
}
