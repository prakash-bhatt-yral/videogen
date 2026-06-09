use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use axum::body::Bytes;

use super::{
    AcceptedGeneration, CompletedGeneration, GenerateRequest, GenerateResponse, HealthResponse,
    JobStatus, UploadResponse, VideoGenBackend,
};
use crate::webhook::OutputFile;
use crate::worker::WorkerBackend;

/// Stub backend for local E2E testing without ComfyUI or GPU.
/// Immediately "accepts" any workflow and returns a placeholder MP4 file.
pub struct StubBackend {
    output_dir: String,
    counter: Arc<AtomicU64>,
}

impl StubBackend {
    pub fn new(output_dir: &str) -> Self {
        Self {
            output_dir: output_dir.to_string(),
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    fn next_id(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }
}

// ─── WorkerBackend (RabbitMQ consumer path) ──────────────────────────────────

#[async_trait::async_trait]
impl WorkerBackend for StubBackend {
    async fn submit_workflow(
        &self,
        request_id: &str,
        _workflow_json: serde_json::Value,
    ) -> anyhow::Result<AcceptedGeneration> {
        Ok(AcceptedGeneration {
            request_id: request_id.to_string(),
            prompt_id: format!("stub-{request_id}"),
            client_id: "stub-client".to_string(),
        })
    }

    async fn monitor_generation(
        &self,
        accepted: &AcceptedGeneration,
        _timeout_secs: u64,
    ) -> anyhow::Result<CompletedGeneration> {
        let filename = format!("stub-{}.mp4", self.next_id());
        let path = std::path::Path::new(&self.output_dir).join(&filename);

        // Write a real minimal MP4 so the upload service can extract a thumbnail.
        static STUB_MP4: &[u8] = include_bytes!("../../fixtures/stub.mp4");
        tokio::fs::create_dir_all(&self.output_dir).await?;
        tokio::fs::write(&path, STUB_MP4).await?;

        tracing::info!(
            request_id = %accepted.request_id,
            path = %path.display(),
            "stub backend: wrote placeholder output"
        );

        Ok(CompletedGeneration {
            prompt_id: accepted.prompt_id.clone(),
            outputs: vec![OutputFile {
                filename,
                local_path: Some(path.to_string_lossy().into_owned()),
                url: None,
                subfolder: None,
                node_id: None,
                output_type: Some("videos".to_string()),
            }],
        })
    }
}

// ─── VideoGenBackend (HTTP routes) ───────────────────────────────────────────

#[async_trait::async_trait]
impl VideoGenBackend for StubBackend {
    async fn generate(
        &self,
        request: GenerateRequest,
        _http_client: &reqwest::Client,
    ) -> Result<GenerateResponse> {
        Ok(GenerateResponse {
            id: format!("stub-{}", request.input.request_id),
            status: "accepted".to_string(),
            message: Some("stub backend — no real generation".to_string()),
        })
    }

    async fn get_job_status(&self, _job_id: &str) -> Result<Option<JobStatus>> {
        Ok(None)
    }

    async fn upload_image(
        &self,
        filename: &str,
        _data: Bytes,
        _content_type: &str,
    ) -> Result<UploadResponse> {
        Ok(UploadResponse {
            name: filename.to_string(),
        })
    }

    async fn get_file(
        &self,
        _filename: &str,
        _subfolder: Option<&str>,
        _file_type: Option<&str>,
    ) -> Result<(axum::http::HeaderMap, Bytes)> {
        Ok((axum::http::HeaderMap::new(), Bytes::from_static(b"")))
    }

    async fn health_check(&self) -> Result<HealthResponse> {
        Ok(HealthResponse {
            status: "ok".to_string(),
            backend: self.name().to_string(),
            details: None,
        })
    }

    fn name(&self) -> &str {
        "stub"
    }
}
