mod client;
mod types;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use anyhow::{Context, Result};
use axum::body::Bytes;
use tracing::{error, info, warn};

use super::{
    AcceptedGeneration, CompletedGeneration, GenerateRequest, GenerateResponse, HealthResponse,
    JobStatus, UploadResponse, VideoGenBackend,
};
use crate::webhook;
use client::ComfyUIClient;

/// ComfyUI backend adapter
pub struct ComfyUIBackend {
    client: ComfyUIClient,
    jobs: Arc<RwLock<HashMap<String, JobState>>>,
}

/// Internal job tracking state
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct JobState {
    pub id: String,
    pub prompt_id: String,
    pub status: String,
    pub progress: Option<f64>,
    pub output: Option<Vec<webhook::OutputFile>>,
    pub message: Option<String>,
}

impl ComfyUIBackend {
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            client: ComfyUIClient::new(host, port),
            jobs: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl VideoGenBackend for ComfyUIBackend {
    async fn generate(
        &self,
        request: GenerateRequest,
        http_client: &reqwest::Client,
    ) -> Result<GenerateResponse> {
        let job_id = request.input.request_id.clone();
        let client_id = uuid::Uuid::new_v4().to_string();

        info!(job_id, "Submitting workflow to ComfyUI");

        // Submit to ComfyUI
        let prompt_id = self
            .client
            .queue_prompt(&request.input.workflow_json, &client_id)
            .await?;

        // Track the job
        let job_state = JobState {
            id: job_id.clone(),
            prompt_id: prompt_id.clone(),
            status: "pending".into(),
            progress: Some(0.0),
            output: None,
            message: None,
        };
        self.jobs.write().await.insert(job_id.clone(), job_state);

        // Spawn background monitor
        let jobs = self.jobs.clone();
        let client = self.client.clone();
        let webhook_config = request.input.webhook.clone();
        let http = http_client.clone();
        let jid = job_id.clone();
        let pid = prompt_id.clone();

        tokio::spawn(async move {
            let result = client.monitor_job(&jid, &pid, &client_id, &jobs).await;

            match result {
                Ok(outputs) => {
                    // Update job state
                    if let Some(job) = jobs.write().await.get_mut(&jid) {
                        job.status = "completed".into();
                        job.output = Some(outputs.clone());
                    }
                    info!(job_id = jid, outputs = outputs.len(), "Job completed");

                    // Send webhook
                    if let Some(ref wh) = webhook_config {
                        if let Err(e) =
                            webhook::send_webhook(&http, wh, &jid, "completed", Some(outputs), None)
                                .await
                        {
                            error!(job_id = jid, error = %e, "Failed to send completion webhook");
                        }
                    }
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    if let Some(job) = jobs.write().await.get_mut(&jid) {
                        job.status = "failed".into();
                        job.message = Some(error_msg.clone());
                    }
                    error!(job_id = jid, error = %e, "Job failed");

                    // Send failure webhook
                    if let Some(ref wh) = webhook_config {
                        if let Err(e) =
                            webhook::send_webhook(&http, wh, &jid, "failed", None, Some(error_msg))
                                .await
                        {
                            error!(job_id = jid, error = %e, "Failed to send failure webhook");
                        }
                    }
                }
            }
        });

        Ok(GenerateResponse {
            id: job_id,
            status: "accepted".into(),
            message: Some(format!("Job queued with prompt_id={prompt_id}")),
        })
    }

    async fn get_job_status(&self, job_id: &str) -> Result<Option<JobStatus>> {
        let jobs = self.jobs.read().await;
        Ok(jobs.get(job_id).map(|j| JobStatus {
            id: j.id.clone(),
            status: j.status.clone(),
            message: j.message.clone(),
            output: j.output.clone(),
            progress: j.progress,
        }))
    }

    async fn upload_image(
        &self,
        filename: &str,
        data: Bytes,
        content_type: &str,
    ) -> Result<UploadResponse> {
        self.client.upload_image(filename, data, content_type).await
    }

    async fn get_file(
        &self,
        filename: &str,
        subfolder: Option<&str>,
        file_type: Option<&str>,
    ) -> Result<(axum::http::HeaderMap, Bytes)> {
        self.client.get_file(filename, subfolder, file_type).await
    }

    async fn health_check(&self) -> Result<HealthResponse> {
        self.client.health_check().await
    }

    fn name(&self) -> &str {
        "comfyui"
    }
}

impl ComfyUIBackend {
    /// Download any URL-valued `image` inputs in LoadImage nodes and re-upload them
    /// to ComfyUI so the workflow can reference them by local filename.
    async fn resolve_image_urls(
        &self,
        mut workflow: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        if let Some(obj) = workflow.as_object_mut() {
            for (_node_id, node) in obj.iter_mut() {
                if node.get("class_type").and_then(|v| v.as_str()) != Some("LoadImage") {
                    continue;
                }
                let url = match node
                    .get("inputs")
                    .and_then(|i| i.get("image"))
                    .and_then(|v| v.as_str())
                {
                    Some(s) if s.starts_with("http://") || s.starts_with("https://") => {
                        s.to_string()
                    }
                    _ => continue,
                };

                info!(url = %url, "downloading LoadImage URL for ComfyUI upload");
                let resp = reqwest::get(&url)
                    .await
                    .with_context(|| format!("failed to download image: {url}"))?;
                let content_type = resp
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("image/jpeg")
                    .to_string();
                let bytes = resp.bytes().await.context("failed to read image bytes")?;
                let ext = if content_type.contains("png") {
                    "png"
                } else {
                    "jpg"
                };
                let filename = format!("{}.{ext}", uuid::Uuid::new_v4());
                let upload = self
                    .client
                    .upload_image(&filename, bytes, &content_type)
                    .await
                    .context("failed to upload image to ComfyUI")?;
                info!(original_url = %url, uploaded_as = %upload.name, "image uploaded to ComfyUI");
                node["inputs"]["image"] = serde_json::Value::String(upload.name);
            }
        }
        Ok(workflow)
    }
}

#[async_trait::async_trait]
impl crate::worker::WorkerBackend for ComfyUIBackend {
    async fn submit_workflow(
        &self,
        request_id: &str,
        workflow_json: serde_json::Value,
    ) -> anyhow::Result<AcceptedGeneration> {
        info!(request_id, "resolving LoadImage URLs in workflow");
        let workflow_json = self.resolve_image_urls(workflow_json).await?;
        let client_id = uuid::Uuid::new_v4().to_string();
        let prompt_id = self.client.queue_prompt(&workflow_json, &client_id).await?;
        info!(request_id, prompt_id = %prompt_id, "workflow queued in ComfyUI");

        // Track the job in the in-memory state map
        let job_state = JobState {
            id: request_id.to_string(),
            prompt_id: prompt_id.clone(),
            status: "pending".into(),
            progress: Some(0.0),
            output: None,
            message: None,
        };
        self.jobs
            .write()
            .await
            .insert(request_id.to_string(), job_state);

        Ok(AcceptedGeneration {
            request_id: request_id.to_string(),
            prompt_id,
            client_id,
        })
    }

    async fn monitor_generation(
        &self,
        accepted: &AcceptedGeneration,
        timeout_secs: u64,
    ) -> anyhow::Result<CompletedGeneration> {
        let outputs = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            self.client.monitor_job(
                &accepted.request_id,
                &accepted.prompt_id,
                &accepted.client_id,
                &self.jobs,
            ),
        )
        .await
        .map_err(|_| anyhow::anyhow!("generation timed out after {timeout_secs}s"))??;
        info!(
            request_id = %accepted.request_id,
            prompt_id = %accepted.prompt_id,
            "ComfyUI generation finished, downloading outputs"
        );

        // Download video outputs to local temp files so the uploader has a path to read.
        let mut resolved = Vec::with_capacity(outputs.len());
        for mut output in outputs {
            if output.output_type.as_deref() == Some("videos") {
                let tmp_path = format!(
                    "/tmp/videogen-{}-{}.mp4",
                    accepted.request_id,
                    uuid::Uuid::new_v4()
                );
                match self
                    .client
                    .get_file(
                        &output.filename,
                        output.subfolder.as_deref(),
                        Some("output"),
                    )
                    .await
                {
                    Ok((_headers, bytes)) => {
                        if let Err(e) = tokio::fs::write(&tmp_path, &bytes).await {
                            warn!(file = %output.filename, error = %e, "failed to write temp output");
                        } else {
                            output.local_path = Some(tmp_path);
                        }
                    }
                    Err(e) => {
                        warn!(file = %output.filename, error = %e, "failed to download output from ComfyUI");
                    }
                }
            }
            resolved.push(output);
        }

        Ok(CompletedGeneration { outputs: resolved })
    }
}
