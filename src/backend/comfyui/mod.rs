mod client;
mod types;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use anyhow::Result;
use axum::body::Bytes;
use tracing::{error, info};

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

#[async_trait::async_trait]
impl crate::worker::WorkerBackend for ComfyUIBackend {
    async fn submit_workflow(
        &self,
        request_id: &str,
        workflow_json: serde_json::Value,
    ) -> anyhow::Result<AcceptedGeneration> {
        let client_id = uuid::Uuid::new_v4().to_string();
        let prompt_id = self.client.queue_prompt(&workflow_json, &client_id).await?;

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

        Ok(CompletedGeneration {
            prompt_id: accepted.prompt_id.clone(),
            outputs,
        })
    }
}
