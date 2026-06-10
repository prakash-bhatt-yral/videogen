use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::body::Bytes;
use futures_util::StreamExt;
use tokio::sync::RwLock;
use tracing::{info, warn};

use super::types::*;
use super::JobState;
use crate::backend::HealthResponse;
use crate::backend::UploadResponse;
use crate::webhook::OutputFile;

/// Low-level ComfyUI HTTP + WebSocket client
#[derive(Clone)]
pub struct ComfyUIClient {
    base_url: String,
    ws_url: String,
    http: reqwest::Client,
}

impl ComfyUIClient {
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            base_url: format!("http://{host}:{port}"),
            ws_url: format!("ws://{host}:{port}"),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to build HTTP client"),
        }
    }

    /// Submit a workflow to ComfyUI's /prompt endpoint
    pub async fn queue_prompt(
        &self,
        workflow: &serde_json::Value,
        client_id: &str,
    ) -> Result<String> {
        let payload = serde_json::json!({
            "prompt": workflow,
            "client_id": client_id,
        });

        let resp = self
            .http
            .post(format!("{}/prompt", self.base_url))
            .json(&payload)
            .send()
            .await
            .context("Failed to connect to ComfyUI")?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("ComfyUI rejected workflow: {body}");
        }

        let data: PromptResponse = resp
            .json()
            .await
            .context("Failed to parse prompt response")?;
        Ok(data.prompt_id)
    }

    /// Monitor a job via WebSocket until completion or failure.
    /// Updates job state in the shared map as progress changes.
    /// Returns output files on success.
    pub async fn monitor_job(
        &self,
        job_id: &str,
        prompt_id: &str,
        client_id: &str,
        jobs: &Arc<RwLock<HashMap<String, JobState>>>,
    ) -> Result<Vec<OutputFile>> {
        let ws_url = format!("{}/ws?clientId={}", self.ws_url, client_id);

        let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
            .await
            .context("Failed to connect to ComfyUI WebSocket")?;

        // Update status
        if let Some(job) = jobs.write().await.get_mut(job_id) {
            job.status = "processing".into();
        }

        info!(job_id, "Connected to ComfyUI WebSocket, monitoring...");

        let (_, mut read) = ws_stream.split();

        loop {
            let msg = tokio::time::timeout(Duration::from_secs(600), read.next()).await;

            match msg {
                Ok(Some(Ok(tungstenite::Message::Text(text)))) => {
                    let ws_msg: WsMessage = match serde_json::from_str(&text) {
                        Ok(m) => m,
                        Err(_) => continue,
                    };

                    match ws_msg.msg_type.as_str() {
                        "progress" => {
                            let current = ws_msg
                                .data
                                .get("value")
                                .and_then(|v| v.as_f64())
                                .unwrap_or(0.0);
                            let total = ws_msg
                                .data
                                .get("max")
                                .and_then(|v| v.as_f64())
                                .unwrap_or(1.0);
                            let pct = if total > 0.0 {
                                (current / total) * 100.0
                            } else {
                                0.0
                            };

                            if let Some(job) = jobs.write().await.get_mut(job_id) {
                                job.progress = Some(pct);
                            }
                        }
                        "executing" => {
                            let node = ws_msg.data.get("node");
                            let msg_prompt_id =
                                ws_msg.data.get("prompt_id").and_then(|v| v.as_str());

                            if (node.is_none() || (node.is_some() && node.unwrap().is_null()))
                                && msg_prompt_id == Some(prompt_id)
                            {
                                info!(job_id, "Execution complete");
                                break;
                            }
                        }
                        "execution_error" => {
                            let error_msg = ws_msg
                                .data
                                .get("exception_message")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Unknown execution error");
                            anyhow::bail!("ComfyUI execution error: {error_msg}");
                        }
                        "execution_cached" => {
                            info!(job_id, "Using cached execution");
                        }
                        _ => {}
                    }
                }
                Ok(Some(Ok(tungstenite::Message::Binary(_)))) => {
                    // Preview images — ignore
                }
                Ok(Some(Ok(tungstenite::Message::Close(_)))) => {
                    warn!(job_id, "WebSocket closed unexpectedly");
                    anyhow::bail!("WebSocket closed before job completed");
                }
                Ok(Some(Err(e))) => {
                    anyhow::bail!("WebSocket error: {e}");
                }
                Ok(None) => {
                    anyhow::bail!("WebSocket stream ended unexpectedly");
                }
                Err(_) => {
                    anyhow::bail!("Timeout: no updates for 10 minutes");
                }
                _ => {}
            }
        }

        // Fetch outputs from history
        self.get_outputs(prompt_id).await
    }

    /// Fetch output files from ComfyUI /history endpoint
    async fn get_outputs(&self, prompt_id: &str) -> Result<Vec<OutputFile>> {
        let resp = self
            .http
            .get(format!("{}/history/{}", self.base_url, prompt_id))
            .send()
            .await
            .context("Failed to fetch history")?;

        if !resp.status().is_success() {
            anyhow::bail!("Failed to fetch history: {}", resp.status());
        }

        let history: HashMap<String, HistoryOutput> =
            resp.json().await.context("Failed to parse history")?;

        let Some(entry) = history.get(prompt_id) else {
            return Ok(vec![]);
        };

        let mut outputs = Vec::new();

        for (node_id, node_output) in &entry.outputs {
            // "gifs" key — legacy ComfyUI video output path
            for file in &node_output.gifs {
                outputs.push(OutputFile {
                    filename: file.filename.clone(),
                    subfolder: Some(file.subfolder.clone()).filter(|s| !s.is_empty()),
                    local_path: None,
                    url: None,
                    node_id: Some(node_id.clone()),
                    output_type: Some("videos".into()),
                });
            }

            // "images" key — used for both real images and video files (e.g. .mp4)
            for file in &node_output.images {
                let is_video = file.filename.ends_with(".mp4")
                    || file.filename.ends_with(".webm")
                    || file.filename.ends_with(".gif");
                outputs.push(OutputFile {
                    filename: file.filename.clone(),
                    subfolder: Some(file.subfolder.clone()).filter(|s| !s.is_empty()),
                    local_path: None,
                    url: None,
                    node_id: Some(node_id.clone()),
                    output_type: Some(if is_video { "videos" } else { "images" }.into()),
                });
            }
        }

        Ok(outputs)
    }

    /// Upload an image to ComfyUI's /upload/image endpoint
    pub async fn upload_image(
        &self,
        filename: &str,
        data: Bytes,
        content_type: &str,
    ) -> Result<UploadResponse> {
        let part = reqwest::multipart::Part::bytes(data.to_vec())
            .file_name(filename.to_string())
            .mime_str(content_type)
            .context("Invalid content type")?;

        let form = reqwest::multipart::Form::new()
            .part("image", part)
            .text("overwrite", "true");

        let resp = self
            .http
            .post(format!("{}/upload/image", self.base_url))
            .multipart(form)
            .send()
            .await
            .context("Failed to upload image to ComfyUI")?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Image upload failed: {body}");
        }

        let upload_resp: ComfyUploadResponse = resp
            .json()
            .await
            .context("Failed to parse upload response")?;

        info!(filename = upload_resp.name, "Image uploaded to ComfyUI");
        Ok(UploadResponse {
            name: upload_resp.name,
        })
    }

    /// Proxy a file download from ComfyUI's /view endpoint
    pub async fn get_file(
        &self,
        filename: &str,
        subfolder: Option<&str>,
        file_type: Option<&str>,
    ) -> Result<(axum::http::HeaderMap, Bytes)> {
        let mut url = format!(
            "{}/view?filename={}",
            self.base_url,
            urlencoding::encode(filename)
        );
        if let Some(sf) = subfolder {
            url.push_str(&format!("&subfolder={}", urlencoding::encode(sf)));
        }
        if let Some(ft) = file_type {
            url.push_str(&format!("&type={ft}"));
        }

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("Failed to fetch file from ComfyUI")?;

        if !resp.status().is_success() {
            anyhow::bail!("File fetch failed: {}", resp.status());
        }

        let headers = resp.headers().clone();
        let body = resp.bytes().await.context("Failed to read file body")?;

        Ok((headers, body))
    }

    /// Check ComfyUI health via /system_stats
    pub async fn health_check(&self) -> Result<HealthResponse> {
        let resp = self
            .http
            .get(format!("{}/system_stats", self.base_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await;

        match resp {
            Ok(r) if r.status().is_success() => {
                let stats: serde_json::Value = r.json().await.unwrap_or_default();
                Ok(HealthResponse {
                    status: "healthy".into(),
                    backend: "comfyui".into(),
                    details: Some(stats),
                })
            }
            Ok(r) => Ok(HealthResponse {
                status: "degraded".into(),
                backend: "comfyui".into(),
                details: Some(serde_json::json!({ "http_status": r.status().as_u16() })),
            }),
            Err(e) => Ok(HealthResponse {
                status: "unhealthy".into(),
                backend: "comfyui".into(),
                details: Some(serde_json::json!({ "error": e.to_string() })),
            }),
        }
    }
}
