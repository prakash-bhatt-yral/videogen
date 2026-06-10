use crate::backend::{AcceptedGeneration, CompletedGeneration};
use crate::completion::PrakashCompletionClient;
use crate::rabbitmq::types::PrakashVideoJob;
use crate::upload::UploadedVideo;
use tracing::{info, warn};

// ─── Traits ──────────────────────────────────────────────────────────────────

/// Abstraction over the generation backend — submit a workflow and monitor it.
/// Decoupled from the HTTP/WebSocket details so tests can use fakes.
#[async_trait::async_trait]
pub trait WorkerBackend: Send + Sync {
    /// Submit a workflow to the backend and return after acceptance (before generation).
    async fn submit_workflow(
        &self,
        request_id: &str,
        workflow_json: serde_json::Value,
    ) -> anyhow::Result<AcceptedGeneration>;

    /// Block until generation finishes (or the timeout elapses), then return outputs.
    async fn monitor_generation(
        &self,
        accepted: &AcceptedGeneration,
        timeout_secs: u64,
    ) -> anyhow::Result<CompletedGeneration>;
}

/// Abstraction over video upload — takes the local file and job context.
#[async_trait::async_trait]
pub trait VideoUploader: Send + Sync {
    async fn upload(
        &self,
        job: &PrakashVideoJob,
        local_path: &std::path::Path,
    ) -> anyhow::Result<UploadedVideo>;
}

// ─── Public orchestration function ───────────────────────────────────────────

/// Full pipeline: submit → monitor → select output → upload → send completion.
///
/// Returns `Ok(())` on full success. Returns `Err(...)` on any failure so the
/// caller (RabbitMQ consumer) can nack and redeliver.
pub async fn run_prakash_job(
    backend: &dyn WorkerBackend,
    uploader: &dyn VideoUploader,
    client: &dyn PrakashCompletionClient,
    job: PrakashVideoJob,
) -> anyhow::Result<()> {
    let request_id = &job.request_id;
    info!(request_id, model_id = %job.model_id, "job received — submitting workflow");

    // 1. Submit to ComfyUI
    let accepted = backend
        .submit_workflow(request_id, job.workflow_json.clone())
        .await?;
    info!(request_id, prompt_id = %accepted.prompt_id, "workflow accepted by ComfyUI");

    // 2. Monitor generation (backend handles its own timeout)
    let generated = backend.monitor_generation(&accepted, 1800).await?;
    info!(
        request_id,
        prompt_id = %accepted.prompt_id,
        outputs = generated.outputs.len(),
        "generation complete"
    );

    // 3. Select primary video output
    let output = crate::upload::select_primary_video_output(&generated.outputs)?;
    info!(request_id, filename = %output.filename, "selected primary video output");
    let local_path = output
        .local_path
        .as_ref()
        .map(std::path::PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("output has no local_path"))?;

    // 4. Upload
    let uploaded = uploader.upload(&job, &local_path).await;
    if let Err(e) = tokio::fs::remove_file(&local_path).await {
        warn!(request_id, path = %local_path.display(), error = %e, "failed to remove temp file");
    }
    let uploaded = uploaded?;
    info!(
        request_id,
        video_id = %uploaded.video_id,
        object_key = %uploaded.object_key,
        file_size = uploaded.file_size,
        "video uploaded to Storj"
    );

    // 5. Send success completion
    let bucket_url = uploaded.require_bucket_url()?;
    let body = build_success_completion(&job, &uploaded, bucket_url);
    client.send_completion(&job.callback_url, &body).await?;
    info!(request_id, callback_url = %job.callback_url, "completion sent");

    Ok(())
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

pub fn build_success_completion(
    job: &PrakashVideoJob,
    uploaded: &UploadedVideo,
    bucket_url: &str,
) -> crate::completion::CompleteVideoRequest {
    use crate::completion::{CompleteVideoRequest, CompletionRequestKey, CompletionStatus};
    CompleteVideoRequest {
        request_key: CompletionRequestKey {
            principal: job.request_key.principal.clone(),
            counter: job.request_key.counter,
        },
        user_principal: job.user_principal.clone(),
        request_id: job.request_id.clone(),
        provider: job.model_id.clone(),
        status: CompletionStatus::Success,
        bucket_url: Some(bucket_url.to_string()),
        video_id: Some(uploaded.video_id.clone()),
        object_key: Some(uploaded.object_key.clone()),
        file_size: Some(uploaded.file_size),
        content_type: Some(uploaded.content_type.clone()),
        checksum: Some(uploaded.checksum.clone()),
        failure_reason: None,
        encrypted_identity: job.upload_destination.encrypted_identity.clone(),
        staged_image_key: job.staged_image_key.clone(),
    }
}

pub fn build_failure_completion(
    job: &PrakashVideoJob,
    reason: &str,
) -> crate::completion::CompleteVideoRequest {
    use crate::completion::{CompleteVideoRequest, CompletionRequestKey, CompletionStatus};
    CompleteVideoRequest {
        request_key: CompletionRequestKey {
            principal: job.request_key.principal.clone(),
            counter: job.request_key.counter,
        },
        user_principal: job.user_principal.clone(),
        request_id: job.request_id.clone(),
        provider: job.model_id.clone(),
        status: CompletionStatus::Failure,
        bucket_url: None,
        video_id: None,
        object_key: None,
        file_size: None,
        content_type: None,
        checksum: None,
        failure_reason: Some(reason.to_string()),
        encrypted_identity: None,
        staged_image_key: job.staged_image_key.clone(),
    }
}

// ─── Runtime delivery worker ─────────────────────────────────────────────────

/// Bridges the RabbitMQ `DeliveryWorker` trait to `run_prakash_job`.
/// Holds the RabbitMQ message unacked for the full pipeline, then acks on
/// success or nacks on failure (RabbitMQ redelivers).
pub struct RuntimeDeliveryWorker {
    pub backend: std::sync::Arc<dyn WorkerBackend>,
    pub uploader: std::sync::Arc<dyn VideoUploader>,
    pub client: std::sync::Arc<dyn PrakashCompletionClient>,
}

#[async_trait::async_trait]
impl crate::rabbitmq::consumer::DeliveryWorker for RuntimeDeliveryWorker {
    async fn accept(&self, job: PrakashVideoJob) -> crate::rabbitmq::consumer::WorkerDecision {
        match run_prakash_job(
            self.backend.as_ref(),
            self.uploader.as_ref(),
            self.client.as_ref(),
            job,
        )
        .await
        {
            Ok(()) => crate::rabbitmq::consumer::WorkerDecision::Accepted,
            Err(e) => crate::rabbitmq::consumer::WorkerDecision::TransientError(e.to_string()),
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webhook::OutputFile;

    fn sample_job() -> PrakashVideoJob {
        serde_json::from_str(
            r#"{
            "request_id": "11111111-1111-4111-8111-111111111111",
            "request_key": { "principal": "aaaaa-aa", "counter": 17 },
            "user_principal": "aaaaa-aa",
            "model_id": "ltx2",
            "workflow_json": {},
            "input": {},
            "callback_url": "https://prakash.example/api/v2/videogen/complete",
            "upload_destination": {
                "video_id": "video-1",
                "object_key": "videos/video-1.mp4",
                "upload_url": "https://upload.example/secret",
                "expires_at": "2099-01-01T00:00:00Z",
                "bucket_url": "https://bucket.example/videos/video-1.mp4"
            }
        }"#,
        )
        .unwrap()
    }

    fn video_output() -> OutputFile {
        OutputFile {
            filename: "output.mp4".to_string(),
            subfolder: None,
            output_type: Some("videos".to_string()),
            local_path: Some("/tmp/output.mp4".to_string()),
            url: None,
            node_id: None,
        }
    }

    fn uploaded_video() -> UploadedVideo {
        UploadedVideo {
            bucket_url: Some("https://bucket.example/videos/video-1.mp4".to_string()),
            video_id: "video-1".to_string(),
            object_key: "videos/video-1.mp4".to_string(),
            file_size: 12345,
            content_type: "video/mp4".to_string(),
            checksum: "sha256:abc".to_string(),
        }
    }

    // ── Fake backend ────────────────────────────────────────────────────────

    struct FakeBackend {
        submit_result: bool, // true = ok, false = error
        outputs: Vec<OutputFile>,
    }

    impl FakeBackend {
        fn completes_with(outputs: Vec<OutputFile>) -> Self {
            Self {
                submit_result: true,
                outputs,
            }
        }

        fn fails_submit() -> Self {
            Self {
                submit_result: false,
                outputs: vec![],
            }
        }
    }

    #[async_trait::async_trait]
    impl WorkerBackend for FakeBackend {
        async fn submit_workflow(
            &self,
            request_id: &str,
            _workflow: serde_json::Value,
        ) -> anyhow::Result<AcceptedGeneration> {
            if self.submit_result {
                Ok(AcceptedGeneration {
                    request_id: request_id.to_string(),
                    prompt_id: "prompt-1".to_string(),
                    client_id: "fake-client".to_string(),
                })
            } else {
                Err(anyhow::anyhow!("ComfyUI execution error"))
            }
        }

        async fn monitor_generation(
            &self,
            accepted: &AcceptedGeneration,
            _timeout: u64,
        ) -> anyhow::Result<CompletedGeneration> {
            Ok(CompletedGeneration {
                prompt_id: accepted.prompt_id.clone(),
                outputs: self.outputs.clone(),
            })
        }
    }

    // ── Fake uploader ───────────────────────────────────────────────────────

    struct SimpleUploader(Option<UploadedVideo>);

    #[async_trait::async_trait]
    impl VideoUploader for SimpleUploader {
        async fn upload(
            &self,
            _job: &PrakashVideoJob,
            _path: &std::path::Path,
        ) -> anyhow::Result<UploadedVideo> {
            self.0
                .clone()
                .ok_or_else(|| anyhow::anyhow!("uploader should not be called"))
        }
    }

    // ── Fake completion client ──────────────────────────────────────────────

    use std::sync::{Arc, Mutex};

    struct FakeCompletionClient {
        received: Arc<Mutex<Vec<crate::completion::CompleteVideoRequest>>>,
    }

    impl FakeCompletionClient {
        fn new() -> Self {
            Self {
                received: Arc::new(Mutex::new(vec![])),
            }
        }

        fn received_bodies(&self) -> Vec<crate::completion::CompleteVideoRequest> {
            self.received.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl crate::completion::PrakashCompletionClient for FakeCompletionClient {
        async fn send_completion(
            &self,
            _url: &str,
            body: &crate::completion::CompleteVideoRequest,
        ) -> anyhow::Result<crate::completion::CompletionDeliveryResult> {
            self.received.lock().unwrap().push(body.clone());
            Ok(crate::completion::CompletionDeliveryResult::Accepted(200))
        }

        async fn refresh_upload_url(
            &self,
            _url: &str,
            _body: &crate::completion::UploadRefreshRequest,
        ) -> anyhow::Result<crate::completion::UploadRefreshResponse> {
            Err(anyhow::anyhow!("not used in tests"))
        }
    }

    // ── Tests ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn successful_job_runs_full_pipeline() {
        let backend = FakeBackend::completes_with(vec![video_output()]);
        let uploader = SimpleUploader(Some(uploaded_video()));
        let client = FakeCompletionClient::new();

        run_prakash_job(&backend, &uploader, &client, sample_job())
            .await
            .unwrap();

        let completions = client.received_bodies();
        assert_eq!(completions.len(), 1);
        let c = &completions[0];
        assert!(
            matches!(c.status, crate::completion::CompletionStatus::Success),
            "expected success completion"
        );
        assert_eq!(
            c.bucket_url.as_deref(),
            Some("https://bucket.example/videos/video-1.mp4")
        );
        assert_eq!(c.video_id.as_deref(), Some("video-1"));
    }

    #[tokio::test]
    async fn generation_failure_returns_error() {
        let backend = FakeBackend::fails_submit();
        let uploader = SimpleUploader(None);
        let client = FakeCompletionClient::new();

        let result = run_prakash_job(&backend, &uploader, &client, sample_job()).await;
        assert!(result.is_err(), "expected Err on generation failure");
        assert!(result.unwrap_err().to_string().contains("ComfyUI"));

        // No completion should have been sent
        assert_eq!(client.received_bodies().len(), 0);
    }
}
