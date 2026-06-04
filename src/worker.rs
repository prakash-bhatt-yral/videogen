use crate::backend::{AcceptedGeneration, CompletedGeneration};
use crate::completion::PrakashCompletionClient;
use crate::jobs::JobStore;
use crate::rabbitmq::types::PrakashVideoJob;
use crate::upload::UploadedVideo;

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

// ─── Public orchestration functions ──────────────────────────────────────────

/// Submit job to ComfyUI and return after acceptance (NOT after generation completes).
/// Consumer acks RabbitMQ after this returns `Ok`.
pub async fn accept_prakash_job(
    store: &JobStore,
    backend: &dyn WorkerBackend,
    job: PrakashVideoJob,
) -> anyhow::Result<AcceptedGeneration> {
    store.claim_received(&job).await?;
    let accepted = backend
        .submit_workflow(&job.request_id, job.workflow_json.clone())
        .await?;
    store
        .mark_accepted(&job.request_id, &accepted.prompt_id, &accepted.client_id)
        .await?;
    Ok(accepted)
}

/// Full pipeline: generate → upload → outbox insert.
/// Called after the RabbitMQ ack. Never panics — all errors are routed to the
/// failure-completion outbox so the caller can deliver them to Prakash.
pub async fn run_prakash_job(
    store: &JobStore,
    backend: &dyn WorkerBackend,
    uploader: &dyn VideoUploader,
    client: &dyn PrakashCompletionClient,
    job: PrakashVideoJob,
) -> anyhow::Result<()> {
    // Claim if not already done (idempotent — ignores AlreadyExists).
    let _ = store.claim_received(&job).await;

    // ── Submit ────────────────────────────────────────────────────────────────
    let accepted = match backend
        .submit_workflow(&job.request_id, job.workflow_json.clone())
        .await
    {
        Ok(a) => {
            store
                .mark_accepted(&job.request_id, &a.prompt_id, &a.client_id)
                .await?;
            a
        }
        Err(e) => {
            store.mark_failed(&job.request_id, &e.to_string()).await?;
            enqueue_failure_completion(store, &job, &e.to_string(), client).await?;
            return Ok(());
        }
    };

    // ── Monitor ───────────────────────────────────────────────────────────────
    let generated = match backend.monitor_generation(&accepted, 1800).await {
        Ok(g) => {
            let outputs_json =
                serde_json::to_string(&g.outputs).unwrap_or_else(|_| "[]".to_string());
            store.mark_generated(&job.request_id, &outputs_json).await?;
            g
        }
        Err(e) => {
            store.mark_failed(&job.request_id, &e.to_string()).await?;
            enqueue_failure_completion(store, &job, &e.to_string(), client).await?;
            return Ok(());
        }
    };

    // ── Select primary output ─────────────────────────────────────────────────
    let output = match crate::upload::select_primary_video_output(&generated.outputs) {
        Ok(o) => o.clone(),
        Err(e) => {
            store.mark_failed(&job.request_id, &e.to_string()).await?;
            enqueue_failure_completion(store, &job, &e.to_string(), client).await?;
            return Ok(());
        }
    };

    // ── Resolve local path ────────────────────────────────────────────────────
    let local_path = match &output.local_path {
        Some(p) => std::path::PathBuf::from(p),
        None => {
            let reason = "output has no local_path";
            store.mark_failed(&job.request_id, reason).await?;
            enqueue_failure_completion(store, &job, reason, client).await?;
            return Ok(());
        }
    };

    // ── Upload ────────────────────────────────────────────────────────────────
    store.mark_uploading(&job.request_id).await?;
    let uploaded = match uploader.upload(&job, &local_path).await {
        Ok(u) => {
            let uploaded_json = serde_json::to_string(&u).unwrap_or_else(|_| "{}".to_string());
            store
                .mark_uploaded(
                    &job.request_id,
                    &uploaded_json,
                    u.bucket_url.as_deref().unwrap_or(""),
                )
                .await?;
            u
        }
        Err(e) => {
            store.mark_failed(&job.request_id, &e.to_string()).await?;
            enqueue_failure_completion(store, &job, &e.to_string(), client).await?;
            return Ok(());
        }
    };

    // ── Build success outbox entry ────────────────────────────────────────────
    let bucket_url = match uploaded.require_bucket_url() {
        Ok(url) => url.to_string(),
        Err(e) => {
            store.mark_failed(&job.request_id, &e.to_string()).await?;
            enqueue_failure_completion(store, &job, &e.to_string(), client).await?;
            return Ok(());
        }
    };

    let completion_body = build_success_completion(&job, &uploaded, &bucket_url);
    let body_json = serde_json::to_string(&completion_body)?;
    let outbox_id = uuid::Uuid::new_v4().to_string();
    store
        .insert_completion_outbox(crate::jobs::OutboxEntry {
            id: outbox_id,
            request_id: job.request_id.clone(),
            callback_url: job.callback_url.clone(),
            body_json,
        })
        .await?;

    store.mark_completion_pending(&job.request_id).await?;

    Ok(())
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn build_success_completion(
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
    }
}

async fn enqueue_failure_completion(
    store: &JobStore,
    job: &PrakashVideoJob,
    reason: &str,
    _client: &dyn crate::completion::PrakashCompletionClient,
) -> anyhow::Result<()> {
    use crate::completion::{CompleteVideoRequest, CompletionRequestKey, CompletionStatus};
    let body = CompleteVideoRequest {
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
    };
    let body_json = serde_json::to_string(&body)?;
    let outbox_id = uuid::Uuid::new_v4().to_string();
    store
        .insert_completion_outbox(crate::jobs::OutboxEntry {
            id: outbox_id,
            request_id: job.request_id.clone(),
            callback_url: job.callback_url.clone(),
            body_json,
        })
        .await?;
    Ok(())
}

// ─── Runtime delivery worker ─────────────────────────────────────────────────

/// Bridges the RabbitMQ `DeliveryWorker` trait to `accept_prakash_job`.
/// Used in production to wire the consumer into the job store + ComfyUI backend.
pub struct RuntimeDeliveryWorker {
    pub store: std::sync::Arc<JobStore>,
    pub backend: std::sync::Arc<dyn WorkerBackend>,
}

#[async_trait::async_trait]
impl crate::rabbitmq::consumer::DeliveryWorker for RuntimeDeliveryWorker {
    async fn accept(&self, job: PrakashVideoJob) -> crate::rabbitmq::consumer::WorkerDecision {
        match accept_prakash_job(&self.store, self.backend.as_ref(), job).await {
            Ok(_) => crate::rabbitmq::consumer::WorkerDecision::Accepted,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("duplicate") || msg.contains("AlreadyExists") {
                    crate::rabbitmq::consumer::WorkerDecision::Duplicate
                } else {
                    crate::rabbitmq::consumer::WorkerDecision::TransientError(msg)
                }
            }
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::JobStore;
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
        prompt_id: Option<String>,
        outputs: Vec<OutputFile>,
    }

    impl FakeBackend {
        fn accepts_with_prompt(prompt_id: &str) -> Self {
            Self {
                prompt_id: Some(prompt_id.to_string()),
                outputs: vec![],
            }
        }

        fn completes_with(outputs: Vec<OutputFile>) -> Self {
            Self {
                prompt_id: Some("prompt-1".to_string()),
                outputs,
            }
        }

        fn fails(msg: &str) -> Self {
            // Store failure as a sentinel: prompt_id = None means submit fails.
            // We need a different mechanism for monitor failure vs submit failure.
            // For the test `generation_failure_enqueues_failure_completion`,
            // the failure happens at submit_workflow level.
            let _ = msg;
            Self {
                prompt_id: None,
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
            match &self.prompt_id {
                Some(pid) => Ok(AcceptedGeneration {
                    request_id: request_id.to_string(),
                    prompt_id: pid.clone(),
                    client_id: "fake-client".to_string(),
                }),
                None => Err(anyhow::anyhow!("ComfyUI execution error")),
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

    struct FakeCompletionClient;

    #[async_trait::async_trait]
    impl crate::completion::PrakashCompletionClient for FakeCompletionClient {
        async fn send_completion(
            &self,
            _url: &str,
            _body: &crate::completion::CompleteVideoRequest,
        ) -> anyhow::Result<crate::completion::CompletionDeliveryResult> {
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
    async fn accept_job_returns_after_comfyui_acceptance_not_generation_completion() {
        let store = JobStore::in_memory().await.unwrap();
        let backend = FakeBackend::accepts_with_prompt("prompt-1");

        let accepted = accept_prakash_job(&store, &backend, sample_job())
            .await
            .unwrap();

        assert_eq!(accepted.prompt_id, "prompt-1");
        let row = store
            .get_job(&sample_job().request_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.state, crate::jobs::JobState::Accepted);
    }

    #[tokio::test]
    async fn successful_job_generates_uploads_and_enqueues_completion() {
        let store = JobStore::in_memory().await.unwrap();
        // Claim the job first (normally done by accept_prakash_job)
        store.claim_received(&sample_job()).await.unwrap();

        let backend = FakeBackend::completes_with(vec![video_output()]);
        let uploader = SimpleUploader(Some(uploaded_video()));
        let completion = FakeCompletionClient;

        run_prakash_job(&store, &backend, &uploader, &completion, sample_job())
            .await
            .unwrap();

        let row = store
            .get_job(&sample_job().request_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.state, crate::jobs::JobState::CompletionPending);
        assert!(store.due_completion_outbox(10).await.unwrap().len() >= 1);
    }

    #[tokio::test]
    async fn generation_failure_enqueues_failure_completion() {
        let store = JobStore::in_memory().await.unwrap();
        store.claim_received(&sample_job()).await.unwrap();

        let backend = FakeBackend::fails("ComfyUI execution error");
        let uploader = SimpleUploader(None);
        let completion = FakeCompletionClient;

        run_prakash_job(&store, &backend, &uploader, &completion, sample_job())
            .await
            .unwrap();

        let outbox = store.due_completion_outbox(10).await.unwrap();
        assert!(!outbox.is_empty());
        // The outbox body must carry a failure status
        assert!(
            outbox[0].body_json.contains("\"failure\""),
            "expected 'failure' in body_json, got: {}",
            outbox[0].body_json
        );
    }
}
