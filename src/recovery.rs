use std::sync::Arc;

use crate::completion::PrakashCompletionClient;
use crate::jobs::JobStore;

// ─── Outbox runner ────────────────────────────────────────────────────────────

/// Attempt delivery for all due outbox rows in a single pass.
///
/// For each due row:
/// - `Accepted`      → mark terminal (`completion_sent`)
/// - `NonRetryable`  → mark terminal (`completion_failed`)
/// - `Retryable`     → increment attempt counter + apply exponential backoff
///
/// Rows are never deleted; only their state / next_attempt_at fields change.
pub async fn run_one_outbox_attempt(
    store: &JobStore,
    client: &dyn PrakashCompletionClient,
) -> anyhow::Result<()> {
    let due = store.due_completion_outbox(25).await?;

    for row in &due {
        let body: crate::completion::CompleteVideoRequest =
            match serde_json::from_str(&row.body_json) {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!(
                        outbox_id = %row.id,
                        error = %e,
                        "failed to deserialise outbox body — marking non-retryable"
                    );
                    store
                        .mark_completion_failed(&row.id, &format!("deserialise error: {e}"))
                        .await?;
                    continue;
                }
            };

        let result = client.send_completion(&row.callback_url, &body).await?;

        match result {
            crate::completion::CompletionDeliveryResult::Accepted(code) => {
                tracing::info!(
                    outbox_id = %row.id,
                    request_id = %row.request_id,
                    status = code,
                    "completion accepted — marking terminal"
                );
                store.mark_completion_sent(&row.id, code as i64).await?;
            }
            crate::completion::CompletionDeliveryResult::NonRetryable(reason) => {
                tracing::error!(
                    outbox_id = %row.id,
                    request_id = %row.request_id,
                    reason = %reason,
                    "non-retryable completion failure"
                );
                store.mark_completion_failed(&row.id, &reason).await?;
            }
            crate::completion::CompletionDeliveryResult::Retryable(reason) => {
                tracing::warn!(
                    outbox_id = %row.id,
                    request_id = %row.request_id,
                    reason = %reason,
                    "retryable completion error — scheduling retry"
                );
                store.record_outbox_retry(&row.id, &reason).await?;
            }
        }
    }

    Ok(())
}

/// Background loop: runs one outbox delivery pass every 5 seconds.
pub async fn run_outbox_loop(store: Arc<JobStore>, client: Arc<dyn PrakashCompletionClient>) {
    loop {
        if let Err(e) = run_one_outbox_attempt(&store, client.as_ref()).await {
            tracing::error!(error = %e, "outbox runner error");
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::completion::{
        CompleteVideoRequest, CompletionDeliveryResult, PrakashCompletionClient,
        UploadRefreshRequest, UploadRefreshResponse,
    };
    use crate::jobs::{JobStore, OutboxEntry};

    fn sample_success_outbox() -> OutboxEntry {
        OutboxEntry {
            id: "outbox-1".to_string(),
            request_id: "11111111-1111-4111-8111-111111111111".to_string(),
            callback_url: "https://prakash.example/api/v2/videogen/complete".to_string(),
            body_json: r#"{"request_key":{"principal":"aaaaa-aa","counter":17},"user_principal":"aaaaa-aa","request_id":"11111111-1111-4111-8111-111111111111","provider":"comfyui","status":"success"}"#.to_string(),
        }
    }

    struct FakePrakash {
        status: u16,
    }

    impl FakePrakash {
        fn status(s: u16) -> Self {
            Self { status: s }
        }
    }

    #[async_trait::async_trait]
    impl PrakashCompletionClient for FakePrakash {
        async fn send_completion(
            &self,
            _url: &str,
            _body: &CompleteVideoRequest,
        ) -> anyhow::Result<CompletionDeliveryResult> {
            Ok(match self.status {
                200 | 202 | 409 => CompletionDeliveryResult::Accepted(self.status),
                401 | 403 => {
                    CompletionDeliveryResult::NonRetryable(format!("auth: {}", self.status))
                }
                _ => CompletionDeliveryResult::Retryable(format!("err: {}", self.status)),
            })
        }

        async fn refresh_upload_url(
            &self,
            _url: &str,
            _body: &UploadRefreshRequest,
        ) -> anyhow::Result<UploadRefreshResponse> {
            Err(anyhow::anyhow!("not used"))
        }
    }

    fn sample_job() -> crate::rabbitmq::types::PrakashVideoJob {
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

    #[tokio::test]
    async fn outbox_treats_200_202_409_as_terminal() {
        for status in [200u16, 202, 409] {
            let store = JobStore::in_memory().await.unwrap();
            // Insert parent job first for FK
            store.claim_received(&sample_job()).await.unwrap();
            store
                .insert_completion_outbox(sample_success_outbox())
                .await
                .unwrap();
            run_one_outbox_attempt(&store, &FakePrakash::status(status))
                .await
                .unwrap();
            assert_eq!(
                store.due_completion_outbox(10).await.unwrap().len(),
                0,
                "status {status} should be terminal"
            );
        }
    }

    #[tokio::test]
    async fn outbox_retries_5xx_with_bounded_backoff() {
        let store = JobStore::in_memory().await.unwrap();
        store.claim_received(&sample_job()).await.unwrap();
        store
            .insert_completion_outbox(sample_success_outbox())
            .await
            .unwrap();
        run_one_outbox_attempt(&store, &FakePrakash::status(503))
            .await
            .unwrap();
        // After 1 retry, should not be immediately due (backoff applied)
        assert_eq!(store.due_completion_outbox(10).await.unwrap().len(), 0);
    }
}
