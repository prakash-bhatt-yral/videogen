use std::sync::Arc;

use crate::rabbitmq::types::PrakashVideoJob;

// ─── Decision types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryDecision {
    Ack,
    NackRequeue,
    RejectNoRequeue,
}

pub enum WorkerDecision {
    Accepted,
    TransientError(String),
    ValidationFailure(String),
    Duplicate,
}

// ─── AMQP delivery properties ────────────────────────────────────────────────

#[derive(Default)]
pub struct DeliveryProperties {
    pub message_id: Option<String>,
    pub correlation_id: Option<String>,
    pub content_type: Option<String>,
}

// ─── Worker trait ────────────────────────────────────────────────────────────

#[async_trait::async_trait]
pub trait DeliveryWorker: Send + Sync {
    async fn accept(&self, job: PrakashVideoJob) -> WorkerDecision;
}

// ─── Handler (pure dispatcher — unit-testable seam) ──────────────────────────

/// Thin wrapper: no AMQP properties to validate, just parse and dispatch.
pub async fn handle_delivery_body(body: &[u8], worker: &dyn DeliveryWorker) -> DeliveryDecision {
    handle_delivery(body, DeliveryProperties::default(), worker).await
}

/// Full handler: validates AMQP properties then delegates to the worker.
///
/// Flow:
/// 1. Parse JSON → else `RejectNoRequeue`
/// 2. Check AMQP property mismatches → else `RejectNoRequeue`
/// 3. Call `worker.accept(job)` → map to delivery decision
///
/// The worker is responsible for its own business-level validation
/// (e.g. calling `job.validate()`). The handler is a pure dispatcher.
pub async fn handle_delivery(
    body: &[u8],
    props: DeliveryProperties,
    worker: &dyn DeliveryWorker,
) -> DeliveryDecision {
    // Step 1: parse JSON
    let job: PrakashVideoJob = match serde_json::from_slice(body) {
        Ok(j) => j,
        Err(e) => {
            tracing::error!(error = %e, "invalid JSON in RabbitMQ delivery");
            return DeliveryDecision::RejectNoRequeue;
        }
    };

    // Step 2: validate AMQP properties against job fields
    if let Some(mid) = &props.message_id {
        if mid != &job.request_id {
            tracing::error!(
                message_id = %mid,
                request_id = %job.request_id,
                "message_id mismatch — rejecting delivery"
            );
            return DeliveryDecision::RejectNoRequeue;
        }
    }
    if let Some(cid) = &props.correlation_id {
        if cid != &job.request_id {
            tracing::error!(
                correlation_id = %cid,
                request_id = %job.request_id,
                "correlation_id mismatch — rejecting delivery"
            );
            return DeliveryDecision::RejectNoRequeue;
        }
    }

    // Step 3: delegate to the worker
    match worker.accept(job).await {
        WorkerDecision::Accepted | WorkerDecision::Duplicate => DeliveryDecision::Ack,
        WorkerDecision::ValidationFailure(reason) => {
            tracing::warn!(reason = %reason, "worker rejected job as validation failure — acking to drain queue");
            DeliveryDecision::Ack
        }
        WorkerDecision::TransientError(e) => {
            tracing::warn!(error = %e, "transient worker error — nacking with requeue");
            DeliveryDecision::NackRequeue
        }
    }
}

// ─── AMQPS connection spawner ─────────────────────────────────────────────────

/// Connect to the broker, open a channel, set prefetch, and start consuming.
/// Returns a `JoinHandle` that drives the consumer loop.
pub async fn spawn_consumer(
    config: &crate::config::RabbitMqConfig,
    worker: Arc<dyn DeliveryWorker>,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    use lapin::{
        options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions, BasicRejectOptions},
        types::FieldTable,
        Connection, ConnectionProperties,
    };

    let url = config
        .amqps_urls
        .first()
        .ok_or_else(|| anyhow::anyhow!("no AMQPS URLs configured"))?;

    // Note: URL is never logged to avoid leaking credentials.
    let conn = Connection::connect(url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    channel
        .basic_qos(config.prefetch, BasicQosOptions::default())
        .await?;

    let mut consumer = channel
        .basic_consume(
            &config.queue,
            "videogen-worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    tracing::info!(queue = %config.queue, prefetch = config.prefetch, "RabbitMQ consumer started");

    let handle = tokio::spawn(async move {
        use futures_util::StreamExt;

        while let Some(delivery_result) = consumer.next().await {
            let delivery = match delivery_result {
                Ok(d) => d,
                Err(e) => {
                    tracing::error!(error = %e, "RabbitMQ consumer channel error");
                    break;
                }
            };

            let props = DeliveryProperties {
                message_id: delivery.properties.message_id().as_ref().map(|s| s.to_string()),
                correlation_id: delivery.properties.correlation_id().as_ref().map(|s| s.to_string()),
                content_type: delivery.properties.content_type().as_ref().map(|s| s.to_string()),
            };

            let decision = handle_delivery(&delivery.data, props, worker.as_ref()).await;

            let tag = delivery.delivery_tag;
            let ack_result = match decision {
                DeliveryDecision::Ack => {
                    channel.basic_ack(tag, BasicAckOptions::default()).await
                }
                DeliveryDecision::NackRequeue => {
                    channel
                        .basic_nack(tag, BasicNackOptions { requeue: true, ..Default::default() })
                        .await
                }
                DeliveryDecision::RejectNoRequeue => {
                    channel
                        .basic_reject(tag, BasicRejectOptions { requeue: false })
                        .await
                }
            };

            if let Err(e) = ack_result {
                tracing::error!(error = %e, "failed to ack/nack/reject RabbitMQ delivery");
            }
        }

        tracing::warn!("RabbitMQ consumer loop exited");
    });

    Ok(handle)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request_id() -> String {
        "11111111-1111-4111-8111-111111111111".to_string()
    }

    fn sample_job_json() -> String {
        serde_json::json!({
            "request_id": sample_request_id(),
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
        })
        .to_string()
    }

    struct FakeWorker {
        mode: FakeWorkerMode,
        failure_count: std::sync::Arc<std::sync::atomic::AtomicU32>,
    }

    enum FakeWorkerMode {
        Ok,
        TransientError,
        ValidationFailure,
    }

    impl FakeWorker {
        fn new() -> Self {
            Self {
                mode: FakeWorkerMode::Ok,
                failure_count: Default::default(),
            }
        }
        fn transient_error() -> Self {
            Self {
                mode: FakeWorkerMode::TransientError,
                failure_count: Default::default(),
            }
        }
        fn validation_failure_with_failure_outbox() -> Self {
            Self {
                mode: FakeWorkerMode::ValidationFailure,
                failure_count: Default::default(),
            }
        }
        fn failure_outbox_count(&self) -> u32 {
            self.failure_count
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl DeliveryWorker for FakeWorker {
        async fn accept(&self, _job: crate::rabbitmq::types::PrakashVideoJob) -> WorkerDecision {
            match self.mode {
                FakeWorkerMode::Ok => WorkerDecision::Accepted,
                FakeWorkerMode::TransientError => {
                    WorkerDecision::TransientError("db down".to_string())
                }
                FakeWorkerMode::ValidationFailure => {
                    self.failure_count
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    WorkerDecision::ValidationFailure("bad field".to_string())
                }
            }
        }
    }

    #[tokio::test]
    async fn invalid_json_is_rejected_without_requeue() {
        let result = handle_delivery_body(b"not-json", &FakeWorker::new()).await;
        assert_eq!(result, DeliveryDecision::RejectNoRequeue);
    }

    #[tokio::test]
    async fn worker_error_nacks_with_requeue_for_transient_failure() {
        let worker = FakeWorker::transient_error();
        let result = handle_delivery_body(sample_job_json().as_bytes(), &worker).await;
        assert_eq!(result, DeliveryDecision::NackRequeue);
    }

    #[tokio::test]
    async fn valid_body_validation_failure_enqueues_failure_completion_and_acks() {
        let worker = FakeWorker::validation_failure_with_failure_outbox();
        let result = handle_delivery_body(sample_job_json().as_bytes(), &worker).await;
        assert_eq!(result, DeliveryDecision::Ack);
        assert_eq!(worker.failure_outbox_count(), 1);
    }

    #[tokio::test]
    async fn mismatched_message_id_rejects_without_requeue() {
        let props = DeliveryProperties {
            message_id: Some("different-request-id".to_string()),
            correlation_id: Some(sample_request_id()),
            content_type: Some("application/json".to_string()),
        };
        let result = handle_delivery(sample_job_json().as_bytes(), props, &FakeWorker::new()).await;
        assert_eq!(result, DeliveryDecision::RejectNoRequeue);
    }
}
