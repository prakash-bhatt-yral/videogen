# Vast RabbitMQ Consumer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `videogen-worker` consume Prakash RabbitMQ jobs, generate LTX/ComfyUI videos, upload successful outputs to the reserved bucket destination, clean local disk after durable upload, and call Prakash completion endpoints with HMAC authentication, without sending migrated jobs through off-chain-agent.

**Architecture:** Keep the existing HTTP `/generate`, `/result/{id}`, and `/upload/image` endpoints for local/manual testing and short rollback only; they are not the production path for migrated LTX jobs. Add a RabbitMQ consumer path that persists each `request_id` to a local durable SQLite job store, submits the workflow to ComfyUI, acknowledges RabbitMQ only after ComfyUI acceptance is durably stored, uploads the first completed video output to Prakash's reserved upload destination, then stores and delivers completion callbacks through a durable outbox. Completion delivery reliability is handled by the outbox and startup recovery, not by holding RabbitMQ deliveries open for the full generation window.

**Tech Stack:** Rust, Axum, Tokio, ComfyUI HTTP/WebSocket backend, `lapin` AMQPS RabbitMQ client, SQLite via `sqlx`, `reqwest` multipart upload, HMAC-SHA256 (`hmac`, `sha2`, `hex`, `base64`), existing GitHub Actions Vast deploy.

---

## Scope

This plan is only for `/Users/prk-jr/Desktop/work/dolr/videogen`.

The target production path is:

`mobile -> yral-video-storage-service/Prakash -> moderation -> RateLimiter -> RabbitMQ -> videogen/Vast -> bucket upload -> Prakash completion -> draft pipeline`

`off-chain-agent` is legacy drain only. Migrated LTX jobs must not call off-chain `/comfyui/webhook`, QStash video generation callbacks, or off-chain draft upload paths. Keep old HTTP behavior only for local/manual testing and emergency rollback while legacy in-flight off-chain requests drain.

It depends on the broker contract already deployed in `prakash-rabbitmq`:

- vhost: `/videogen`
- exchange: `videogen.jobs`
- routing key: `ltx.generate`
- queue: `videogen.ltx.generate`
- consumer user: `vast_ltx_consumer`

It also depends on the Prakash publisher/completion plan in `yral-video-storage-service`:

- Prakash publishes one JSON job per generation request.
- Prakash expects Vast to call `POST /api/v2/videogen/complete`.
- Prakash optionally exposes `POST /api/v2/videogen/upload-url/refresh`.
- Both Vast -> Prakash endpoints use the same HMAC headers:
  - `X-Timestamp`
  - `X-Body-SHA256`
  - `X-Key-Id`
  - `Authorization: HMAC-SHA256 <hex_signature>`
- Signature message is `METHOD + "\n" + PATH + "\n" + X-Timestamp + "\n" + X-Body-SHA256`.

Do not edit `src/bin/benchmark.rs` unless a new task explicitly asks for it; it is currently dirty from unrelated work.

## Message Contract

RabbitMQ message body:

```json
{
  "request_id": "<uuid-v4>",
  "request_key": { "principal": "...", "counter": 123 },
  "user_principal": "...",
  "model_id": "ltx2",
  "workflow_json": {},
  "input": {},
  "callback_url": "https://prakash.example/api/v2/videogen/complete",
  "upload_url_refresh_url": "https://prakash.example/api/v2/videogen/upload-url/refresh",
  "upload_destination": {
    "video_id": "...",
    "object_key": "...",
    "upload_url": "...",
    "expires_at": "2026-06-03T12:00:00Z",
    "bucket_url": "https://<hetzner-endpoint>/<bucket>/<object_key>"
  }
}
```

RabbitMQ properties expected from Prakash:

- `content_type = application/json`
- `delivery_mode = persistent`
- `message_id = request_id`
- `correlation_id = request_id`

Completion success body:

```json
{
  "request_key": { "principal": "...", "counter": 123 },
  "user_principal": "...",
  "request_id": "<uuid-v4>",
  "provider": "ltx2",
  "status": "success",
  "bucket_url": "https://...",
  "video_id": "...",
  "object_key": "...",
  "file_size": 123456,
  "content_type": "video/mp4",
  "checksum": "sha256:<hex>"
}
```

Prakash requires `bucket_url` for success. Vast reads it from `upload_destination.bucket_url` in the job payload (Prakash pre-computes it) and echoes it unchanged. No change to this success body shape is needed.

Completion failure body:

```json
{
  "request_key": { "principal": "...", "counter": 123 },
  "user_principal": "...",
  "request_id": "<uuid-v4>",
  "provider": "ltx2",
  "status": "failure",
  "failure_reason": "..."
}
```

Locked integration contracts (Task 0 complete):

- **`bucket_url` source:** Prakash pre-computes `bucket_url` as `https://{HETZNER_S3_ENDPOINT}/{HETZNER_S3_BUCKET}/{object_key}` and includes it in `upload_destination` in the RabbitMQ job. Vast reads it from the job and echoes it in the success completion callback. The upload service does not return `bucket_url`. Confirmed from off-chain implementation analysis (2026-06-03).
- **Upload protocol:** `multipart/form-data`, field name `file`, content type `video/mp4`. `POST` to `upload_destination.upload_url`. Confirmed from off-chain `upload_ai_generated_video_to_canister_impl` implementation (2026-06-03).
- **HMAC env split:** Prakash owns validating key registry (`VIDEOGEN_COMPLETION_HMAC_KEYS`). Vast owns one active key: `PRAKASH_COMPLETION_HMAC_KEY_ID` and `PRAKASH_COMPLETION_HMAC_SECRET_B64`. The key id must be registered in Prakash before traffic flows. Confirmed 2026-06-03.
- **`/update-video-metadata` contract:** POST body `{ delegated_identity_wire, meta: {}, post_details: { id, video_uid, creator_principal, status: "Draft", hashtags: [], description: "" } }`. Idempotency-Key header accepted. Confirmed from off-chain implementation (2026-06-03). This is handled by Prakash, not Vast.

## File Structure

- `Cargo.toml`
  - Add AMQP, SQLite, HMAC, time, checksum, and TLS support dependencies.
- `src/config.rs`
  - Add RabbitMQ, outbox, upload, refresh, and HMAC config.
- `src/main.rs`
  - Initialize durable store and spawn RabbitMQ consumer/outbox recovery tasks when enabled.
- `src/lib.rs`
  - Create if needed to make modules testable without only binary-level tests.
- `src/rabbitmq/mod.rs`
  - Module boundary for RabbitMQ consumer code.
- `src/rabbitmq/types.rs`
  - Prakash job DTOs and validation helpers.
- `src/rabbitmq/consumer.rs`
  - AMQPS connection, queue consume, message claim/ack/nack, concurrency limits.
- `src/jobs.rs`
  - Durable SQLite job/outbox store and state transition methods.
- `src/completion.rs`
  - Prakash completion and refresh DTOs plus HMAC signing.
- `src/upload.rs`
  - Resolve ComfyUI output path, refresh upload URL when needed, upload video, checksum, cleanup.
- `src/staged_inputs.rs`
  - Track and clean ComfyUI input images staged through `/upload/image` for migrated I2V jobs.
- `src/worker.rs`
  - Orchestrates one RabbitMQ job through acceptance, generation, upload, outbox insert, and cleanup.
- `src/backend/mod.rs`
  - Add an optional backend method for submitting and waiting in one worker-owned flow, or expose enough primitives without changing HTTP behavior.
- `src/backend/comfyui/mod.rs`
  - Add a worker-facing generation method that returns `prompt_id` and completed outputs without relying on legacy webhook.
- `src/backend/comfyui/client.rs`
  - Expose history lookup/recovery helpers needed after restart.
- `src/routes/health.rs`
  - Include RabbitMQ/outbox status when enabled.
- `deploy/start.sh`
  - Pass RabbitMQ, HMAC, state DB, upload, and outbox env vars to the worker process.
- `.github/workflows/deploy.yml`
  - Add deployment secrets/env propagation for Vast.
- `docker-compose.yml`
  - Add optional envs and a state volume for non-Vast deployments.
- `README.md`
  - Document RabbitMQ consumer mode, required envs, and rollback behavior.

## State Machine

Local persisted states:

- `received`: message parsed and stored.
- `accepted`: ComfyUI accepted prompt and `prompt_id` is stored.
- `running`: monitor has observed processing or recovery found an active prompt.
- `generated`: ComfyUI completed and output metadata is stored.
- `uploading`: selected local video is being uploaded.
- `uploaded`: bucket upload succeeded and local cleanup is allowed.
- `completion_pending`: completion outbox record exists.
- `completion_sent`: Prakash accepted completion with `200`, `202`, or `409`.
- `failed`: terminal local failure before success callback.
- `completion_failed`: terminal local state when Prakash completion delivery exhausted retry budget or returned non-retryable auth/config failure.

Rules:

- `request_id` is unique in SQLite.
- Duplicate RabbitMQ delivery for an existing non-terminal job must not start a second ComfyUI generation.
- Duplicate RabbitMQ delivery for `completion_sent` must be acknowledged without mutation.
- Startup recovery must scan `accepted`, `running`, `generated`, `uploading`, `uploaded`, and `completion_pending`.
- Local output files are deleted only after upload success has been persisted.
- Staged input images are deleted after terminal generation state when they are known, and orphan staged inputs are deleted by TTL.
- Failure states that have enough request metadata must still enqueue a signed failure completion to Prakash. Invalid messages that cannot be correlated to a Prakash request are rejected without requeue and must raise metrics/logs.

## Task 0: Lock Cross-Repo Runtime Contracts

**Files:**
- Modify: `docs/superpowers/plans/2026-06-03-vast-rabbitmq-consumer.md` if any decision changes this plan.
- Cross-check: `/Users/prk-jr/Desktop/work/dolr/yral-video-storage-service/docs/superpowers/plans/2026-06-03-videogen-rabbitmq-publisher.md`
- Cross-check: `/Users/prk-jr/Desktop/work/dolr/yral-video-storage-service/docs/superpowers/specs/2026-05-27-lean-videogen-migration-design.md`

- [x] **Step 1: Confirm `bucket_url` source** — LOCKED 2026-06-03

Decision: Prakash pre-computes `bucket_url` from Hetzner S3 endpoint + bucket + `object_key` and includes it inside `upload_destination` in the RabbitMQ job. Vast reads `upload_destination.bucket_url` and echoes it in the success completion callback unchanged. The upload service does not return `bucket_url`.

Test impact:
- Vast success completion builder must set `bucket_url` from `upload_destination.bucket_url`.
- If `bucket_url` is absent from the job (Prakash bug), mark job failed, enqueue failure completion, do not guess the URL.

Prakash change required: add `bucket_url: String` to `UploadDestination` struct and pre-compute at reservation time.

- [x] **Step 2: Confirm upload protocol** — LOCKED 2026-06-03

Confirmed: `multipart/form-data`, field `file`, content type `video/mp4`, `POST` to `upload_destination.upload_url`. Verified from off-chain implementation (`upload_ai_generated_video_to_canister_impl`). Raw PUT is not used.

- [x] **Step 3: Confirm HMAC runtime env names** — LOCKED 2026-06-03

Vast envs:
- `PRAKASH_COMPLETION_HMAC_KEY_ID`
- `PRAKASH_COMPLETION_HMAC_SECRET_B64`

`PRAKASH_COMPLETION_HMAC_KEY_ID` must be registered in Prakash `VIDEOGEN_COMPLETION_HMAC_KEYS` before traffic flows. Vast uses its own key pair. Key rotation affects retried outbox rows — re-sign on rotation or flush outbox first.

- [x] **Step 4: Confirm off-chain deprecation boundary** — LOCKED 2026-06-03

No new migrated LTX request uses off-chain QStash, `/comfyui/webhook`, or off-chain draft upload. Legacy off-chain routes stay live only until old in-flight jobs drain. Rollback must move both submission and callback/draft handling together.

- [x] **Step 5: Commit contract updates**

```bash
git add docs/superpowers/plans/2026-06-03-vast-rabbitmq-consumer.md
git commit -m "docs: lock videogen rabbitmq runtime contracts"
```

## Task 1: Add Configuration And Dependencies

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/config.rs`
- Test: `src/config.rs`

- [ ] **Step 1: Write failing config tests**

Add tests in `src/config.rs`:

```rust
#[test]
fn rabbitmq_disabled_by_default() {
    let cfg = AppConfig::from_env_map(TestEnv::default()).unwrap();
    assert!(!cfg.rabbitmq.enabled);
}

#[test]
fn rabbitmq_enabled_requires_urls_queue_and_hmac_key() {
    let env = TestEnv::from_pairs([
        ("VIDEOGEN_RABBITMQ_ENABLED", "true"),
        ("VIDEOGEN_RABBITMQ_QUEUE", "videogen.ltx.generate"),
    ]);
    let err = AppConfig::from_env_map(env).unwrap_err().to_string();
    assert!(err.contains("VIDEOGEN_RABBITMQ_AMQPS_URLS"));
}

#[test]
fn parses_rabbitmq_and_outbox_defaults() {
    let cfg = AppConfig::from_env_map(TestEnv::from_pairs([
        ("VIDEOGEN_RABBITMQ_ENABLED", "true"),
        ("VIDEOGEN_RABBITMQ_AMQPS_URLS", "amqps://user:pass@94.130.13.115:5671/%2Fvideogen"),
        ("VIDEOGEN_RABBITMQ_QUEUE", "videogen.ltx.generate"),
        ("PRAKASH_COMPLETION_HMAC_KEY_ID", "v1"),
        ("PRAKASH_COMPLETION_HMAC_SECRET_B64", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
    ])).unwrap();

    assert!(cfg.rabbitmq.enabled);
    assert_eq!(cfg.rabbitmq.prefetch, 1);
    assert_eq!(cfg.upload.refresh_margin_secs, 300);
    assert_eq!(cfg.outbox.max_attempts, 10);
    assert_eq!(cfg.generation_timeout_secs, 1800);
    assert_eq!(cfg.staged_input_ttl_hours, 24);
}
```

If `AppConfig::from_env_map` does not exist yet, add it as a testable constructor and have `from_env()` delegate to it.

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test config
```

Expected: fail because RabbitMQ config fields and test env parser do not exist.

- [ ] **Step 3: Add dependencies**

Add dependencies:

```toml
lapin = { version = "2", default-features = false, features = ["rustls"] }
sqlx = { version = "0.8", default-features = false, features = ["runtime-tokio-rustls", "sqlite", "macros", "chrono", "json"] }
chrono = { version = "0.4", features = ["serde"] }
hmac = "0.12"
sha2 = "0.10"
hex = "0.4"
base64 = "0.22"
```

Add dev dependency if not already present:

```toml
[dev-dependencies]
tempfile = "3"
```

If `lapin` requires a Tokio executor integration version available to this dependency set, prefer the current `lapin` Tokio/rustls integration. Only add `tokio-amqp` if `lapin` cannot compile with Tokio without it.

- [ ] **Step 4: Implement config structs**

Add:

```rust
#[derive(Clone, Debug)]
pub struct RabbitMqConfig {
    pub enabled: bool,
    pub amqps_urls: Vec<String>,
    pub queue: String,
    pub prefetch: u16,
    pub concurrency: usize,
    pub tls_ca_cert_pem_b64: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CompletionAuthConfig {
    pub key_id: String,
    pub secret_b64: String,
}

#[derive(Clone, Debug)]
pub struct WorkerStateConfig {
    pub db_path: String,
}

#[derive(Clone, Debug)]
pub struct UploadConfig {
    pub refresh_margin_secs: i64,
    pub upload_timeout_secs: u64,
    pub cleanup_after_upload: bool,
    pub multipart_field_name: String,
}

#[derive(Clone, Debug)]
pub struct OutboxConfig {
    pub initial_backoff_secs: u64,
    pub max_backoff_secs: u64,
    pub max_attempts: u32,
    pub timeout_secs: u64,
    pub retention_hours: u64,
}
```

Defaults:

- `VIDEOGEN_RABBITMQ_ENABLED=false`
- `VIDEOGEN_RABBITMQ_QUEUE=videogen.ltx.generate`
- `VIDEOGEN_RABBITMQ_PREFETCH=1`
- `VIDEOGEN_RABBITMQ_CONCURRENCY=1`
- `VIDEOGEN_STATE_DB_PATH=/workspace/videogen-worker/state.db`
- `VIDEOGEN_VAST_UPLOAD_EXPIRY_REFRESH_MARGIN_SECS=300`
- `VIDEOGEN_BUCKET_UPLOAD_TIMEOUT_SECS=300`
- `VIDEOGEN_BUCKET_UPLOAD_MULTIPART_FIELD=file`
- `VIDEOGEN_CLEANUP_AFTER_UPLOAD=true`
- `VIDEOGEN_LTX_GENERATION_TIMEOUT_SECS=1800`
- `VIDEOGEN_VAST_STAGED_IMAGE_TTL_HOURS=24`
- `VIDEOGEN_COMPLETION_OUTBOX_INITIAL_BACKOFF_SECS=10`
- `VIDEOGEN_COMPLETION_OUTBOX_MAX_BACKOFF_SECS=120`
- `VIDEOGEN_COMPLETION_OUTBOX_MAX_ATTEMPTS=10`
- `VIDEOGEN_COMPLETION_TIMEOUT_SECS=30`
- `VIDEOGEN_VAST_OUTBOX_RETENTION_HOURS=72`

When RabbitMQ is enabled, require:

- `VIDEOGEN_RABBITMQ_AMQPS_URLS`
- `PRAKASH_COMPLETION_HMAC_KEY_ID`
- `PRAKASH_COMPLETION_HMAC_SECRET_B64`

Validation:

- `VIDEOGEN_VAST_STAGED_IMAGE_TTL_HOURS` must be greater than `VIDEOGEN_LTX_GENERATION_TIMEOUT_SECS / 3600`, rounded up to at least 1 hour.
- `PRAKASH_COMPLETION_HMAC_SECRET_B64` must decode to exactly 32 bytes.
- RabbitMQ enabled in production requires TLS AMQPS URLs.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test config
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/config.rs
git commit -m "feat: add videogen rabbitmq config"
```

## Task 2: Define RabbitMQ Job DTOs

**Files:**
- Create: `src/rabbitmq/mod.rs`
- Create: `src/rabbitmq/types.rs`
- Modify: `src/main.rs`
- Test: `src/rabbitmq/types.rs`

- [ ] **Step 1: Write DTO parsing tests**

Create `src/rabbitmq/types.rs` tests:

```rust
#[test]
fn parses_prakash_job_message() {
    let raw = r#"{
      "request_id": "11111111-1111-4111-8111-111111111111",
      "request_key": { "principal": "aaaaa-aa", "counter": 17 },
      "user_principal": "aaaaa-aa",
      "model_id": "ltx2",
      "workflow_json": { "1": { "class_type": "CheckpointLoaderSimple" } },
      "input": { "prompt": "make a video" },
      "callback_url": "https://prakash.example/api/v2/videogen/complete",
      "upload_url_refresh_url": "https://prakash.example/api/v2/videogen/upload-url/refresh",
      "upload_destination": {
        "video_id": "video-1",
        "object_key": "videos/video-1.mp4",
        "upload_url": "https://upload.example/secret",
        "expires_at": "2026-06-03T12:00:00Z"
      }
    }"#;

    let job: PrakashVideoJob = serde_json::from_str(raw).unwrap();
    assert_eq!(job.request_id, "11111111-1111-4111-8111-111111111111");
    assert_eq!(job.request_key.counter, 17);
    assert_eq!(job.upload_destination.object_key, "videos/video-1.mp4");
}

#[test]
fn rejects_principal_mismatch() {
    let mut job = sample_job();
    job.user_principal = "bbbbb-bb".to_string();
    assert!(job.validate().unwrap_err().to_string().contains("principal"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test rabbitmq::types
```

Expected: fail because module and types do not exist.

- [ ] **Step 3: Implement DTOs**

Add:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrakashVideoJob {
    pub request_id: String,
    pub request_key: RequestKey,
    pub user_principal: String,
    pub model_id: String,
    pub workflow_json: serde_json::Value,
    #[serde(default)]
    pub input: serde_json::Value,
    pub callback_url: String,
    #[serde(default)]
    pub upload_url_refresh_url: Option<String>,
    pub upload_destination: UploadDestination,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestKey {
    pub principal: String,
    pub counter: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadDestination {
    pub video_id: String,
    pub object_key: String,
    pub upload_url: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    pub bucket_url: Option<String>,
}
```

Validation:

- `request_id` must parse as UUID.
- `request_key.principal == user_principal`.
- `model_id` must be non-empty.
- `callback_url` must be HTTPS except in local tests.
- `upload_destination.video_id`, `object_key`, and `upload_url` must be non-empty.
- `upload_destination.bucket_url`, when present, must be HTTPS except in local tests.
- `upload_url_refresh_url`, when present, must be HTTPS except in local tests.
- If the Task 0 decision says Prakash includes expected `bucket_url`, make `upload_destination.bucket_url` required by validation.

- [ ] **Step 4: Register module**

Add `mod rabbitmq;` in `src/main.rs` or create `src/lib.rs` and expose modules there if tests need library access.

- [ ] **Step 5: Run tests**

```bash
cargo test rabbitmq::types
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/rabbitmq/mod.rs src/rabbitmq/types.rs src/main.rs
git commit -m "feat: add prakash rabbitmq job dto"
```

## Task 3: Add Durable SQLite Job Store And Outbox

**Files:**
- Create: `src/jobs.rs`
- Modify: `src/main.rs`
- Test: `src/jobs.rs`

- [ ] **Step 1: Write job store tests**

```rust
#[tokio::test]
async fn claim_is_idempotent_by_request_id() {
    let store = JobStore::in_memory().await.unwrap();
    let job = sample_job();

    let first = store.claim_received(&job).await.unwrap();
    let second = store.claim_received(&job).await.unwrap();

    assert_eq!(first, ClaimResult::New);
    assert_eq!(second, ClaimResult::AlreadyExists(JobState::Received));
}

#[tokio::test]
async fn outbox_due_query_uses_backoff_and_attempts() {
    let store = JobStore::in_memory().await.unwrap();
    store.insert_completion_outbox(sample_success_completion()).await.unwrap();

    let due = store.due_completion_outbox(100).await.unwrap();
    assert_eq!(due.len(), 1);

    store.record_outbox_retry(&due[0].id, "network timeout").await.unwrap();
    let due = store.due_completion_outbox(100).await.unwrap();
    assert!(due.is_empty());
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test jobs
```

Expected: fail because `JobStore` does not exist.

- [ ] **Step 3: Implement schema and migrations in code**

Use `sqlx` SQLite and initialize with `CREATE TABLE IF NOT EXISTS`.

Tables:

```sql
CREATE TABLE IF NOT EXISTS videogen_jobs (
  request_id TEXT PRIMARY KEY,
  principal TEXT NOT NULL,
  counter INTEGER NOT NULL,
  user_principal TEXT NOT NULL,
  model_id TEXT NOT NULL,
  workflow_json TEXT NOT NULL,
  input_json TEXT NOT NULL,
  callback_url TEXT NOT NULL,
  upload_url_refresh_url TEXT,
  upload_destination_json TEXT NOT NULL,
  state TEXT NOT NULL,
  prompt_id TEXT,
  client_id TEXT,
  selected_output_json TEXT,
  staged_inputs_json TEXT,
  uploaded_json TEXT,
  bucket_url TEXT,
  failure_reason TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS completion_outbox (
  id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL,
  callback_url TEXT NOT NULL,
  body_json TEXT NOT NULL,
  state TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT NOT NULL,
  terminal_status_code INTEGER,
  completed_at TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(request_id) REFERENCES videogen_jobs(request_id)
);
```

Indexes:

```sql
CREATE INDEX IF NOT EXISTS idx_videogen_jobs_state ON videogen_jobs(state, updated_at);
CREATE INDEX IF NOT EXISTS idx_completion_outbox_due ON completion_outbox(state, next_attempt_at);
```

- [ ] **Step 4: Implement transition methods**

Methods:

- `open(path)`.
- `in_memory()` for tests.
- `claim_received(job) -> ClaimResult`.
- `mark_accepted(request_id, prompt_id, client_id)`.
- `mark_running(request_id)`.
- `mark_generated(request_id, outputs_json)`.
- `mark_uploading(request_id)`.
- `mark_uploaded(request_id, uploaded_json)`.
- `insert_completion_outbox(body)`.
- `mark_completion_sent(outbox_id, status_code)`.
- `mark_completion_failed(outbox_id, reason)`.
- `record_outbox_retry(outbox_id, error)`.
- `mark_failed(request_id, reason)`.
- `record_staged_input(request_id, staged_input_json)`.
- `recoverable_jobs(limit)`.
- `due_completion_outbox(limit)`.

Every state mutation must update `updated_at`.

- [ ] **Step 5: Run tests**

```bash
cargo test jobs
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/jobs.rs src/main.rs Cargo.toml Cargo.lock
git commit -m "feat: add durable videogen job store"
```

## Task 4: Add HMAC-Signed Prakash Client

**Files:**
- Create: `src/completion.rs`
- Test: `src/completion.rs`

- [ ] **Step 1: Write signing tests using the Prakash formula**

```rust
#[test]
fn signs_prakash_hmac_message() {
    let key = CompletionHmacKey::from_base64("v1", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").unwrap();
    let body = br#"{"status":"success"}"#;
    let signed = sign_prakash_request(
        "POST",
        "/api/v2/videogen/complete",
        1_777_000_000,
        body,
        &key,
    ).unwrap();

    assert_eq!(signed.body_sha256_hex, "912d0c07da7bdb22cdae025b96da26d01523aaab7362edb28544e3949deb369d");
    assert_eq!(signed.key_id, "v1");
    assert!(signed.authorization.starts_with("HMAC-SHA256 "));
}

#[test]
fn rejects_non_32_byte_hmac_secret() {
    let err = CompletionHmacKey::from_base64("v1", "AA==").unwrap_err();
    assert!(err.to_string().contains("32 bytes"));
}
```

Use a real expected SHA-256 value in the test rather than computing both expected and actual through the same helper.

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test completion
```

Expected: fail because `completion.rs` does not exist.

- [ ] **Step 3: Implement HMAC helpers**

Implement:

- `CompletionHmacKey::from_base64(key_id, secret_b64)`.
- `body_sha256_hex(raw_body)`.
- `sign_prakash_request(method, path, unix_timestamp, raw_body, key)`.
- `completion_path_from_url(url)`.

The signature message must match Prakash:

```rust
format!("{method}\n{path}\n{timestamp}\n{body_hash_hex}")
```

Headers:

```text
X-Timestamp: <unix_seconds>
X-Body-SHA256: <hex>
X-Key-Id: <key id>
Authorization: HMAC-SHA256 <hex_signature>
```

- [ ] **Step 4: Add Prakash completion DTOs**

Types:

- `CompletionRequestKey`.
- `CompletionStatus`.
- `CompleteVideoRequest`.
- `UploadRefreshRequest`.
- `UploadRefreshResponse`.

Keep field names identical to Prakash's Rust structs.

- [ ] **Step 5: Add client methods with fakeable trait**

Add:

```rust
#[async_trait::async_trait]
pub trait PrakashCompletionClient: Send + Sync {
    async fn send_completion(&self, url: &str, body: &CompleteVideoRequest) -> anyhow::Result<CompletionDeliveryResult>;
    async fn refresh_upload_url(&self, url: &str, body: &UploadRefreshRequest) -> anyhow::Result<UploadRefreshResponse>;
}
```

`CompletionDeliveryResult` should distinguish:

- terminal accepted: HTTP `200`, `202`, `409`.
- retryable: timeout, network error, HTTP `5xx`.
- non-retryable auth/config: HTTP `401`, `403`.
- bad request/conflict not covered above: non-retryable with body logged.

- [ ] **Step 6: Run tests**

```bash
cargo test completion
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add src/completion.rs Cargo.toml Cargo.lock
git commit -m "feat: add prakash completion client"
```

## Task 5: Add Output Resolution, Upload, Refresh, And Cleanup

**Files:**
- Create: `src/upload.rs`
- Modify: `src/backend/comfyui/client.rs`
- Test: `src/upload.rs`

- [ ] **Step 1: Write path-resolution tests**

```rust
#[test]
fn resolves_video_output_inside_comfyui_output_dir() {
    let output = OutputFile {
        filename: "video.mp4".to_string(),
        subfolder: Some("2026-06-03".to_string()),
        output_type: Some("videos".to_string()),
        local_path: None,
        url: None,
        node_id: None,
    };

    let path = resolve_comfy_output_path("/workspace/ComfyUI/output", &output).unwrap();
    assert_eq!(path, PathBuf::from("/workspace/ComfyUI/output/2026-06-03/video.mp4"));
}

#[test]
fn rejects_path_traversal_in_output_filename() {
    let output = OutputFile { filename: "../secret".to_string(), ..video_output() };
    assert!(resolve_comfy_output_path("/workspace/ComfyUI/output", &output).is_err());
}
```

- [ ] **Step 2: Write refresh decision tests**

```rust
#[test]
fn refreshes_when_upload_url_near_expiry() {
    let expires_at = Utc::now() + chrono::Duration::seconds(100);
    assert!(should_refresh_upload_url(expires_at, 300));
}
```

- [ ] **Step 2b: Write bucket URL and staged-input cleanup tests**

```rust
#[test]
fn uploaded_video_requires_bucket_url_for_success_completion() {
    let uploaded = UploadedVideo {
        bucket_url: None,
        video_id: "video-1".to_string(),
        object_key: "videos/video-1.mp4".to_string(),
        file_size: 100,
        content_type: "video/mp4".to_string(),
        checksum: "sha256:abc".to_string(),
    };

    assert!(uploaded.require_bucket_url().is_err());
}

#[tokio::test]
async fn terminal_job_deletes_tracked_staged_inputs() {
    let temp = tempfile::tempdir().unwrap();
    let staged = temp.path().join("input.png");
    tokio::fs::write(&staged, b"image").await.unwrap();

    cleanup_staged_inputs(&[staged.clone()], temp.path()).await.unwrap();

    assert!(!staged.exists());
}
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
cargo test upload
```

Expected: fail because upload module does not exist.

- [ ] **Step 4: Implement output selection and path resolution**

Functions:

- `select_primary_video_output(outputs: &[OutputFile]) -> Result<OutputFile>`.
- `resolve_comfy_output_path(output_dir: &str, output: &OutputFile) -> Result<PathBuf>`.

Rules:

- Prefer `output_type == Some("videos")`.
- Accept common video extensions: `.mp4`, `.webm`, `.mov`.
- Reject absolute filenames.
- Reject `..` path components in filename or subfolder.
- Canonicalize parent output dir when possible and verify final path stays under output dir.

- [ ] **Step 5: Implement upload destination refresh**

Before upload:

- Compare `upload_destination.expires_at` with `Utc::now() + refresh_margin`.
- If too close and `upload_url_refresh_url` exists, call Prakash refresh endpoint using HMAC.
- If too close and no refresh URL exists, return a retryable upload-destination error.
- If initial upload receives `403 Forbidden`, call refresh once and retry if refresh URL exists.
- A refreshed destination replaces the stored upload destination metadata before the retry.

- [ ] **Step 6: Implement bucket upload**

Upload protocol (locked): `POST` to `upload_destination.upload_url` with `multipart/form-data`, field name `file`, content type `video/mp4`.

After a successful upload (2xx), record:

- `video_id` — from `upload_destination.video_id`.
- `object_key` — from `upload_destination.object_key`.
- `bucket_url` — from `upload_destination.bucket_url` (Prakash pre-computed). Do NOT attempt to read it from the upload service response body; the upload service does not return it.
- `file_size`.
- `content_type`.
- `checksum = sha256:<hex>`.

If `upload_destination.bucket_url` is absent (Prakash bug, should never happen after Prakash adds the field), return `UploadError::MissingBucketUrl`, mark the job failed, enqueue a signed failure completion, and keep the local output for operator recovery. Do not invent arbitrary bucket URLs.

- [ ] **Step 7: Implement cleanup after persisted upload**

Add `cleanup_local_output(path)`:

- Only delete files under `COMFYUI_OUTPUT_DIR`.
- Only called after `JobStore::mark_uploaded` succeeds.
- Log and continue if deletion fails; completion callback should still proceed if upload succeeded.
- Delete staged input images tracked for this request after generation reaches terminal state and either success-upload metadata or failure-completion metadata is durable.
- Add TTL cleanup for orphan staged inputs older than `VIDEOGEN_VAST_STAGED_IMAGE_TTL_HOURS`.

- [ ] **Step 8: Run tests**

```bash
cargo test upload
```

Expected: pass.

- [ ] **Step 9: Commit**

```bash
git add src/upload.rs src/backend/comfyui/client.rs Cargo.toml Cargo.lock
git commit -m "feat: add videogen upload handling"
```

## Task 6: Add Worker Acceptance And Orchestration

**Files:**
- Create: `src/worker.rs`
- Modify: `src/backend/mod.rs`
- Modify: `src/backend/comfyui/mod.rs`
- Modify: `src/backend/comfyui/client.rs`
- Test: `src/worker.rs`

- [ ] **Step 1: Write orchestration tests with fake backend/upload/outbox**

```rust
#[tokio::test]
async fn successful_job_generates_uploads_and_enqueues_completion() {
    let store = JobStore::in_memory().await.unwrap();
    let backend = FakeBackend::completes_with(vec![video_output()]);
    let uploader = FakeUploader::success(uploaded_video());
    let completion = FakeCompletionClient::new();

    run_prakash_job(&store, &backend, &uploader, &completion, sample_job()).await.unwrap();

    let row = store.get_job(sample_job().request_id).await.unwrap().unwrap();
    assert_eq!(row.state, JobState::CompletionPending);
    assert_eq!(store.due_completion_outbox(10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn accept_job_returns_after_comfyui_acceptance_not_generation_completion() {
    let store = JobStore::in_memory().await.unwrap();
    let backend = FakeBackend::accepts_with_prompt("prompt-1");

    let accepted = accept_prakash_job(&store, &backend, sample_job()).await.unwrap();

    assert_eq!(accepted.prompt_id, "prompt-1");
    let row = store.get_job(sample_job().request_id).await.unwrap().unwrap();
    assert_eq!(row.state, JobState::Accepted);
}

#[tokio::test]
async fn generation_failure_enqueues_failure_completion() {
    let store = JobStore::in_memory().await.unwrap();
    let backend = FakeBackend::fails("ComfyUI execution error");

    run_prakash_job(&store, &backend, &FakeUploader::unused(), &FakeCompletionClient::new(), sample_job()).await.unwrap();

    let outbox = store.due_completion_outbox(10).await.unwrap();
    assert!(outbox[0].body_json.contains("\"status\":\"failure\""));
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test worker
```

Expected: fail because worker module does not exist.

- [ ] **Step 3: Split backend submission, monitoring, and legacy webhook**

Add worker-facing backend methods or helpers:

```rust
async fn submit_workflow(
    &self,
    request_id: &str,
    workflow_json: serde_json::Value,
    http_client: &reqwest::Client,
) -> anyhow::Result<AcceptedGeneration>;

async fn monitor_generation(
    &self,
    accepted: &AcceptedGeneration,
    http_client: &reqwest::Client,
) -> anyhow::Result<CompletedGeneration>;
```

`AcceptedGeneration` and `CompletedGeneration`:

```rust
pub struct AcceptedGeneration {
    pub request_id: String,
    pub prompt_id: String,
    pub client_id: String,
}

pub struct CompletedGeneration {
    pub prompt_id: String,
    pub outputs: Vec<crate::webhook::OutputFile>,
}
```

Do not remove existing `generate()` behavior. HTTP `/generate` should continue supporting local/manual testing when RabbitMQ is disabled, but migrated RabbitMQ jobs must not use legacy off-chain webhooks.

- [ ] **Step 4: Implement acceptance path used by RabbitMQ delivery handling**

Flow:

1. `claim_received(job)`.
2. If duplicate terminal job, return success without starting work.
3. Convert `job.workflow_json` to backend generation request.
4. Submit to ComfyUI and persist `prompt_id`/`client_id` as `accepted`.
5. Return `AcceptedJob` immediately to the consumer so it can ack RabbitMQ.

If ComfyUI rejects before `accepted`, enqueue a signed failure completion when the request has enough metadata, then return an ackable terminal decision. Prakash has already marked the context `submitted` after RabbitMQ publish-confirm, so this failure must be visible through Prakash completion rather than silently dropping the message.

- [ ] **Step 5: Implement post-ack job continuation**

Flow:

1. Load accepted job from SQLite.
2. Monitor ComfyUI using stored `prompt_id`/`client_id`.
3. Persist generated outputs.
4. Select video output.
5. Upload to `upload_destination`, refreshing if needed.
6. Persist uploaded metadata, including required `bucket_url`.
7. Insert success completion outbox record.
8. Delete local output if configured.
9. Delete tracked staged input images after terminal metadata is durable.
10. On generation/upload failure, insert failure completion outbox record.

Failure callbacks must include `request_id`, `request_key`, `user_principal`, and `provider`.

- [ ] **Step 6: Run tests**

```bash
cargo test worker
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add src/worker.rs src/backend/mod.rs src/backend/comfyui/mod.rs src/backend/comfyui/client.rs
git commit -m "feat: orchestrate rabbitmq videogen jobs"
```

## Task 7: Add RabbitMQ Consumer

**Files:**
- Create: `src/rabbitmq/consumer.rs`
- Modify: `src/rabbitmq/mod.rs`
- Modify: `src/main.rs`
- Test: `src/rabbitmq/consumer.rs`

- [ ] **Step 1: Write consumer behavior tests around handler seam**

Avoid tests that require a live broker for unit coverage. Put RabbitMQ delivery handling behind a small trait/fakeable function:

```rust
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test rabbitmq::consumer
```

Expected: fail because consumer module does not exist.

- [ ] **Step 3: Implement AMQPS connection**

Implement:

- Parse `VIDEOGEN_RABBITMQ_AMQPS_URLS` as comma-separated failover list.
- Use TLS for all broker URLs.
- Load optional CA from `VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64`.
- Connect to first healthy URL.
- Set `basic_qos(prefetch)`.
- Consume `VIDEOGEN_RABBITMQ_QUEUE`.

- [ ] **Step 4: Implement delivery ack policy**

Policy:

- Parse/validate message.
- Validate AMQP properties when present: `message_id` and `correlation_id` must match body `request_id`; `content_type` must be `application/json`.
- Persist and claim job before ack.
- Submit to ComfyUI and persist `accepted` with `prompt_id`.
- Acknowledge RabbitMQ after the job is durably accepted by ComfyUI, then spawn post-ack continuation.
- If local persistence fails, `nack(requeue=true)`.
- If JSON is invalid or has no enough request metadata for failure completion, `reject(requeue=false)` and alert.
- If schema/business validation fails after request key, principal, and request id are known and principal is internally consistent, enqueue a signed failure completion and `ack`.
- If `request_key.principal != user_principal`, reject without requeue and alert; do not forge a failure callback with corrected identity.
- If ComfyUI submit fails before acceptance, enqueue a signed failure completion and `ack`.
- If duplicate `request_id` is already in progress or terminal, `ack`.
- Do not hold RabbitMQ deliveries unacked for the full generation window.

Rationale: long-running generation can exceed broker delivery/consumer timeouts. Durable local state and recovery carry the async work after ack.

- [ ] **Step 5: Add concurrency guard**

Use a `tokio::sync::Semaphore` sized by `VIDEOGEN_RABBITMQ_CONCURRENCY`, default `1`.

The Vast GPU worker should start with concurrency `1`; only raise this after model memory behavior is measured.

- [ ] **Step 6: Run tests**

```bash
cargo test rabbitmq::consumer
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add src/rabbitmq/consumer.rs src/rabbitmq/mod.rs src/main.rs Cargo.toml Cargo.lock
git commit -m "feat: consume videogen jobs from rabbitmq"
```

## Task 8: Add Durable Completion Outbox Runner And Recovery

**Files:**
- Modify: `src/jobs.rs`
- Modify: `src/completion.rs`
- Create: `src/recovery.rs`
- Modify: `src/main.rs`
- Test: `src/recovery.rs`

- [ ] **Step 1: Write outbox retry tests**

```rust
#[tokio::test]
async fn outbox_treats_200_202_409_as_terminal() {
    for status in [200, 202, 409] {
        let store = JobStore::in_memory().await.unwrap();
        store.insert_completion_outbox(sample_success_completion()).await.unwrap();
        run_one_outbox_attempt(&store, &FakePrakash::status(status)).await.unwrap();
        assert_eq!(store.due_completion_outbox(10).await.unwrap().len(), 0);
    }
}

#[tokio::test]
async fn outbox_retries_5xx_with_bounded_backoff() {
    let store = JobStore::in_memory().await.unwrap();
    store.insert_completion_outbox(sample_success_completion()).await.unwrap();
    run_one_outbox_attempt(&store, &FakePrakash::status(503)).await.unwrap();
    let row = store.outbox_by_request_id("11111111-1111-4111-8111-111111111111").await.unwrap();
    assert_eq!(row.attempts, 1);
    assert!(row.next_attempt_at > Utc::now());
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test recovery
```

Expected: fail because recovery/outbox runner does not exist.

- [ ] **Step 3: Implement outbox runner**

Loop:

- Every 5 seconds, load due outbox rows up to a small batch size, default `25`.
- Send signed completion request at send time.
- Treat `200`, `202`, and `409` as terminal success for delivery.
- Retry network, timeout, and `5xx`.
- Do not retry `401` or `403`; mark non-retryable and log/Sentry.
- Exponential backoff: initial 10s, cap 120s, max attempts 10.

- [ ] **Step 4: Implement startup recovery**

On startup:

- Resume due outbox delivery.
- Recover `completion_pending` rows by ensuring an outbox row exists.
- Recover `uploaded` rows by inserting success outbox if missing.
- Recover `generated` or `uploading` rows by retrying upload if local output still exists.
- Recover `accepted` or `running` rows by querying ComfyUI history using stored `prompt_id`.
- If ComfyUI has no recoverable prompt/history and the job is older than `VIDEOGEN_LTX_GENERATION_TIMEOUT_SECS`, insert failure completion outbox.
- Purge terminal outbox rows only after `VIDEOGEN_VAST_OUTBOX_RETENTION_HOURS`.
- Recover `completion_failed` rows only through an explicit operator replay command; do not silently retry exhausted/non-retryable completion delivery forever.

Do not delete local files during recovery until upload success is persisted.

- [ ] **Step 5: Run tests**

```bash
cargo test recovery
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/recovery.rs src/jobs.rs src/completion.rs src/main.rs
git commit -m "feat: add videogen completion outbox recovery"
```

## Task 9: Wire Runtime Startup And Health

**Files:**
- Modify: `src/main.rs`
- Modify: `src/routes/health.rs`
- Modify: `src/config.rs`
- Test: `src/routes/health.rs`

- [ ] **Step 1: Write health output test**

```rust
#[tokio::test]
async fn health_includes_rabbitmq_when_enabled() {
    let state = test_state_with_rabbitmq_status("connected");
    let body = handle_health(State(state)).await.0;
    assert_eq!(body["rabbitmq"]["enabled"], true);
    assert_eq!(body["rabbitmq"]["status"], "connected");
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test routes::health
```

Expected: fail because health does not include RabbitMQ status.

- [ ] **Step 3: Wire startup**

In `main()`:

- Open `JobStore` from `VIDEOGEN_STATE_DB_PATH`.
- If `rabbitmq.enabled`:
  - Create Prakash completion client.
  - Spawn recovery runner.
  - Spawn completion outbox runner.
  - Spawn RabbitMQ consumer.
- If RabbitMQ is disabled:
  - Keep current behavior unchanged.

If RabbitMQ startup fails while enabled, fail process startup. Do not run a worker that looks healthy but cannot consume jobs.

- [ ] **Step 4: Add health state**

Expose:

```json
{
  "rabbitmq": {
    "enabled": true,
    "status": "connected",
    "queue": "videogen.ltx.generate"
  },
  "outbox": {
    "pending": 0,
    "failed": 0
  }
}
```

Do not include AMQPS URLs, passwords, upload URLs, HMAC secrets, or bearer tokens in health responses or logs.

- [ ] **Step 5: Run tests**

```bash
cargo test routes::health
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add src/main.rs src/routes/health.rs src/config.rs
git commit -m "feat: wire rabbitmq worker runtime"
```

## Task 10: Deployment And Documentation

**Files:**
- Modify: `deploy/start.sh`
- Modify: `.github/workflows/deploy.yml`
- Modify: `docker-compose.yml`
- Modify: `README.md`
- Test: workflow/build validation

- [ ] **Step 1: Update `deploy/start.sh`**

Pass through these env vars to the `tmux` worker session:

```bash
VIDEOGEN_RABBITMQ_ENABLED
VIDEOGEN_RABBITMQ_AMQPS_URLS
VIDEOGEN_RABBITMQ_QUEUE
VIDEOGEN_RABBITMQ_PREFETCH
VIDEOGEN_RABBITMQ_CONCURRENCY
VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64
VIDEOGEN_STATE_DB_PATH
PRAKASH_COMPLETION_HMAC_KEY_ID
PRAKASH_COMPLETION_HMAC_SECRET_B64
VIDEOGEN_VAST_UPLOAD_EXPIRY_REFRESH_MARGIN_SECS
VIDEOGEN_BUCKET_UPLOAD_TIMEOUT_SECS
VIDEOGEN_BUCKET_UPLOAD_MULTIPART_FIELD
VIDEOGEN_LTX_GENERATION_TIMEOUT_SECS
VIDEOGEN_COMPLETION_OUTBOX_INITIAL_BACKOFF_SECS
VIDEOGEN_COMPLETION_OUTBOX_MAX_BACKOFF_SECS
VIDEOGEN_COMPLETION_OUTBOX_MAX_ATTEMPTS
VIDEOGEN_COMPLETION_TIMEOUT_SECS
VIDEOGEN_VAST_OUTBOX_RETENTION_HOURS
VIDEOGEN_VAST_STAGED_IMAGE_TTL_HOURS
```

Set default `VIDEOGEN_STATE_DB_PATH=/workspace/videogen-worker/state.db` and create its parent directory before starting the worker.

- [ ] **Step 2: Update GitHub Actions deploy env**

Add production secrets:

- `VIDEOGEN_RABBITMQ_AMQPS_URLS`
- `VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64`
- `PRAKASH_COMPLETION_HMAC_KEY_ID`
- `PRAKASH_COMPLETION_HMAC_SECRET_B64`

Export:

```bash
export VIDEOGEN_RABBITMQ_ENABLED=true
export VIDEOGEN_RABBITMQ_AMQPS_URLS="${{ secrets.VIDEOGEN_RABBITMQ_AMQPS_URLS }}"
export VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64="${{ secrets.VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64 }}"
export PRAKASH_COMPLETION_HMAC_KEY_ID="${{ secrets.PRAKASH_COMPLETION_HMAC_KEY_ID }}"
export PRAKASH_COMPLETION_HMAC_SECRET_B64="${{ secrets.PRAKASH_COMPLETION_HMAC_SECRET_B64 }}"
```

Keep `AUTH_TOKEN` for rollback HTTP endpoints.

- [ ] **Step 3: Update `docker-compose.yml`**

Add the same optional envs and a durable state volume:

```yaml
volumes:
  videogen_state:
```

Mount:

```yaml
- videogen_state:/workspace/videogen-worker
```

- [ ] **Step 4: Update README**

Document:

- Existing HTTP mode is rollback/manual.
- Production migrated mode uses RabbitMQ.
- Off-chain-agent is legacy drain only and must not receive migrated jobs.
- Required broker values.
- Required Prakash completion HMAC values.
- `bucket_url` source selected in Task 0.
- Upload protocol: multipart field `file` unless Task 0 changed it.
- Local state DB path and recovery behavior.
- Local output cleanup happens only after bucket upload success is persisted.
- Staged input cleanup and TTL behavior.
- How to check health and logs.

- [ ] **Step 5: Run format and tests**

```bash
cargo fmt
cargo test
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add deploy/start.sh .github/workflows/deploy.yml docker-compose.yml README.md
git commit -m "chore: document and deploy rabbitmq consumer mode"
```

## Task 11: Broker Smoke Test

**Files:**
- Create: `scripts/smoke-rabbitmq-consume.sh`
- Test: manual smoke against deployed RabbitMQ and Vast worker

- [ ] **Step 1: Add a local smoke script**

Script responsibilities:

- Read `VIDEOGEN_RABBITMQ_AMQPS_URLS`.
- Verify TLS connection to broker.
- Print certificate subject/SANs without secrets.
- Optionally publish a test message when `SMOKE_PUBLISH=true`.
- Optionally run worker in `--consume-once` mode if that flag is implemented.

Do not print AMQPS credentials.

- [ ] **Step 2: Add `--consume-once` or equivalent test mode**

If cheap to add, support:

```bash
videogen-worker --consume-once
```

It should:

- Start config.
- Consume one RabbitMQ message.
- Process it.
- Exit after terminal local state or a clear timeout.

If CLI plumbing is too large, leave this as an integration-only script that checks connectivity and document manual test steps.

- [ ] **Step 3: Run broker TLS pre-check**

Before consuming on Vast:

```bash
openssl s_client -connect 94.130.13.115:5671 -servername rabbitmq.prakash.internal </dev/null
```

Expected:

- certificate validates against the configured CA.
- SAN covers the hostnames or IPs used in `VIDEOGEN_RABBITMQ_AMQPS_URLS`.

- [ ] **Step 4: Run controlled end-to-end smoke**

Use a tiny known-good ComfyUI workflow and a disposable upload destination from Prakash staging.

Expected:

- RabbitMQ message is consumed once.
- Job row enters `accepted`, then `generated`, then `uploaded`.
- Success outbox row contains non-empty `bucket_url`.
- Local output file is deleted after upload success.
- Tracked staged image input is deleted after terminal state.
- Completion outbox delivers to Prakash.
- Prakash context reaches `complete` or expected draft state.

- [ ] **Step 5: Commit smoke tooling**

```bash
git add scripts/smoke-rabbitmq-consume.sh
git commit -m "test: add videogen rabbitmq smoke tooling"
```

## Task 12: Final Verification And Rollout Gates

**Files:**
- No new files unless verification reveals issues.

- [ ] **Step 1: Run full verification**

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: all pass.

- [ ] **Step 2: Verify no secret leakage**

Search for accidental logs or docs containing credentials:

```bash
rg -n "upload_url|PRAKASH_COMPLETION_HMAC_SECRET|VIDEOGEN_RABBITMQ_AMQPS_URLS|password|secret" src deploy README.md .github
```

Expected:

- Env var names may appear.
- Secret values and scoped upload URLs must not appear in logs, tests, docs, or errors.

- [ ] **Step 3: Verify old HTTP rollback path**

Run:

```bash
cargo test routes::generate
```

Then manually verify `/generate` still accepts the existing request shape when RabbitMQ is disabled.

- [ ] **Step 4: Verify startup behavior**

Cases:

- RabbitMQ disabled: worker starts and health is backend-only.
- RabbitMQ enabled with missing AMQPS URL: startup fails.
- RabbitMQ enabled with invalid HMAC secret: startup fails.
- RabbitMQ enabled with broker down: startup fails or reports unhealthy according to the chosen startup policy.
- RabbitMQ enabled with staged input TTL lower than generation timeout: startup fails.

- [ ] **Step 5: Rollout gates**

Do not switch Prakash submit transport to RabbitMQ until all are true:

- Broker quorum queue is healthy.
- Vast worker is deployed with RabbitMQ enabled.
- Vast worker health shows RabbitMQ connected.
- Completion HMAC key id matches Prakash accepted registry.
- One staging end-to-end generation completes.
- Local output cleanup is verified after upload.
- Staged input cleanup is verified.
- No migrated request hits off-chain `/comfyui/webhook`, QStash video generation callbacks, or off-chain draft upload paths.
- Completion outbox has no stuck rows after smoke.

- [ ] **Step 6: Commit any final fixes**

```bash
git status --short
git add <changed-files>
git commit -m "fix: finalize videogen rabbitmq consumer"
```

## Notes For Implementers

- Do not remove direct HTTP `/generate`; it is useful rollback.
- Do not use off-chain-agent for migrated runtime work. Off-chain is legacy drain only.
- Do not trust arbitrary callback or refresh URLs from unvalidated messages in tests. Production messages come from authenticated RabbitMQ, but validation should still require HTTPS.
- Do not log `upload_url`, AMQPS credentials, bearer tokens, or HMAC secrets.
- Ack RabbitMQ only after durable local claim and ComfyUI acceptance; outbox handles later completion delivery.
- If the worker crashes after RabbitMQ ack, recovery must either resume from stored `prompt_id` or send a failure completion after the configured timeout.
- Do not send success completion without a real `bucket_url` while Prakash requires that field.
- Use multipart upload field `file` unless Task 0 explicitly changes the upload service contract and the Prakash docs are updated in the same change.
- Treat `202` from Prakash completion as success for Vast delivery. Prakash reconciliation owns any crash after it has acknowledged the callback.
- Treat `409` from Prakash completion as terminal for Vast delivery. It means Prakash has already terminally handled or rejected the context.
- Keep RabbitMQ consumer concurrency at `1` until GPU memory behavior is proven safe.
