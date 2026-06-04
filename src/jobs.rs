use crate::rabbitmq::types::PrakashVideoJob;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Received,
    Accepted,
    Running,
    Generated,
    Uploading,
    Uploaded,
    CompletionPending,
    CompletionSent,
    Failed,
    CompletionFailed,
}

impl JobState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::Generated => "generated",
            Self::Uploading => "uploading",
            Self::Uploaded => "uploaded",
            Self::CompletionPending => "completion_pending",
            Self::CompletionSent => "completion_sent",
            Self::Failed => "failed",
            Self::CompletionFailed => "completion_failed",
        }
    }

    pub fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "received" => Ok(Self::Received),
            "accepted" => Ok(Self::Accepted),
            "running" => Ok(Self::Running),
            "generated" => Ok(Self::Generated),
            "uploading" => Ok(Self::Uploading),
            "uploaded" => Ok(Self::Uploaded),
            "completion_pending" => Ok(Self::CompletionPending),
            "completion_sent" => Ok(Self::CompletionSent),
            "failed" => Ok(Self::Failed),
            "completion_failed" => Ok(Self::CompletionFailed),
            _ => Err(anyhow::anyhow!("unknown job state: {s}")),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClaimResult {
    New,
    AlreadyExists(JobState),
}

pub struct OutboxEntry {
    pub id: String,
    pub request_id: String,
    pub callback_url: String,
    pub body_json: String,
}

pub struct OutboxRow {
    pub id: String,
    pub request_id: String,
    pub callback_url: String,
    pub body_json: String,
    pub attempts: i64,
}

/// A minimal row returned by `get_job`.
pub struct JobRow {
    pub request_id: String,
    pub state: JobState,
}

pub struct JobStore {
    pool: sqlx::SqlitePool,
}

const MIGRATE_SQL: &str = "
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
  state TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT NOT NULL,
  terminal_status_code INTEGER,
  completed_at TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(request_id) REFERENCES videogen_jobs(request_id)
);

CREATE INDEX IF NOT EXISTS idx_videogen_jobs_state ON videogen_jobs(state, updated_at);
CREATE INDEX IF NOT EXISTS idx_completion_outbox_due ON completion_outbox(state, next_attempt_at);
";

impl JobStore {
    pub async fn in_memory() -> anyhow::Result<Self> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    pub async fn open(path: &str) -> anyhow::Result<Self> {
        use sqlx::sqlite::SqliteConnectOptions;
        use std::str::FromStr;
        let opts = SqliteConnectOptions::from_str(&format!("sqlite:{path}"))?
            .create_if_missing(true);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_with(opts)
            .await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        for statement in MIGRATE_SQL.split(';') {
            let statement = statement.trim();
            if !statement.is_empty() {
                sqlx::query(statement).execute(&self.pool).await?;
            }
        }
        Ok(())
    }

    pub async fn claim_received(&self, job: &PrakashVideoJob) -> anyhow::Result<ClaimResult> {
        let now = chrono::Utc::now().to_rfc3339();
        let principal = &job.request_key.principal;
        let counter = job.request_key.counter as i64;
        let workflow_json = serde_json::to_string(&job.workflow_json)?;
        let input_json = serde_json::to_string(&job.input)?;
        let upload_destination_json = serde_json::to_string(&job.upload_destination)?;

        let result = sqlx::query(
            "INSERT OR IGNORE INTO videogen_jobs
             (request_id, principal, counter, user_principal, model_id, workflow_json, input_json,
              callback_url, upload_url_refresh_url, upload_destination_json, state, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'received', ?, ?)",
        )
        .bind(&job.request_id)
        .bind(principal)
        .bind(counter)
        .bind(&job.user_principal)
        .bind(&job.model_id)
        .bind(&workflow_json)
        .bind(&input_json)
        .bind(&job.callback_url)
        .bind(&job.upload_url_refresh_url)
        .bind(&upload_destination_json)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() > 0 {
            Ok(ClaimResult::New)
        } else {
            let row = sqlx::query_as::<_, (String,)>(
                "SELECT state FROM videogen_jobs WHERE request_id = ?",
            )
            .bind(&job.request_id)
            .fetch_one(&self.pool)
            .await?;
            Ok(ClaimResult::AlreadyExists(JobState::from_str(&row.0)?))
        }
    }

    pub async fn insert_completion_outbox(&self, entry: OutboxEntry) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO completion_outbox
             (id, request_id, callback_url, body_json, state, next_attempt_at, created_at, updated_at)
             VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)",
        )
        .bind(&entry.id)
        .bind(&entry.request_id)
        .bind(&entry.callback_url)
        .bind(&entry.body_json)
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn due_completion_outbox(&self, limit: i64) -> anyhow::Result<Vec<OutboxRow>> {
        let now = chrono::Utc::now().to_rfc3339();
        let rows = sqlx::query_as::<_, (String, String, String, String, i64)>(
            "SELECT id, request_id, callback_url, body_json, attempts
             FROM completion_outbox
             WHERE state = 'pending' AND next_attempt_at <= ?
             LIMIT ?",
        )
        .bind(&now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| OutboxRow {
                id: r.0,
                request_id: r.1,
                callback_url: r.2,
                body_json: r.3,
                attempts: r.4,
            })
            .collect())
    }

    pub async fn record_outbox_retry(&self, outbox_id: &str, error: &str) -> anyhow::Result<()> {
        let row = sqlx::query_as::<_, (i64,)>(
            "SELECT attempts FROM completion_outbox WHERE id = ?",
        )
        .bind(outbox_id)
        .fetch_one(&self.pool)
        .await?;

        let attempts = row.0 + 1;
        let backoff_secs = (10u64 * 2u64.pow(attempts as u32)).min(120);
        let next_attempt_at = (chrono::Utc::now()
            + chrono::Duration::seconds(backoff_secs as i64))
        .to_rfc3339();
        let now = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            "UPDATE completion_outbox
             SET attempts = ?, last_error = ?, next_attempt_at = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(attempts)
        .bind(error)
        .bind(&next_attempt_at)
        .bind(&now)
        .bind(outbox_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_accepted(
        &self,
        request_id: &str,
        prompt_id: &str,
        client_id: &str,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE videogen_jobs
             SET state = 'accepted', prompt_id = ?, client_id = ?, updated_at = ?
             WHERE request_id = ?",
        )
        .bind(prompt_id)
        .bind(client_id)
        .bind(&now)
        .bind(request_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_running(&self, request_id: &str) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE videogen_jobs SET state = 'running', updated_at = ? WHERE request_id = ?",
        )
        .bind(&now)
        .bind(request_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_generated(
        &self,
        request_id: &str,
        selected_output_json: &str,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE videogen_jobs
             SET state = 'generated', selected_output_json = ?, updated_at = ?
             WHERE request_id = ?",
        )
        .bind(selected_output_json)
        .bind(&now)
        .bind(request_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_uploading(&self, request_id: &str) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE videogen_jobs SET state = 'uploading', updated_at = ? WHERE request_id = ?",
        )
        .bind(&now)
        .bind(request_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_uploaded(
        &self,
        request_id: &str,
        uploaded_json: &str,
        bucket_url: &str,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE videogen_jobs
             SET state = 'uploaded', uploaded_json = ?, bucket_url = ?, updated_at = ?
             WHERE request_id = ?",
        )
        .bind(uploaded_json)
        .bind(bucket_url)
        .bind(&now)
        .bind(request_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_failed(&self, request_id: &str, reason: &str) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE videogen_jobs
             SET state = 'failed', failure_reason = ?, updated_at = ?
             WHERE request_id = ?",
        )
        .bind(reason)
        .bind(&now)
        .bind(request_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_completion_sent(
        &self,
        outbox_id: &str,
        status_code: i64,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE completion_outbox
             SET state = 'completion_sent', terminal_status_code = ?, completed_at = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(status_code)
        .bind(&now)
        .bind(&now)
        .bind(outbox_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_completion_failed(&self, outbox_id: &str, reason: &str) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE completion_outbox
             SET state = 'completion_failed', last_error = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(reason)
        .bind(&now)
        .bind(outbox_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record_staged_input(
        &self,
        request_id: &str,
        staged_input_json: &str,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE videogen_jobs
             SET staged_inputs_json = ?, updated_at = ?
             WHERE request_id = ?",
        )
        .bind(staged_input_json)
        .bind(&now)
        .bind(request_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Fetch a single job row by request_id. Returns `None` if not found.
    pub async fn get_job(&self, request_id: &str) -> anyhow::Result<Option<JobRow>> {
        let row = sqlx::query_as::<_, (String, String)>(
            "SELECT request_id, state FROM videogen_jobs WHERE request_id = ?",
        )
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            None => Ok(None),
            Some((rid, state_str)) => Ok(Some(JobRow {
                request_id: rid,
                state: JobState::from_str(&state_str)?,
            })),
        }
    }

    /// Transition a job to `completion_pending` state (after outbox entry is inserted).
    pub async fn mark_completion_pending(&self, request_id: &str) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE videogen_jobs SET state = 'completion_pending', updated_at = ? WHERE request_id = ?",
        )
        .bind(&now)
        .bind(request_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn recoverable_jobs(&self, limit: i64) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT request_id FROM videogen_jobs
             WHERE state IN ('accepted', 'running', 'generated', 'uploading', 'uploaded', 'completion_pending')
             ORDER BY updated_at ASC
             LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.0).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rabbitmq::types::{PrakashVideoJob, RequestKey, UploadDestination};

    fn sample_job() -> PrakashVideoJob {
        serde_json::from_str(r#"{
            "request_id": "11111111-1111-4111-8111-111111111111",
            "request_key": { "principal": "aaaaa-aa", "counter": 17 },
            "user_principal": "aaaaa-aa",
            "model_id": "ltx2",
            "workflow_json": { "1": {} },
            "input": {},
            "callback_url": "https://prakash.example/api/v2/videogen/complete",
            "upload_destination": {
                "video_id": "video-1",
                "object_key": "videos/video-1.mp4",
                "upload_url": "https://upload.example/secret",
                "expires_at": "2026-06-03T12:00:00Z",
                "bucket_url": "https://bucket.example/videos/video-1.mp4"
            }
        }"#).unwrap()
    }

    fn sample_success_completion() -> OutboxEntry {
        OutboxEntry {
            id: uuid::Uuid::new_v4().to_string(),
            request_id: "11111111-1111-4111-8111-111111111111".to_string(),
            callback_url: "https://prakash.example/api/v2/videogen/complete".to_string(),
            body_json: r#"{"status":"success"}"#.to_string(),
        }
    }

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
        // Insert a job first to satisfy the FK constraint
        store.claim_received(&sample_job()).await.unwrap();
        store
            .insert_completion_outbox(sample_success_completion())
            .await
            .unwrap();

        let due = store.due_completion_outbox(100).await.unwrap();
        assert_eq!(due.len(), 1);

        store
            .record_outbox_retry(&due[0].id, "network timeout")
            .await
            .unwrap();
        let due = store.due_completion_outbox(100).await.unwrap();
        assert!(due.is_empty());
    }
}
