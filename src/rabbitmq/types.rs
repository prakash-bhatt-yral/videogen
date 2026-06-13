use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
pub struct VideoGenerationJob {
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_image_key: Option<String>,
}

impl std::fmt::Debug for VideoGenerationJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VideoGenerationJob")
            .field("request_id", &self.request_id)
            .field("request_key", &self.request_key)
            .field("user_principal", &self.user_principal)
            .field("model_id", &self.model_id)
            .field("workflow_json", &self.workflow_json)
            .field("input", &self.input)
            .field("callback_url", &self.callback_url)
            .field("upload_url_refresh_url", &self.upload_url_refresh_url)
            .field("upload_destination", &self.upload_destination)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestKey {
    pub principal: String,
    pub counter: u64,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadDestination {
    pub video_id: String,
    pub object_key: String,
    // sensitive: pre-signed upload URL — redacted in Debug
    pub upload_url: String,
    pub expires_at: DateTime<Utc>,
    #[serde(default)]
    // sensitive: public bucket URL — redacted in Debug
    pub bucket_url: Option<String>,
    #[serde(default)]
    // sensitive: AES-256-GCM encrypted delegated identity — redacted in Debug
    pub encrypted_identity: Option<String>,
}

impl std::fmt::Debug for UploadDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadDestination")
            .field("video_id", &self.video_id)
            .field("object_key", &self.object_key)
            .field("upload_url", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .field(
                "bucket_url",
                &self.bucket_url.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "encrypted_identity",
                &self.encrypted_identity.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub enum JobValidationError {
    #[error("invalid request_id: {0}")]
    InvalidRequestId(String),
    #[error("principal mismatch: request_key.principal ({0}) != user_principal ({1})")]
    PrincipalMismatch(String, String),
    #[error("model_id is empty")]
    EmptyModelId,
    #[error("upload_destination.video_id is empty")]
    EmptyVideoId,
    #[error("upload_destination.object_key is empty")]
    EmptyObjectKey,
    #[error("upload_destination.upload_url is empty")]
    EmptyUploadUrl,
    #[error("upload_destination.bucket_url is required in the job payload")]
    MissingBucketUrl,
}

impl VideoGenerationJob {
    #[allow(dead_code)]
    pub fn validate(&self) -> Result<(), JobValidationError> {
        Uuid::parse_str(&self.request_id)
            .map_err(|e| JobValidationError::InvalidRequestId(e.to_string()))?;

        if self.request_key.principal != self.user_principal {
            return Err(JobValidationError::PrincipalMismatch(
                self.request_key.principal.clone(),
                self.user_principal.clone(),
            ));
        }
        if self.model_id.is_empty() {
            return Err(JobValidationError::EmptyModelId);
        }
        if self.upload_destination.video_id.is_empty() {
            return Err(JobValidationError::EmptyVideoId);
        }
        if self.upload_destination.object_key.is_empty() {
            return Err(JobValidationError::EmptyObjectKey);
        }
        if self.upload_destination.upload_url.is_empty() {
            return Err(JobValidationError::EmptyUploadUrl);
        }
        if self.upload_destination.bucket_url.is_none() {
            return Err(JobValidationError::MissingBucketUrl);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_job() -> VideoGenerationJob {
        serde_json::from_str(
            r#"{
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
                "expires_at": "2026-06-03T12:00:00Z",
                "bucket_url": "https://bucket.example/videos/video-1.mp4"
            }
        }"#,
        )
        .unwrap()
    }

    #[test]
    fn parses_video_generation_job_message() {
        let job = sample_job();
        assert_eq!(job.request_id, "11111111-1111-4111-8111-111111111111");
        assert_eq!(job.request_key.counter, 17);
        assert_eq!(job.upload_destination.object_key, "videos/video-1.mp4");
        assert_eq!(
            job.upload_destination.bucket_url,
            Some("https://bucket.example/videos/video-1.mp4".to_string())
        );
    }

    #[test]
    fn rejects_principal_mismatch() {
        let mut job = sample_job();
        job.user_principal = "bbbbb-bb".to_string();
        assert!(job
            .validate()
            .unwrap_err()
            .to_string()
            .contains("principal"));
    }

    #[test]
    fn rejects_missing_bucket_url() {
        let raw = r#"{
            "request_id": "11111111-1111-4111-8111-111111111111",
            "request_key": { "principal": "aaaaa-aa", "counter": 17 },
            "user_principal": "aaaaa-aa",
            "model_id": "ltx2",
            "workflow_json": {},
            "callback_url": "https://prakash.example/api/v2/videogen/complete",
            "upload_destination": {
                "video_id": "video-1",
                "object_key": "videos/video-1.mp4",
                "upload_url": "https://upload.example/secret",
                "expires_at": "2026-06-03T12:00:00Z"
            }
        }"#;
        let job: VideoGenerationJob = serde_json::from_str(raw).unwrap();
        assert!(job
            .validate()
            .unwrap_err()
            .to_string()
            .contains("bucket_url"));
    }

    #[test]
    fn rejects_invalid_uuid_request_id() {
        let mut job = sample_job();
        job.request_id = "not-a-uuid".to_string();
        assert!(job.validate().is_err());
    }
}
