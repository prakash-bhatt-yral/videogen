use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

// ─── HMAC Key ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct CompletionHmacKey {
    pub key_id: String,
    secret: Vec<u8>,
}

impl CompletionHmacKey {
    /// Build key from a raw string (e.g. AUTH_TOKEN). Uses the string bytes directly.
    pub fn from_str(key_id: &str, secret: &str) -> anyhow::Result<Self> {
        if secret.is_empty() {
            return Err(anyhow::anyhow!("HMAC secret must not be empty"));
        }
        Ok(Self {
            key_id: key_id.to_string(),
            secret: secret.as_bytes().to_vec(),
        })
    }

    /// Build key from a base64-encoded secret (kept for compatibility).
    pub fn from_base64(key_id: &str, secret_b64: &str) -> anyhow::Result<Self> {
        let secret = base64::engine::general_purpose::STANDARD
            .decode(secret_b64)
            .map_err(|_| anyhow::anyhow!("HMAC secret is not valid base64"))?;
        if secret.is_empty() {
            return Err(anyhow::anyhow!("HMAC secret must not be empty"));
        }
        Ok(Self {
            key_id: key_id.to_string(),
            secret,
        })
    }
}

// ─── Signing helpers ─────────────────────────────────────────────────────────

pub struct SignedRequest {
    pub key_id: String,
    pub timestamp: i64,
    pub body_sha256_hex: String,
    pub authorization: String,
}

pub fn body_sha256_hex(raw_body: &[u8]) -> String {
    hex::encode(Sha256::digest(raw_body))
}

/// Signs an API request using HMAC-SHA256.
///
/// Message format: `"{METHOD}\n{PATH}\n{UNIX_TIMESTAMP}\n{BODY_SHA256_HEX}"`
///
/// # Security
/// The HMAC secret is never logged or exposed via this function.
pub fn sign_hmac_request(
    method: &str,
    path: &str,
    unix_timestamp: i64,
    raw_body: &[u8],
    key: &CompletionHmacKey,
) -> anyhow::Result<SignedRequest> {
    let body_hash = body_sha256_hex(raw_body);
    let message = format!("{method}\n{path}\n{unix_timestamp}\n{body_hash}");
    let mut mac = HmacSha256::new_from_slice(&key.secret)
        .map_err(|e| anyhow::anyhow!("HMAC init failed: {e}"))?;
    mac.update(message.as_bytes());
    let sig = hex::encode(mac.finalize().into_bytes());
    Ok(SignedRequest {
        key_id: key.key_id.clone(),
        timestamp: unix_timestamp,
        body_sha256_hex: body_hash,
        authorization: format!("HMAC-SHA256 {sig}"),
    })
}

pub fn completion_path_from_url(url: &str) -> anyhow::Result<String> {
    let parsed = url::Url::parse(url)?;
    let path = parsed.path();
    match parsed.query() {
        Some(q) => Ok(format!("{path}?{q}")),
        None => Ok(path.to_string()),
    }
}

// ─── DTOs ────────────────────────────────────────────────────────────────────

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionRequestKey {
    pub principal: String,
    pub counter: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompletionStatus {
    Success,
    Failure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteVideoRequest {
    pub request_key: CompletionRequestKey,
    pub user_principal: String,
    pub request_id: String,
    pub provider: String,
    pub status: CompletionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encrypted_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_image_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadRefreshRequest {
    pub request_key: CompletionRequestKey,
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadRefreshResponse {
    pub upload_url: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

// ─── Delivery result ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionDeliveryResult {
    /// 200, 202, 409 — terminal (no retry needed)
    Accepted(u16),
    /// timeout, network error, 5xx — should be retried
    Retryable(String),
    /// 401, 403, other permanent errors — do not retry
    NonRetryable(String),
}

impl CompletionDeliveryResult {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Accepted(_))
    }
}

// ─── Client trait ────────────────────────────────────────────────────────────

#[async_trait::async_trait]
pub trait CompletionClient: Send + Sync {
    async fn send_completion(
        &self,
        url: &str,
        body: &CompleteVideoRequest,
    ) -> anyhow::Result<CompletionDeliveryResult>;

    async fn refresh_upload_url(
        &self,
        url: &str,
        body: &UploadRefreshRequest,
    ) -> anyhow::Result<UploadRefreshResponse>;
}

// ─── Runtime HMAC client ─────────────────────────────────────────────────────

pub struct HmacCompletionClient {
    http: reqwest::Client,
    key: CompletionHmacKey,
}

impl HmacCompletionClient {
    pub fn new(key: CompletionHmacKey) -> Self {
        Self {
            http: reqwest::Client::new(),
            key,
        }
    }

    async fn post_signed<B: Serialize>(
        &self,
        url: &str,
        body: &B,
    ) -> anyhow::Result<reqwest::Response> {
        let raw = serde_json::to_vec(body)?;
        let path = completion_path_from_url(url)?;
        let timestamp = chrono::Utc::now().timestamp();
        let signed = sign_hmac_request("POST", &path, timestamp, &raw, &self.key)?;
        self.http
            .post(url)
            .header("Content-Type", "application/json")
            .header("X-Timestamp", signed.timestamp.to_string())
            .header("X-Body-SHA256", &signed.body_sha256_hex)
            .header("X-Key-Id", &signed.key_id)
            .header("Authorization", &signed.authorization)
            .body(raw)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("HTTP error: {e}"))
    }
}

#[async_trait::async_trait]
impl CompletionClient for HmacCompletionClient {
    async fn send_completion(
        &self,
        url: &str,
        body: &CompleteVideoRequest,
    ) -> anyhow::Result<CompletionDeliveryResult> {
        let resp = self.post_signed(url, body).await;
        match resp {
            Err(e) => Ok(CompletionDeliveryResult::Retryable(e.to_string())),
            Ok(r) => {
                let status = r.status().as_u16();
                match status {
                    200 | 202 | 409 => Ok(CompletionDeliveryResult::Accepted(status)),
                    401 | 403 => {
                        let body = r.text().await.unwrap_or_default();
                        Ok(CompletionDeliveryResult::NonRetryable(format!(
                            "auth error: {status}: {body}"
                        )))
                    }
                    500..=599 => {
                        let body = r.text().await.unwrap_or_default();
                        Ok(CompletionDeliveryResult::Retryable(format!(
                            "server error: {status}: {body}"
                        )))
                    }
                    _ => {
                        let body = r.text().await.unwrap_or_default();
                        Ok(CompletionDeliveryResult::NonRetryable(format!(
                            "unexpected status: {status}: {body}"
                        )))
                    }
                }
            }
        }
    }

    async fn refresh_upload_url(
        &self,
        url: &str,
        body: &UploadRefreshRequest,
    ) -> anyhow::Result<UploadRefreshResponse> {
        let resp = self.post_signed(url, body).await?;
        if !resp.status().is_success() {
            return Err(anyhow::anyhow!("upload refresh failed: {}", resp.status()));
        }
        Ok(resp.json::<UploadRefreshResponse>().await?)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_hmac_request_message() {
        let key =
            CompletionHmacKey::from_base64("v1", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
                .unwrap();
        let body = br#"{"status":"success"}"#;
        let signed = sign_hmac_request(
            "POST",
            "/api/v2/videogen/complete",
            1_777_000_000,
            body,
            &key,
        )
        .unwrap();

        // Verified independently: echo -n '{"status":"success"}' | sha256sum
        assert_eq!(
            signed.body_sha256_hex,
            "912d0c07da7bdb22cdae025b96da26d01523aaab7362edb28544e3949deb369d"
        );
        assert_eq!(signed.key_id, "v1");
        assert!(signed.authorization.starts_with("HMAC-SHA256 "));
    }

    #[test]
    fn from_str_uses_raw_bytes_as_key() {
        let key = CompletionHmacKey::from_str("v1", "my-auth-token").unwrap();
        assert_eq!(key.key_id, "v1");
    }

    #[test]
    fn rejects_empty_secret() {
        assert!(CompletionHmacKey::from_str("v1", "").is_err());
    }
}
