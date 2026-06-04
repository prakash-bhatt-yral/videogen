use crate::rabbitmq::types::UploadDestination;
use crate::webhook::OutputFile;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

// ─── Output selection ──────────────────────────────────────────────────────

pub fn select_primary_video_output(outputs: &[OutputFile]) -> Result<&OutputFile> {
    let video_exts = [".mp4", ".webm", ".mov"];
    if let Some(o) = outputs
        .iter()
        .find(|o| o.output_type.as_deref() == Some("videos"))
    {
        return Ok(o);
    }
    outputs
        .iter()
        .find(|o| {
            video_exts
                .iter()
                .any(|ext| o.filename.to_lowercase().ends_with(ext))
        })
        .ok_or_else(|| anyhow!("no video output found among {} files", outputs.len()))
}

pub fn resolve_comfy_output_path(output_dir: &str, output: &OutputFile) -> Result<PathBuf> {
    if output.filename.contains("..") || output.filename.starts_with('/') {
        return Err(anyhow!("unsafe filename: {}", output.filename));
    }
    if let Some(sub) = &output.subfolder {
        if sub.contains("..") || sub.starts_with('/') {
            return Err(anyhow!("unsafe subfolder: {sub}"));
        }
    }

    let mut path = PathBuf::from(output_dir);
    if let Some(sub) = &output.subfolder {
        if !sub.is_empty() {
            path.push(sub);
        }
    }
    path.push(&output.filename);

    let base = std::fs::canonicalize(output_dir).unwrap_or_else(|_| PathBuf::from(output_dir));
    let resolved = if path.is_absolute() {
        path.clone()
    } else {
        base.join(&path)
    };
    let resolved_str = resolved.to_string_lossy();
    let base_str = base.to_string_lossy();
    if !resolved_str.starts_with(base_str.as_ref()) {
        return Err(anyhow!("path escapes output directory"));
    }
    Ok(path)
}

// ─── Upload URL refresh ───────────────────────────────────────────────────

pub fn should_refresh_upload_url(expires_at: DateTime<Utc>, refresh_margin_secs: i64) -> bool {
    let deadline = expires_at - chrono::Duration::seconds(refresh_margin_secs);
    Utc::now() >= deadline
}

// ─── Uploaded video metadata ──────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UploadedVideo {
    pub bucket_url: Option<String>,
    pub video_id: String,
    pub object_key: String,
    pub file_size: u64,
    pub content_type: String,
    pub checksum: String,
}

impl UploadedVideo {
    pub fn require_bucket_url(&self) -> Result<&str> {
        self.bucket_url.as_deref().ok_or_else(|| {
            anyhow!("bucket_url is required for success completion but was absent in Prakash job")
        })
    }
}

// ─── Upload errors ────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("upload URL expired and no refresh URL available")]
    UrlExpiredNoRefresh,
    #[error("upload URL refresh failed: {0}")]
    RefreshFailed(String),
    #[error("upload request failed: {0}")]
    RequestFailed(String),
    #[error("bucket_url missing from Prakash job — cannot build success completion")]
    MissingBucketUrl,
    #[error("I/O error reading local output: {0}")]
    Io(#[from] std::io::Error),
}

// ─── Core upload function ─────────────────────────────────────────────────

pub async fn upload_video(
    http: &reqwest::Client,
    local_path: &Path,
    destination: &UploadDestination,
    multipart_field: &str,
    timeout_secs: u64,
) -> Result<UploadedVideo, UploadError> {
    let video_bytes = tokio::fs::read(local_path).await?;
    let file_size = video_bytes.len() as u64;
    let checksum = format!("sha256:{}", hex::encode(Sha256::digest(&video_bytes)));

    let filename = local_path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "video.mp4".to_string());

    let part = reqwest::multipart::Part::bytes(video_bytes)
        .file_name(filename)
        .mime_str("video/mp4")
        .map_err(|e| UploadError::RequestFailed(e.to_string()))?;

    let form = reqwest::multipart::Form::new().part(multipart_field.to_string(), part);

    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        http.post(&destination.upload_url).multipart(form).send(),
    )
    .await
    .map_err(|_| UploadError::RequestFailed("upload timed out".to_string()))?
    .map_err(|e| UploadError::RequestFailed(e.to_string()))?;

    if !resp.status().is_success() {
        return Err(UploadError::RequestFailed(format!(
            "upload status: {}",
            resp.status()
        )));
    }

    let bucket_url = destination.bucket_url.clone();
    Ok(UploadedVideo {
        bucket_url,
        video_id: destination.video_id.clone(),
        object_key: destination.object_key.clone(),
        file_size,
        content_type: "video/mp4".to_string(),
        checksum,
    })
}

// ─── Cleanup ──────────────────────────────────────────────────────────────

pub async fn cleanup_local_output(path: &Path, output_dir: &str) -> Result<()> {
    let base = std::fs::canonicalize(output_dir).unwrap_or_else(|_| PathBuf::from(output_dir));
    let canonical = match std::fs::canonicalize(path) {
        Ok(p) => p,
        Err(_) => return Ok(()),
    };
    if !canonical.starts_with(&base) {
        tracing::warn!(
            "refusing to delete file outside output dir: {}",
            path.display()
        );
        return Ok(());
    }
    if let Err(e) = tokio::fs::remove_file(path).await {
        tracing::warn!("cleanup failed for {}: {e}", path.display());
    }
    Ok(())
}

pub async fn cleanup_staged_inputs(paths: &[PathBuf], allowed_dir: &Path) -> Result<()> {
    let canonical_allowed =
        std::fs::canonicalize(allowed_dir).unwrap_or_else(|_| allowed_dir.to_path_buf());
    for path in paths {
        let canonical = match std::fs::canonicalize(path) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if !canonical.starts_with(&canonical_allowed) {
            tracing::warn!(
                "refusing to delete staged input outside allowed dir: {}",
                path.display()
            );
            continue;
        }
        if let Err(e) = tokio::fs::remove_file(path).await {
            tracing::warn!("failed to delete staged input {}: {e}", path.display());
        }
    }
    Ok(())
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn video_output() -> OutputFile {
        OutputFile {
            filename: "video.mp4".to_string(),
            subfolder: Some("2026-06-03".to_string()),
            output_type: Some("videos".to_string()),
            local_path: None,
            url: None,
            node_id: None,
        }
    }

    #[test]
    fn resolves_video_output_inside_comfyui_output_dir() {
        let output = video_output();
        let path = resolve_comfy_output_path("/workspace/ComfyUI/output", &output).unwrap();
        assert_eq!(
            path,
            std::path::PathBuf::from("/workspace/ComfyUI/output/2026-06-03/video.mp4")
        );
    }

    #[test]
    fn rejects_path_traversal_in_output_filename() {
        let output = OutputFile {
            filename: "../secret".to_string(),
            ..video_output()
        };
        assert!(resolve_comfy_output_path("/workspace/ComfyUI/output", &output).is_err());
    }

    #[test]
    fn refreshes_when_upload_url_near_expiry() {
        let expires_at = Utc::now() + chrono::Duration::seconds(100);
        assert!(should_refresh_upload_url(expires_at, 300));
    }

    #[test]
    fn no_refresh_needed_when_plenty_of_time() {
        let expires_at = Utc::now() + chrono::Duration::seconds(600);
        assert!(!should_refresh_upload_url(expires_at, 300));
    }

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

        cleanup_staged_inputs(&[staged.clone()], temp.path())
            .await
            .unwrap();

        assert!(!staged.exists());
    }
}
