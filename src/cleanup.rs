use std::time::Duration;
use tokio::fs;
use tracing::{debug, error, info};

use crate::config::AppConfig;

const VIDEO_EXTENSIONS: &[&str] = &["mp4", "avi", "mov", "webm", "mkv"];

pub async fn start_cleanup_task(config: AppConfig) {
    let output_dir = config.comfyui_output_dir.clone();
    let ttl = Duration::from_secs(config.video_ttl_minutes * 60);
    let interval = Duration::from_secs(config.cleanup_check_interval);

    info!(
        "Started video cleanup task. TTL: {} minutes, Interval: {} seconds, Dir: {}",
        config.video_ttl_minutes, config.cleanup_check_interval, output_dir
    );

    loop {
        debug!("Running periodic video cleanup...");

        match run_cleanup(&output_dir, ttl).await {
            Ok((deleted_count, freed_bytes)) => {
                if deleted_count > 0 {
                    info!(
                        "Cleanup complete: deleted {} files, freed {}",
                        deleted_count,
                        format_size(freed_bytes)
                    );
                }
            }
            Err(e) => {
                error!("Error during video cleanup: {}", e);
            }
        }

        tokio::time::sleep(interval).await;
    }
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

async fn run_cleanup(output_dir: &str, ttl: Duration) -> anyhow::Result<(usize, u64)> {
    let mut deleted_count = 0;
    let mut freed_bytes = 0;

    if !fs::try_exists(output_dir).await.unwrap_or(false) {
        debug!(
            "Output directory {} does not exist, skipping cleanup",
            output_dir
        );
        return Ok((0, 0));
    }

    let mut dirs = vec![output_dir.to_string()];
    while let Some(dir) = dirs.pop() {
        let mut entries = match fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(e) => {
                error!("Failed to read dir {}: {}", dir, e);
                continue;
            }
        };

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.is_dir() {
                dirs.push(path.to_string_lossy().into_owned());
                continue;
            }

            let is_video = path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| VIDEO_EXTENSIONS.contains(&ext.to_lowercase().as_str()))
                .unwrap_or(false);

            if !is_video {
                continue;
            }

            let metadata = match entry.metadata().await {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to read metadata for {:?}: {}", path, e);
                    continue;
                }
            };

            let modified = match metadata.modified() {
                Ok(m) => m,
                Err(_) => continue,
            };

            if let Ok(elapsed) = modified.elapsed() {
                if elapsed > ttl {
                    let size = metadata.len();
                    match fs::remove_file(&path).await {
                        Ok(_) => {
                            deleted_count += 1;
                            freed_bytes += size;
                            debug!("Deleted expired video: {:?}", path);
                        }
                        Err(e) => {
                            error!("Failed to delete {:?}: {}", path, e);
                        }
                    }
                }
            }
        }
    }

    Ok((deleted_count, freed_bytes))
}
