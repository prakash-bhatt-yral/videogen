use anyhow::Result;

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
    pub secret: String,
}

#[derive(Clone, Debug)]
pub struct UploadConfig {
    pub refresh_margin_secs: i64,
    pub upload_timeout_secs: u64,
    pub cleanup_after_upload: bool,
    pub multipart_field_name: String,
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub port: u16,
    pub backend_type: String,
    pub auth_token: Option<String>,
    pub sentry_dsn: Option<String>,

    pub comfyui_host: String,
    pub comfyui_port: u16,
    pub comfyui_output_dir: String,

    pub video_ttl_minutes: u64,
    pub cleanup_check_interval: u64,

    pub rabbitmq: RabbitMqConfig,
    pub completion_auth: Option<CompletionAuthConfig>,
    pub upload: UploadConfig,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let env: std::collections::HashMap<String, String> = std::env::vars().collect();
        Self::from_env_map(&env)
    }

    pub fn from_env_map(env: &std::collections::HashMap<String, String>) -> Result<Self> {
        let get = |key: &str| env.get(key).map(|s| s.as_str()).unwrap_or("");
        let get_opt =
            |key: &str| -> Option<String> { env.get(key).filter(|s| !s.is_empty()).cloned() };
        let parse_u64 = |key: &str, default: u64| -> Result<u64> {
            match env.get(key) {
                Some(v) => v
                    .parse::<u64>()
                    .map_err(|_| anyhow::anyhow!("{key} must be a valid integer: {v}")),
                None => Ok(default),
            }
        };
        let parse_u16 = |key: &str, default: u16| -> Result<u16> {
            match env.get(key) {
                Some(v) => v
                    .parse::<u16>()
                    .map_err(|_| anyhow::anyhow!("{key} must be a valid integer: {v}")),
                None => Ok(default),
            }
        };
        let parse_usize = |key: &str, default: usize| -> Result<usize> {
            match env.get(key) {
                Some(v) => v
                    .parse::<usize>()
                    .map_err(|_| anyhow::anyhow!("{key} must be a valid integer: {v}")),
                None => Ok(default),
            }
        };
        let parse_bool = |key: &str, default: bool| -> bool {
            env.get(key)
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                .unwrap_or(default)
        };

        let rabbitmq_enabled = parse_bool("VIDEOGEN_RABBITMQ_ENABLED", false);

        if rabbitmq_enabled {
            if get("VIDEOGEN_RABBITMQ_CONSUMER_PASSWORD").is_empty()
                && get("VIDEOGEN_RABBITMQ_AMQPS_URLS").is_empty()
            {
                return Err(anyhow::anyhow!(
                    "VIDEOGEN_RABBITMQ_CONSUMER_PASSWORD or VIDEOGEN_RABBITMQ_AMQPS_URLS is required when VIDEOGEN_RABBITMQ_ENABLED=true"
                ));
            }
            if get("AUTH_TOKEN").is_empty() {
                return Err(anyhow::anyhow!("AUTH_TOKEN is required when VIDEOGEN_RABBITMQ_ENABLED=true (used as HMAC signing secret)"));
            }
        }

        let rabbitmq = RabbitMqConfig {
            enabled: rabbitmq_enabled,
            amqps_urls: {
                let password = get("VIDEOGEN_RABBITMQ_CONSUMER_PASSWORD");
                if !password.is_empty() {
                    vec![
                        format!(
                            "amqps://vast_ltx_consumer:{password}@94.130.13.115:5671/%2Fvideogen"
                        ),
                        format!(
                            "amqps://vast_ltx_consumer:{password}@88.99.151.102:5671/%2Fvideogen"
                        ),
                        format!(
                            "amqps://vast_ltx_consumer:{password}@138.201.129.173:5671/%2Fvideogen"
                        ),
                    ]
                } else {
                    get("VIDEOGEN_RABBITMQ_AMQPS_URLS")
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                }
            },
            queue: env
                .get("VIDEOGEN_RABBITMQ_QUEUE")
                .cloned()
                .unwrap_or_else(|| "videogen.ltx.generate".to_string()),
            prefetch: parse_u16("VIDEOGEN_RABBITMQ_PREFETCH", 1)?,
            concurrency: parse_usize("VIDEOGEN_RABBITMQ_CONCURRENCY", 1)?,
            tls_ca_cert_pem_b64: get_opt("VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64"),
        };

        let completion_auth = if rabbitmq_enabled {
            Some(CompletionAuthConfig {
                key_id: env
                    .get("VIDEOGEN_CALLBACK_SIGNING_KEY_ID")
                    .cloned()
                    .unwrap_or_else(|| "v1".to_string()),
                secret: get("AUTH_TOKEN").to_string(),
            })
        } else {
            None
        };

        let (comfyui_host, comfyui_port) = if let Some(base) = env.get("COMFYUI_API_BASE") {
            let url = url::Url::parse(base)?;
            let host = url.host_str().unwrap_or("127.0.0.1").to_string();
            let port = url.port().unwrap_or(18188);
            (host, port)
        } else {
            let host = env
                .get("COMFYUI_HOST")
                .cloned()
                .unwrap_or_else(|| "127.0.0.1".into());
            let port: u16 = env
                .get("COMFYUI_PORT")
                .map(|v| v.parse())
                .transpose()?
                .unwrap_or(18188);
            (host, port)
        };

        Ok(Self {
            port: parse_u16("PORT", 18288)?,
            backend_type: env
                .get("BACKEND_TYPE")
                .cloned()
                .unwrap_or_else(|| "comfyui".into()),
            auth_token: get_opt("AUTH_TOKEN"),
            sentry_dsn: get_opt("SENTRY_DSN"),
            comfyui_host,
            comfyui_port,
            comfyui_output_dir: env
                .get("COMFYUI_OUTPUT_DIR")
                .cloned()
                .unwrap_or_else(|| "/workspace/ComfyUI/output".into()),
            video_ttl_minutes: parse_u64("VIDEO_TTL_MINUTES", 10)?,
            cleanup_check_interval: parse_u64("CLEANUP_CHECK_INTERVAL", 300)?,
            rabbitmq,
            completion_auth,
            upload: UploadConfig {
                refresh_margin_secs: parse_u64(
                    "VIDEOGEN_VAST_UPLOAD_EXPIRY_REFRESH_MARGIN_SECS",
                    300,
                )? as i64,
                upload_timeout_secs: parse_u64("VIDEOGEN_BUCKET_UPLOAD_TIMEOUT_SECS", 300)?,
                cleanup_after_upload: parse_bool("VIDEOGEN_CLEANUP_AFTER_UPLOAD", true),
                multipart_field_name: env
                    .get("VIDEOGEN_BUCKET_UPLOAD_MULTIPART_FIELD")
                    .cloned()
                    .unwrap_or_else(|| "file".into()),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    type TestEnv = HashMap<String, String>;

    fn env(pairs: &[(&str, &str)]) -> TestEnv {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn rabbitmq_disabled_by_default() {
        let cfg = AppConfig::from_env_map(&TestEnv::default()).unwrap();
        assert!(!cfg.rabbitmq.enabled);
    }

    #[test]
    fn rabbitmq_enabled_requires_password_or_urls_and_hmac_key() {
        let e = env(&[
            ("VIDEOGEN_RABBITMQ_ENABLED", "true"),
            ("VIDEOGEN_RABBITMQ_QUEUE", "videogen.ltx.generate"),
        ]);
        let err = AppConfig::from_env_map(&e).unwrap_err().to_string();
        assert!(
            err.contains("VIDEOGEN_RABBITMQ_CONSUMER_PASSWORD")
                || err.contains("VIDEOGEN_RABBITMQ_AMQPS_URLS")
        );
    }

    #[test]
    fn parses_rabbitmq_and_upload_defaults() {
        let e = env(&[
            ("VIDEOGEN_RABBITMQ_ENABLED", "true"),
            (
                "VIDEOGEN_RABBITMQ_AMQPS_URLS",
                "amqps://user:pass@94.130.13.115:5671/%2Fvideogen",
            ),
            ("VIDEOGEN_RABBITMQ_QUEUE", "videogen.ltx.generate"),
            ("AUTH_TOKEN", "my-test-auth-token"),
        ]);
        let cfg = AppConfig::from_env_map(&e).unwrap();
        assert!(cfg.rabbitmq.enabled);
        assert_eq!(cfg.rabbitmq.prefetch, 1);
        assert_eq!(cfg.upload.refresh_margin_secs, 300);
        assert_eq!(cfg.upload.upload_timeout_secs, 300);
    }
}
