# videogen-worker

Video generation worker with pluggable backend adapters. Runs alongside GPU inference servers (e.g. ComfyUI on Vast.ai) and exposes a REST API for the [off-chain-agent](https://github.com/dolr-ai/off-chain-agent).

## Architecture

```
off-chain-agent (baremetal)
       │
       │  HTTPS (static URL, never changes)
       ▼
┌─────────────────────────────────────────────┐
│  comfyui.prakash.yral.com                   │
│  (Cloudflare Named Tunnel)                  │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  Vast.ai GPU Instance (H100)               │
│                                             │
│  ┌─────────────────────────────┐            │
│  │  videogen-worker (:8288)    │            │
│  │  ├── POST /generate         │            │
│  │  ├── POST /upload/image     │            │
│  │  ├── GET  /view             │            │
│  │  ├── GET  /health           │            │
│  │  └── GET  /swagger-ui       │            │
│  └────────────┬────────────────┘            │
│               │ localhost                   │
│  ┌────────────▼────────────────┐            │
│  │  ComfyUI (:8188)            │            │
│  │  + LTX-2 19B Distilled      │            │
│  │  + Gemma 3 12B              │            │
│  │  + Spatial Upscaler 2x      │            │
│  └─────────────────────────────┘            │
└─────────────────────────────────────────────┘
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/generate` | Submit a video generation job |
| `GET` | `/result/{id}` | Check job status |
| `POST` | `/upload/image` | Upload an image (multipart) |
| `GET` | `/view` | Download output file |
| `GET` | `/health` | Backend health check |
| `GET` | `/swagger-ui` | Interactive API documentation |

## Backend Adapters

The worker uses an adapter pattern (`VideoGenBackend` trait). Currently supported:

- **`comfyui`** — Proxies to a local ComfyUI instance via HTTP + WebSocket

Future adapters can be added for LTX hosted API, RunPod, etc.

## Quick Start

### Local development

```bash
# Start ComfyUI on port 8188 first, then:
COMFYUI_HOST=127.0.0.1 COMFYUI_PORT=8188 cargo run
# Visit http://localhost:8288/swagger-ui
```

### Deploy to Vast.ai

#### One-time setup (new instance)

```bash
# SSH into the instance
ssh -p <PORT> root@<IP>

# Copy and run setup script
bash /workspace/deploy/setup.sh
```

#### Via GitHub Actions (recommended)

1. **Create Cloudflare tunnel** (one-time):
   - Cloudflare Zero Trust → Networks → Tunnels → Create
   - Name: `comfyui-worker`
   - Public hostname: `comfyui.prakash.yral.com` → `http://localhost:8288`
   - Copy the tunnel token

2. **Set GitHub secrets**:

   | Secret | Description |
   |--------|-------------|
   | `VASTAI_SSH_KEY` | SSH private key for Vast.ai instance |
   | `VASTAI_HOST` | Instance IP address |
   | `VASTAI_PORT` | Instance SSH port |
   | `AUTH_TOKEN` | Bearer token for API auth |
   | `CF_TUNNEL_TOKEN` | Cloudflare tunnel token |
   | `SENTRY_DSN` | Sentry DSN (optional) |

3. **Push to `main`** — the deploy workflow builds and deploys automatically.

#### Manual deploy

```bash
cargo build --release
scp target/release/videogen-worker root@<IP>:/workspace/videogen-worker
scp deploy/start.sh root@<IP>:/workspace/start.sh
ssh -p <PORT> root@<IP> "AUTH_TOKEN=xxx CF_TUNNEL_TOKEN=yyy bash /workspace/start.sh"
```

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `PORT` | `8288` | Worker listen port |
| `BACKEND_TYPE` | `comfyui` | Backend adapter to use |
| `AUTH_TOKEN` | *(none)* | Bearer token (disabled if empty) |
| `COMFYUI_HOST` | `127.0.0.1` | ComfyUI hostname |
| `COMFYUI_PORT` | `8188` | ComfyUI port |
| `SENTRY_DSN` | *(none)* | Sentry error reporting |
| `CF_TUNNEL_TOKEN` | *(none)* | Cloudflare named tunnel token |

## RabbitMQ Consumer Mode

Production video generation uses RabbitMQ as the job source. Enable it by setting `VIDEOGEN_RABBITMQ_ENABLED=true`. When enabled, the worker consumes jobs from the configured queue instead of waiting for HTTP `/generate` requests.

### Mode summary

| Mode | Purpose |
|------|---------|
| RabbitMQ consumer (`VIDEOGEN_RABBITMQ_ENABLED=true`) | Production — all new jobs arrive via queue |
| HTTP `POST /generate` | Rollback / manual testing only |
| off-chain-agent integration | Legacy drain only — migrated jobs must NOT use it |

### Required env vars (RabbitMQ mode)

| Env Var | Description |
|---------|-------------|
| `VIDEOGEN_RABBITMQ_AMQPS_URLS` | Comma-separated AMQPS broker URLs (include credentials) |
| `VIDEOGEN_RABBITMQ_QUEUE` | Queue name (default: `videogen.ltx.generate`) |
| `VIDEOGEN_RABBITMQ_PREFETCH` | Per-consumer prefetch count (default: `1`) |
| `VIDEOGEN_RABBITMQ_CONCURRENCY` | Parallel job workers (default: `1`) |
| `VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64` | Base64-encoded CA cert PEM for TLS verification (optional) |
| `VIDEOGEN_STATE_DB_PATH` | SQLite state DB path (default: `/workspace/videogen-worker/state.db`) |
| `AUTH_TOKEN` | HMAC signing secret and bearer token |
| `VIDEOGEN_CALLBACK_SIGNING_KEY_ID` | HMAC key ID used to sign completion callbacks |

### Job protocol

- `bucket_url` is pre-computed by the upstream service and included in the job message; the worker does not construct it.
- Upload uses `POST` multipart/form-data with field name `file` (configurable via `VIDEOGEN_BUCKET_UPLOAD_MULTIPART_FIELD`).
- Local output file is deleted after a successful upload is persisted to the outbox.

### State DB

The worker persists job state and the completion outbox to a SQLite database at `VIDEOGEN_STATE_DB_PATH` (default `/workspace/videogen-worker/state.db`). The parent directory is created automatically by `deploy/start.sh`. In Docker, a named volume (`videogen_state`) is mounted at `/workspace/videogen-worker`.

### Health check

`GET /health` returns a JSON object that includes `rabbitmq.status` when consumer mode is active. Credentials are never included in health output.

### Additional tuning env vars

| Env Var | Default | Description |
|---------|---------|-------------|
| `VIDEOGEN_VAST_UPLOAD_EXPIRY_REFRESH_MARGIN_SECS` | `300` | Seconds before expiry to refresh upload URL |
| `VIDEOGEN_BUCKET_UPLOAD_TIMEOUT_SECS` | `300` | Upload HTTP timeout |
| `VIDEOGEN_LTX_GENERATION_TIMEOUT_SECS` | `1800` | Max time to wait for LTX generation |
| `VIDEOGEN_COMPLETION_OUTBOX_INITIAL_BACKOFF_SECS` | `10` | Initial retry backoff for completion callbacks |
| `VIDEOGEN_COMPLETION_OUTBOX_MAX_BACKOFF_SECS` | `120` | Maximum retry backoff |
| `VIDEOGEN_COMPLETION_OUTBOX_MAX_ATTEMPTS` | `10` | Max completion callback attempts |
| `VIDEOGEN_COMPLETION_TIMEOUT_SECS` | `30` | HTTP timeout for completion callbacks |
| `VIDEOGEN_VAST_OUTBOX_RETENTION_HOURS` | `72` | Hours to retain completed outbox entries |
| `VIDEOGEN_VAST_STAGED_IMAGE_TTL_HOURS` | `24` | Hours before staged images are pruned |

### GitHub Secrets (for deploy workflow)

Add these secrets to the repository to enable RabbitMQ mode in CI/CD:

| Secret | Description |
|--------|-------------|
| `VIDEOGEN_RABBITMQ_AMQPS_URLS` | AMQPS broker URL(s) with credentials |
| `VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64` | Base64 CA cert PEM (if using custom CA) |
| `AUTH_TOKEN` | HMAC signing secret and bearer token |
| `VIDEOGEN_CALLBACK_SIGNING_KEY_ID` | HMAC key ID for completion callbacks |

Note: `VIDEOGEN_RABBITMQ_ENABLED` is intentionally not set in the workflow — operators set it on the target instance.

## off-chain-agent Integration

Once deployed, set these static env vars on the off-chain-agent (they never change):

```env
COMFYUI_API_URL=https://comfyui.prakash.yral.com
COMFYUI_VIEW_URL=https://comfyui.prakash.yral.com
COMFYUI_API_TOKEN=<your-auth-token>
```
