#!/bin/bash
# =============================================================================
# Start videogen-worker + Cloudflare tunnel on Vast.ai
# =============================================================================
# Starts ComfyUI if not already running (port 18188), then starts:
#   - videogen-worker on port 18288 (mapped to external 8288)
#   - cloudflared tunnel (named if CF_TUNNEL_TOKEN set, else quick tunnel)
#
# Required env vars:
#   AUTH_TOKEN        - Bearer token for API auth
#
# Optional env vars:
#   CF_TUNNEL_TOKEN   - Cloudflare named tunnel token
#   SENTRY_DSN        - Sentry error reporting DSN
#   COMFYUI_API_BASE  - ComfyUI URL (default: http://localhost:18188)
#   PORT              - Worker port (default: 18288)
# =============================================================================

set -euo pipefail

LOG_DIR="/var/log/comfyui"
BINARY="/usr/local/bin/videogen-worker"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[START]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

mkdir -p "$LOG_DIR"

# Fallback: check /workspace for the binary (manual deploy)
if [ ! -f "$BINARY" ] && [ -f "/workspace/videogen-worker" ]; then
    BINARY="/workspace/videogen-worker"
fi

if [ ! -f "$BINARY" ]; then
    echo "ERROR: videogen-worker binary not found"
    echo "Expected at /usr/local/bin/videogen-worker or /workspace/videogen-worker"
    exit 1
fi

# =============================================================================
# Kill existing sessions (preserve tunnel to keep URL stable across redeploys)
# =============================================================================
for session in worker beszel; do
    tmux kill-session -t "$session" 2>/dev/null || true
done

# Stop the pre-installed Python API wrapper (occupies port 18288 on some templates)
supervisorctl stop api-wrapper 2>/dev/null || true

# =============================================================================
# RabbitMQ consumer mode (optional, disabled by default)
# =============================================================================
export VIDEOGEN_RABBITMQ_ENABLED="${VIDEOGEN_RABBITMQ_ENABLED:-false}"
export VIDEOGEN_RABBITMQ_CONSUMER_PASSWORD="${VIDEOGEN_RABBITMQ_CONSUMER_PASSWORD:-}"
export VIDEOGEN_RABBITMQ_AMQPS_URLS="${VIDEOGEN_RABBITMQ_AMQPS_URLS:-}"
export VIDEOGEN_RABBITMQ_QUEUE="${VIDEOGEN_RABBITMQ_QUEUE:-videogen.ltx.generate}"
export VIDEOGEN_RABBITMQ_PREFETCH="${VIDEOGEN_RABBITMQ_PREFETCH:-1}"
export VIDEOGEN_RABBITMQ_CONCURRENCY="${VIDEOGEN_RABBITMQ_CONCURRENCY:-1}"
export VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64="${VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64:-}"
export VIDEOGEN_CALLBACK_SIGNING_KEY_ID="${VIDEOGEN_CALLBACK_SIGNING_KEY_ID:-}"
export VIDEOGEN_CALLBACK_SIGNING_SECRET_B64="${VIDEOGEN_CALLBACK_SIGNING_SECRET_B64:-}"
export VIDEOGEN_VAST_UPLOAD_EXPIRY_REFRESH_MARGIN_SECS="${VIDEOGEN_VAST_UPLOAD_EXPIRY_REFRESH_MARGIN_SECS:-300}"
export VIDEOGEN_BUCKET_UPLOAD_TIMEOUT_SECS="${VIDEOGEN_BUCKET_UPLOAD_TIMEOUT_SECS:-300}"
export VIDEOGEN_BUCKET_UPLOAD_MULTIPART_FIELD="${VIDEOGEN_BUCKET_UPLOAD_MULTIPART_FIELD:-file}"
export VIDEOGEN_LTX_GENERATION_TIMEOUT_SECS="${VIDEOGEN_LTX_GENERATION_TIMEOUT_SECS:-1800}"
export VIDEOGEN_COMPLETION_OUTBOX_INITIAL_BACKOFF_SECS="${VIDEOGEN_COMPLETION_OUTBOX_INITIAL_BACKOFF_SECS:-10}"
export VIDEOGEN_COMPLETION_OUTBOX_MAX_BACKOFF_SECS="${VIDEOGEN_COMPLETION_OUTBOX_MAX_BACKOFF_SECS:-120}"
export VIDEOGEN_COMPLETION_OUTBOX_MAX_ATTEMPTS="${VIDEOGEN_COMPLETION_OUTBOX_MAX_ATTEMPTS:-10}"
export VIDEOGEN_COMPLETION_TIMEOUT_SECS="${VIDEOGEN_COMPLETION_TIMEOUT_SECS:-30}"
export VIDEOGEN_VAST_OUTBOX_RETENTION_HOURS="${VIDEOGEN_VAST_OUTBOX_RETENTION_HOURS:-72}"
export VIDEOGEN_VAST_STAGED_IMAGE_TTL_HOURS="${VIDEOGEN_VAST_STAGED_IMAGE_TTL_HOURS:-24}"

# =============================================================================
# Start ComfyUI if not already running
# =============================================================================
COMFYUI_BASE="${COMFYUI_API_BASE:-http://localhost:18188}"
if ! curl -sf "${COMFYUI_BASE}/system_stats" > /dev/null 2>&1; then
    if ! pgrep -f "python.*main.py.*18188" > /dev/null 2>&1; then
        log "ComfyUI not running — starting..."
        tmux kill-session -t comfyui 2>/dev/null || true
        tmux new-session -d -s comfyui \
            "cd /workspace/ComfyUI && python3 main.py --listen 127.0.0.1 --port 18188 2>&1 | tee ${LOG_DIR}/comfyui.log"
    else
        log "ComfyUI process found, waiting for it to become ready..."
    fi
fi

# =============================================================================
# Wait for ComfyUI to be ready
# =============================================================================
log "Waiting for ComfyUI at ${COMFYUI_BASE}..."
for i in $(seq 1 90); do
    if curl -sf "${COMFYUI_BASE}/system_stats" > /dev/null 2>&1; then
        log "ComfyUI ready!"
        break
    fi
    [ "$i" -eq 90 ] && warn "ComfyUI not responding after 3 minutes"
    sleep 2
done

# =============================================================================
# Start videogen-worker
# =============================================================================
WORKER_PORT="${PORT:-18288}"
log "Starting videogen-worker on port ${WORKER_PORT}..."

tmux new-session -d -s worker \
    "AUTH_TOKEN='${AUTH_TOKEN:-}' \
     SENTRY_DSN='${SENTRY_DSN:-}' \
     COMFYUI_API_BASE='${COMFYUI_BASE}' \
     PORT=${WORKER_PORT} \
     RUST_LOG='${RUST_LOG:-info,videogen_worker=debug}' \
     VIDEOGEN_RABBITMQ_ENABLED='${VIDEOGEN_RABBITMQ_ENABLED:-false}' \
     VIDEOGEN_RABBITMQ_AMQPS_URLS='${VIDEOGEN_RABBITMQ_AMQPS_URLS:-}' \
     VIDEOGEN_RABBITMQ_CONSUMER_PASSWORD='${VIDEOGEN_RABBITMQ_CONSUMER_PASSWORD:-}' \
     VIDEOGEN_RABBITMQ_QUEUE='${VIDEOGEN_RABBITMQ_QUEUE:-videogen.ltx.generate}' \
     VIDEOGEN_RABBITMQ_PREFETCH='${VIDEOGEN_RABBITMQ_PREFETCH:-1}' \
     VIDEOGEN_RABBITMQ_CONCURRENCY='${VIDEOGEN_RABBITMQ_CONCURRENCY:-1}' \
     VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64='${VIDEOGEN_RABBITMQ_TLS_CA_CERT_PEM_B64:-}' \
     VIDEOGEN_CALLBACK_SIGNING_KEY_ID='${VIDEOGEN_CALLBACK_SIGNING_KEY_ID:-}' \
     VIDEOGEN_CALLBACK_SIGNING_SECRET_B64='${VIDEOGEN_CALLBACK_SIGNING_SECRET_B64:-}' \
     ${BINARY} 2>&1 | tee ${LOG_DIR}/worker.log"

sleep 3
if curl -sf "http://localhost:${WORKER_PORT}/health" > /dev/null 2>&1; then
    log "Worker ready!"
else
    warn "Worker may still be starting — check: tmux attach -t worker"
fi

# =============================================================================
# Start Cloudflare tunnel
# =============================================================================
if [ -n "${CF_TUNNEL_TOKEN:-}" ]; then
    tmux kill-session -t tunnel 2>/dev/null || true
    log "Starting Cloudflare named tunnel..."
    tmux new-session -d -s tunnel \
        "cloudflared tunnel run --token '${CF_TUNNEL_TOKEN}' 2>&1 | tee ${LOG_DIR}/tunnel.log"
    log "Tunnel started — connected to ${CF_TUNNEL_HOSTNAME:-videogen.prakash.yral.com}"
elif tmux has-session -t tunnel 2>/dev/null; then
    log "Quick tunnel already running — reusing URL (tmux attach -t tunnel)"
else
    log "Starting Cloudflare quick tunnel..."
    tmux new-session -d -s tunnel \
        "cloudflared tunnel --url http://localhost:${WORKER_PORT} 2>&1 | tee ${LOG_DIR}/tunnel.log"
    sleep 8
    TUNNEL_URL=$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' "${LOG_DIR}/tunnel.log" 2>/dev/null | head -1 || true)
    if [ -n "${TUNNEL_URL:-}" ]; then
        log "Quick tunnel URL: ${TUNNEL_URL}"
        log "Set COMFYUI_API_URL=${TUNNEL_URL} in off-chain-agent"
    else
        warn "Quick tunnel started — check URL: tmux attach -t tunnel"
    fi
fi

# =============================================================================
# Start Beszel Agent
# =============================================================================
if [ -x "/usr/local/bin/beszel-agent" ]; then
    log "Starting Beszel Agent..."
    tmux new-session -d -s beszel \
        "LISTEN=${BESZEL_PORT:-45876} \
         KEY='${BESZEL_KEY:-}' \
         TOKEN='${BESZEL_TOKEN:-}' \
         HUB_URL='${BESZEL_HUB_URL:-https://beszel.yral.com}' \
         /usr/local/bin/beszel-agent 2>&1 | tee ${LOG_DIR}/beszel.log"
else
    warn "Beszel Agent not found. Run setup.sh to install it."
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo -e "${CYAN}══════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  videogen-worker started${NC}"
echo -e "${CYAN}══════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ComfyUI:     ${COMFYUI_BASE} (tmux: comfyui)"
echo -e "  Worker:      http://localhost:${WORKER_PORT}"
echo -e "  Swagger UI:  http://localhost:${WORKER_PORT}/swagger-ui"
echo -e "  External:    http://localhost:8288 (via Vast.ai port mapping)"
if [ -n "${CF_TUNNEL_TOKEN:-}" ]; then
    echo -e "  Public URL:  https://${CF_TUNNEL_HOSTNAME:-comfyui.prakash.yral.com}"
elif [ -n "${TUNNEL_URL:-}" ]; then
    echo -e "  Quick URL:   ${TUNNEL_URL}"
    echo -e "               (ephemeral — stable until instance reboot)"
fi
echo ""
echo -e "  tmux attach -t worker   # Worker logs"
echo -e "  tmux attach -t tunnel   # Tunnel logs"
echo -e "  tmux attach -t beszel   # Beszel logs"
echo ""
