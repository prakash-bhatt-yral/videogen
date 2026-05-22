#!/bin/bash
# =============================================================================
# One-time Vast.ai setup: ComfyUI + models + system dependencies
# =============================================================================
# Usage: bash setup.sh
# Run this ONCE when provisioning a new Vast.ai instance.
# =============================================================================

set -euo pipefail

WORKSPACE="/workspace"
COMFYUI_DIR="${WORKSPACE}/ComfyUI"
LOG_DIR="/var/log/comfyui"

# Redirect caches to /workspace to avoid filling the small root overlay
export HF_HOME="${WORKSPACE}/hf_cache"
export TRANSFORMERS_CACHE="${WORKSPACE}/hf_cache"
export PIP_CACHE_DIR="${WORKSPACE}/pip_cache"
mkdir -p "${WORKSPACE}/hf_cache" "${WORKSPACE}/pip_cache"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[SETUP]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

mkdir -p "$LOG_DIR"

# =============================================================================
# System dependencies
# =============================================================================
log "Installing system dependencies..."
apt-get update -qq
apt-get install -y -qq git wget curl tmux ffmpeg jq > /dev/null 2>&1

# =============================================================================
# cloudflared
# =============================================================================
if ! command -v cloudflared &> /dev/null; then
    log "Installing cloudflared..."
    curl -sL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 \
        -o /usr/local/bin/cloudflared
    chmod +x /usr/local/bin/cloudflared
fi

# =============================================================================
# Beszel Agent
# =============================================================================
if [ ! -f "/usr/local/bin/beszel-agent" ]; then
    log "Installing Beszel Agent..."
    wget -q --show-progress -O beszel-agent.tar.gz https://github.com/henrygd/beszel/releases/latest/download/beszel-agent_linux_amd64.tar.gz
    tar -xzf beszel-agent.tar.gz beszel-agent
    mv beszel-agent /usr/local/bin/
    rm beszel-agent.tar.gz
fi

# =============================================================================
# ComfyUI
# =============================================================================
if [ -d "$COMFYUI_DIR" ]; then
    log "ComfyUI already installed, skipping update (to avoid filling root overlay)."
else
    log "Cloning ComfyUI..."
    cd "$WORKSPACE"
    git clone https://github.com/comfyanonymous/ComfyUI.git
    cd "$COMFYUI_DIR"
    log "Installing ComfyUI dependencies..."
    pip install --no-cache-dir -r requirements.txt -q
fi

# =============================================================================
# Custom nodes
# =============================================================================
log "Installing custom nodes..."
cd "${COMFYUI_DIR}/custom_nodes"

for repo in \
    "https://github.com/Lightricks/ComfyUI-LTXVideo.git" \
    "https://github.com/kijai/ComfyUI-KJNodes.git" \
    "https://github.com/Kosinkadink/ComfyUI-VideoHelperSuite.git"; do

    dir=$(basename "$repo" .git)
    if [ ! -d "$dir" ]; then
        log "  -> $dir"
        git clone "$repo"
        cd "$dir"
        [ -f requirements.txt ] && pip install --no-cache-dir -r requirements.txt -q 2>/dev/null || true
        cd ..
    else
        log "  -> $dir (exists)"
    fi
done

pip install --no-cache-dir sageattention -q 2>/dev/null || warn "SageAttention failed"

# =============================================================================
# Model weights
# =============================================================================
log "Downloading model weights..."

CKPT="${COMFYUI_DIR}/models/checkpoints"
mkdir -p "$CKPT"

# LTX-2.3 22B Dev FP8 (~27GB)
if [ ! -s "${CKPT}/ltx-2.3-22b-dev-fp8.safetensors" ]; then
    rm -f "${CKPT}/ltx-2.3-22b-dev-fp8.safetensors"
    log "  -> LTX-2.3 22B Dev FP8 (~27GB)..."
    wget -q --show-progress -O "${CKPT}/ltx-2.3-22b-dev-fp8.safetensors" \
        "https://huggingface.co/Lightricks/LTX-2.3-fp8/resolve/main/ltx-2.3-22b-dev-fp8.safetensors" \
        || { rm -f "${CKPT}/ltx-2.3-22b-dev-fp8.safetensors"; false; }
fi

# LTX-2.3 Distilled LoRA (~7GB)
LORA_DIR="${COMFYUI_DIR}/models/loras"
mkdir -p "$LORA_DIR"
if [ ! -s "${LORA_DIR}/ltx-2.3-22b-distilled-lora-384.safetensors" ]; then
    rm -f "${LORA_DIR}/ltx-2.3-22b-distilled-lora-384.safetensors"
    log "  -> LTX-2.3 Distilled LoRA (~7GB)..."
    wget -q --show-progress -O "${LORA_DIR}/ltx-2.3-22b-distilled-lora-384.safetensors" \
        "https://huggingface.co/Lightricks/LTX-2.3/resolve/main/ltx-2.3-22b-distilled-lora-384.safetensors" \
        || { rm -f "${LORA_DIR}/ltx-2.3-22b-distilled-lora-384.safetensors"; false; }
fi

# Gemma 3 12B text encoder (multi-shard, required by LTXVGemmaCLIPLoader / LTXAVTextEncoderLoader)
TE_DIR="${COMFYUI_DIR}/models/text_encoders"
GEMMA="${TE_DIR}/gemma-3-12b-it-qat-q4_0-unquantized"
mkdir -p "$GEMMA"
if [ ! -s "${GEMMA}/model-00001-of-00005.safetensors" ]; then
    log "  -> Gemma 3 12B text encoder (~38GB total)..."
    HF_BASE="https://huggingface.co/google/gemma-3-12b-it-qat-q4_0-unquantized/resolve/main"
    for i in $(seq -w 1 5); do
        file="${GEMMA}/model-0000${i}-of-00005.safetensors"
        if [ ! -s "$file" ]; then
            rm -f "$file"
            wget -q --show-progress --header="Authorization: Bearer ${HF_TOKEN:-}" \
                -O "$file" "${HF_BASE}/model-0000${i}-of-00005.safetensors" \
                || { rm -f "$file"; false; }
        fi
    done
fi
log "  -> Gemma 3 12B config files..."
for f in tokenizer.model tokenizer_config.json config.json model.safetensors.index.json \
          special_tokens_map.json tokenizer.json preprocessor_config.json generation_config.json; do
    if [ ! -s "${GEMMA}/${f}" ]; then
        wget -q --show-progress --header="Authorization: Bearer ${HF_TOKEN:-}" \
            -O "${GEMMA}/${f}" \
            "https://huggingface.co/google/gemma-3-12b-it-qat-q4_0-unquantized/resolve/main/${f}" || true
    fi
done

# Spatial upscaler 2x v1.1 (~950MB)
UP="${COMFYUI_DIR}/models/latent_upscale_models"
mkdir -p "$UP"
if [ ! -s "${UP}/ltx-2.3-spatial-upscaler-x2-1.1.safetensors" ]; then
    rm -f "${UP}/ltx-2.3-spatial-upscaler-x2-1.1.safetensors"
    log "  -> Spatial Upscaler 2x v1.1 (~950MB)..."
    wget -q --show-progress -O "${UP}/ltx-2.3-spatial-upscaler-x2-1.1.safetensors" \
        "https://huggingface.co/Lightricks/LTX-2.3/resolve/main/ltx-2.3-spatial-upscaler-x2-1.1.safetensors" || true
fi

# =============================================================================
# Restart ComfyUI to load new custom nodes
# =============================================================================
log "Restarting ComfyUI to apply custom nodes..."
supervisorctl restart comfyui || warn "Failed to restart ComfyUI, you may need to restart it manually."

# =============================================================================
# Done
# =============================================================================
echo ""
log "Setup complete! Run 'bash /workspace/start.sh' to start services."
