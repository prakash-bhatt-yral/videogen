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

# Pin kornia — 0.8+ removes pyramid.pad which ComfyUI-LTXVideo requires
pip install --quiet 'kornia>=0.7,<0.8'

# =============================================================================
# PyTorch — pin to version compatible with installed CUDA driver
# =============================================================================
CUDA_VER=$(nvidia-smi 2>/dev/null | grep -oP 'CUDA Version: \K[\d.]+' | head -1 || echo "")
if [ -n "$CUDA_VER" ]; then
    CUDA_MAJOR=$(echo "$CUDA_VER" | cut -d. -f1)
    CUDA_MINOR=$(echo "$CUDA_VER" | cut -d. -f2)
    CUDA_INT=$((CUDA_MAJOR * 10 + CUDA_MINOR))
    if   [ "$CUDA_INT" -ge 130 ]; then TORCH_CUDA="cu130"
    elif [ "$CUDA_INT" -ge 126 ]; then TORCH_CUDA="cu126"
    elif [ "$CUDA_INT" -ge 124 ]; then TORCH_CUDA="cu124"
    elif [ "$CUDA_INT" -ge 121 ]; then TORCH_CUDA="cu121"
    else                                TORCH_CUDA="cu118"
    fi
    log "CUDA ${CUDA_VER} → installing PyTorch for ${TORCH_CUDA}..."
    pip install --force-reinstall --quiet torch torchvision torchaudio \
        --index-url "https://download.pytorch.org/whl/${TORCH_CUDA}"
else
    warn "Could not detect CUDA version — keeping default PyTorch"
fi

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

# Gemma 3 12B abliterated LoRA (required by LTX-2.3 text encoder node)
if [ ! -s "${LORA_DIR}/gemma-3-12b-it-abliterated_lora_rank64_bf16.safetensors" ]; then
    rm -f "${LORA_DIR}/gemma-3-12b-it-abliterated_lora_rank64_bf16.safetensors"
    log "  -> Gemma 3 12B abliterated LoRA..."
    wget -q --show-progress \
        -O "${LORA_DIR}/gemma-3-12b-it-abliterated_lora_rank64_bf16.safetensors" \
        "https://huggingface.co/Comfy-Org/ltx-2/resolve/main/split_files/loras/gemma-3-12b-it-abliterated_lora_rank64_bf16.safetensors" \
        || { rm -f "${LORA_DIR}/gemma-3-12b-it-abliterated_lora_rank64_bf16.safetensors"; false; }
fi

# Gemma 3 12B FP4 text encoder (~8.8GB, single file packaged by Comfy-Org)
TE_DIR="${COMFYUI_DIR}/models/text_encoders"
mkdir -p "$TE_DIR"
if [ ! -s "${TE_DIR}/gemma_3_12B_it_fp4_mixed.safetensors" ]; then
    rm -f "${TE_DIR}/gemma_3_12B_it_fp4_mixed.safetensors"
    log "  -> Gemma 3 12B FP4 text encoder (~8.8GB)..."
    wget -q --show-progress \
        -O "${TE_DIR}/gemma_3_12B_it_fp4_mixed.safetensors" \
        "https://huggingface.co/Comfy-Org/ltx-2/resolve/main/split_files/text_encoders/gemma_3_12B_it_fp4_mixed.safetensors" \
        || { rm -f "${TE_DIR}/gemma_3_12B_it_fp4_mixed.safetensors"; false; }
fi

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
# (Re)start ComfyUI — supervisord on ComfyUI template, tmux on CUDA base
# =============================================================================
if supervisorctl status comfyui 2>/dev/null | grep -qE 'RUNNING|STOPPED'; then
    log "Restarting ComfyUI via supervisord..."
    supervisorctl restart comfyui
else
    log "Starting ComfyUI in tmux (CUDA base image)..."
    tmux kill-session -t comfyui 2>/dev/null || true
    tmux new-session -d -s comfyui \
        "cd ${COMFYUI_DIR} && python3 main.py --listen 0.0.0.0 --port 18188 --enable-cors-header 2>&1 | tee ${LOG_DIR}/comfyui.log"
    log "ComfyUI started. Logs: ${LOG_DIR}/comfyui.log | tmux attach -t comfyui"
fi

# =============================================================================
# Done
# =============================================================================
echo ""
log "Setup complete! Run 'bash /workspace/start.sh' to start services."
