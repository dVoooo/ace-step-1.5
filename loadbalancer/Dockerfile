# =============================================================================
# ACE-Step 1.5 Load Balancer Worker - Multi-stage Dockerfile
# =============================================================================
# This image includes the ACE-Step models (~15GB total) and the loadbalancer
# worker that proxies requests to the internal API with separate health checks.
#
# Build with: docker build --build-arg HF_TOKEN=your_token -f loadbalancer/Dockerfile -t acestep-loadbalancer:latest ..
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Model Downloader - Download models from HuggingFace
# -----------------------------------------------------------------------------
FROM python:3.11-slim as model-downloader

# Accept HuggingFace token as build argument (required for gated models)
ARG HF_TOKEN
ENV HF_TOKEN=${HF_TOKEN}

WORKDIR /models

# Install huggingface-hub
RUN pip install --no-cache-dir "huggingface-hub[cli]"

# Prefer stable downloader behavior in CI builders
ENV HF_HUB_ENABLE_HF_TRANSFER=0

# Download model packages with retry support.
RUN python - <<'PY'
import os
import time
from huggingface_hub import snapshot_download

token = (os.environ.get("HF_TOKEN") or "").strip() or None

downloads = [
    (
        "ACE-Step/Ace-Step1.5",
        "/models/checkpoints",
        ["acestep-v15-turbo/*"],
    ),
    (
        "ACE-Step/acestep-v15-base",
        "/models/checkpoints/acestep-v15-base",
        None,
    ),
]

for repo_id, local_dir, ignore_patterns in downloads:
    last_error = None
    for attempt in range(1, 4):
        try:
            snapshot_download(
                repo_id=repo_id,
                local_dir=local_dir,
                token=token,
                ignore_patterns=ignore_patterns,
            )
            print(f"Downloaded {repo_id}")
            last_error = None
            break
        except Exception as error:
            last_error = error
            print(f"Attempt {attempt}/3 failed for {repo_id}: {error}")
            if attempt < 3:
                time.sleep(10)
    if last_error is not None:
        raise last_error
PY

# -----------------------------------------------------------------------------
# Stage 2: Runtime - Install ACE-Step and loadbalancer worker
# -----------------------------------------------------------------------------
FROM nvidia/cuda:12.8.0-base-ubuntu22.04 as runtime

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # ACE-Step configuration
    ACESTEP_PROJECT_ROOT=/app \
    ACESTEP_OUTPUT_DIR=/app/outputs \
    ACESTEP_TMPDIR=/app/outputs \
    ACESTEP_DEVICE=cuda \
    # ACE-Step API model paths (full paths to pre-baked models)
    ACESTEP_CONFIG_PATH=/app/checkpoints/acestep-v15-base \
    ACESTEP_LM_MODEL_PATH=/app/checkpoints/acestep-5Hz-lm-1.7B \
    ACESTEP_LM_BACKEND=pt \
    # Server configuration
    ACESTEP_API_HOST=127.0.0.1 \
    ACESTEP_API_PORT=8001 \
    ACESTEP_INTERNAL_API_HOST=127.0.0.1 \
    # Loadbalancer configuration
    PORT=8000 \
    PORT_HEALTH=8000 \
    RUNPOD_MODE=loadbalancer

WORKDIR /app

# Install system dependencies including Python, pip, git, and build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-dev \
    python3-pip \
    git \
    curl \
    build-essential \
    libsndfile1 \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3.11 /usr/bin/python

# Install uv for faster dependency resolution
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# Clone ACE-Step directly into /app and install
RUN git clone https://github.com/ace-step/ACE-Step-1.5.git /app && \
    rm -rf /app/.git && \
    uv pip install --system --no-cache .

# Create symlink so ACE-Step's model discovery finds /app/checkpoints
# ACE-Step uses __file__ to locate checkpoints relative to its install path
RUN ln -s /app/checkpoints /usr/local/lib/python3.11/dist-packages/checkpoints

# Copy models from model-downloader stage into /app/checkpoints
COPY --from=model-downloader /models/checkpoints /app/checkpoints

# Create placeholder for acestep-v15-turbo to satisfy check_main_model_exists()
# We use acestep-v15-base instead, but the check looks for all MAIN_MODEL_COMPONENTS
RUN mkdir -p /app/checkpoints/acestep-v15-turbo

# Copy loadbalancer app
COPY loadbalancer/ /app/loadbalancer/
WORKDIR /app/loadbalancer

# Install loadbalancer dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Go back to /app
WORKDIR /app

# Copy startup script for loadbalancer
COPY loadbalancer/start.sh /app/start_loadbalancer.sh
RUN chmod +x /app/start_loadbalancer.sh

# Create output directory
RUN mkdir -p /app/outputs

# Expose ports:
# - 8000: Main API proxy and health check (for Runpod to call)
# - 8001: Internal ACE-Step API (not exposed externally)
EXPOSE 8000 8001

# Health check for the loadbalancer worker
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/ping')" || exit 1

# Run the loadbalancer startup script
CMD ["/app/start_loadbalancer.sh"]