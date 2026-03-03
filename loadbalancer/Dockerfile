# =============================================================================
# ACE-Step 1.5 Load Balancer Worker - Dockerfile
# =============================================================================
# This Dockerfile creates a load balancer worker that proxies requests to the
# internal ACE-Step API server and provides a separate /ping endpoint for
# Runpod health checks.
#
# Build with: docker build -f loadbalancer/Dockerfile -t acestep-loadbalancer:latest ..
# =============================================================================

# Use the same base as the main Dockerfile (with models pre-baked)
# This assumes you've already built the main image with models
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3-pip \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3.11 /usr/bin/python

# Install uv for faster dependency resolution
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# Clone ACE-Step and install (for the API server)
RUN git clone https://github.com/ace-step/ACE-Step-1.5.git /app && \
    rm -rf /app/.git && \
    uv pip install --system --no-cache .

# Create symlink so ACE-Step's model discovery finds /app/checkpoints
RUN ln -s /app/checkpoints /usr/local/lib/python3.11/dist-packages/checkpoints

# Copy loadbalancer app
COPY loadbalancer/ /app/loadbalancer/
WORKDIR /app/loadbalancer

# Install loadbalancer dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create output directory
RUN mkdir -p /app/outputs

# Expose ports:
# - 8000: Main API proxy (for Runpod to call)
# - 8001: Health check /ping endpoint (for Runpod health checks)
EXPOSE 8000 8001

# Environment variables
ENV ACESTEP_API_PORT=8000
ENV ACESTEP_INTERNAL_API_HOST=127.0.0.1
ENV PORT=8000
ENV PORT_HEALTH=8001

# Health check for the main API
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/')" || exit 1

# Run the loadbalancer worker
# This starts both the health check server (on port 8001) and the API proxy (on port 8000)
CMD ["python", "app.py", "--mode", "both"]