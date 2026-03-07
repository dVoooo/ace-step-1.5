#!/bin/bash
# Start script for ACE-Step Load Balancer Worker
# 
# This script starts:
# 1. ACE-Step API server on port 8001 (internal)
# 2. Load balancer worker on port 8000 (handles both /ping and API proxy)

set -e

echo "=========================================="
echo "ACE-Step Load Balancer Worker Startup"
echo "=========================================="

# Model paths (pre-baked in Docker image)
CONFIG_PATH="${ACESTEP_CONFIG_PATH:-/app/checkpoints/acestep-v15-base}"
LM_MODEL_PATH="${ACESTEP_LM_MODEL_PATH:-/app/checkpoints/acestep-5Hz-lm-1.7B}"

# Port configuration
INTERNAL_API_PORT="${ACESTEP_API_PORT:-8001}"
PROXY_PORT="${PORT:-8000}"

echo "Using DiT model: $CONFIG_PATH"
echo "Using LM model: $LM_MODEL_PATH"
echo "Internal API port: $INTERNAL_API_PORT"
echo "Proxy port: $PROXY_PORT"

# Create output directory
mkdir -p /app/outputs

# =============================================================================
# Start ACE-Step API server (internal)
# =============================================================================
echo ""
echo "Starting ACE-Step API server on port $INTERNAL_API_PORT..."
acestep-api --host 127.0.0.1 --port $INTERNAL_API_PORT 2>&1 | tee /app/outputs/api.log &
API_PID=$!
echo "API server started with PID $API_PID"

# Wait for API to be ready
echo "Waiting for API server to be ready..."
STARTED_AT=$(date +%s)
TIMEOUT=300  # 5 minutes timeout

while true; do
    if curl -s -f "http://127.0.0.1:$INTERNAL_API_PORT/health" > /dev/null 2>&1; then
        echo "API server is ready!"
        break
    fi
    
    ELAPSED=$(( $(date +%s) - STARTED_AT ))
    if [ $ELAPSED -gt $TIMEOUT ]; then
        echo "ERROR: Timeout waiting for API server to start"
        kill $API_PID 2>/dev/null || true
        exit 1
    fi
    
    # Check if process died
    if ! kill -0 $API_PID 2>/dev/null; then
        echo "ERROR: API server process died"
        cat /app/outputs/api.log
        exit 1
    fi
    
    echo "  Waiting... ($(($(date +%s) - STARTED_AT)) s)"
    sleep 5
done

# =============================================================================
# Start Load Balancer Worker (single app, handles /ping + proxy)
# =============================================================================
echo ""
echo "Starting Load Balancer Worker on port $PROXY_PORT..."
cd /app/loadbalancer
python app.py --port $PROXY_PORT --host 0.0.0.0 2>&1 | tee /app/outputs/loadbalancer.log &
LB_PID=$!
echo "Load Balancer started with PID $LB_PID"

echo ""
echo "=========================================="
echo "All services started successfully!"
echo "=========================================="
echo ""
echo "Endpoints:"
echo "  - API Proxy + Health: http://0.0.0.0:$PROXY_PORT"
echo "    (proxies to internal API at 127.0.0.1:$INTERNAL_API_PORT)"
echo ""
echo "Logs:"
echo "  - API server: /app/outputs/api.log"
echo "  - Load Balancer: /app/outputs/loadbalancer.log"
echo ""

# Exit as soon as any child process terminates (requires bash 4.3+)
# This ensures the container restarts if either the API or load balancer dies.
wait -n
exit $?