#!/usr/bin/env python3
"""
Load Balancer Worker for ACE-Step 1.5 REST API

This FastAPI application serves as a load balancing worker for Runpod Serverless.
It proxies requests to the ACE-Step API server running on a separate internal port
and provides a /ping endpoint for Runpod health checks on the same port.

Key features:
- Single port (8000) for both health checks and API proxy
- Proxies all ACE-Step API endpoints to the internal API server
- Health check verifies internal API is running
- /metrics returns RunPod-compatible {"available": N} schema
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, Header
from fastapi.responses import JSONResponse, Response
import uvicorn
import httpx

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

# Internal API server configuration (on a different port)
API_REQUEST_HOST = os.environ.get("ACESTEP_INTERNAL_API_HOST", "127.0.0.1")
API_PORT = int(os.environ.get("ACESTEP_API_PORT", "8001"))
API_BASE_URL = f"http://{API_REQUEST_HOST}:{API_PORT}"

# Main server port (same as PORT env var for Runpod)
PORT = int(os.environ.get("PORT", 8000))

# HTTP client timeout
HTTP_TIMEOUT = float(os.environ.get("ACESTEP_HTTP_TIMEOUT", "300.0"))

# RunPod API Key for authentication (required for load balancer health checks)
RUNPOD_API_KEY = os.environ.get("RUNPOD_API_KEY", "")

logger.info(f"Internal API URL: {API_BASE_URL}")
logger.info(f"Main server port: {PORT}")
logger.info(f"RunPod API Key configured: {bool(RUNPOD_API_KEY)}")

# =============================================================================
# Global state
# =============================================================================

_http_client: Optional[httpx.AsyncClient] = None


# =============================================================================
# Internal API health helper
# =============================================================================

async def _internal_api_healthy() -> bool:
    """Return True when the internal ACE-Step API is ready to accept requests."""
    global _http_client
    if _http_client is None:
        return False
    try:
        response = await _http_client.get(f"{API_BASE_URL}/health", timeout=10.0)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("status") == "ok"
        return False
    except Exception:
        return False


# =============================================================================
# FastAPI Application
# =============================================================================

@asynccontextmanager
async def lifespan(application: FastAPI):
    """Manage HTTP client lifecycle (replaces deprecated @app.on_event)."""
    global _http_client
    logger.info("Starting ACE-Step Load Balancer...")
    _http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(HTTP_TIMEOUT),
        follow_redirects=True
    )
    yield
    logger.info("Shutting down ACE-Step Load Balancer...")
    if _http_client:
        await _http_client.aclose()


app = FastAPI(
    title="ACE-Step Load Balancer",
    description="Load balancer worker for ACE-Step 1.5 REST API with health checks",
    version="1.5.0",
    lifespan=lifespan
)


# =============================================================================
# Health check endpoint
# =============================================================================

@app.get("/ping")
@app.get("/health")
async def health_check():
    """
    Health check endpoint for Runpod load balancer.

    This endpoint is called by Runpod to verify the worker is healthy.
    It checks if the internal API server is running and responsive.
    """
    if await _internal_api_healthy():
        return {
            "status": "healthy",
            "api_status": "ok",
            "service": "ACE-Step Load Balancer"
        }

    return JSONResponse(
        status_code=503,
        content={
            "status": "unhealthy",
            "api_status": "not_ready",
            "service": "ACE-Step Load Balancer"
        }
    )


# =============================================================================
# RunPod Metrics endpoint (required for load balancer routing weight)
# =============================================================================

@app.get("/metrics")
async def metrics(authorization: Optional[str] = Header(None)):
    """
    Metrics endpoint for RunPod load balancer.

    RunPod's serverless load balancer requires this endpoint to determine
    routing weight. It MUST return {"available": N} where N >= 1 means
    the worker can accept requests, and N == 0 means it is busy/loading.

    The endpoint is called with Authorization: Bearer <RUNPOD_API_KEY>.
    """
    # Validate authorization if RUNPOD_API_KEY is configured
    if RUNPOD_API_KEY:
        if not authorization:
            return JSONResponse(
                status_code=403,
                content={"error": "Missing authorization header"}
            )

        auth_scheme, _, token = authorization.partition(' ')
        if auth_scheme.lower() != 'bearer' or token != RUNPOD_API_KEY:
            return JSONResponse(
                status_code=403,
                content={"error": "Invalid authorization"}
            )

    # Return RunPod-required schema: {"available": N}
    # available=1 → worker is ready; available=0 → model still loading
    available = 1 if await _internal_api_healthy() else 0
    return {"available": available}


# =============================================================================
# Root endpoint
# =============================================================================

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "ACE-Step API Proxy",
        "version": "1.5.0",
        "docs": "Use /release_task, /query_result, /v1/audio, /v1/models, /v1/stats endpoints"
    }


# =============================================================================
# Proxy endpoints - forward requests to internal ACE-Step API
# =============================================================================

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_request(path: str, request: Request):
    """
    Proxy all requests to the internal ACE-Step API server.

    This handles all API endpoints including:
    - POST /release_task - Submit generation task
    - POST /query_result - Query task results
    - GET /v1/audio - Download audio files
    - GET /v1/models - List available models
    - GET /v1/stats - Get server statistics
    - And all other ACE-Step API endpoints
    """
    global _http_client

    # Build the target URL
    target_url = f"{API_BASE_URL}/{path}"

    # Get request body
    body = await request.body()

    # Prepare headers (exclude host header)
    headers = dict(request.headers)
    headers.pop("host", None)

    try:
        # Forward the request
        proxy_response = await _http_client.request(
            method=request.method,
            url=target_url,
            content=body,
            headers=headers,
            params=request.query_params
        )

        # Return the response
        return Response(
            content=proxy_response.content,
            status_code=proxy_response.status_code,
            headers=dict(proxy_response.headers),
            media_type=proxy_response.headers.get("content-type", "application/json")
        )

    except httpx.ConnectError as e:
        logger.error(f"Failed to connect to internal API: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Internal API server not available at {API_BASE_URL}"
        )
    except httpx.TimeoutException as e:
        logger.error(f"Request to internal API timed out: {e}")
        raise HTTPException(
            status_code=504,
            detail="Internal API request timed out"
        )
    except Exception as e:
        logger.error(f"Proxy error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Proxy error: {str(e)}"
        )


# =============================================================================
# Main entry point
# =============================================================================

def main():
    """Main entry point for the load balancer worker."""
    import argparse

    parser = argparse.ArgumentParser(description="ACE-Step Load Balancer Worker")
    parser.add_argument(
        "--port",
        type=int,
        default=PORT,
        help="Port for the server"
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind to"
    )

    args = parser.parse_args()

    logger.info(f"Starting ACE-Step Load Balancer on {args.host}:{args.port}")
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="info"
    )


if __name__ == "__main__":
    main()
