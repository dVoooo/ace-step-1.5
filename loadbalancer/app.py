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
- POST / implements the RunPod job-format bridge so that RunPod's test runner
  (and regular serverless invocations) work without a separate handler.py
"""

import asyncio
import json
import logging
import os
import time
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

# Job handler defaults (mirrors handler.py so tests.json works unchanged)
DEFAULT_DURATION = int(os.environ.get("ACESTEP_DEFAULT_DURATION", "90"))
DEFAULT_POLL_INTERVAL = float(os.environ.get("ACESTEP_POLL_INTERVAL", "3"))
DEFAULT_JOB_TIMEOUT = int(os.environ.get("ACESTEP_JOB_TIMEOUT", "1800"))

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
# RunPod Metrics endpoint (not required by RunPod spec but kept as extension)
# =============================================================================

@app.get("/metrics")
async def metrics(authorization: Optional[str] = Header(None)):
    """
    Optional metrics endpoint.

    Returns {"available": 1} when the internal API is ready, {"available": 0}
    while the model is still loading. Authorization via Bearer token is
    validated when RUNPOD_API_KEY is set.
    """
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

    available = 1 if await _internal_api_healthy() else 0
    return {"available": available}


# =============================================================================
# Root GET endpoint (informational)
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
# RunPod job-format bridge  (POST /)
#
# RunPod's serverless test runner — and regular RunPod /run invocations sent
# to a load-balancer endpoint — POST a job payload to the root path:
#
#   POST /
#   {"id": "job-xxx", "input": {"caption": "...", "duration": 30, ...}}
#
# This handler translates that into the ACE-Step REST API calls (same logic as
# handler.py), then returns the result in the RunPod output envelope:
#
#   {"output": {"task_id": "...", "status": "completed", "results": [...]}}
#   {"error":  "..."}          ← on failure
# =============================================================================

@app.post("/")
async def runpod_job_handler(request: Request):
    """
    RunPod job-format bridge.

    Accepts the RunPod job envelope {"id": ..., "input": {...}}, drives the
    ACE-Step generation pipeline, and returns {"output": {...}}.
    """
    global _http_client

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON body"})

    job_input = body.get("input", {})

    caption = (job_input.get("caption") or "").strip()
    if not caption:
        return JSONResponse(content={"error": "Input 'caption' is required"})

    lyrics = job_input.get("lyrics") or ""
    duration = int(job_input.get("duration", DEFAULT_DURATION))
    batch_size = int(job_input.get("batch_size", 1))
    timeout_seconds = int(job_input.get("timeout_seconds", DEFAULT_JOB_TIMEOUT))
    poll_interval = float(job_input.get("poll_interval", DEFAULT_POLL_INTERVAL))

    logger.info(
        f"Job received — caption='{caption[:60]}' duration={duration}s "
        f"batch_size={batch_size} timeout={timeout_seconds}s"
    )

    # ------------------------------------------------------------------
    # Submit generation task to internal ACE-Step API
    # ------------------------------------------------------------------
    try:
        submit_resp = await _http_client.post(
            f"{API_BASE_URL}/release_task",
            json={
                "caption": caption,
                "lyrics": lyrics,
                "duration": duration,
                "batch_size": batch_size,
            },
            timeout=30.0,
        )
        submit_data = submit_resp.json()
    except Exception as exc:
        logger.error(f"Failed to submit task: {exc}")
        return JSONResponse(content={"error": f"Failed to contact internal API: {exc}"})

    if submit_data.get("code") != 200:
        err = submit_data.get("error") or f"Unexpected code {submit_data.get('code')}"
        logger.error(f"Task submission failed: {err}")
        return JSONResponse(content={"error": err})

    task_id = (submit_data.get("data") or {}).get("task_id")
    if not task_id:
        return JSONResponse(content={"error": "Task submitted but no task_id was returned"})

    logger.info(f"Task submitted: {task_id}")

    # ------------------------------------------------------------------
    # Poll until the task completes, fails, or times out
    # ------------------------------------------------------------------
    started_at = time.time()

    while True:
        elapsed = time.time() - started_at
        if elapsed > timeout_seconds:
            return JSONResponse(
                content={"error": f"Generation timed out after {int(elapsed)}s"}
            )

        try:
            poll_resp = await _http_client.post(
                f"{API_BASE_URL}/query_result",
                json={"task_id_list": [task_id]},
                timeout=30.0,
            )
            poll_data = poll_resp.json()
        except Exception as exc:
            logger.warning(f"Poll request failed (will retry): {exc}")
            await asyncio.sleep(poll_interval)
            continue

        items = poll_data.get("data") or []
        if items:
            task_result = items[0]
            status = task_result.get("status", 0)

            if status == 1:  # completed
                result_data = json.loads(task_result.get("result") or "[]")
                logger.info(f"Task {task_id} completed in {int(elapsed)}s")
                return {
                    "output": {
                        "task_id": task_id,
                        "status": "completed",
                        "duration": duration,
                        "batch_size": batch_size,
                        "results": result_data,
                    }
                }

            if status == 2:  # failed
                logger.error(f"Task {task_id} failed")
                return JSONResponse(content={"error": "Generation failed"})

        await asyncio.sleep(poll_interval)


# =============================================================================
# Catch-all proxy — forward every other request to internal ACE-Step API
# =============================================================================

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_request(path: str, request: Request):
    """
    Proxy all requests to the internal ACE-Step API server.

    Handles:
    - POST /release_task  — submit generation task
    - POST /query_result  — query task results
    - GET  /v1/audio      — download audio files
    - GET  /v1/models     — list available models
    - GET  /v1/stats      — server statistics
    """
    global _http_client

    target_url = f"{API_BASE_URL}/{path}"
    body = await request.body()
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}

    try:
        proxy_response = await _http_client.request(
            method=request.method,
            url=target_url,
            content=body,
            headers=headers,
            params=request.query_params,
        )
        return Response(
            content=proxy_response.content,
            status_code=proxy_response.status_code,
            headers=dict(proxy_response.headers),
            media_type=proxy_response.headers.get("content-type", "application/json"),
        )

    except httpx.ConnectError as exc:
        logger.error(f"Failed to connect to internal API: {exc}")
        raise HTTPException(
            status_code=503,
            detail=f"Internal API server not available at {API_BASE_URL}"
        )
    except httpx.TimeoutException as exc:
        logger.error(f"Request to internal API timed out: {exc}")
        raise HTTPException(status_code=504, detail="Internal API request timed out")
    except Exception as exc:
        logger.error(f"Proxy error: {exc}")
        raise HTTPException(status_code=500, detail=f"Proxy error: {exc}")


# =============================================================================
# Main entry point
# =============================================================================

def main():
    """Main entry point for the load balancer worker."""
    import argparse

    parser = argparse.ArgumentParser(description="ACE-Step Load Balancer Worker")
    parser.add_argument("--port", type=int, default=PORT, help="Port for the server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    args = parser.parse_args()

    logger.info(f"Starting ACE-Step Load Balancer on {args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
