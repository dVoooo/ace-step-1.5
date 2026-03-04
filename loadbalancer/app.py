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
"""

import os
import sys
import logging
import threading
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
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

logger.info(f"Internal API URL: {API_BASE_URL}")
logger.info(f"Main server port: {PORT}")

# =============================================================================
# Global state
# =============================================================================

_http_client: Optional[httpx.AsyncClient] = None


# =============================================================================
# FastAPI Application
# =============================================================================

app = FastAPI(
    title="ACE-Step Load Balancer",
    description="Load balancer worker for ACE-Step 1.5 REST API with health checks",
    version="1.5.0"
)


@app.on_event("startup")
async def startup():
    """Initialize HTTP client on startup."""
    global _http_client
    logger.info("Starting ACE-Step Load Balancer...")
    _http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(HTTP_TIMEOUT),
        follow_redirects=True
    )


@app.on_event("shutdown")
async def shutdown():
    """Cleanup HTTP client on shutdown."""
    global _http_client
    logger.info("Shutting down ACE-Step Load Balancer...")
    if _http_client:
        await _http_client.aclose()


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
    global _http_client
    
    try:
        # Check if internal API is healthy
        response = await _http_client.get(f"{API_BASE_URL}/health", timeout=10.0)
        if response.status_code == 200:
            data = response.json()
            if data.get("data", {}).get("status") == "ok":
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
    except httpx.ConnectError:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "api_status": "not_running",
                "service": "ACE-Step Load Balancer"
            }
        )
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e),
                "service": "ACE-Step Load Balancer"
            }
        )


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