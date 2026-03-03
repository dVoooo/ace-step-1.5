#!/usr/bin/env python3
"""
Load Balancer Worker for ACE-Step 1.5 REST API

This FastAPI application serves as a load balancing worker for Runpod Serverless.
It proxies requests to the ACE-Step API server running on a separate port and
provides a separate /ping endpoint for Runpod health checks.

Key features:
- Separate /ping endpoint on PORT_HEALTH for Runpod health checks
- Proxies all ACE-Step API endpoints to the internal API server
- Supports both serverless (queue + handler) and loadbalancer modes
"""

import os
import sys
import json
import asyncio
import logging
import threading
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse, PlainTextResponse
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

# Internal API server configuration
API_REQUEST_HOST = os.environ.get("ACESTEP_INTERNAL_API_HOST", "127.0.0.1")
API_PORT = int(os.environ.get("ACESTEP_API_PORT", "8000"))
API_BASE_URL = f"http://{API_REQUEST_HOST}:{API_PORT}"

# Health check port (separate from main API port for Runpod load balancer)
HEALTH_PORT = int(os.environ.get("PORT_HEALTH", os.environ.get("PORT", 8000)))

# Main API port
API_PORT_MAIN = int(os.environ.get("PORT", 8000))

# Startup configuration
STARTUP_TIMEOUT = int(os.environ.get("ACESTEP_API_STARTUP_TIMEOUT", "900"))
POLL_INTERVAL = int(os.environ.get("ACESTEP_POLL_INTERVAL", "3"))

# HTTP client timeout
HTTP_TIMEOUT = float(os.environ.get("ACESTEP_HTTP_TIMEOUT", "300.0"))

# =============================================================================
# Global state
# =============================================================================

_api_server_process = None
_http_client: Optional[httpx.AsyncClient] = None


# =============================================================================
# Health check server (runs on separate port)
# =============================================================================

health_app = FastAPI(title="ACE-Step Health Check")


@health_app.get("/ping")
@health_app.get("/health")
async def health_check():
    """
    Health check endpoint for Runpod load balancer.
    
    This endpoint is called by Runpod to verify the worker is healthy.
    It checks if the internal API server is running and responsive.
    """
    try:
        # Check if internal API is healthy
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{API_BASE_URL}/health")
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


@health_app.get("/")
async def root():
    """Root endpoint for health check server."""
    return {"message": "ACE-Step Health Check Server", "endpoints": ["/ping", "/health"]}


def run_health_server():
    """Run the health check server on the designated port."""
    logger.info(f"Starting health check server on port {HEALTH_PORT}")
    uvicorn.run(
        health_app,
        host="0.0.0.0",
        port=HEALTH_PORT,
        log_level="info"
    )


# =============================================================================
# Main API proxy server
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown."""
    global _http_client
    
    # Startup
    logger.info("Starting ACE-Step Load Balancer API server...")
    _http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(HTTP_TIMEOUT),
        follow_redirects=True
    )
    
    yield
    
    # Shutdown
    logger.info("Shutting down ACE-Step Load Balancer API server...")
    if _http_client:
        await _http_client.aclose()


# Create main FastAPI app
main_app = FastAPI(
    title="ACE-Step API Proxy",
    description="Load balancer worker for ACE-Step 1.5 REST API",
    version="1.5.0",
    lifespan=lifespan
)


# =============================================================================
# Proxy endpoints - forward requests to internal ACE-Step API
# =============================================================================

@main_app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
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


@main_app.get("/")
async def root():
    """Root endpoint - redirect to health check."""
    return {
        "message": "ACE-Step API Proxy",
        "version": "1.5.0",
        "docs": "Use /release_task, /query_result, /v1/audio, /v1/models, /v1/stats endpoints"
    }


# =============================================================================
# Main entry point
# =============================================================================

def main():
    """Main entry point for the load balancer worker."""
    import argparse
    
    parser = argparse.ArgumentParser(description="ACE-Step Load Balancer Worker")
    parser.add_argument(
        "--mode",
        choices=["health", "api", "both"],
        default="both",
        help="Run mode: health (ping server), api (proxy server), or both"
    )
    parser.add_argument(
        "--health-port",
        type=int,
        default=HEALTH_PORT,
        help="Port for health check server"
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=API_PORT_MAIN,
        help="Port for API proxy server"
    )
    
    args = parser.parse_args()
    
    if args.mode == "health":
        run_health_server()
    elif args.mode == "api":
        uvicorn.run(
            main_app,
            host="0.0.0.0",
            port=args.api_port,
            log_level="info"
        )
    else:  # both
        # Run health server in a separate thread
        health_thread = threading.Thread(
            target=run_health_server,
            daemon=True
        )
        health_thread.start()
        
        # Run main API server in the main thread
        logger.info(f"Starting main API server on port {args.api_port}")
        uvicorn.run(
            main_app,
            host="0.0.0.0",
            port=args.api_port,
            log_level="info"
        )


if __name__ == "__main__":
    main()