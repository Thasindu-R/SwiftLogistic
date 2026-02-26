"""
API Gateway – FastAPI application entry-point.

Single entry-point for the SwiftTrack platform.  All requests from
the frontend (Client Portal / Driver App) arrive here and are routed
to the appropriate internal microservice: auth-service, order-service,
or tracking-service.

Also exposes a WebSocket endpoint for real-time tracking.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from shared.common.config import settings

from .routes import auth_router, orders_router, tracking_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("api-gateway")


@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.info("API Gateway starting on port %s …", settings.PORT)
    logger.info("  → auth-service  : %s", settings.AUTH_SERVICE_URL)
    logger.info("  → order-service : %s", settings.ORDER_SERVICE_URL)
    logger.info("  → tracking-svc  : %s", settings.TRACKING_SERVICE_URL)
    yield
    logger.info("API Gateway shutting down.")


app = FastAPI(
    title="SwiftTrack API Gateway",
    description="Unified entry-point for the SwiftLogistics platform",
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS ─────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Route registration ───────────────────────────────────────
app.include_router(auth_router)
app.include_router(orders_router)
app.include_router(tracking_router)


# ── Health / root ────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service": "api-gateway",
        "status": "running",
        "docs": "/docs",
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "services": ["auth", "orders", "tracking"]}
