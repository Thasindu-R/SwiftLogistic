"""
Auth Service – FastAPI application entry-point.
Handles user registration, login, and JWT-based authentication.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from shared.common.config import settings
from shared.common.database import init_db

from .routes import router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("auth-service")


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Startup / shutdown lifecycle hooks."""
    logger.info("Auth-service starting on port %s …", settings.PORT)
    await init_db()
    logger.info("Database tables ensured.")
    yield
    logger.info("Auth-service shutting down.")


app = FastAPI(
    title="SwiftTrack Auth Service",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS – allow the frontend and api-gateway
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/")
async def root():
    return {"service": "auth-service", "status": "running"}
