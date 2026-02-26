"""
Tracking Service – FastAPI application entry-point.
Provides real-time package tracking via REST + WebSocket,
and consumes tracking events from RabbitMQ.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from shared.common.config import settings
from shared.common.database import init_db
from shared.common.rabbitmq import rabbitmq_client

from .consumers import on_tracking_event
from .routes import router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("tracking-service")


@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.info("Tracking-service starting on port %s …", settings.PORT)

    await init_db()
    logger.info("Database tables ensured.")

    # Connect to RabbitMQ and start consuming tracking events
    await rabbitmq_client.connect()
    await rabbitmq_client.consume(
        queue_name="tracking.updates",
        callback=on_tracking_event,
        exchange_name=settings.TRACKING_EXCHANGE,
        routing_key="tracking.#",
    )
    logger.info("Listening for tracking events on RabbitMQ.")

    yield

    await rabbitmq_client.close()
    logger.info("Tracking-service shutting down.")


app = FastAPI(
    title="SwiftTrack Tracking Service",
    version="1.0.0",
    lifespan=lifespan,
)

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
    return {"service": "tracking-service", "status": "running"}
