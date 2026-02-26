"""
Order Service – FastAPI application entry-point.
Manages delivery orders, coordinates with external systems via RabbitMQ (Saga).
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from shared.common.config import settings
from shared.common.database import init_db
from shared.common.rabbitmq import rabbitmq_client

from .routes import router
from .saga import handle_saga_reply

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("order-service")


@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.info("Order-service starting on port %s …", settings.PORT)

    # Initialise DB tables
    await init_db()
    logger.info("Database tables ensured.")

    # Connect to RabbitMQ and start consuming saga replies
    await rabbitmq_client.connect()
    await rabbitmq_client.consume(
        queue_name="order.saga.replies",
        callback=handle_saga_reply,
        exchange_name=settings.ORDER_EXCHANGE,
        routing_key="order.saga.reply",
    )
    logger.info("Listening for saga replies on RabbitMQ.")

    yield

    await rabbitmq_client.close()
    logger.info("Order-service shutting down.")


app = FastAPI(
    title="SwiftTrack Order Service",
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
    return {"service": "order-service", "status": "running"}
