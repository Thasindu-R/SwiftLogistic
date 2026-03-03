"""
Order Service – FastAPI application entry-point.
Manages delivery orders, coordinates with external systems via RabbitMQ (Saga).
Integrates full async processing & reliability on startup:
  - QueueManager with DLQ support
  - FailureRecoveryService for incomplete saga / DLQ recovery
  - Integration event logging
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from shared.common.errors import register_exception_handlers
from shared.common.middleware import SecurityHeadersMiddleware, RequestLoggingMiddleware

from shared.common.config import settings
from shared.common.database import init_db, async_session_factory
from shared.common.rabbitmq import rabbitmq_client

# Async processing components
from shared.common.async_processor.queue_manager import queue_manager
from shared.common.async_processor.event_store import IntegrationEventStore
from shared.common.async_processor.recovery_service import FailureRecoveryService

from .routes import router
from .saga import handle_saga_reply

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("order-service")


@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.info("Order-service starting on port %s …", settings.PORT)

    # ── 1. Initialise DB tables ──────────────────────────────
    await init_db()
    logger.info("Database tables ensured.")

    # ── 2. Connect basic RabbitMQ client ─────────────────────
    await rabbitmq_client.connect()
    await rabbitmq_client.consume(
        queue_name="order.saga.replies",
        callback=handle_saga_reply,
        exchange_name=settings.ORDER_EXCHANGE,
        routing_key="order.saga.reply",
    )
    logger.info("Listening for saga replies on RabbitMQ.")

    # ── 3. Connect QueueManager (DLQ-enabled) ────────────────
    try:
        await queue_manager.connect()
        logger.info("QueueManager connected (DLQ support active).")

        # Declare DLQ-backed queues for each integration
        for q, ex, rk in [
            ("order.processing", settings.ORDER_EXCHANGE, "order.created"),
            ("order.cms", settings.ORDER_EXCHANGE, "cms.billing"),
            ("order.wms", settings.ORDER_EXCHANGE, "wms.package"),
            ("order.ros", settings.ORDER_EXCHANGE, "ros.route"),
        ]:
            await queue_manager.declare_queue_with_dlq(q, ex, rk)
            logger.info("DLQ pair declared for %s", q)
    except Exception as exc:
        logger.error("QueueManager setup error (non-fatal): %s", exc)

    # ── 4. Run failure-recovery on startup ───────────────────
    try:
        async with async_session_factory() as session:
            event_store = IntegrationEventStore(session)
            recovery = FailureRecoveryService(
                db=session,
                queue_manager=queue_manager,
                event_store=event_store,
            )
            result = await recovery.run_full_recovery()
            logger.info(
                "Startup recovery: status=%s recovered=%d failed=%d",
                result.status.value,
                result.recovered_count,
                result.failed_count,
            )
    except Exception as exc:
        logger.error("Startup recovery error (non-fatal): %s", exc)

    yield

    # ── Shutdown ─────────────────────────────────────────────
    await rabbitmq_client.close()
    if queue_manager.is_connected:
        await queue_manager.connection.close()
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
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RequestLoggingMiddleware)

# ── Exception Handlers ───────────────────────────────────────
register_exception_handlers(app)

app.include_router(router)


@app.get("/")
async def root():
    return {"service": "order-service", "status": "running"}
