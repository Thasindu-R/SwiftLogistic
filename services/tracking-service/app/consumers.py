"""
Tracking Service – RabbitMQ consumers.
Listens for tracking events from other services and:
  1. Persists them to the tracking_events table.
  2. Pushes them to connected WebSocket clients in real time.
"""

import json
import logging
from datetime import datetime, timezone

import aio_pika

from shared.common.database import async_session_factory
from .models import TrackingEvent
from .websocket_manager import ws_manager

logger = logging.getLogger(__name__)


async def on_tracking_event(message: aio_pika.abc.AbstractIncomingMessage):
    """
    Callback for messages arriving on the tracking.updates queue.
    Fired for events like package_received, in_transit, delivered, etc.
    """
    async with message.process():
        try:
            body = json.loads(message.body.decode())
            order_id = body.get("order_id", "")
            event_type = body.get("event_type", "unknown")
            description = body.get("description", "")
            location = body.get("location", "")
            driver_id = body.get("driver_id")
            latitude = body.get("latitude")
            longitude = body.get("longitude")

            # Persist
            async with async_session_factory() as session:
                event = TrackingEvent(
                    order_id=order_id,
                    event_type=event_type,
                    description=description,
                    location=location,
                    latitude=latitude,
                    longitude=longitude,
                    driver_id=driver_id,
                )
                session.add(event)
                await session.commit()

            # Push to WebSocket clients watching this order
            await ws_manager.broadcast(order_id, body)

            logger.info(
                "Persisted & broadcast tracking event: %s for order %s",
                event_type,
                order_id,
            )
        except Exception:
            logger.exception("Failed to process tracking event")
