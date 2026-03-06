"""
Tracking Service – RabbitMQ consumers.
Listens for tracking events from other services and:
  1. Persists them to the tracking_events table.
  2. Pushes them to connected WebSocket clients in real time
     (per-order AND global user/role channels).
"""

import json
import logging
from datetime import datetime, timezone

import aio_pika
from sqlalchemy import select

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
            client_id = body.get("client_id")

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

                # Resolve client_id from order if not in message
                if not client_id and order_id:
                    from .models import _Order
                    row = (await session.execute(
                        select(_Order.client_id, _Order.assigned_driver_id)
                        .where(_Order.order_id == order_id)
                    )).first()
                    if row:
                        client_id = row.client_id
                        if not driver_id:
                            driver_id = row.assigned_driver_id

            # Enrich payload with channel hint
            push_data = {**body, "channel": "tracking"}

            # 1. Per-order broadcast (existing – for Track Delivery tab)
            await ws_manager.broadcast(order_id, push_data)

            # 2. Global: push to the owning client
            if client_id:
                await ws_manager.send_to_user(int(client_id), push_data)

            # 3. Global: push to the assigned driver
            if driver_id:
                await ws_manager.send_to_user(int(driver_id), push_data)

            # 4. Global: always push to admins (integration log, all-orders)
            await ws_manager.send_to_role("admin", push_data)

            logger.info(
                "Persisted & broadcast tracking event: %s for order %s",
                event_type,
                order_id,
            )
        except Exception:
            logger.exception("Failed to process tracking event")
