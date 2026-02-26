"""
Order Saga – orchestrates distributed transaction across CMS, WMS, and ROS.

Pattern: Saga (orchestration-based) with compensating transactions.
When a step fails, the saga runs compensations for already-completed steps.
"""

import json
import logging
from datetime import datetime, timezone

import aio_pika

from shared.common.config import settings
from shared.common.rabbitmq import rabbitmq_client

logger = logging.getLogger(__name__)


class OrderSaga:
    """
    Saga steps for a new order:
      1. Confirm order in CMS  (SOAP/XML  → mock-cms)
      2. Register package in WMS (TCP/IP  → mock-wms)
      3. Request route from ROS  (REST    → mock-ros)

    Each step publishes an event; the mock services reply on dedicated queues.
    On failure of any step, compensating events are published.
    """

    def __init__(self, order_data: dict):
        self.order_data = order_data
        self.order_id = order_data["order_id"]
        self.completed_steps: list[str] = []

    async def execute(self):
        """Publish the order.created event; mock services pick it up asynchronously."""
        event = {
            "event": settings.ORDER_CREATED_KEY,
            "order_id": self.order_id,
            "client_id": self.order_data["client_id"],
            "status": "pending",
            "pickup_address": self.order_data["pickup_address"],
            "delivery_address": self.order_data["delivery_address"],
            "package_description": self.order_data.get("package_description", ""),
            "package_weight": self.order_data.get("package_weight", 0.0),
            "priority": self.order_data.get("priority", "normal"),
            "recipient_name": self.order_data.get("recipient_name", ""),
            "recipient_phone": self.order_data.get("recipient_phone", ""),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Publish to order exchange – CMS, WMS, and ROS queues are all bound
        await rabbitmq_client.publish(
            exchange_name=settings.ORDER_EXCHANGE,
            routing_key=settings.ORDER_CREATED_KEY,
            message=event,
        )
        logger.info("Saga started for order %s", self.order_id)

    async def compensate(self, failed_step: str, reason: str):
        """
        Publish compensating events for steps that already succeeded.
        E.g. if WMS step succeeded but ROS failed → cancel the WMS entry.
        """
        compensation = {
            "event": settings.ORDER_FAILED_KEY,
            "order_id": self.order_id,
            "failed_step": failed_step,
            "reason": reason,
            "completed_steps": self.completed_steps,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await rabbitmq_client.publish(
            exchange_name=settings.ORDER_EXCHANGE,
            routing_key=settings.ORDER_FAILED_KEY,
            message=compensation,
        )
        logger.warning(
            "Saga compensation for order %s – failed at %s: %s",
            self.order_id,
            failed_step,
            reason,
        )


async def handle_saga_reply(message: aio_pika.abc.AbstractIncomingMessage):
    """
    Callback for saga-reply messages from mock services.
    Updates order status based on the reply.
    """
    async with message.process():
        body = json.loads(message.body.decode())
        order_id = body.get("order_id")
        step = body.get("step")
        success = body.get("success", False)
        logger.info(
            "Saga reply for order %s – step=%s success=%s",
            order_id,
            step,
            success,
        )
        # In a full implementation this would update the DB and
        # potentially trigger compensation. For the prototype the
        # order status is updated via the tracking-service events.
