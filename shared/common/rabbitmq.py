"""
RabbitMQ helper using aio-pika for async publish / consume.
Provides a singleton `rabbitmq_client` that every service can import.
"""

import asyncio
import json
import logging
from typing import Callable

import aio_pika

from .config import settings

logger = logging.getLogger(__name__)


class RabbitMQClient:
    """Thin wrapper around aio-pika for topic-based pub/sub."""

    def __init__(self):
        self.connection: aio_pika.abc.AbstractRobustConnection | None = None
        self.channel: aio_pika.abc.AbstractChannel | None = None

    # ── lifecycle ────────────────────────────────────────────
    async def connect(self, retries: int = 10, delay: float = 3.0):
        """Open a robust connection and channel to RabbitMQ with retry."""
        for attempt in range(1, retries + 1):
            try:
                self.connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=10)
                logger.info("Connected to RabbitMQ at %s", settings.RABBITMQ_URL)
                return
            except Exception as e:
                logger.warning(
                    "RabbitMQ connection attempt %d/%d failed: %s",
                    attempt, retries, e,
                )
                if attempt < retries:
                    await asyncio.sleep(delay)
                else:
                    raise

    async def close(self):
        """Gracefully close the connection."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("Disconnected from RabbitMQ")

    # ── helpers ──────────────────────────────────────────────
    async def declare_exchange(
        self,
        name: str,
        exchange_type: str = "topic",
    ) -> aio_pika.abc.AbstractExchange:
        """Declare (or re-use) a durable exchange."""
        return await self.channel.declare_exchange(
            name,
            aio_pika.ExchangeType[exchange_type.upper()],
            durable=True,
        )

    async def declare_queue(
        self,
        queue_name: str,
        exchange_name: str | None = None,
        routing_key: str | None = None,
    ) -> aio_pika.abc.AbstractQueue:
        """Declare a durable queue, optionally binding it to an exchange."""
        queue = await self.channel.declare_queue(queue_name, durable=True)
        if exchange_name and routing_key:
            exchange = await self.declare_exchange(exchange_name)
            await queue.bind(exchange, routing_key=routing_key)
        return queue

    # ── publish ──────────────────────────────────────────────
    async def publish(
        self,
        exchange_name: str,
        routing_key: str,
        message: dict,
    ):
        """Publish a JSON message to an exchange with the given routing key."""
        exchange = await self.declare_exchange(exchange_name)
        body = json.dumps(message).encode()
        await exchange.publish(
            aio_pika.Message(
                body=body,
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
        )
        logger.info(
            "Published message to %s [%s]: %s",
            exchange_name,
            routing_key,
            message.get("event", ""),
        )

    # ── consume ──────────────────────────────────────────────
    async def consume(
        self,
        queue_name: str,
        callback: Callable,
        exchange_name: str | None = None,
        routing_key: str | None = None,
    ):
        """Start consuming messages from a queue (non-blocking)."""
        queue = await self.declare_queue(queue_name, exchange_name, routing_key)
        await queue.consume(callback)
        logger.info("Consuming from queue: %s", queue_name)


# Singleton instance – import this in services
rabbitmq_client = RabbitMQClient()
