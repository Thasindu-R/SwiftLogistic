"""
Queue Manager – Enhanced RabbitMQ handling with Dead Letter Queue support.

Features:
- Publish order events to message broker
- Consume and process events asynchronously
- Dead Letter Queue (DLQ) for failed messages
- Message priority support
- Persistent message delivery

Usage:
    queue_manager = QueueManager()
    await queue_manager.connect()
    
    # Publish with retry support
    await queue_manager.publish_with_retry(
        exchange="swifttrack.orders",
        routing_key="order.created",
        message={"order_id": "...", ...},
        max_retries=3
    )
    
    # Consume with automatic DLQ routing
    await queue_manager.consume_with_dlq(
        queue="order.processing",
        callback=process_order,
        dlq_queue="order.processing.dlq"
    )
"""

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional

import aio_pika
from aio_pika import Message, DeliveryMode, ExchangeType

from shared.common.config import settings

logger = logging.getLogger(__name__)


class MessagePriority(Enum):
    """Message priority levels (0-9, higher = more priority)."""
    LOW = 1
    NORMAL = 5
    HIGH = 8
    CRITICAL = 9


@dataclass
class DLQEntry:
    """Dead Letter Queue entry."""
    message_id: str
    original_queue: str
    original_exchange: str
    original_routing_key: str
    payload: dict
    error_reason: str
    retry_count: int
    max_retries: int
    first_failure_at: datetime
    last_failure_at: datetime
    headers: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            "message_id": self.message_id,
            "original_queue": self.original_queue,
            "original_exchange": self.original_exchange,
            "original_routing_key": self.original_routing_key,
            "payload": self.payload,
            "error_reason": self.error_reason,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "first_failure_at": self.first_failure_at.isoformat(),
            "last_failure_at": self.last_failure_at.isoformat(),
            "headers": self.headers,
        }


class QueueManager:
    """
    Enhanced RabbitMQ queue manager with DLQ support.
    
    Supports:
    - Durable exchanges and queues
    - Dead Letter Queues for failed messages
    - Message priority
    - Automatic retries with exponential backoff
    - Message persistence
    """
    
    # Exchange names
    ORDERS_EXCHANGE = "swifttrack.orders"
    TRACKING_EXCHANGE = "swifttrack.tracking"
    NOTIFICATIONS_EXCHANGE = "swifttrack.notifications"
    DLQ_EXCHANGE = "swifttrack.dlq"
    
    def __init__(self, rabbitmq_url: str | None = None):
        self.rabbitmq_url = rabbitmq_url or settings.RABBITMQ_URL
        self.connection: aio_pika.abc.AbstractRobustConnection | None = None
        self.channel: aio_pika.abc.AbstractChannel | None = None
        self._exchanges: dict[str, aio_pika.abc.AbstractExchange] = {}
        self._queues: dict[str, aio_pika.abc.AbstractQueue] = {}
        self._consumers: dict[str, str] = {}  # queue -> consumer_tag
        self._connected = False
    
    async def connect(self, retries: int = 10, delay: float = 3.0) -> None:
        """
        Connect to RabbitMQ with retry logic.
        
        Args:
            retries: Maximum number of connection attempts
            delay: Delay between retries in seconds
        """
        for attempt in range(1, retries + 1):
            try:
                self.connection = await aio_pika.connect_robust(
                    self.rabbitmq_url,
                    client_properties={"connection_name": "queue-manager"}
                )
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=10)
                
                # Declare DLQ exchange
                await self._declare_exchange(self.DLQ_EXCHANGE, ExchangeType.TOPIC)
                
                self._connected = True
                logger.info("QueueManager connected to RabbitMQ at %s", self.rabbitmq_url)
                return
            except Exception as e:
                logger.warning(
                    "QueueManager connection attempt %d/%d failed: %s",
                    attempt, retries, e
                )
                if attempt < retries:
                    await asyncio.sleep(delay)
                else:
                    raise ConnectionError(f"Failed to connect after {retries} attempts: {e}")
    
    async def close(self) -> None:
        """Close RabbitMQ connection gracefully."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("QueueManager disconnected from RabbitMQ")
        self._connected = False
        self._exchanges.clear()
        self._queues.clear()
        self._consumers.clear()
    
    @property
    def is_connected(self) -> bool:
        return self._connected and self.connection and not self.connection.is_closed
    
    async def _declare_exchange(
        self,
        name: str,
        exchange_type: ExchangeType = ExchangeType.TOPIC
    ) -> aio_pika.abc.AbstractExchange:
        """Declare or get cached exchange."""
        if name not in self._exchanges:
            self._exchanges[name] = await self.channel.declare_exchange(
                name,
                exchange_type,
                durable=True
            )
        return self._exchanges[name]
    
    async def _declare_queue(
        self,
        name: str,
        dlq_exchange: str | None = None,
        dlq_routing_key: str | None = None,
        message_ttl: int | None = None,
        max_length: int | None = None,
    ) -> aio_pika.abc.AbstractQueue:
        """
        Declare or get cached queue with optional DLQ configuration.
        
        Args:
            name: Queue name
            dlq_exchange: Dead letter exchange name
            dlq_routing_key: Dead letter routing key
            message_ttl: Message TTL in milliseconds
            max_length: Maximum queue length
        """
        if name not in self._queues:
            arguments = {}
            
            if dlq_exchange:
                arguments["x-dead-letter-exchange"] = dlq_exchange
            if dlq_routing_key:
                arguments["x-dead-letter-routing-key"] = dlq_routing_key
            if message_ttl:
                arguments["x-message-ttl"] = message_ttl
            if max_length:
                arguments["x-max-length"] = max_length
            
            self._queues[name] = await self.channel.declare_queue(
                name,
                durable=True,
                arguments=arguments if arguments else None
            )
        return self._queues[name]
    
    async def declare_queue_with_dlq(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str,
        dlq_routing_key: str | None = None,
    ) -> tuple[aio_pika.abc.AbstractQueue, aio_pika.abc.AbstractQueue]:
        """
        Declare a queue with its corresponding Dead Letter Queue.
        
        Args:
            queue_name: Main queue name
            exchange_name: Exchange to bind to
            routing_key: Routing key for binding
            dlq_routing_key: Custom DLQ routing key (default: queue_name)
            
        Returns:
            Tuple of (main_queue, dlq_queue)
        """
        dlq_routing = dlq_routing_key or queue_name
        dlq_queue_name = f"{queue_name}.dlq"
        
        # Declare DLQ first
        dlq_exchange = await self._declare_exchange(self.DLQ_EXCHANGE, ExchangeType.TOPIC)
        dlq_queue = await self._declare_queue(dlq_queue_name)
        await dlq_queue.bind(dlq_exchange, routing_key=dlq_routing)
        
        # Declare main queue with DLQ reference
        main_queue = await self._declare_queue(
            queue_name,
            dlq_exchange=self.DLQ_EXCHANGE,
            dlq_routing_key=dlq_routing
        )
        
        # Bind main queue to its exchange
        exchange = await self._declare_exchange(exchange_name)
        await main_queue.bind(exchange, routing_key=routing_key)
        
        logger.info(
            "Declared queue %s with DLQ %s (routing: %s)",
            queue_name, dlq_queue_name, dlq_routing
        )
        
        return main_queue, dlq_queue
    
    async def publish(
        self,
        exchange_name: str,
        routing_key: str,
        message: dict,
        priority: MessagePriority = MessagePriority.NORMAL,
        message_id: str | None = None,
        correlation_id: str | None = None,
        headers: dict | None = None,
        expiration: int | None = None,
    ) -> str:
        """
        Publish a message to an exchange.
        
        Args:
            exchange_name: Target exchange
            routing_key: Routing key
            message: Message payload (dict)
            priority: Message priority
            message_id: Custom message ID (auto-generated if not provided)
            correlation_id: Correlation ID for request/response patterns
            headers: Additional message headers
            expiration: Message expiration in milliseconds
            
        Returns:
            Message ID
        """
        if not self.is_connected:
            raise ConnectionError("Not connected to RabbitMQ")
        
        exchange = await self._declare_exchange(exchange_name)
        
        msg_id = message_id or str(uuid.uuid4())
        msg_headers = headers or {}
        msg_headers["published_at"] = datetime.now(timezone.utc).isoformat()
        msg_headers["retry_count"] = msg_headers.get("retry_count", 0)
        
        body = json.dumps(message).encode()
        
        aio_message = Message(
            body=body,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=priority.value,
            message_id=msg_id,
            correlation_id=correlation_id,
            headers=msg_headers,
            expiration=str(expiration) if expiration else None,
            timestamp=datetime.now(timezone.utc),
        )
        
        await exchange.publish(aio_message, routing_key=routing_key)
        
        logger.info(
            "Published message %s to %s [%s] priority=%s",
            msg_id, exchange_name, routing_key, priority.name
        )
        
        return msg_id
    
    async def publish_with_retry(
        self,
        exchange_name: str,
        routing_key: str,
        message: dict,
        max_retries: int = 3,
        priority: MessagePriority = MessagePriority.NORMAL,
        correlation_id: str | None = None,
    ) -> str:
        """
        Publish a message with retry metadata for automatic retry handling.
        
        Args:
            exchange_name: Target exchange
            routing_key: Routing key
            message: Message payload
            max_retries: Maximum retry attempts
            priority: Message priority
            correlation_id: Correlation ID
            
        Returns:
            Message ID
        """
        headers = {
            "max_retries": max_retries,
            "retry_count": 0,
            "original_exchange": exchange_name,
            "original_routing_key": routing_key,
        }
        
        return await self.publish(
            exchange_name=exchange_name,
            routing_key=routing_key,
            message=message,
            priority=priority,
            correlation_id=correlation_id,
            headers=headers,
        )
    
    async def consume(
        self,
        queue_name: str,
        callback: Callable,
        exchange_name: str | None = None,
        routing_key: str | None = None,
        auto_ack: bool = False,
    ) -> str:
        """
        Start consuming messages from a queue.
        
        Args:
            queue_name: Queue to consume from
            callback: Async callback function(message: aio_pika.Message)
            exchange_name: Optional exchange to bind to
            routing_key: Optional routing key for binding
            auto_ack: Auto acknowledge messages
            
        Returns:
            Consumer tag
        """
        if not self.is_connected:
            raise ConnectionError("Not connected to RabbitMQ")
        
        queue = await self._declare_queue(queue_name)
        
        if exchange_name and routing_key:
            exchange = await self._declare_exchange(exchange_name)
            await queue.bind(exchange, routing_key=routing_key)
        
        consumer_tag = await queue.consume(callback, no_ack=auto_ack)
        self._consumers[queue_name] = consumer_tag
        
        logger.info("Started consuming from queue: %s (tag: %s)", queue_name, consumer_tag)
        
        return consumer_tag
    
    async def consume_with_dlq(
        self,
        queue_name: str,
        callback: Callable,
        exchange_name: str,
        routing_key: str,
        max_retries: int = 3,
    ) -> str:
        """
        Consume from a queue with automatic DLQ routing for failed messages.
        
        Args:
            queue_name: Queue to consume from
            callback: Async callback(message, payload) that returns success bool
            exchange_name: Exchange to bind to
            routing_key: Routing key for binding
            max_retries: Maximum retries before sending to DLQ
            
        Returns:
            Consumer tag
        """
        main_queue, dlq_queue = await self.declare_queue_with_dlq(
            queue_name, exchange_name, routing_key
        )
        
        async def wrapped_callback(message: aio_pika.abc.AbstractIncomingMessage):
            """Wrapper that handles retry logic and DLQ routing."""
            async with message.process(requeue=False):
                try:
                    payload = json.loads(message.body.decode())
                    retry_count = message.headers.get("retry_count", 0) if message.headers else 0
                    
                    # Call the actual handler
                    success = await callback(message, payload)
                    
                    if not success:
                        raise RuntimeError("Handler returned failure")
                        
                except Exception as e:
                    retry_count = (message.headers.get("retry_count", 0) if message.headers else 0) + 1
                    
                    if retry_count < max_retries:
                        # Re-publish with incremented retry count
                        logger.warning(
                            "Message processing failed (attempt %d/%d): %s. Retrying...",
                            retry_count, max_retries, e
                        )
                        
                        headers = dict(message.headers) if message.headers else {}
                        headers["retry_count"] = retry_count
                        headers["last_error"] = str(e)
                        headers["last_retry_at"] = datetime.now(timezone.utc).isoformat()
                        
                        # Exponential backoff delay
                        delay = 2 ** retry_count
                        await asyncio.sleep(delay)
                        
                        payload = json.loads(message.body.decode())
                        await self.publish(
                            exchange_name=exchange_name,
                            routing_key=routing_key,
                            message=payload,
                            headers=headers,
                        )
                    else:
                        # Send to DLQ
                        logger.error(
                            "Message exceeded max retries (%d). Moving to DLQ: %s",
                            max_retries, e
                        )
                        await self._send_to_dlq(message, str(e), queue_name)
        
        consumer_tag = await main_queue.consume(wrapped_callback, no_ack=False)
        self._consumers[queue_name] = consumer_tag
        
        logger.info(
            "Started consuming from %s with DLQ support (max_retries=%d)",
            queue_name, max_retries
        )
        
        return consumer_tag
    
    async def _send_to_dlq(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        error_reason: str,
        original_queue: str,
    ) -> None:
        """Send a failed message to the Dead Letter Queue."""
        dlq_exchange = await self._declare_exchange(self.DLQ_EXCHANGE)
        
        payload = json.loads(message.body.decode())
        headers = dict(message.headers) if message.headers else {}
        
        dlq_entry = DLQEntry(
            message_id=message.message_id or str(uuid.uuid4()),
            original_queue=original_queue,
            original_exchange=headers.get("original_exchange", "unknown"),
            original_routing_key=headers.get("original_routing_key", "unknown"),
            payload=payload,
            error_reason=error_reason,
            retry_count=headers.get("retry_count", 0),
            max_retries=headers.get("max_retries", 3),
            first_failure_at=datetime.fromisoformat(
                headers.get("first_failure_at", datetime.now(timezone.utc).isoformat())
            ),
            last_failure_at=datetime.now(timezone.utc),
            headers=headers,
        )
        
        dlq_message = Message(
            body=json.dumps(dlq_entry.to_dict()).encode(),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=dlq_entry.message_id,
            headers={
                "dlq_reason": error_reason,
                "original_queue": original_queue,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        
        await dlq_exchange.publish(dlq_message, routing_key=original_queue)
        
        logger.info(
            "Message %s moved to DLQ for queue %s: %s",
            dlq_entry.message_id, original_queue, error_reason
        )
    
    async def get_queue_info(self, queue_name: str) -> dict:
        """Get queue statistics."""
        if not self.is_connected:
            raise ConnectionError("Not connected to RabbitMQ")
        
        queue = await self._declare_queue(queue_name)
        
        return {
            "name": queue.name,
            "message_count": queue.declaration_result.message_count,
            "consumer_count": queue.declaration_result.consumer_count,
        }
    
    async def stop_consumer(self, queue_name: str) -> None:
        """Stop consuming from a queue."""
        if queue_name in self._consumers:
            if queue_name in self._queues:
                await self._queues[queue_name].cancel(self._consumers[queue_name])
            del self._consumers[queue_name]
            logger.info("Stopped consumer for queue: %s", queue_name)


# Singleton instance
queue_manager = QueueManager()
