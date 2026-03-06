"""
Message Processor – Asynchronous message processing orchestrator.

Features:
- Process messages from multiple queues
- Coordinate with retry handler and event store
- Handle message acknowledgment
- Support for batch processing
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Coroutine, Optional

import aio_pika

from .queue_manager import QueueManager, MessagePriority
from .retry_handler import RetryHandler, RetryPolicy, RetryResult, RetryStatus
from .event_store import IntegrationEventStore, EventStatus, EventTracker

logger = logging.getLogger(__name__)


class ProcessingResult(Enum):
    """Message processing result."""
    SUCCESS = "success"
    RETRY = "retry"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class MessageHandler:
    """Message handler configuration."""
    event_type: str
    handler: Callable[[dict], Coroutine[Any, Any, ProcessingResult]]
    queue_name: str
    exchange_name: str
    routing_key: str
    retry_policy: RetryPolicy | None = None
    enabled: bool = True
    
    # Statistics
    processed_count: int = 0
    success_count: int = 0
    retry_count: int = 0
    failed_count: int = 0


@dataclass
class ProcessingStats:
    """Processing statistics."""
    total_processed: int = 0
    total_success: int = 0
    total_retry: int = 0
    total_failed: int = 0
    total_skipped: int = 0
    avg_processing_time_ms: float = 0.0
    last_processed_at: datetime | None = None
    
    def to_dict(self) -> dict:
        return {
            "total_processed": self.total_processed,
            "total_success": self.total_success,
            "total_retry": self.total_retry,
            "total_failed": self.total_failed,
            "total_skipped": self.total_skipped,
            "success_rate": self.total_success / self.total_processed if self.total_processed > 0 else 0,
            "avg_processing_time_ms": self.avg_processing_time_ms,
            "last_processed_at": self.last_processed_at.isoformat() if self.last_processed_at else None,
        }


class AsyncMessageProcessor:
    """
    Orchestrates asynchronous message processing.
    
    Features:
    - Multi-queue consumption
    - Automatic retry with configurable policies
    - Event logging and audit
    - Graceful shutdown
    - Health monitoring
    """
    
    def __init__(
        self,
        queue_manager: QueueManager,
        event_store: IntegrationEventStore | None = None,
        retry_handler: RetryHandler | None = None,
        default_retry_policy: RetryPolicy | None = None,
    ):
        """
        Initialize message processor.
        
        Args:
            queue_manager: Queue manager for RabbitMQ operations
            event_store: Event store for logging (optional)
            retry_handler: Retry handler (optional, creates default if not provided)
            default_retry_policy: Default retry policy for handlers
        """
        self.queue_manager = queue_manager
        self.event_store = event_store
        self.retry_handler = retry_handler or RetryHandler()
        self.default_retry_policy = default_retry_policy or RetryPolicy(max_attempts=3)
        
        self._handlers: dict[str, MessageHandler] = {}
        self._running = False
        self._consumer_tasks: list[asyncio.Task] = []
        self._stats = ProcessingStats()
        self._processing_times: list[float] = []
    
    def register_handler(
        self,
        event_type: str,
        handler: Callable[[dict], Coroutine[Any, Any, ProcessingResult]],
        queue_name: str,
        exchange_name: str = "swifttrack.orders",
        routing_key: str | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        """
        Register a message handler.
        
        Args:
            event_type: Event type to handle (e.g., "order.created")
            handler: Async handler function
            queue_name: Queue to consume from
            exchange_name: Exchange name
            routing_key: Routing key (defaults to event_type)
            retry_policy: Custom retry policy
        """
        self._handlers[event_type] = MessageHandler(
            event_type=event_type,
            handler=handler,
            queue_name=queue_name,
            exchange_name=exchange_name,
            routing_key=routing_key or event_type,
            retry_policy=retry_policy or self.default_retry_policy,
        )
        
        logger.info(
            "Registered handler for %s (queue: %s, key: %s)",
            event_type, queue_name, routing_key or event_type
        )
    
    async def start(self) -> None:
        """Start processing messages from all registered handlers."""
        if self._running:
            logger.warning("Message processor already running")
            return
        
        if not self._handlers:
            logger.warning("No handlers registered")
            return
        
        if not self.queue_manager.is_connected:
            await self.queue_manager.connect()
        
        self._running = True
        
        logger.info("Starting message processor with %d handlers", len(self._handlers))
        
        # Start consumers for each handler
        for handler in self._handlers.values():
            if handler.enabled:
                task = asyncio.create_task(
                    self._start_consumer(handler),
                    name=f"consumer-{handler.event_type}"
                )
                self._consumer_tasks.append(task)
        
        logger.info("Message processor started")
    
    async def stop(self, timeout: float = 30.0) -> None:
        """
        Stop processing gracefully.
        
        Args:
            timeout: Maximum time to wait for pending messages
        """
        if not self._running:
            return
        
        logger.info("Stopping message processor...")
        
        self._running = False
        
        # Cancel consumer tasks
        for task in self._consumer_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        if self._consumer_tasks:
            await asyncio.wait(
                self._consumer_tasks,
                timeout=timeout,
                return_when=asyncio.ALL_COMPLETED
            )
        
        self._consumer_tasks.clear()
        
        logger.info("Message processor stopped")
    
    async def _start_consumer(self, handler: MessageHandler) -> None:
        """Start consuming messages for a handler."""
        try:
            # Declare queue with DLQ
            main_queue, dlq_queue = await self.queue_manager.declare_queue_with_dlq(
                queue_name=handler.queue_name,
                exchange_name=handler.exchange_name,
                routing_key=handler.routing_key,
            )
            
            async def message_callback(message: aio_pika.abc.AbstractIncomingMessage):
                """Process a single message."""
                await self._process_message(message, handler)
            
            await main_queue.consume(message_callback, no_ack=False)
            
            logger.info("Consumer started for %s", handler.event_type)
            
            # Keep running until stopped
            while self._running:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logger.info("Consumer cancelled for %s", handler.event_type)
        except Exception as e:
            logger.exception("Consumer failed for %s: %s", handler.event_type, e)
    
    async def _process_message(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        handler: MessageHandler,
    ) -> None:
        """
        Process a single message with retry logic.
        
        Args:
            message: RabbitMQ message
            handler: Handler configuration
        """
        start_time = datetime.now(timezone.utc)
        order_id: str | None = None
        
        async with message.process(requeue=False):
            try:
                payload = json.loads(message.body.decode())
                order_id = payload.get("order_id")
                event_type = payload.get("event", handler.event_type)
                
                logger.debug(
                    "Processing message: %s (order: %s)",
                    event_type, order_id
                )
                
                # Log event start
                if self.event_store and order_id:
                    await self.event_store.log_event(
                        source="message-processor",
                        target=handler.queue_name,
                        event_type=event_type,
                        order_id=order_id,
                        request_data=payload,
                        status=EventStatus.IN_PROGRESS,
                    )
                
                # Execute handler with retry
                retry_result = await self.retry_handler.execute_with_retry(
                    operation=handler.handler,
                    args=(payload,),
                    policy=handler.retry_policy,
                    system_name=handler.queue_name,
                    operation_name=event_type,
                )
                
                # Handle result
                if retry_result.status == RetryStatus.SUCCESS:
                    result = retry_result.result
                    if result == ProcessingResult.SUCCESS:
                        handler.success_count += 1
                        self._stats.total_success += 1
                    elif result == ProcessingResult.SKIPPED:
                        self._stats.total_skipped += 1
                    else:
                        handler.retry_count += 1
                        self._stats.total_retry += 1
                    
                    # Log success
                    if self.event_store and order_id:
                        await self.event_store.log_event(
                            source="message-processor",
                            target=handler.queue_name,
                            event_type=event_type,
                            order_id=order_id,
                            response_data={"result": result.value if result else "success"},
                            status=EventStatus.SUCCESS,
                        )
                        
                elif retry_result.status == RetryStatus.EXHAUSTED:
                    handler.failed_count += 1
                    self._stats.total_failed += 1
                    
                    # Send to DLQ
                    await self.queue_manager._send_to_dlq(
                        message=message,
                        error_reason=str(retry_result.error),
                        original_queue=handler.queue_name,
                    )
                    
                    # Log failure
                    if self.event_store and order_id:
                        await self.event_store.log_event(
                            source="message-processor",
                            target=handler.queue_name,
                            event_type=event_type,
                            order_id=order_id,
                            error_message=str(retry_result.error),
                            status=EventStatus.FAILED,
                        )
                
                elif retry_result.status == RetryStatus.CIRCUIT_OPEN:
                    # Re-queue for later
                    logger.warning(
                        "Circuit breaker open for %s, re-queuing message",
                        handler.queue_name
                    )
                    await self.queue_manager.publish(
                        exchange_name=handler.exchange_name,
                        routing_key=handler.routing_key,
                        message=payload,
                        headers={
                            "circuit_breaker_retry": "true",
                            "retry_at": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                
                handler.processed_count += 1
                self._stats.total_processed += 1
                
            except Exception as e:
                logger.exception(
                    "Message processing failed for %s (order: %s): %s",
                    handler.event_type, order_id, e
                )
                handler.failed_count += 1
                self._stats.total_failed += 1
                
                # Send to DLQ
                await self.queue_manager._send_to_dlq(
                    message=message,
                    error_reason=str(e),
                    original_queue=handler.queue_name,
                )
            
            finally:
                # Update timing stats
                duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
                self._processing_times.append(duration)
                
                # Keep only last 1000 times
                if len(self._processing_times) > 1000:
                    self._processing_times = self._processing_times[-1000:]
                
                self._stats.avg_processing_time_ms = sum(self._processing_times) / len(self._processing_times)
                self._stats.last_processed_at = datetime.now(timezone.utc)
    
    async def process_single(
        self,
        event_type: str,
        payload: dict,
    ) -> ProcessingResult:
        """
        Process a single message directly (for testing/manual processing).
        
        Args:
            event_type: Event type
            payload: Message payload
            
        Returns:
            Processing result
        """
        if event_type not in self._handlers:
            logger.warning("No handler registered for %s", event_type)
            return ProcessingResult.SKIPPED
        
        handler = self._handlers[event_type]
        
        try:
            retry_result = await self.retry_handler.execute_with_retry(
                operation=handler.handler,
                args=(payload,),
                policy=handler.retry_policy,
                system_name=handler.queue_name,
                operation_name=event_type,
            )
            
            if retry_result.status == RetryStatus.SUCCESS:
                return retry_result.result or ProcessingResult.SUCCESS
            else:
                return ProcessingResult.FAILED
                
        except Exception as e:
            logger.exception("Direct processing failed: %s", e)
            return ProcessingResult.FAILED
    
    def get_handler_stats(self, event_type: str | None = None) -> dict:
        """Get handler statistics."""
        if event_type:
            handler = self._handlers.get(event_type)
            if handler:
                return {
                    "event_type": handler.event_type,
                    "queue_name": handler.queue_name,
                    "enabled": handler.enabled,
                    "processed_count": handler.processed_count,
                    "success_count": handler.success_count,
                    "retry_count": handler.retry_count,
                    "failed_count": handler.failed_count,
                    "success_rate": handler.success_count / handler.processed_count if handler.processed_count > 0 else 0,
                }
            return {}
        
        return {
            event_type: self.get_handler_stats(event_type)
            for event_type in self._handlers
        }
    
    def get_stats(self) -> dict:
        """Get overall processing statistics."""
        return {
            "running": self._running,
            "handlers_count": len(self._handlers),
            "active_consumers": len([t for t in self._consumer_tasks if not t.done()]),
            "stats": self._stats.to_dict(),
            "circuit_breakers": self.retry_handler.get_circuit_breaker_status(),
        }
    
    def enable_handler(self, event_type: str) -> bool:
        """Enable a handler."""
        if event_type in self._handlers:
            self._handlers[event_type].enabled = True
            return True
        return False
    
    def disable_handler(self, event_type: str) -> bool:
        """Disable a handler."""
        if event_type in self._handlers:
            self._handlers[event_type].enabled = False
            return True
        return False


# Factory function for creating configured processor
def create_message_processor(
    db=None,
) -> AsyncMessageProcessor:
    """
    Create a configured message processor.
    
    Args:
        db: Database session for event store
        
    Returns:
        Configured AsyncMessageProcessor
    """
    from .queue_manager import queue_manager
    
    event_store = IntegrationEventStore(db) if db else None
    
    processor = AsyncMessageProcessor(
        queue_manager=queue_manager,
        event_store=event_store,
        default_retry_policy=RetryPolicy(
            max_attempts=3,
            base_delay=1.0,
            max_delay=30.0,
        ),
    )
    
    return processor
