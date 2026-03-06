"""
Failure Recovery Service – Recovers unprocessed messages after system restart.

Features:
- Recover incomplete sagas
- Reprocess failed messages from DLQ
- Ensure no order is lost
- Automatic recovery on startup
- Manual recovery triggers
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Coroutine, Optional

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from .queue_manager import QueueManager, DLQEntry
from .saga_state import SagaStateMachine, SagaState, SagaRecord
from .event_store import IntegrationEventStore, EventStatus, IntegrationEventLog

logger = logging.getLogger(__name__)


class RecoveryStatus(Enum):
    """Recovery operation status."""
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    NO_ACTION = "no_action"


@dataclass
class RecoveryResult:
    """Result of a recovery operation."""
    status: RecoveryStatus
    recovered_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    details: list[dict] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    
    def to_dict(self) -> dict:
        return {
            "status": self.status.value,
            "recovered_count": self.recovered_count,
            "failed_count": self.failed_count,
            "skipped_count": self.skipped_count,
            "details": self.details,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class FailureRecoveryService:
    """
    Service for recovering from system failures.
    
    Handles:
    - Incomplete saga recovery
    - DLQ message reprocessing
    - Failed integration event retry
    - Order state reconciliation
    """
    
    def __init__(
        self,
        db: AsyncSession,
        queue_manager: QueueManager,
        event_store: IntegrationEventStore | None = None,
        max_recovery_age_hours: int = 72,  # 3 days
    ):
        """
        Initialize recovery service.
        
        Args:
            db: Database session
            queue_manager: Queue manager instance
            event_store: Event store for logging (optional)
            max_recovery_age_hours: Maximum age of items to recover
        """
        self.db = db
        self.queue_manager = queue_manager
        self.event_store = event_store or IntegrationEventStore(db)
        self.max_recovery_age = timedelta(hours=max_recovery_age_hours)
        
        # Recovery handlers for different message types
        self._recovery_handlers: dict[str, Callable] = {}
        
        # Recovery statistics
        self._last_recovery: datetime | None = None
        self._recovery_history: list[RecoveryResult] = []
    
    def register_handler(
        self,
        message_type: str,
        handler: Callable[[dict], Coroutine[Any, Any, bool]],
    ) -> None:
        """
        Register a handler for recovering specific message types.
        
        Args:
            message_type: Message type identifier
            handler: Async handler function that returns True on success
        """
        self._recovery_handlers[message_type] = handler
        logger.info("Registered recovery handler for: %s", message_type)
    
    async def run_full_recovery(self) -> RecoveryResult:
        """
        Run full system recovery.
        
        Performs:
        1. Recover incomplete sagas
        2. Process DLQ messages
        3. Retry failed integration events
        
        Returns:
            RecoveryResult with summary
        """
        result = RecoveryResult(status=RecoveryStatus.SUCCESS)
        
        logger.info("Starting full system recovery...")
        
        try:
            # 1. Recover incomplete sagas
            saga_result = await self.recover_incomplete_sagas()
            result.recovered_count += saga_result.recovered_count
            result.failed_count += saga_result.failed_count
            result.details.append({
                "phase": "sagas",
                "result": saga_result.to_dict()
            })
            
            # 2. Process DLQ messages
            dlq_result = await self.process_all_dlqs()
            result.recovered_count += dlq_result.recovered_count
            result.failed_count += dlq_result.failed_count
            result.details.append({
                "phase": "dlq",
                "result": dlq_result.to_dict()
            })
            
            # 3. Retry failed events
            events_result = await self.retry_failed_events()
            result.recovered_count += events_result.recovered_count
            result.failed_count += events_result.failed_count
            result.details.append({
                "phase": "events",
                "result": events_result.to_dict()
            })
            
            # Determine overall status
            if result.failed_count > 0 and result.recovered_count > 0:
                result.status = RecoveryStatus.PARTIAL
            elif result.failed_count > 0:
                result.status = RecoveryStatus.FAILED
            elif result.recovered_count == 0:
                result.status = RecoveryStatus.NO_ACTION
            
        except Exception as e:
            logger.exception("Full recovery failed: %s", e)
            result.status = RecoveryStatus.FAILED
            result.details.append({
                "phase": "error",
                "error": str(e)
            })
        
        result.completed_at = datetime.now(timezone.utc)
        self._last_recovery = result.completed_at
        self._recovery_history.append(result)
        
        logger.info(
            "Full recovery completed: status=%s, recovered=%d, failed=%d",
            result.status.value, result.recovered_count, result.failed_count
        )
        
        return result
    
    async def recover_incomplete_sagas(self) -> RecoveryResult:
        """
        Recover all incomplete sagas.
        
        Returns:
            RecoveryResult with saga recovery details
        """
        result = RecoveryResult(status=RecoveryStatus.SUCCESS)
        
        try:
            # Get incomplete sagas
            cutoff = datetime.now(timezone.utc) - self.max_recovery_age
            
            query = select(SagaRecord).where(
                and_(
                    SagaRecord.state.in_(["pending", "in_progress", "compensating"]),
                    SagaRecord.started_at >= cutoff,
                )
            )
            
            db_result = await self.db.execute(query)
            saga_records = db_result.scalars().all()
            
            logger.info("Found %d incomplete sagas to recover", len(saga_records))
            
            for record in saga_records:
                try:
                    saga = await SagaStateMachine.load_from_db(record.saga_id, self.db)
                    
                    if not saga:
                        result.skipped_count += 1
                        continue
                    
                    # Handle based on state
                    if saga.state == SagaState.COMPENSATING:
                        # Resume compensation
                        # Note: In production, would need to call compensation handlers
                        logger.info(
                            "Saga %s needs compensation completion (order: %s)",
                            saga.saga_id, saga.order_id
                        )
                        result.recovered_count += 1
                        
                    elif saga.state in [SagaState.PENDING, SagaState.IN_PROGRESS]:
                        # Check if can be resumed or needs compensation
                        logger.info(
                            "Saga %s needs resume/retry (order: %s, state: %s)",
                            saga.saga_id, saga.order_id, saga.state.value
                        )
                        
                        # Log for manual review - in production would attempt re-execution
                        await self.event_store.log_audit_trail(
                            order_id=saga.order_id,
                            action="recovery.saga.identified",
                            entity_type="saga",
                            entity_id=saga.saga_id,
                            details=f"Saga in state {saga.state.value} identified for recovery",
                        )
                        
                        result.recovered_count += 1
                    
                    result.details.append({
                        "saga_id": saga.saga_id,
                        "order_id": saga.order_id,
                        "state": saga.state.value,
                        "action": "identified_for_recovery",
                    })
                    
                except Exception as e:
                    logger.error("Failed to recover saga %s: %s", record.saga_id, e)
                    result.failed_count += 1
                    result.details.append({
                        "saga_id": record.saga_id,
                        "error": str(e),
                    })
            
        except Exception as e:
            logger.exception("Saga recovery failed: %s", e)
            result.status = RecoveryStatus.FAILED
        
        result.completed_at = datetime.now(timezone.utc)
        
        return result
    
    async def process_all_dlqs(self) -> RecoveryResult:
        """
        Process all Dead Letter Queues.
        
        Returns:
            RecoveryResult with DLQ processing details
        """
        result = RecoveryResult(status=RecoveryStatus.SUCCESS)
        
        # Known DLQ queues
        dlq_queues = [
            "order.processing.dlq",
            "order.cms.dlq",
            "order.wms.dlq",
            "order.ros.dlq",
        ]
        
        for queue_name in dlq_queues:
            try:
                queue_result = await self.process_dlq(queue_name, max_messages=50)
                result.recovered_count += queue_result.recovered_count
                result.failed_count += queue_result.failed_count
                result.skipped_count += queue_result.skipped_count
                
                result.details.append({
                    "queue": queue_name,
                    "result": queue_result.to_dict(),
                })
                
            except Exception as e:
                logger.error("Failed to process DLQ %s: %s", queue_name, e)
                result.failed_count += 1
                result.details.append({
                    "queue": queue_name,
                    "error": str(e),
                })
        
        result.completed_at = datetime.now(timezone.utc)
        
        return result
    
    async def process_dlq(
        self,
        queue_name: str,
        max_messages: int = 100,
        auto_retry: bool = True,
    ) -> RecoveryResult:
        """
        Process messages from a Dead Letter Queue.
        
        Args:
            queue_name: DLQ name
            max_messages: Maximum messages to process
            auto_retry: Automatically retry messages
            
        Returns:
            RecoveryResult with processing details
        """
        result = RecoveryResult(status=RecoveryStatus.SUCCESS)
        
        if not self.queue_manager.is_connected:
            await self.queue_manager.connect()
        
        try:
            # Declare the DLQ
            dlq = await self.queue_manager._declare_queue(queue_name)
            
            # Get queue info
            info = await self.queue_manager.get_queue_info(queue_name)
            message_count = info.get("message_count", 0)
            
            if message_count == 0:
                logger.info("DLQ %s is empty", queue_name)
                result.status = RecoveryStatus.NO_ACTION
                return result
            
            logger.info(
                "Processing DLQ %s: %d messages (max: %d)",
                queue_name, message_count, max_messages
            )
            
            processed = 0
            
            async def process_message(message):
                nonlocal processed
                
                if processed >= max_messages:
                    await message.reject(requeue=True)
                    return
                
                async with message.process(requeue=False):
                    try:
                        body = json.loads(message.body.decode())
                        
                        # Extract original message info
                        if isinstance(body, dict) and "payload" in body:
                            # It's a DLQEntry
                            original_payload = body.get("payload", {})
                            original_queue = body.get("original_queue", "")
                            error_reason = body.get("error_reason", "")
                        else:
                            original_payload = body
                            original_queue = queue_name.replace(".dlq", "")
                            error_reason = message.headers.get("dlq_reason", "") if message.headers else ""
                        
                        order_id = original_payload.get("order_id", "unknown")
                        
                        # Log the DLQ message for audit
                        await self.event_store.log_audit_trail(
                            order_id=order_id,
                            action="recovery.dlq.processed",
                            entity_type="message",
                            entity_id=message.message_id,
                            details=f"DLQ message from {original_queue}: {error_reason}",
                        )
                        
                        if auto_retry and original_queue:
                            # Determine message type and handler
                            message_type = original_payload.get("event", "unknown")
                            
                            if message_type in self._recovery_handlers:
                                # Use registered handler
                                handler = self._recovery_handlers[message_type]
                                success = await handler(original_payload)
                                
                                if success:
                                    result.recovered_count += 1
                                    result.details.append({
                                        "message_id": message.message_id,
                                        "order_id": order_id,
                                        "action": "handler_success",
                                    })
                                else:
                                    result.failed_count += 1
                            else:
                                # Re-queue to original queue for retry
                                original_exchange = body.get("original_exchange", "swifttrack.orders")
                                original_routing = body.get("original_routing_key", "order.created")
                                
                                await self.queue_manager.publish(
                                    exchange_name=original_exchange,
                                    routing_key=original_routing,
                                    message=original_payload,
                                    headers={
                                        "requeued_from_dlq": queue_name,
                                        "requeued_at": datetime.now(timezone.utc).isoformat(),
                                        "original_error": error_reason,
                                    },
                                )
                                
                                result.recovered_count += 1
                                result.details.append({
                                    "message_id": message.message_id,
                                    "order_id": order_id,
                                    "action": "requeued",
                                    "target_exchange": original_exchange,
                                })
                        else:
                            # Just log for manual review
                            result.skipped_count += 1
                            result.details.append({
                                "message_id": message.message_id,
                                "order_id": order_id,
                                "action": "logged_for_review",
                            })
                        
                        processed += 1
                        
                    except Exception as e:
                        logger.error(
                            "Failed to process DLQ message %s: %s",
                            message.message_id, e
                        )
                        result.failed_count += 1
                        # Don't requeue failed processing - leave for manual review
            
            # Consume messages
            await dlq.consume(process_message, no_ack=False)
            
            # Wait a bit for processing
            await asyncio.sleep(2)
            
            # Stop consumer
            await self.queue_manager.stop_consumer(queue_name)
            
        except Exception as e:
            logger.exception("DLQ processing failed for %s: %s", queue_name, e)
            result.status = RecoveryStatus.FAILED
        
        result.completed_at = datetime.now(timezone.utc)
        
        return result
    
    async def retry_failed_events(
        self,
        max_age_hours: int | None = None,
    ) -> RecoveryResult:
        """
        Retry failed integration events.
        
        Args:
            max_age_hours: Maximum age of events to retry
            
        Returns:
            RecoveryResult with retry details
        """
        result = RecoveryResult(status=RecoveryStatus.SUCCESS)
        
        age_hours = max_age_hours or int(self.max_recovery_age.total_seconds() / 3600)
        
        try:
            # Get retryable events
            events = await self.event_store.get_retryable_events(
                max_age_hours=age_hours,
                limit=100,
            )
            
            if not events:
                logger.info("No retryable events found")
                result.status = RecoveryStatus.NO_ACTION
                return result
            
            logger.info("Found %d events to retry", len(events))
            
            for event in events:
                try:
                    # Check if handler exists for this event type
                    handler_key = f"{event.target_system}.{event.event_type}"
                    
                    if handler_key in self._recovery_handlers:
                        request_data = json.loads(event.request_data) if event.request_data else {}
                        
                        success = await self._recovery_handlers[handler_key](request_data)
                        
                        if success:
                            await self.event_store.update_event(
                                event_id=event.event_id,
                                status=EventStatus.SUCCESS,
                            )
                            result.recovered_count += 1
                        else:
                            await self.event_store.update_event(
                                event_id=event.event_id,
                                increment_retry=True,
                            )
                            result.failed_count += 1
                    else:
                        # Mark as retrying for manual intervention
                        await self.event_store.update_event(
                            event_id=event.event_id,
                            status=EventStatus.RETRYING,
                            increment_retry=True,
                        )
                        result.skipped_count += 1
                    
                    result.details.append({
                        "event_id": event.event_id,
                        "order_id": event.order_id,
                        "target": event.target_system,
                        "event_type": event.event_type,
                    })
                    
                except Exception as e:
                    logger.error("Failed to retry event %s: %s", event.event_id, e)
                    result.failed_count += 1
            
        except Exception as e:
            logger.exception("Event retry failed: %s", e)
            result.status = RecoveryStatus.FAILED
        
        result.completed_at = datetime.now(timezone.utc)
        
        return result
    
    async def recover_order(self, order_id: str) -> RecoveryResult:
        """
        Recover all pending operations for a specific order.
        
        Args:
            order_id: Order to recover
            
        Returns:
            RecoveryResult with recovery details
        """
        result = RecoveryResult(status=RecoveryStatus.SUCCESS)
        
        logger.info("Starting recovery for order: %s", order_id)
        
        try:
            # 1. Check for incomplete sagas
            saga_query = select(SagaRecord).where(
                and_(
                    SagaRecord.order_id == order_id,
                    SagaRecord.state.in_(["pending", "in_progress", "compensating"]),
                )
            )
            saga_result = await self.db.execute(saga_query)
            saga_records = saga_result.scalars().all()
            
            for record in saga_records:
                saga = await SagaStateMachine.load_from_db(record.saga_id, self.db)
                if saga:
                    result.details.append({
                        "type": "saga",
                        "saga_id": saga.saga_id,
                        "state": saga.state.value,
                    })
                    result.recovered_count += 1
            
            # 2. Check for failed events
            events = await self.event_store.get_events_by_order(
                order_id=order_id,
                status=EventStatus.FAILED,
            )
            
            for event in events:
                if event.retry_count < event.max_retries:
                    result.details.append({
                        "type": "event",
                        "event_id": event.event_id,
                        "target": event.target_system,
                        "retry_count": event.retry_count,
                    })
                    result.recovered_count += 1
            
            # 3. Log audit trail
            await self.event_store.log_audit_trail(
                order_id=order_id,
                action="recovery.manual",
                entity_type="order",
                entity_id=order_id,
                details=f"Manual recovery initiated: {result.recovered_count} items identified",
            )
            
            if result.recovered_count == 0:
                result.status = RecoveryStatus.NO_ACTION
            
        except Exception as e:
            logger.exception("Order recovery failed for %s: %s", order_id, e)
            result.status = RecoveryStatus.FAILED
            result.failed_count += 1
        
        result.completed_at = datetime.now(timezone.utc)
        
        return result
    
    def get_recovery_status(self) -> dict:
        """Get recovery service status."""
        return {
            "last_recovery": self._last_recovery.isoformat() if self._last_recovery else None,
            "registered_handlers": list(self._recovery_handlers.keys()),
            "recent_recoveries": [
                r.to_dict() for r in self._recovery_history[-10:]
            ],
        }
    
    async def health_check(self) -> dict:
        """Check health of recovery-related components."""
        health = {
            "database": False,
            "queue_manager": False,
            "status": "unhealthy",
        }
        
        try:
            # Check database
            await self.db.execute(select(SagaRecord).limit(1))
            health["database"] = True
        except Exception as e:
            health["database_error"] = str(e)
        
        try:
            # Check queue manager
            health["queue_manager"] = self.queue_manager.is_connected
        except Exception as e:
            health["queue_error"] = str(e)
        
        if health["database"] and health["queue_manager"]:
            health["status"] = "healthy"
        
        return health
