"""
Event Store – Integration event logging and audit trail.

Features:
- Store all integration events in database
- Maintain complete audit trail
- Query events by order, system, status, time
- Support for event replay and debugging

Usage:
    event_store = IntegrationEventStore(db_session)
    
    # Log an integration event
    event = await event_store.log_event(
        source="order-service",
        target="cms",
        event_type="client.validate",
        order_id="ORD-001",
        request_data={"client_id": 1},
        status=EventStatus.SUCCESS,
        response_data={"valid": True}
    )
    
    # Query events
    events = await event_store.get_events_by_order("ORD-001")
    audit_trail = await event_store.get_audit_trail("ORD-001")
"""

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Optional

from sqlalchemy import Column, DateTime, Integer, String, Text, func, select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from shared.common.database import Base

logger = logging.getLogger(__name__)


class EventStatus(Enum):
    """Integration event status."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    COMPENSATED = "compensated"


class EventSeverity(Enum):
    """Event severity level."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# Database model
class IntegrationEventLog(Base):
    """Persistent integration event log."""
    __tablename__ = "integration_event_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String(36), unique=True, nullable=False, index=True)
    correlation_id = Column(String(36), nullable=True, index=True)
    order_id = Column(String(36), nullable=True, index=True)
    saga_id = Column(String(36), nullable=True, index=True)
    
    source_system = Column(String(50), nullable=False, index=True)
    target_system = Column(String(50), nullable=False, index=True)
    event_type = Column(String(100), nullable=False, index=True)
    
    status = Column(String(20), nullable=False, default="pending", index=True)
    severity = Column(String(20), nullable=False, default="info")
    
    request_data = Column(Text, nullable=True)
    response_data = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    error_code = Column(String(50), nullable=True)
    
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    
    duration_ms = Column(Integer, nullable=True)
    
    metadata_json = Column(Text, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class AuditTrailLog(Base):
    """Audit trail for order lifecycle."""
    __tablename__ = "audit_trail_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    trail_id = Column(String(36), unique=True, nullable=False, index=True)
    order_id = Column(String(36), nullable=False, index=True)
    
    actor_type = Column(String(30), nullable=False)  # system, user, admin, driver
    actor_id = Column(String(50), nullable=True)
    actor_name = Column(String(100), nullable=True)
    
    action = Column(String(100), nullable=False)
    entity_type = Column(String(50), nullable=False)  # order, tracking, billing, route
    entity_id = Column(String(36), nullable=True)
    
    old_value = Column(Text, nullable=True)
    new_value = Column(Text, nullable=True)
    
    details = Column(Text, nullable=True)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(String(255), nullable=True)
    
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


@dataclass
class AuditTrailEntry:
    """Audit trail entry data class."""
    trail_id: str
    order_id: str
    actor_type: str
    actor_id: str | None
    actor_name: str | None
    action: str
    entity_type: str
    entity_id: str | None
    old_value: Any
    new_value: Any
    details: str | None
    timestamp: datetime
    
    def to_dict(self) -> dict:
        return {
            "trail_id": self.trail_id,
            "order_id": self.order_id,
            "actor_type": self.actor_type,
            "actor_id": self.actor_id,
            "actor_name": self.actor_name,
            "action": self.action,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


class IntegrationEventStore:
    """
    Manages integration event logging and audit trail.
    
    Provides:
    - Event persistence with full context
    - Status tracking and history
    - Audit trail for compliance
    - Query capabilities for debugging
    """
    
    def __init__(self, db: AsyncSession):
        """Initialize event store with database session."""
        self.db = db
    
    async def log_event(
        self,
        source: str,
        target: str,
        event_type: str,
        status: EventStatus = EventStatus.PENDING,
        order_id: str | None = None,
        saga_id: str | None = None,
        correlation_id: str | None = None,
        request_data: dict | None = None,
        response_data: dict | None = None,
        error_message: str | None = None,
        error_code: str | None = None,
        severity: EventSeverity = EventSeverity.INFO,
        duration_ms: int | None = None,
        metadata: dict | None = None,
    ) -> IntegrationEventLog:
        """
        Log an integration event.
        
        Args:
            source: Source system (e.g., "order-service")
            target: Target system (e.g., "cms", "ros", "wms")
            event_type: Event type (e.g., "client.validate", "route.optimize")
            status: Event status
            order_id: Associated order ID
            saga_id: Associated saga ID
            correlation_id: Correlation ID for tracing
            request_data: Request payload
            response_data: Response payload
            error_message: Error message if failed
            error_code: Error code if failed
            severity: Event severity
            duration_ms: Operation duration in milliseconds
            metadata: Additional metadata
            
        Returns:
            Created event log record
        """
        event = IntegrationEventLog(
            event_id=str(uuid.uuid4()),
            correlation_id=correlation_id or str(uuid.uuid4()),
            order_id=order_id,
            saga_id=saga_id,
            source_system=source,
            target_system=target,
            event_type=event_type,
            status=status.value,
            severity=severity.value,
            request_data=json.dumps(request_data) if request_data else None,
            response_data=json.dumps(response_data) if response_data else None,
            error_message=error_message,
            error_code=error_code,
            duration_ms=duration_ms,
            metadata_json=json.dumps(metadata) if metadata else None,
        )
        
        self.db.add(event)
        await self.db.commit()
        await self.db.refresh(event)
        
        logger.info(
            "Logged event: %s → %s [%s] status=%s order=%s",
            source, target, event_type, status.value, order_id
        )
        
        return event
    
    async def update_event(
        self,
        event_id: str,
        status: EventStatus | None = None,
        response_data: dict | None = None,
        error_message: str | None = None,
        error_code: str | None = None,
        duration_ms: int | None = None,
        increment_retry: bool = False,
    ) -> Optional[IntegrationEventLog]:
        """
        Update an existing event.
        
        Args:
            event_id: Event ID to update
            status: New status
            response_data: Response data
            error_message: Error message
            error_code: Error code
            duration_ms: Duration
            increment_retry: Increment retry count
            
        Returns:
            Updated event or None if not found
        """
        result = await self.db.execute(
            select(IntegrationEventLog).where(IntegrationEventLog.event_id == event_id)
        )
        event = result.scalar_one_or_none()
        
        if not event:
            logger.warning("Event not found: %s", event_id)
            return None
        
        if status:
            event.status = status.value
        if response_data:
            event.response_data = json.dumps(response_data)
        if error_message:
            event.error_message = error_message
        if error_code:
            event.error_code = error_code
        if duration_ms is not None:
            event.duration_ms = duration_ms
        if increment_retry:
            event.retry_count += 1
        
        event.updated_at = datetime.now(timezone.utc)
        
        await self.db.commit()
        
        logger.debug("Updated event %s: status=%s", event_id, event.status)
        
        return event
    
    async def get_event(self, event_id: str) -> Optional[IntegrationEventLog]:
        """Get event by ID."""
        result = await self.db.execute(
            select(IntegrationEventLog).where(IntegrationEventLog.event_id == event_id)
        )
        return result.scalar_one_or_none()
    
    async def get_events_by_order(
        self,
        order_id: str,
        status: EventStatus | None = None,
        limit: int = 100,
    ) -> list[IntegrationEventLog]:
        """Get all events for an order."""
        query = select(IntegrationEventLog).where(
            IntegrationEventLog.order_id == order_id
        )
        
        if status:
            query = query.where(IntegrationEventLog.status == status.value)
        
        query = query.order_by(IntegrationEventLog.created_at.desc()).limit(limit)
        
        result = await self.db.execute(query)
        return list(result.scalars().all())
    
    async def get_events_by_status(
        self,
        status: EventStatus,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[IntegrationEventLog]:
        """Get events by status."""
        query = select(IntegrationEventLog).where(
            IntegrationEventLog.status == status.value
        )
        
        if since:
            query = query.where(IntegrationEventLog.created_at >= since)
        
        query = query.order_by(IntegrationEventLog.created_at.desc()).limit(limit)
        
        result = await self.db.execute(query)
        return list(result.scalars().all())
    
    async def get_failed_events(
        self,
        since: datetime | None = None,
        target_system: str | None = None,
        limit: int = 100,
    ) -> list[IntegrationEventLog]:
        """Get failed events for retry/analysis."""
        conditions = [IntegrationEventLog.status == EventStatus.FAILED.value]
        
        if since:
            conditions.append(IntegrationEventLog.created_at >= since)
        if target_system:
            conditions.append(IntegrationEventLog.target_system == target_system)
        
        query = select(IntegrationEventLog).where(
            and_(*conditions)
        ).order_by(IntegrationEventLog.created_at.desc()).limit(limit)
        
        result = await self.db.execute(query)
        return list(result.scalars().all())
    
    async def get_retryable_events(
        self,
        max_age_hours: int = 24,
        limit: int = 100,
    ) -> list[IntegrationEventLog]:
        """Get events that can still be retried."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        
        query = select(IntegrationEventLog).where(
            and_(
                IntegrationEventLog.status.in_([
                    EventStatus.FAILED.value,
                    EventStatus.RETRYING.value
                ]),
                IntegrationEventLog.created_at >= cutoff,
                IntegrationEventLog.retry_count < IntegrationEventLog.max_retries,
            )
        ).order_by(IntegrationEventLog.created_at).limit(limit)
        
        result = await self.db.execute(query)
        return list(result.scalars().all())
    
    async def log_audit_trail(
        self,
        order_id: str,
        action: str,
        entity_type: str,
        actor_type: str = "system",
        actor_id: str | None = None,
        actor_name: str | None = None,
        entity_id: str | None = None,
        old_value: Any = None,
        new_value: Any = None,
        details: str | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> AuditTrailLog:
        """
        Log an audit trail entry.
        
        Args:
            order_id: Order ID
            action: Action performed (e.g., "create", "update", "status_change")
            entity_type: Entity type (e.g., "order", "tracking", "route")
            actor_type: Actor type (system, user, admin, driver)
            actor_id: Actor identifier
            actor_name: Actor display name
            entity_id: Entity identifier
            old_value: Previous value
            new_value: New value
            details: Additional details
            ip_address: Client IP address
            user_agent: Client user agent
            
        Returns:
            Created audit trail record
        """
        trail = AuditTrailLog(
            trail_id=str(uuid.uuid4()),
            order_id=order_id,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            old_value=json.dumps(old_value) if old_value and not isinstance(old_value, str) else old_value,
            new_value=json.dumps(new_value) if new_value and not isinstance(new_value, str) else new_value,
            details=details,
            ip_address=ip_address,
            user_agent=user_agent,
        )
        
        self.db.add(trail)
        await self.db.commit()
        await self.db.refresh(trail)
        
        logger.debug(
            "Audit trail: %s [%s.%s] by %s",
            action, entity_type, entity_id, actor_type
        )
        
        return trail
    
    async def get_audit_trail(
        self,
        order_id: str,
        entity_type: str | None = None,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[AuditTrailEntry]:
        """
        Get audit trail for an order.
        
        Args:
            order_id: Order ID
            entity_type: Filter by entity type
            since: Filter by timestamp
            limit: Maximum entries
            
        Returns:
            List of audit trail entries
        """
        query = select(AuditTrailLog).where(
            AuditTrailLog.order_id == order_id
        )
        
        if entity_type:
            query = query.where(AuditTrailLog.entity_type == entity_type)
        if since:
            query = query.where(AuditTrailLog.timestamp >= since)
        
        query = query.order_by(AuditTrailLog.timestamp.desc()).limit(limit)
        
        result = await self.db.execute(query)
        logs = result.scalars().all()
        
        return [
            AuditTrailEntry(
                trail_id=log.trail_id,
                order_id=log.order_id,
                actor_type=log.actor_type,
                actor_id=log.actor_id,
                actor_name=log.actor_name,
                action=log.action,
                entity_type=log.entity_type,
                entity_id=log.entity_id,
                old_value=json.loads(log.old_value) if log.old_value and log.old_value.startswith('{') else log.old_value,
                new_value=json.loads(log.new_value) if log.new_value and log.new_value.startswith('{') else log.new_value,
                details=log.details,
                timestamp=log.timestamp,
            )
            for log in logs
        ]
    
    async def get_event_statistics(
        self,
        since: datetime | None = None,
        target_system: str | None = None,
    ) -> dict:
        """
        Get event statistics for monitoring.
        
        Returns:
            Statistics dict with counts by status, system, etc.
        """
        conditions = []
        if since:
            conditions.append(IntegrationEventLog.created_at >= since)
        if target_system:
            conditions.append(IntegrationEventLog.target_system == target_system)
        
        # Get all events matching conditions
        query = select(IntegrationEventLog)
        if conditions:
            query = query.where(and_(*conditions))
        
        result = await self.db.execute(query)
        events = result.scalars().all()
        
        # Calculate statistics
        stats = {
            "total": len(events),
            "by_status": {},
            "by_target": {},
            "by_source": {},
            "avg_duration_ms": 0,
            "retry_rate": 0,
        }
        
        duration_sum = 0
        duration_count = 0
        retry_count = 0
        
        for event in events:
            # By status
            stats["by_status"][event.status] = stats["by_status"].get(event.status, 0) + 1
            
            # By target
            stats["by_target"][event.target_system] = stats["by_target"].get(event.target_system, 0) + 1
            
            # By source
            stats["by_source"][event.source_system] = stats["by_source"].get(event.source_system, 0) + 1
            
            # Duration
            if event.duration_ms:
                duration_sum += event.duration_ms
                duration_count += 1
            
            # Retry count
            if event.retry_count > 0:
                retry_count += 1
        
        if duration_count > 0:
            stats["avg_duration_ms"] = duration_sum / duration_count
        
        if stats["total"] > 0:
            stats["retry_rate"] = retry_count / stats["total"]
        
        return stats


# Context manager for event tracking
class EventTracker:
    """
    Context manager for tracking integration operations.
    
    Usage:
        async with EventTracker(event_store, "order-service", "cms", "validate") as tracker:
            tracker.set_order_id("ORD-001")
            tracker.set_request({"client_id": 1})
            
            result = await cms_client.validate(client_id)
            
            tracker.set_response(result)
            tracker.set_status(EventStatus.SUCCESS)
    """
    
    def __init__(
        self,
        event_store: IntegrationEventStore,
        source: str,
        target: str,
        event_type: str,
        order_id: str | None = None,
        saga_id: str | None = None,
    ):
        self.event_store = event_store
        self.source = source
        self.target = target
        self.event_type = event_type
        self.order_id = order_id
        self.saga_id = saga_id
        
        self._request_data: dict | None = None
        self._response_data: dict | None = None
        self._status = EventStatus.PENDING
        self._error_message: str | None = None
        self._error_code: str | None = None
        self._start_time: datetime | None = None
        self._event: IntegrationEventLog | None = None
    
    def set_order_id(self, order_id: str) -> None:
        self.order_id = order_id
    
    def set_request(self, data: dict) -> None:
        self._request_data = data
    
    def set_response(self, data: dict) -> None:
        self._response_data = data
    
    def set_status(self, status: EventStatus) -> None:
        self._status = status
    
    def set_error(self, message: str, code: str | None = None) -> None:
        self._error_message = message
        self._error_code = code
        self._status = EventStatus.FAILED
    
    async def __aenter__(self) -> "EventTracker":
        self._start_time = datetime.now(timezone.utc)
        self._event = await self.event_store.log_event(
            source=self.source,
            target=self.target,
            event_type=self.event_type,
            order_id=self.order_id,
            saga_id=self.saga_id,
            status=EventStatus.IN_PROGRESS,
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        duration_ms = None
        if self._start_time:
            duration = datetime.now(timezone.utc) - self._start_time
            duration_ms = int(duration.total_seconds() * 1000)
        
        if exc_val:
            self._status = EventStatus.FAILED
            self._error_message = str(exc_val)
        
        if self._event:
            await self.event_store.update_event(
                event_id=self._event.event_id,
                status=self._status,
                response_data=self._response_data,
                error_message=self._error_message,
                error_code=self._error_code,
                duration_ms=duration_ms,
            )
