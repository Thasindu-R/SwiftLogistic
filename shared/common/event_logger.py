"""
Integration event logger – records all middleware interactions
(CMS/ROS/WMS calls) for audit, monitoring, and retry support.
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, Integer, String, Text, func
from sqlalchemy.ext.asyncio import AsyncSession

from .database import Base

logger = logging.getLogger(__name__)


class IntegrationEvent(Base):
    """Persistent log of every middleware interaction."""
    __tablename__ = "integration_events"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String(36), unique=True, nullable=False, index=True)
    order_id = Column(String(36), nullable=True, index=True)
    source_system = Column(String(30), nullable=False)
    target_system = Column(String(30), nullable=False)
    event_type = Column(String(60), nullable=False)
    status = Column(String(20), nullable=False, default="pending")
    request_data = Column(Text, nullable=True)
    response_data = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Notification(Base):
    """User notification record."""
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    title = Column(String(200), nullable=False)
    message = Column(Text, nullable=False, default="")
    type = Column(String(30), nullable=False, default="info")
    is_read = Column(String(5), nullable=False, default="false")
    order_id = Column(String(36), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


async def log_integration_event(
    db: AsyncSession,
    *,
    source_system: str,
    target_system: str,
    event_type: str,
    order_id: str | None = None,
    request_data: dict | str | None = None,
    response_data: dict | str | None = None,
    status: str = "pending",
    error_message: str | None = None,
) -> IntegrationEvent:
    """Create an integration event log entry."""
    event = IntegrationEvent(
        event_id=str(uuid.uuid4()),
        order_id=order_id,
        source_system=source_system,
        target_system=target_system,
        event_type=event_type,
        status=status,
        request_data=json.dumps(request_data) if isinstance(request_data, dict) else request_data,
        response_data=json.dumps(response_data) if isinstance(response_data, dict) else response_data,
        error_message=error_message,
    )
    db.add(event)
    await db.commit()
    await db.refresh(event)
    logger.info(
        "Integration event logged: %s → %s [%s] status=%s order=%s",
        source_system, target_system, event_type, status, order_id,
    )
    return event


async def update_integration_event(
    db: AsyncSession,
    event: IntegrationEvent,
    *,
    status: str | None = None,
    response_data: dict | str | None = None,
    error_message: str | None = None,
    increment_retry: bool = False,
):
    """Update an existing integration event."""
    if status:
        event.status = status
    if response_data:
        event.response_data = json.dumps(response_data) if isinstance(response_data, dict) else response_data
    if error_message:
        event.error_message = error_message
    if increment_retry:
        event.retry_count += 1
    event.updated_at = datetime.now(timezone.utc)
    await db.commit()
