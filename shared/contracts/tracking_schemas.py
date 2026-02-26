"""Pydantic schemas for real-time tracking events."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class TrackingEventCreate(BaseModel):
    order_id: str
    event_type: str = Field(...)
    description: str = Field(default="")
    location: str = Field(default="")
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    driver_id: Optional[int] = None


class TrackingEventResponse(BaseModel):
    id: int
    order_id: str
    event_type: str
    description: str
    location: str
    latitude: Optional[float]
    longitude: Optional[float]
    driver_id: Optional[int]
    timestamp: datetime

    class Config:
        from_attributes = True


class TrackingHistory(BaseModel):
    order_id: str
    events: list[TrackingEventResponse]


# ── Delivery manifest (driver's daily schedule) ─────────────
class ManifestCreate(BaseModel):
    driver_id: int
    date: str = Field(...)
    order_ids: list[str]


class DeliveryItemUpdate(BaseModel):
    status: str = Field(..., pattern="^(pending|picked_up|in_transit|delivered|failed)$")
    proof_of_delivery: Optional[str] = None  # base64 image
    signature_data: Optional[str] = None  # base64 signature
    failure_reason: Optional[str] = None
    notes: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class ManifestResponse(BaseModel):
    manifest_id: str
    driver_id: int
    date: str
    status: str
    route_data: Optional[str] = None
    items: list[dict] = []

    class Config:
        from_attributes = True


# ── RabbitMQ tracking event payload ──────────────────────────
class TrackingUpdateEvent(BaseModel):
    event: str  # "tracking.update"
    order_id: str
    event_type: str
    description: str = ""
    location: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    driver_id: Optional[int] = None
    timestamp: str = ""


# ── Integration event schemas ────────────────────────────────
class IntegrationEventResponse(BaseModel):
    id: int
    event_id: str
    order_id: Optional[str]
    source_system: str
    target_system: str
    event_type: str
    status: str
    request_data: Optional[str]
    response_data: Optional[str]
    error_message: Optional[str]
    retry_count: int
    max_retries: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class IntegrationEventList(BaseModel):
    events: list[IntegrationEventResponse]
    total: int


class NotificationResponse(BaseModel):
    id: int
    user_id: int
    title: str
    message: str
    type: str
    is_read: bool
    order_id: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True
