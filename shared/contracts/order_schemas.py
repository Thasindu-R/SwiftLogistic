"""Pydantic schemas for order-related API requests / responses."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# ── Request schemas ──────────────────────────────────────────
class OrderCreate(BaseModel):
    pickup_address: str = Field(..., min_length=5)
    delivery_address: str = Field(..., min_length=5)
    package_description: str = Field(default="")
    package_weight: float = Field(default=1.0, gt=0)
    priority: str = Field(default="normal", pattern="^(normal|high|urgent)$")
    recipient_name: str = Field(..., min_length=1)
    recipient_phone: str = Field(..., min_length=9)
    notes: str = Field(default="")


class OrderStatusUpdate(BaseModel):
    status: str = Field(
        ...,
        pattern="^(pending|confirmed|processing|in_warehouse|in_transit|out_for_delivery|delivered|failed|cancelled)$",
    )
    reason: Optional[str] = None


class OrderAssignDriver(BaseModel):
    driver_id: int


# ── Response schemas ─────────────────────────────────────────
class OrderResponse(BaseModel):
    order_id: str
    client_id: int
    assigned_driver_id: Optional[int] = None
    status: str
    pickup_address: str
    delivery_address: str
    package_description: str
    package_weight: float
    priority: str
    recipient_name: str
    recipient_phone: str
    estimated_cost: Optional[float] = None
    notes: str = ""
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class OrderListResponse(BaseModel):
    orders: list[OrderResponse]
    total: int


class OrderStatsResponse(BaseModel):
    total_orders: int
    pending: int
    confirmed: int
    processing: int
    in_transit: int
    delivered: int
    failed: int
    cancelled: int


# ── RabbitMQ event payload ───────────────────────────────────
class OrderEvent(BaseModel):
    """Canonical event published to the order exchange."""
    event: str  # e.g. "order.created", "order.confirmed"
    order_id: str
    client_id: int
    status: str
    pickup_address: str
    delivery_address: str
    package_description: str = ""
    package_weight: float = 0.0
    priority: str = "normal"
    recipient_name: str = ""
    recipient_phone: str = ""
    assigned_driver_id: Optional[int] = None
    timestamp: str = ""
