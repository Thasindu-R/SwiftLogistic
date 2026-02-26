"""
Order Service – route handlers for order CRUD, assignment, and stats.
"""

import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import case, func as sqlfunc, select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.common.config import settings
from shared.common.database import get_db
from shared.common.rabbitmq import rabbitmq_client
from shared.common.security import get_current_user, require_role
from shared.contracts.order_schemas import (
    OrderAssignDriver,
    OrderCreate,
    OrderListResponse,
    OrderResponse,
    OrderStatsResponse,
    OrderStatusUpdate,
)

from .models import Order
from .saga import OrderSaga

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/orders", tags=["Orders"])


# ── Create order (client / admin) ────────────────────────────
@router.post("/", response_model=OrderResponse, status_code=201)
async def create_order(
    payload: OrderCreate,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new delivery order and trigger the saga."""
    if current_user["role"] not in ("client", "admin"):
        raise HTTPException(status_code=403, detail="Only clients or admins can create orders")

    order_uuid = str(uuid.uuid4())
    estimated_cost = round(payload.package_weight * 150.0, 2)

    order = Order(
        order_id=order_uuid,
        client_id=int(current_user["sub"]),
        status="pending",
        pickup_address=payload.pickup_address,
        delivery_address=payload.delivery_address,
        package_description=payload.package_description,
        package_weight=payload.package_weight,
        priority=payload.priority,
        recipient_name=payload.recipient_name,
        recipient_phone=payload.recipient_phone,
        estimated_cost=estimated_cost,
        notes=payload.notes,
    )
    db.add(order)
    await db.flush()
    await db.refresh(order)

    # Trigger the order saga asynchronously (publishes to RabbitMQ)
    try:
        saga = OrderSaga(
            {
                "order_id": order_uuid,
                "client_id": int(current_user["sub"]),
                "pickup_address": payload.pickup_address,
                "delivery_address": payload.delivery_address,
                "package_description": payload.package_description,
                "package_weight": payload.package_weight,
                "priority": payload.priority,
                "recipient_name": payload.recipient_name,
                "recipient_phone": payload.recipient_phone,
            }
        )
        await saga.execute()
        order.status = "processing"
        await db.flush()
        await db.refresh(order)
        logger.info("Order saga started for %s", order_uuid)
    except Exception as e:
        logger.error("Saga failed to start for %s: %s", order_uuid, e)

    return OrderResponse.model_validate(order)


# ── List orders (role-filtered) ──────────────────────────────
@router.get("/", response_model=OrderListResponse)
async def list_orders(
    status_filter: str = Query(None, alias="status"),
    priority: str = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    List orders. Clients see only their own orders.
    Drivers see orders assigned to them. Admins see all.
    """
    query = select(Order)
    count_query = select(sqlfunc.count(Order.id))

    role = current_user["role"]
    user_id = int(current_user["sub"])

    if role == "client":
        query = query.where(Order.client_id == user_id)
        count_query = count_query.where(Order.client_id == user_id)
    elif role == "driver":
        query = query.where(Order.assigned_driver_id == user_id)
        count_query = count_query.where(Order.assigned_driver_id == user_id)

    if status_filter:
        query = query.where(Order.status == status_filter)
        count_query = count_query.where(Order.status == status_filter)
    if priority:
        query = query.where(Order.priority == priority)
        count_query = count_query.where(Order.priority == priority)

    total = (await db.execute(count_query)).scalar() or 0
    result = await db.execute(
        query.order_by(Order.created_at.desc()).offset(skip).limit(limit)
    )
    orders = result.scalars().all()

    return OrderListResponse(
        orders=[OrderResponse.model_validate(o) for o in orders],
        total=total,
    )


# ── Get single order ─────────────────────────────────────────
@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(
    order_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a single order by its UUID."""
    result = await db.execute(select(Order).where(Order.order_id == order_id))
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    role = current_user["role"]
    user_id = int(current_user["sub"])
    if role == "client" and order.client_id != user_id:
        raise HTTPException(status_code=403, detail="Not your order")
    if role == "driver" and order.assigned_driver_id != user_id:
        raise HTTPException(status_code=403, detail="Order not assigned to you")

    return OrderResponse.model_validate(order)


# ── Update order status ──────────────────────────────────────
@router.patch("/{order_id}/status", response_model=OrderResponse)
async def update_order_status(
    order_id: str,
    payload: OrderStatusUpdate,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update order status. Drivers can update assigned orders, admins can update any."""
    result = await db.execute(select(Order).where(Order.order_id == order_id))
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    role = current_user["role"]
    user_id = int(current_user["sub"])

    if role == "driver" and order.assigned_driver_id != user_id:
        raise HTTPException(status_code=403, detail="Order not assigned to you")
    if role == "client":
        if payload.status != "cancelled" or order.status != "pending":
            raise HTTPException(status_code=403, detail="Clients can only cancel pending orders")

    order.status = payload.status
    order.updated_at = datetime.now(timezone.utc)
    await db.flush()
    await db.refresh(order)

    # Publish tracking event for status change
    try:
        await rabbitmq_client.publish(
            settings.TRACKING_EXCHANGE,
            settings.TRACKING_UPDATE_KEY,
            {
                "event": "tracking.update",
                "order_id": order_id,
                "event_type": f"status_{payload.status}",
                "description": f"Order status changed to {payload.status}" + (f": {payload.reason}" if payload.reason else ""),
                "location": "System",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception as e:
        logger.error("Failed to publish status tracking event: %s", e)

    logger.info("Order %s status updated to %s", order_id, payload.status)
    return OrderResponse.model_validate(order)


# ── Assign driver (admin only) ───────────────────────────────
@router.patch("/{order_id}/assign", response_model=OrderResponse)
async def assign_driver(
    order_id: str,
    payload: OrderAssignDriver,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin-only: assign a driver to an order."""
    result = await db.execute(select(Order).where(Order.order_id == order_id))
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    order.assigned_driver_id = payload.driver_id
    if order.status in ("pending", "processing", "confirmed"):
        order.status = "confirmed"
    order.updated_at = datetime.now(timezone.utc)
    await db.flush()
    await db.refresh(order)

    # Publish tracking event
    try:
        await rabbitmq_client.publish(
            settings.TRACKING_EXCHANGE,
            settings.TRACKING_UPDATE_KEY,
            {
                "event": "tracking.update",
                "order_id": order_id,
                "event_type": "driver_assigned",
                "description": f"Driver {payload.driver_id} assigned to order",
                "location": "System",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception as e:
        logger.error("Failed to publish assignment tracking event: %s", e)

    logger.info("Order %s assigned to driver %d", order_id, payload.driver_id)
    return OrderResponse.model_validate(order)


# ── Order stats (admin only) ─────────────────────────────────
@router.get("/stats/summary", response_model=OrderStatsResponse)
async def order_stats(
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin-only: get aggregate order statistics."""
    result = await db.execute(
        select(
            sqlfunc.count(Order.id).label("total"),
            sqlfunc.count(case((Order.status == "pending", 1))).label("pending"),
            sqlfunc.count(case((Order.status == "confirmed", 1))).label("confirmed"),
            sqlfunc.count(case((Order.status == "processing", 1))).label("processing"),
            sqlfunc.count(case((Order.status == "in_transit", 1))).label("in_transit"),
            sqlfunc.count(case((Order.status == "delivered", 1))).label("delivered"),
            sqlfunc.count(case((Order.status == "failed", 1))).label("failed"),
            sqlfunc.count(case((Order.status == "cancelled", 1))).label("cancelled"),
        )
    )
    row = result.one()
    return OrderStatsResponse(
        total_orders=row.total,
        pending=row.pending,
        confirmed=row.confirmed,
        processing=row.processing,
        in_transit=row.in_transit,
        delivered=row.delivered,
        failed=row.failed,
        cancelled=row.cancelled,
    )


# ── Health ───────────────────────────────────────────────────
@router.get("/health/check")
async def health():
    return {"message": "order-service is healthy"}
