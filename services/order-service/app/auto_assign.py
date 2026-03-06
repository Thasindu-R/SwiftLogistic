"""
Auto-assign driver via ROS after saga completes.

Flow:
  1. Query available drivers from DB
  2. Count each driver's current active deliveries (load)
  3. Send candidates to ROS → ROS picks the best driver
  4. Assign driver to order + create delivery manifest
"""

import json
import logging
import os
import uuid
from datetime import date, datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Integer, String, select, func as sqlfunc
from sqlalchemy.ext.asyncio import AsyncSession

from shared.common.config import settings
from shared.common.database import Base
from shared.common.rabbitmq import rabbitmq_client
from shared.common.integrations.ros_client import ROSClient

from .models import Order

logger = logging.getLogger(__name__)


# Lightweight model to query the users table (shared DB)
class _User(Base):
    __tablename__ = "users"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    username = Column(String(50))
    full_name = Column(String(100))
    role = Column(String(20))
    is_active = Column(Boolean)


# Lightweight models for manifests (shared DB)
class _DeliveryManifest(Base):
    __tablename__ = "delivery_manifests"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    manifest_id = Column(String(36), unique=True)
    driver_id = Column(Integer)
    date = Column(DateTime)
    status = Column(String(20))
    route_data = Column(String)
    created_at = Column(DateTime(timezone=True))


class _DeliveryItem(Base):
    __tablename__ = "delivery_items"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    manifest_id = Column(String(36))
    order_id = Column(String(36))
    sequence = Column(Integer)
    status = Column(String(20))


async def auto_assign_driver(order: Order, db: AsyncSession) -> None:
    """Auto-assign the best available driver to the order via ROS."""

    ros_url = os.getenv("ROS_URL", "http://mock-ros:8005")

    # 1. Get all active drivers
    result = await db.execute(
        select(_User).where(_User.role == "driver", _User.is_active == True)
    )
    drivers = result.scalars().all()
    if not drivers:
        logger.warning("No available drivers for order %s", order.order_id)
        return

    # 2. Count current active deliveries per driver
    active_statuses = ("pending", "processing", "confirmed", "in_transit", "in_warehouse", "out_for_delivery")
    load_result = await db.execute(
        select(
            Order.assigned_driver_id,
            sqlfunc.count(Order.id).label("load"),
        )
        .where(Order.assigned_driver_id.isnot(None), Order.status.in_(active_statuses))
        .group_by(Order.assigned_driver_id)
    )
    load_map = {row.assigned_driver_id: row.load for row in load_result}

    # 3. Build candidate list for ROS
    candidates = [
        {
            "driver_id": d.id,
            "name": d.full_name or d.username,
            "current_load": load_map.get(d.id, 0),
        }
        for d in drivers
    ]

    # 4. Ask ROS to pick the best driver
    try:
        async with ROSClient(ros_url) as ros:
            assignment = await ros.assign_best_driver(
                order_id=order.order_id,
                delivery_address=order.delivery_address,
                drivers=candidates,
            )
    except Exception as e:
        logger.error("ROS driver assignment failed for %s: %s", order.order_id, e)
        return

    assigned_driver_id = assignment.get("assigned_driver_id")
    assigned_driver_name = assignment.get("assigned_driver_name", "")
    if not assigned_driver_id:
        logger.warning("ROS returned no driver for order %s", order.order_id)
        return

    # 5. Update order with assigned driver
    order.assigned_driver_id = assigned_driver_id
    order.assignment_type = "auto"
    order.status = "confirmed"
    order.updated_at = datetime.now(timezone.utc)
    await db.flush()

    # 6. Create delivery manifest
    manifest_id = str(uuid.uuid4())
    manifest = _DeliveryManifest(
        manifest_id=manifest_id,
        driver_id=assigned_driver_id,
        date=date.today(),
        status="pending",
        created_at=datetime.now(timezone.utc),
    )
    db.add(manifest)
    await db.flush()

    item = _DeliveryItem(
        manifest_id=manifest_id,
        order_id=order.order_id,
        sequence=1,
        status="pending",
    )
    db.add(item)
    await db.flush()

    # 7. Publish tracking event
    try:
        await rabbitmq_client.publish(
            settings.TRACKING_EXCHANGE,
            settings.TRACKING_UPDATE_KEY,
            {
                "event": "tracking.update",
                "order_id": order.order_id,
                "client_id": order.client_id,
                "driver_id": assigned_driver_id,
                "event_type": "driver_assigned",
                "description": f"Auto-assigned to driver {assigned_driver_name} by ROS",
                "location": "System (ROS)",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception as e:
        logger.error("Failed to publish auto-assign tracking event: %s", e)

    logger.info(
        "Auto-assigned driver %d (%s) to order %s, manifest %s",
        assigned_driver_id, assigned_driver_name, order.order_id, manifest_id,
    )
