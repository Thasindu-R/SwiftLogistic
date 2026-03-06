"""
Tracking Service – SQLAlchemy ORM models.
"""

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    func,
)

from shared.common.database import Base


class TrackingEvent(Base):
    __tablename__ = "tracking_events"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String(36), nullable=False, index=True)
    event_type = Column(String(50), nullable=False)
    description = Column(Text, nullable=False, default="")
    location = Column(String(200), nullable=False, default="")
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    driver_id = Column(Integer, nullable=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class DeliveryManifest(Base):
    __tablename__ = "delivery_manifests"

    id = Column(Integer, primary_key=True, index=True)
    manifest_id = Column(String(36), unique=True, nullable=False, index=True)
    driver_id = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    status = Column(String(20), nullable=False, default="pending")
    route_data = Column(Text, nullable=True)  # JSON from ROS
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class DeliveryItem(Base):
    __tablename__ = "delivery_items"

    id = Column(Integer, primary_key=True, index=True)
    manifest_id = Column(String(36), nullable=False, index=True)
    order_id = Column(String(36), nullable=False, index=True)
    sequence = Column(Integer, nullable=False, default=0)
    status = Column(String(20), nullable=False, default="pending")
    proof_of_delivery = Column(Text, nullable=True)
    signature_data = Column(Text, nullable=True)
    failure_reason = Column(String(200), nullable=True)
    notes = Column(Text, nullable=False, default="")
    delivered_at = Column(DateTime(timezone=True), nullable=True)


# Lightweight read-only model for order lookup (shared DB)
class _Order(Base):
    __tablename__ = "orders"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    order_id = Column(String(36))
    client_id = Column(Integer)
    assigned_driver_id = Column(Integer)
    status = Column(String(20))
    updated_at = Column(DateTime(timezone=True))
