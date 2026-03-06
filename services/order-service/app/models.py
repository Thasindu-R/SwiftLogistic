"""
Order Service – SQLAlchemy ORM models.
"""

from sqlalchemy import Column, DateTime, Float, Integer, String, Text, func

from shared.common.database import Base


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String(36), unique=True, nullable=False, index=True)
    client_id = Column(Integer, nullable=False, index=True)
    assigned_driver_id = Column(Integer, nullable=True, index=True)
    status = Column(String(30), nullable=False, default="pending")
    pickup_address = Column(Text, nullable=False)
    delivery_address = Column(Text, nullable=False)
    package_description = Column(Text, nullable=False, default="")
    package_weight = Column(Float, nullable=False, default=0.0)
    priority = Column(String(10), nullable=False, default="normal")
    recipient_name = Column(String(100), nullable=False, default="")
    recipient_phone = Column(String(20), nullable=False, default="")
    estimated_cost = Column(Float, nullable=True)
    assignment_type = Column(String(20), nullable=True)  # 'auto' or 'manual'
    notes = Column(Text, nullable=False, default="")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
