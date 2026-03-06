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
from .auto_assign import auto_assign_driver

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
            order_data={
                "order_id": order_uuid,
                "client_id": int(current_user["sub"]),
                "pickup_address": payload.pickup_address,
                "delivery_address": payload.delivery_address,
                "package_description": payload.package_description,
                "package_weight": payload.package_weight,
                "priority": payload.priority,
                "recipient_name": payload.recipient_name,
                "recipient_phone": payload.recipient_phone,
            },
            db=db,
        )
        saga_result = await saga.execute()
        order.status = "processing"
        await db.flush()
        await db.refresh(order)
        logger.info(
            "Order saga completed for %s – state=%s",
            order_uuid,
            saga_result.get("state", "unknown"),
        )

        # Auto-assign driver via ROS after saga completes
        if saga_result.get("state") == "completed":
            try:
                await auto_assign_driver(order, db)
            except Exception as assign_err:
                logger.error("Auto-assign failed for %s: %s", order_uuid, assign_err)
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
                "client_id": order.client_id,
                "driver_id": order.assigned_driver_id,
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
    order.assignment_type = "manual"
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
                "client_id": order.client_id,
                "driver_id": payload.driver_id,
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


# ══════════════════════════════════════════════════════════════
# INTEGRATION ENDPOINTS
# ══════════════════════════════════════════════════════════════

from shared.common.integrations import (
    IntegrationOrchestrator,
    create_orchestrator,
    CMSClient,
    ROSClient,
    WMSClient,
    DataTransformer,
)


@router.get("/integration/health", tags=["Integration"])
async def integration_health(
    _admin: dict = Depends(require_role("admin")),
):
    """Admin: Check health of all integration systems (CMS, ROS, WMS)."""
    orchestrator = create_orchestrator()
    return await orchestrator.health_check_all()


@router.post("/integration/cms/validate/{client_id}", tags=["Integration"])
async def validate_client_cms(
    client_id: int,
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Validate client in CMS via SOAP/XML.
    
    Demonstrates SOAP integration:
    - Converts request to SOAP XML envelope
    - Sends to CMS SOAP endpoint
    - Parses XML response back to JSON
    """
    orchestrator = create_orchestrator()
    try:
        result = await orchestrator.validate_client(client_id)
        return {
            "success": True,
            "protocol": "SOAP/XML",
            "operation": "ValidateClient",
            "result": result
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"CMS SOAP error: {str(e)}")


@router.get("/integration/cms/client/{client_id}", tags=["Integration"])
async def get_client_info_cms(
    client_id: int,
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Get client info from CMS via SOAP/XML.
    
    Demonstrates full SOAP request/response cycle.
    """
    orchestrator = create_orchestrator()
    try:
        result = await orchestrator.get_client_info(client_id)
        return {
            "success": True,
            "protocol": "SOAP/XML",
            "operation": "GetClientInfo",
            "client": result
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"CMS SOAP error: {str(e)}")


@router.post("/integration/ros/optimize", tags=["Integration"])
async def optimize_route_ros(
    order_id: str = Query(...),
    pickup_address: str = Query(...),
    delivery_address: str = Query(...),
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Request route optimization from ROS via REST/JSON.
    
    Demonstrates REST integration:
    - Sends JSON request to ROS REST endpoint
    - Receives optimized route in JSON format
    """
    orchestrator = create_orchestrator()
    try:
        route = await orchestrator.optimize_route(order_id, pickup_address, delivery_address)
        return {
            "success": True,
            "protocol": "REST/JSON",
            "operation": "RouteOptimization",
            "route": route
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"ROS API error: {str(e)}")


@router.post("/integration/wms/receive/{order_id}", tags=["Integration"])
async def receive_package_wms(
    order_id: str,
    weight: float = Query(1.0),
    description: str = Query("Package"),
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Register package in WMS via TCP/IP.
    
    Demonstrates TCP/IP integration:
    - Establishes TCP connection
    - Sends JSON-over-TCP command
    - Receives and parses proprietary response
    """
    orchestrator = create_orchestrator()
    async with WMSClient(orchestrator.wms_host, orchestrator.wms_port) as wms:
        try:
            result = await wms.receive_package(order_id, weight, description)
            return {
                "success": True,
                "protocol": "TCP/IP",
                "command": "RECEIVE_PACKAGE",
                "result": result
            }
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"WMS TCP error: {str(e)}")


@router.get("/integration/wms/status/{order_id}", tags=["Integration"])
async def check_wms_status(
    order_id: str,
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Check package status in WMS via TCP/IP.
    """
    orchestrator = create_orchestrator()
    async with WMSClient(orchestrator.wms_host, orchestrator.wms_port) as wms:
        try:
            result = await wms.check_status(order_id)
            return {
                "success": True,
                "protocol": "TCP/IP",
                "command": "CHECK_STATUS",
                "result": result
            }
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"WMS TCP error: {str(e)}")


@router.post("/integration/wms/load/{order_id}", tags=["Integration"])
async def load_vehicle_wms(
    order_id: str,
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Mark package as loaded onto vehicle in WMS via TCP/IP.
    """
    orchestrator = create_orchestrator()
    async with WMSClient(orchestrator.wms_host, orchestrator.wms_port) as wms:
        try:
            result = await wms.load_vehicle(order_id)
            return {
                "success": True,
                "protocol": "TCP/IP",
                "command": "LOAD_VEHICLE",
                "result": result
            }
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"WMS TCP error: {str(e)}")


@router.post("/integration/full-process/{order_id}", tags=["Integration"])
async def full_integration_process(
    order_id: str,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Run full integration processing for an existing order.
    
    Demonstrates complete middleware integration workflow:
    1. CMS: Validate client (SOAP/XML)
    2. WMS: Register package (TCP/IP)
    3. ROS: Optimize route (REST/JSON)
    
    With data transformations between formats.
    """
    # Get order from database
    result = await db.execute(
        select(Order).where(Order.order_id == order_id)
    )
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    order_data = {
        "order_id": order.order_id,
        "client_id": order.client_id,
        "pickup_address": order.pickup_address,
        "delivery_address": order.delivery_address,
        "package_description": order.package_description,
        "package_weight": order.package_weight,
        "priority": order.priority,
        "recipient_name": order.recipient_name,
        "recipient_phone": order.recipient_phone,
    }
    
    orchestrator = create_orchestrator()
    result = await orchestrator.process_new_order(order_data)
    
    return {
        "order_id": order_id,
        "integration_result": result,
        "transformations": {
            "json_to_soap_xml": "Applied for CMS communication",
            "json_to_tcp": "Applied for WMS communication",
            "rest_json": "Native format for ROS communication"
        }
    }


@router.post("/integration/transform/json-to-xml", tags=["Integration"])
async def transform_json_to_xml(
    data: dict,
    root_tag: str = Query("Order"),
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Transform JSON data to XML format.
    
    Demonstrates JSON → XML conversion for CMS integration.
    """
    transformer = DataTransformer()
    xml_output = transformer.json_to_xml(data, root_tag=root_tag)
    return {
        "input_format": "JSON",
        "output_format": "XML",
        "xml": xml_output
    }


@router.post("/integration/transform/xml-to-json", tags=["Integration"])
async def transform_xml_to_json(
    xml_content: str = Query(..., description="XML string to convert"),
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Transform XML data to JSON format.
    
    Demonstrates XML → JSON conversion from CMS responses.
    """
    transformer = DataTransformer()
    try:
        json_output = transformer.xml_to_json(xml_content)
        return {
            "input_format": "XML",
            "output_format": "JSON",
            "json": json_output
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"XML parsing error: {str(e)}")


# ══════════════════════════════════════════════════════════════
# Async Processing & Reliability Routes
# ══════════════════════════════════════════════════════════════

from shared.common.async_processor.saga_state import SagaStateMachine, SagaRecord, SagaStatusHistory
from shared.common.async_processor.event_store import IntegrationEventStore, IntegrationEventLog, AuditTrailLog, EventStatus
from shared.common.async_processor.retry_handler import retry_handler
from shared.common.async_processor.queue_manager import queue_manager


@router.get("/async/health", tags=["Async Processing"])
async def async_health_check(
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Check health of async processing components.
    
    Returns status of:
    - Message queue connection
    - Retry handler circuit breakers
    """
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "queue_manager": {
            "connected": queue_manager.is_connected,
        },
        "circuit_breakers": retry_handler.get_circuit_breaker_status(),
        "retry_stats": retry_handler.get_retry_stats(),
    }


@router.get("/async/sagas", tags=["Async Processing"])
async def list_sagas(
    state: str | None = Query(None, description="Filter by state"),
    order_id: str | None = Query(None, description="Filter by order ID"),
    limit: int = Query(50, le=100),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: List saga records (distributed transactions).
    
    Shows saga state, steps completed, and any errors.
    """
    query = select(SagaRecord)
    
    if state:
        query = query.where(SagaRecord.state == state)
    if order_id:
        query = query.where(SagaRecord.order_id == order_id)
    
    query = query.order_by(SagaRecord.started_at.desc()).limit(limit)
    
    result = await db.execute(query)
    sagas = result.scalars().all()
    
    return {
        "count": len(sagas),
        "sagas": [
            {
                "saga_id": s.saga_id,
                "order_id": s.order_id,
                "state": s.state,
                "started_at": s.started_at.isoformat() if s.started_at else None,
                "completed_at": s.completed_at.isoformat() if s.completed_at else None,
                "error_message": s.error_message,
            }
            for s in sagas
        ]
    }


@router.get("/async/sagas/{saga_id}", tags=["Async Processing"])
async def get_saga_detail(
    saga_id: str,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Get detailed saga information including step history.
    """
    # Get saga record
    result = await db.execute(
        select(SagaRecord).where(SagaRecord.saga_id == saga_id)
    )
    saga = result.scalar_one_or_none()
    
    if not saga:
        raise HTTPException(status_code=404, detail="Saga not found")
    
    # Get history
    history_result = await db.execute(
        select(SagaStatusHistory)
        .where(SagaStatusHistory.saga_id == saga_id)
        .order_by(SagaStatusHistory.timestamp)
    )
    history = history_result.scalars().all()
    
    import json
    return {
        "saga_id": saga.saga_id,
        "order_id": saga.order_id,
        "state": saga.state,
        "steps": json.loads(saga.steps_json) if saga.steps_json else [],
        "started_at": saga.started_at.isoformat() if saga.started_at else None,
        "completed_at": saga.completed_at.isoformat() if saga.completed_at else None,
        "error_message": saga.error_message,
        "history": [
            {
                "step_name": h.step_name,
                "from_state": h.from_state,
                "to_state": h.to_state,
                "details": h.details,
                "timestamp": h.timestamp.isoformat() if h.timestamp else None,
            }
            for h in history
        ]
    }


@router.get("/async/events", tags=["Async Processing"])
async def list_integration_events(
    order_id: str | None = Query(None, description="Filter by order ID"),
    status: str | None = Query(None, description="Filter by status"),
    target_system: str | None = Query(None, description="Filter by target system"),
    limit: int = Query(50, le=100),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: List integration events for monitoring and debugging.
    """
    from sqlalchemy import and_
    
    conditions = []
    if order_id:
        conditions.append(IntegrationEventLog.order_id == order_id)
    if status:
        conditions.append(IntegrationEventLog.status == status)
    if target_system:
        conditions.append(IntegrationEventLog.target_system == target_system)
    
    query = select(IntegrationEventLog)
    if conditions:
        query = query.where(and_(*conditions))
    
    query = query.order_by(IntegrationEventLog.created_at.desc()).limit(limit)
    
    result = await db.execute(query)
    events = result.scalars().all()
    
    return {
        "count": len(events),
        "events": [
            {
                "event_id": e.event_id,
                "order_id": e.order_id,
                "source_system": e.source_system,
                "target_system": e.target_system,
                "event_type": e.event_type,
                "status": e.status,
                "retry_count": e.retry_count,
                "duration_ms": e.duration_ms,
                "error_message": e.error_message,
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in events
        ]
    }


@router.get("/async/events/stats", tags=["Async Processing"])
async def get_event_statistics(
    hours: int = Query(24, description="Hours to look back"),
    target_system: str | None = Query(None, description="Filter by target system"),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Get integration event statistics for monitoring.
    """
    from datetime import timedelta
    
    event_store = IntegrationEventStore(db)
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    
    stats = await event_store.get_event_statistics(
        since=since,
        target_system=target_system,
    )
    
    return {
        "period_hours": hours,
        "target_system": target_system,
        "statistics": stats,
    }


@router.get("/async/audit-trail/{order_id}", tags=["Async Processing"])
async def get_order_audit_trail(
    order_id: str,
    entity_type: str | None = Query(None, description="Filter by entity type"),
    limit: int = Query(100, le=500),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Get audit trail for a specific order.
    
    Returns complete history of all actions taken on the order.
    """
    event_store = IntegrationEventStore(db)
    trail = await event_store.get_audit_trail(
        order_id=order_id,
        entity_type=entity_type,
        limit=limit,
    )
    
    return {
        "order_id": order_id,
        "count": len(trail),
        "audit_trail": [entry.to_dict() for entry in trail]
    }


@router.post("/async/retry/{event_id}", tags=["Async Processing"])
async def retry_failed_event(
    event_id: str,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Manually retry a failed integration event.
    """
    result = await db.execute(
        select(IntegrationEventLog).where(IntegrationEventLog.event_id == event_id)
    )
    event = result.scalar_one_or_none()
    
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    
    if event.status not in ["failed", "retrying"]:
        raise HTTPException(status_code=400, detail=f"Event status is {event.status}, cannot retry")
    
    if event.retry_count >= event.max_retries:
        raise HTTPException(status_code=400, detail="Event has exceeded max retries")
    
    # Update status to retrying
    event.status = "retrying"
    event.retry_count += 1
    event.updated_at = datetime.now(timezone.utc)
    
    await db.commit()
    
    return {
        "event_id": event_id,
        "status": "queued_for_retry",
        "retry_count": event.retry_count,
    }


@router.get("/async/dlq/stats", tags=["Async Processing"])
async def get_dlq_statistics(
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Get Dead Letter Queue statistics.
    """
    if not queue_manager.is_connected:
        return {
            "status": "disconnected",
            "message": "Queue manager not connected"
        }
    
    dlq_queues = [
        "order.processing.dlq",
        "order.cms.dlq",
        "order.wms.dlq",
        "order.ros.dlq",
    ]
    
    stats = {}
    for queue_name in dlq_queues:
        try:
            info = await queue_manager.get_queue_info(queue_name)
            stats[queue_name] = {
                "message_count": info.get("message_count", 0),
                "consumer_count": info.get("consumer_count", 0),
            }
        except Exception as e:
            stats[queue_name] = {"error": str(e)}
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dlq_statistics": stats,
    }


@router.post("/async/circuit-breaker/{system}/reset", tags=["Async Processing"])
async def reset_circuit_breaker(
    system: str,
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Reset circuit breaker for a system.
    
    Use this to manually recover from circuit-open state.
    """
    await retry_handler.reset_circuit_breaker(system)
    
    return {
        "system": system,
        "status": "reset",
        "circuit_breaker": retry_handler.get_circuit_breaker_status(system),
    }


@router.post("/async/publish-test", tags=["Async Processing"])
async def publish_test_message(
    exchange: str = Query("swifttrack.orders"),
    routing_key: str = Query("order.test"),
    message: dict = None,
    _admin: dict = Depends(require_role("admin")),
):
    """
    Admin: Publish a test message to verify queue connectivity.
    """
    if not queue_manager.is_connected:
        await queue_manager.connect()
    
    test_message = message or {
        "event": "test.message",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "admin-test",
    }
    
    msg_id = await queue_manager.publish_with_retry(
        exchange_name=exchange,
        routing_key=routing_key,
        message=test_message,
        max_retries=1,
    )
    
    return {
        "status": "published",
        "message_id": msg_id,
        "exchange": exchange,
        "routing_key": routing_key,
    }


# ══════════════════════════════════════════════════════════════
# Admin Dashboard Routes
# ══════════════════════════════════════════════════════════════

from shared.common.admin import (
    AdminDashboardService,
    SystemLogService,
    LogFilter,
    LogLevel,
)


@router.get("/admin/dashboard", tags=["Admin Dashboard"])
async def get_dashboard_overview(
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Get complete dashboard overview.
    
    Returns:
    - Order statistics (total, today, by status)
    - Delivery metrics (active, completed, failed)
    - User statistics (clients, drivers)
    - Integration statistics (success rate, pending, failed)
    - System status indicators
    - Active alerts
    """
    service = AdminDashboardService(db)
    overview = await service.get_dashboard_overview()
    return overview.to_dict()


@router.get("/admin/system-status", tags=["Admin Dashboard"])
async def get_system_status(
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Get system status indicators.
    
    Returns:
    - Overall system health
    - Database health
    - Queue health
    - Integration system health (CMS, ROS, WMS)
    - DLQ message count
    - Active saga count
    """
    service = AdminDashboardService(db)
    status = await service.get_system_status()
    return status.to_dict()


@router.get("/admin/integration-status", tags=["Admin Dashboard"])
async def get_integration_status(
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Get detailed integration status for CMS/ROS/WMS.
    
    Returns health check results, response times, and 24h statistics.
    """
    service = AdminDashboardService(db)
    status = await service.get_system_status()
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "integrations": {k: v.to_dict() for k, v in status.integrations.items()},
    }


@router.get("/admin/failed-messages", tags=["Admin Dashboard"])
async def get_failed_messages(
    system: str | None = Query(None, description="Filter by target system (cms, ros, wms)"),
    limit: int = Query(50, le=200),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Get list of failed integration messages.
    
    Shows messages that failed processing for admin review and retry.
    """
    service = AdminDashboardService(db)
    messages = await service.get_failed_messages(system=system, limit=limit)
    
    return {
        "count": len(messages),
        "filter": {"system": system},
        "messages": messages,
    }


@router.post("/admin/retry-event/{event_id}", tags=["Admin Dashboard"])
async def manual_retry_event(
    event_id: str,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Manually retry a failed integration event.
    
    Re-triggers the failed event with automatic error handling.
    """
    service = AdminDashboardService(db)
    result = await service.retry_failed_event(event_id)
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    return result


@router.get("/admin/logs/integration", tags=["Admin Logs"])
async def get_integration_logs(
    source_system: str | None = Query(None, description="Filter by source system"),
    target_system: str | None = Query(None, description="Filter by target system"),
    order_id: str | None = Query(None, description="Filter by order ID"),
    status: str | None = Query(None, description="Filter by status (pending, success, failed, retrying)"),
    severity: str | None = Query(None, description="Filter by severity (debug, info, warning, error, critical)"),
    hours: int = Query(24, description="Hours to look back"),
    limit: int = Query(100, le=500),
    offset: int = Query(0),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: View integration logs with filtering.
    
    Shows CMS/ROS/WMS integration events for debugging and monitoring.
    """
    from datetime import timedelta
    
    log_filter = LogFilter(
        source_system=source_system,
        target_system=target_system,
        order_id=order_id,
        status=status,
        level=LogLevel(severity) if severity else None,
        since=datetime.now(timezone.utc) - timedelta(hours=hours),
    )
    
    service = SystemLogService(db)
    return await service.get_integration_logs(filter=log_filter, limit=limit, offset=offset)


@router.get("/admin/logs/transactions", tags=["Admin Logs"])
async def get_transaction_history(
    order_id: str | None = Query(None, description="Filter by order ID"),
    state: str | None = Query(None, description="Filter by state (pending, in_progress, completed, compensating, compensated, failed)"),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: View transaction (saga) history.
    
    Shows distributed transaction records for monitoring order processing.
    """
    service = SystemLogService(db)
    return await service.get_transaction_history(
        order_id=order_id,
        state=state,
        limit=limit,
        offset=offset,
    )


@router.get("/admin/logs/transactions/{saga_id}", tags=["Admin Logs"])
async def get_transaction_detail(
    saga_id: str,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Get detailed transaction information.
    
    Shows saga steps, state transitions, and any errors.
    """
    service = SystemLogService(db)
    result = await service.get_transaction_detail(saga_id)
    
    if "error" in result and result["error"] == "Transaction not found":
        raise HTTPException(status_code=404, detail="Transaction not found")
    
    return result


@router.get("/admin/logs/audit", tags=["Admin Logs"])
async def get_audit_trail(
    order_id: str | None = Query(None, description="Filter by order ID"),
    entity_type: str | None = Query(None, description="Filter by entity type (order, tracking, billing, route, saga)"),
    action: str | None = Query(None, description="Filter by action"),
    actor_type: str | None = Query(None, description="Filter by actor type (system, user, admin, driver)"),
    hours: int = Query(24, description="Hours to look back"),
    limit: int = Query(100, le=500),
    offset: int = Query(0),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: View audit trail logs.
    
    Shows all actions taken on entities for compliance and debugging.
    """
    from datetime import timedelta
    
    service = SystemLogService(db)
    return await service.get_audit_trail(
        order_id=order_id,
        entity_type=entity_type,
        action=action,
        actor_type=actor_type,
        since=datetime.now(timezone.utc) - timedelta(hours=hours),
        limit=limit,
        offset=offset,
    )


@router.get("/admin/logs/errors/summary", tags=["Admin Logs"])
async def get_error_summary(
    hours: int = Query(24, description="Hours to look back"),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: Get error summary.
    
    Aggregates errors by type, source, and severity for quick overview.
    """
    service = SystemLogService(db)
    return await service.get_error_summary(hours=hours)


@router.get("/admin/logs/dlq", tags=["Admin Logs"])
async def get_dlq_records(
    queue: str | None = Query(None, description="Filter by original queue"),
    processed: bool | None = Query(None, description="Filter by processed status"),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """
    Admin: View Dead Letter Queue records.
    
    Shows messages that failed processing repeatedly.
    """
    service = SystemLogService(db)
    return await service.get_dlq_records(
        queue=queue,
        processed=processed,
        limit=limit,
        offset=offset,
    )


# ══════════════════════════════════════════════════════════════
# FILE UPLOAD ENDPOINTS
# ══════════════════════════════════════════════════════════════

from fastapi import File, UploadFile, Form


@router.post("/{order_id}/proof-of-delivery", tags=["Files"])
async def upload_proof_of_delivery(
    order_id: str,
    file: UploadFile = File(..., description="Proof of delivery image"),
    current_user: dict = Depends(require_role("driver")),
    db: AsyncSession = Depends(get_db),
):
    """
    Driver: Upload proof-of-delivery image for an order.
    
    Accepts JPEG, PNG, GIF, or WebP images up to 10MB.
    Returns file ID and URL for accessing the image.
    """
    from shared.common.file_storage import get_file_storage, FileCategory
    from shared.common.validators import validate_file_upload
    
    # Verify order exists and is assigned to this driver
    result = await db.execute(
        select(Order).where(Order.order_id == order_id)
    )
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    driver_id = int(current_user["sub"])
    if order.assigned_driver_id != driver_id:
        raise HTTPException(status_code=403, detail="Order not assigned to you")
    
    if order.status not in ("in_transit", "processing", "confirmed", "picked_up"):
        raise HTTPException(
            status_code=400, 
            detail=f"Cannot upload proof for order with status '{order.status}'"
        )
    
    # Validate file
    content = await file.read()
    try:
        validate_file_upload(
            filename=file.filename or "unknown",
            file_size=len(content),
            content_type=file.content_type or "application/octet-stream",
            max_size_mb=10.0,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    # Upload file
    file_storage = get_file_storage(db)
    result = await file_storage.upload_proof_of_delivery(
        order_id=order_id,
        file_content=content,
        filename=file.filename or "proof.jpg",
        content_type=file.content_type or "image/jpeg",
        driver_id=driver_id,
    )
    
    logger.info("Proof of delivery uploaded: order=%s, file=%s", order_id, result["file_id"])
    
    return {
        "message": "Proof of delivery uploaded successfully",
        "file_id": result["file_id"],
        "url": result["url"],
        "thumbnail_url": result.get("thumbnail_url"),
    }


@router.post("/{order_id}/signature", tags=["Files"])
async def upload_signature(
    order_id: str,
    signature: str = Form(..., description="Base64 encoded signature image"),
    recipient_name: str = Form(..., description="Name of person who signed"),
    current_user: dict = Depends(require_role("driver")),
    db: AsyncSession = Depends(get_db),
):
    """
    Driver: Upload recipient signature for an order.
    
    Accepts a base64 encoded signature image (PNG format).
    """
    from shared.common.file_storage import get_file_storage
    from shared.common.validators import sanitize_string
    
    # Verify order exists and is assigned to this driver
    result = await db.execute(
        select(Order).where(Order.order_id == order_id)
    )
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    driver_id = int(current_user["sub"])
    if order.assigned_driver_id != driver_id:
        raise HTTPException(status_code=403, detail="Order not assigned to you")
    
    # Sanitize recipient name
    recipient_name = sanitize_string(recipient_name, max_length=100)
    if not recipient_name:
        raise HTTPException(status_code=400, detail="Recipient name is required")
    
    # Upload signature
    file_storage = get_file_storage(db)
    try:
        result = await file_storage.upload_signature(
            order_id=order_id,
            signature_data=signature,
            recipient_name=recipient_name,
            driver_id=driver_id,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    logger.info("Signature uploaded: order=%s, recipient=%s", order_id, recipient_name)
    
    return {
        "message": "Signature uploaded successfully",
        "file_id": result["file_id"],
        "url": result["url"],
        "recipient_name": recipient_name,
    }


@router.get("/{order_id}/files", tags=["Files"])
async def get_order_files(
    order_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all files (proof of delivery, signatures) for an order.
    
    Client can view their own order files.
    Driver can view files for orders assigned to them.
    Admin can view all files.
    """
    from shared.common.file_storage import get_file_storage
    
    # Verify order access
    result = await db.execute(
        select(Order).where(Order.order_id == order_id)
    )
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    user_id = int(current_user["sub"])
    user_role = current_user.get("role")
    
    # Check access
    if user_role not in ("admin",):
        if user_role == "client" and order.client_id != user_id:
            raise HTTPException(status_code=403, detail="Not your order")
        if user_role == "driver" and order.assigned_driver_id != user_id:
            raise HTTPException(status_code=403, detail="Order not assigned to you")
    
    # Get files
    file_storage = get_file_storage(db)
    files = await file_storage.get_files_for_order(order_id)
    
    return {
        "order_id": order_id,
        "files": files,
        "count": len(files),
    }


@router.get("/files/{file_id}", tags=["Files"])
async def get_file(
    file_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get file content by ID.
    
    Returns the file with appropriate content type.
    Access is validated based on order ownership.
    """
    from fastapi.responses import Response
    from shared.common.file_storage import get_file_storage
    
    file_storage = get_file_storage(db)
    file_data = await file_storage.get_file_content(file_id)
    
    if not file_data:
        raise HTTPException(status_code=404, detail="File not found")
    
    content, content_type, filename = file_data
    
    return Response(
        content=content,
        media_type=content_type,
        headers={
            "Content-Disposition": f'inline; filename="{filename}"',
            "Cache-Control": "private, max-age=3600",
        },
    )


@router.get("/files/{file_id}/thumbnail", tags=["Files"])
async def get_file_thumbnail(
    file_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get file thumbnail by ID.
    
    Returns a smaller version of the image for previews.
    """
    from fastapi.responses import Response
    from shared.common.file_storage import get_file_storage
    
    file_storage = get_file_storage(db)
    thumb_data = await file_storage.get_thumbnail_content(file_id)
    
    if not thumb_data:
        raise HTTPException(status_code=404, detail="Thumbnail not found")
    
    content, content_type = thumb_data
    
    return Response(
        content=content,
        media_type=content_type,
        headers={
            "Cache-Control": "public, max-age=86400",
        },
    )


