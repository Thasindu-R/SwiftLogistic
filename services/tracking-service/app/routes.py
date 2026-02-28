"""
Tracking Service – route handlers (REST + WebSocket).
"""

import logging
import uuid
from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from sqlalchemy import func as sqlfunc, select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.common.config import settings
from shared.common.database import get_db
from shared.common.rabbitmq import rabbitmq_client
from shared.common.security import get_current_user, require_role
from shared.contracts.tracking_schemas import (
    DeliveryItemUpdate,
    IntegrationEventList,
    IntegrationEventResponse,
    ManifestCreate,
    ManifestResponse,
    TrackingEventCreate,
    TrackingEventResponse,
    TrackingHistory,
)

from .models import DeliveryItem, DeliveryManifest, TrackingEvent
from .websocket_manager import ws_manager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/tracking", tags=["Tracking"])


# ── Tracking history for an order ────────────────────────────
@router.get("/{order_id}", response_model=TrackingHistory)
async def get_tracking_history(
    order_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(TrackingEvent)
        .where(TrackingEvent.order_id == order_id)
        .order_by(TrackingEvent.timestamp.asc())
    )
    events = result.scalars().all()
    return TrackingHistory(
        order_id=order_id,
        events=[TrackingEventResponse.model_validate(e) for e in events],
    )


# ── Recent tracking events (admin) ──────────────────────────
@router.get("/events/recent", response_model=list[TrackingEventResponse])
async def recent_events(
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin: get recent tracking events across all orders."""
    result = await db.execute(
        select(TrackingEvent)
        .order_by(TrackingEvent.timestamp.desc())
        .limit(limit)
    )
    events = result.scalars().all()
    return [TrackingEventResponse.model_validate(e) for e in events]


# ── Manually add a tracking event (driver / admin) ──────────
@router.post("/events", response_model=TrackingEventResponse, status_code=201)
async def create_tracking_event(
    payload: TrackingEventCreate,
    current_user: dict = Depends(require_role("admin", "driver")),
    db: AsyncSession = Depends(get_db),
):
    event = TrackingEvent(
        order_id=payload.order_id,
        event_type=payload.event_type,
        description=payload.description,
        location=payload.location,
        latitude=payload.latitude,
        longitude=payload.longitude,
        driver_id=payload.driver_id or int(current_user["sub"]),
    )
    db.add(event)
    await db.flush()
    await db.refresh(event)

    # Publish to RabbitMQ so other services and the WebSocket layer react
    msg = {
        "event": "tracking.update",
        "order_id": payload.order_id,
        "event_type": payload.event_type,
        "description": payload.description,
        "location": payload.location,
        "driver_id": event.driver_id,
        "timestamp": event.timestamp.isoformat() if event.timestamp else "",
    }
    try:
        await rabbitmq_client.publish(
            exchange_name=settings.TRACKING_EXCHANGE,
            routing_key=settings.TRACKING_UPDATE_KEY,
            message=msg,
        )
    except Exception as e:
        logger.error("Failed to publish tracking event: %s", e)

    # Also push directly via WebSocket to connected clients
    await ws_manager.broadcast(payload.order_id, msg)

    logger.info("Tracking event created: %s for order %s", payload.event_type, payload.order_id)
    return TrackingEventResponse.model_validate(event)


# ── Delivery manifests ───────────────────────────────────────
@router.post("/manifests", response_model=ManifestResponse, status_code=201)
async def create_manifest(
    payload: ManifestCreate,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin: create a delivery manifest assigning orders to a driver."""
    manifest_id = str(uuid.uuid4())
    manifest = DeliveryManifest(
        manifest_id=manifest_id,
        driver_id=payload.driver_id,
        date=date.fromisoformat(payload.date),
        status="pending",
    )
    db.add(manifest)
    await db.flush()  # Flush manifest first to satisfy FK constraint

    for idx, oid in enumerate(payload.order_ids):
        item = DeliveryItem(
            manifest_id=manifest_id,
            order_id=oid,
            sequence=idx + 1,
            status="pending",
        )
        db.add(item)

    await db.flush()
    await db.refresh(manifest)

    # Fetch the items just created
    items_result = await db.execute(
        select(DeliveryItem).where(DeliveryItem.manifest_id == manifest_id).order_by(DeliveryItem.sequence)
    )
    items = items_result.scalars().all()

    logger.info("Manifest %s created for driver %d with %d items", manifest_id, payload.driver_id, len(payload.order_ids))
    return ManifestResponse(
        manifest_id=manifest.manifest_id,
        driver_id=manifest.driver_id,
        date=str(manifest.date),
        status=manifest.status,
        items=[
            {
                "order_id": i.order_id,
                "sequence": i.sequence,
                "status": i.status,
                "proof_of_delivery": i.proof_of_delivery,
                "notes": i.notes,
            }
            for i in items
        ],
    )


@router.get("/manifests/driver/{driver_id}", response_model=list[ManifestResponse])
async def get_driver_manifests(
    driver_id: int,
    manifest_date: str | None = Query(None, alias="date"),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get manifests for a driver. Drivers can only see their own."""
    role = current_user["role"]
    user_id = int(current_user["sub"])

    if role == "driver" and driver_id != user_id:
        raise HTTPException(status_code=403, detail="Can only view your own manifests")

    query = select(DeliveryManifest).where(DeliveryManifest.driver_id == driver_id)
    if manifest_date:
        query = query.where(DeliveryManifest.date == date.fromisoformat(manifest_date))
    result = await db.execute(query.order_by(DeliveryManifest.date.desc()))
    manifests = result.scalars().all()

    response = []
    for m in manifests:
        items_result = await db.execute(
            select(DeliveryItem)
            .where(DeliveryItem.manifest_id == m.manifest_id)
            .order_by(DeliveryItem.sequence)
        )
        items = items_result.scalars().all()
        response.append(
            ManifestResponse(
                manifest_id=m.manifest_id,
                driver_id=m.driver_id,
                date=str(m.date),
                status=m.status,
                route_data=m.route_data,
                items=[
                    {
                        "order_id": i.order_id,
                        "sequence": i.sequence,
                        "status": i.status,
                        "proof_of_delivery": i.proof_of_delivery,
                        "failure_reason": i.failure_reason,
                        "notes": i.notes,
                        "delivered_at": i.delivered_at.isoformat() if i.delivered_at else None,
                    }
                    for i in items
                ],
            )
        )
    return response


# ── All manifests (admin) ────────────────────────────────────
@router.get("/manifests/all", response_model=list[ManifestResponse])
async def get_all_manifests(
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin: list all manifests."""
    result = await db.execute(
        select(DeliveryManifest).order_by(DeliveryManifest.date.desc()).limit(100)
    )
    manifests = result.scalars().all()

    response = []
    for m in manifests:
        items_result = await db.execute(
            select(DeliveryItem)
            .where(DeliveryItem.manifest_id == m.manifest_id)
            .order_by(DeliveryItem.sequence)
        )
        items = items_result.scalars().all()
        response.append(
            ManifestResponse(
                manifest_id=m.manifest_id,
                driver_id=m.driver_id,
                date=str(m.date),
                status=m.status,
                route_data=m.route_data,
                items=[
                    {
                        "order_id": i.order_id,
                        "sequence": i.sequence,
                        "status": i.status,
                    }
                    for i in items
                ],
            )
        )
    return response


# ── Driver marks delivery status ─────────────────────────────
@router.patch("/delivery-items/{order_id}", response_model=dict)
async def update_delivery_item(
    order_id: str,
    payload: DeliveryItemUpdate,
    current_user: dict = Depends(require_role("driver", "admin")),
    db: AsyncSession = Depends(get_db),
):
    """Driver/Admin: update delivery item status (delivered/failed/picked_up)."""
    result = await db.execute(
        select(DeliveryItem).where(DeliveryItem.order_id == order_id)
    )
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(status_code=404, detail="Delivery item not found")

    item.status = payload.status
    if payload.proof_of_delivery:
        item.proof_of_delivery = payload.proof_of_delivery
    if payload.signature_data:
        item.signature_data = payload.signature_data
    if payload.failure_reason:
        item.failure_reason = payload.failure_reason
    if payload.notes:
        item.notes = payload.notes
    if payload.status == "delivered":
        item.delivered_at = datetime.now(timezone.utc)

    await db.flush()

    # Publish tracking event
    description = payload.failure_reason or f"Package marked as {payload.status}"
    if payload.status == "delivered" and payload.proof_of_delivery:
        description += " (with proof of delivery)"

    msg = {
        "event": "tracking.update",
        "order_id": order_id,
        "event_type": f"delivery_{payload.status}",
        "description": description,
        "location": "",
        "latitude": payload.latitude,
        "longitude": payload.longitude,
        "driver_id": int(current_user["sub"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        await rabbitmq_client.publish(
            exchange_name=settings.TRACKING_EXCHANGE,
            routing_key=settings.TRACKING_UPDATE_KEY,
            message=msg,
        )
    except Exception as e:
        logger.error("Failed to publish delivery update: %s", e)
    await ws_manager.broadcast(order_id, msg)

    return {"message": f"Delivery item updated to {payload.status}", "order_id": order_id}


# ── Integration events (admin) ───────────────────────────────
@router.get("/integration-events", response_model=IntegrationEventList)
async def get_integration_events(
    status_filter: str = Query(None, alias="status"),
    source: str = Query(None),
    limit: int = Query(50, ge=1, le=200),
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin: list integration events for monitoring middleware calls."""
    from shared.common.event_logger import IntegrationEvent

    query = select(IntegrationEvent)
    count_query = select(sqlfunc.count(IntegrationEvent.id))

    if status_filter:
        query = query.where(IntegrationEvent.status == status_filter)
        count_query = count_query.where(IntegrationEvent.status == status_filter)
    if source:
        query = query.where(IntegrationEvent.source_system == source)
        count_query = count_query.where(IntegrationEvent.source_system == source)

    total = (await db.execute(count_query)).scalar() or 0
    result = await db.execute(
        query.order_by(IntegrationEvent.created_at.desc()).limit(limit)
    )
    events = result.scalars().all()
    return IntegrationEventList(
        events=[IntegrationEventResponse.model_validate(e) for e in events],
        total=total,
    )


# ── Retry failed integration (admin) ─────────────────────────
@router.post("/integration-events/{event_id}/retry", response_model=dict)
async def retry_integration_event(
    event_id: str,
    _admin: dict = Depends(require_role("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Admin: manually retry a failed integration event."""
    from shared.common.event_logger import IntegrationEvent

    result = await db.execute(
        select(IntegrationEvent).where(IntegrationEvent.event_id == event_id)
    )
    event = result.scalar_one_or_none()
    if not event:
        raise HTTPException(status_code=404, detail="Integration event not found")
    if event.status != "failed":
        raise HTTPException(status_code=400, detail="Can only retry failed events")
    if event.retry_count >= event.max_retries:
        raise HTTPException(status_code=400, detail="Max retries exceeded")

    # Re-publish the original event
    import json
    try:
        request_data = json.loads(event.request_data) if event.request_data else {}
        await rabbitmq_client.publish(
            exchange_name=settings.ORDER_EXCHANGE,
            routing_key=settings.ORDER_CREATED_KEY,
            message=request_data,
        )
        event.status = "retrying"
        event.retry_count += 1
        event.updated_at = datetime.now(timezone.utc)
        await db.flush()
        logger.info("Retrying integration event %s (attempt %d)", event_id, event.retry_count)
        return {"message": f"Retry #{event.retry_count} initiated", "event_id": event_id}
    except Exception as e:
        logger.error("Retry failed for event %s: %s", event_id, e)
        raise HTTPException(status_code=500, detail=f"Retry failed: {str(e)}")


# ── WebSocket endpoint for real-time tracking ────────────────
@router.websocket("/ws/{order_id}")
async def websocket_tracking(websocket: WebSocket, order_id: str):
    """Clients connect here to receive real-time updates for a specific order."""
    await ws_manager.connect(websocket, order_id)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket, order_id)


# ── Health ───────────────────────────────────────────────────
@router.get("/health/check")
async def health():
    return {"message": "tracking-service is healthy"}
