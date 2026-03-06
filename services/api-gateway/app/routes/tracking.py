"""
API Gateway – tracking route proxies + WebSocket passthrough.
Forwards tracking requests to the tracking-service and provides a WebSocket bridge for real-time updates to the frontend.
"""

import logging

import httpx
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect

from shared.common.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/tracking", tags=["Tracking (Gateway)"])

TRACKING_URL = settings.TRACKING_SERVICE_URL


async def _proxy(method: str, path: str, request: Request):
    url = f"{TRACKING_URL}{path}"
    headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length")
    }
    body = await request.body()
    params = dict(request.query_params)

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.request(method, url, headers=headers, content=body, params=params)
            if resp.status_code >= 400:
                try:
                    detail = resp.json()
                except Exception:
                    detail = resp.text
                raise HTTPException(status_code=resp.status_code, detail=detail.get("detail", detail) if isinstance(detail, dict) else detail)
            return resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text
        except httpx.ConnectError:
            raise HTTPException(status_code=503, detail="Tracking service unavailable")
        except HTTPException:
            raise
        except Exception as e:
            logger.error("Tracking proxy error: %s", e)
            raise HTTPException(status_code=502, detail="Tracking service error")


# ── Order Tracking ────────────────────────────────────────────
@router.get("/events/recent")
async def recent_events(request: Request):
    return await _proxy("GET", "/api/tracking/events/recent", request)


@router.get("/{order_id}")
async def get_tracking(order_id: str, request: Request):
    return await _proxy("GET", f"/api/tracking/{order_id}", request)


@router.post("/events")
async def create_event(request: Request):
    return await _proxy("POST", "/api/tracking/events", request)


# ── Manifests ─────────────────────────────────────────────────
@router.post("/manifests")
async def create_manifest(request: Request):
    return await _proxy("POST", "/api/tracking/manifests", request)


@router.get("/manifests/all")
async def all_manifests(request: Request):
    return await _proxy("GET", "/api/tracking/manifests/all", request)


@router.get("/manifests/driver/{driver_id}")
async def driver_manifests(driver_id: int, request: Request):
    return await _proxy("GET", f"/api/tracking/manifests/driver/{driver_id}", request)


# ── Delivery Items ────────────────────────────────────────────
@router.patch("/delivery-items/{order_id}")
async def update_delivery_item(order_id: str, request: Request):
    return await _proxy("PATCH", f"/api/tracking/delivery-items/{order_id}", request)


# ── Integration Events ───────────────────────────────────────
@router.get("/integration-events")
async def list_integration_events(request: Request):
    return await _proxy("GET", "/api/tracking/integration-events", request)


@router.post("/integration-events/{event_id}/retry")
async def retry_integration_event(event_id: int, request: Request):
    return await _proxy("POST", f"/api/tracking/integration-events/{event_id}/retry", request)


# ── WebSocket passthrough ────────────────────────────────────
@router.websocket("/ws/global/{token}")
async def websocket_global_proxy(websocket: WebSocket, token: str):
    """Gateway WebSocket proxy for the global (user-level) channel."""
    import asyncio
    import websockets

    await websocket.accept()
    ws_url = TRACKING_URL.replace("http://", "ws://").replace("https://", "wss://")
    ws_url = f"{ws_url}/api/tracking/ws/global/{token}"

    try:
        async with websockets.connect(ws_url) as backend_ws:
            async def client_to_backend():
                try:
                    while True:
                        data = await websocket.receive_text()
                        await backend_ws.send(data)
                except WebSocketDisconnect:
                    pass

            async def backend_to_client():
                try:
                    async for msg in backend_ws:
                        await websocket.send_text(msg)
                except Exception:
                    pass

            await asyncio.gather(client_to_backend(), backend_to_client())
    except Exception as e:
        logger.warning("Global WebSocket proxy error: %s", e)
        try:
            await websocket.close()
        except Exception:
            pass


@router.websocket("/ws/{order_id}")
async def websocket_proxy(websocket: WebSocket, order_id: str):
    """
    Gateway-level WebSocket: accepts the browser connection and relays
    messages to/from the tracking-service WebSocket.
    """
    import websockets

    await websocket.accept()
    # Build ws:// URL for the tracking-service
    ws_url = TRACKING_URL.replace("http://", "ws://").replace("https://", "wss://")
    ws_url = f"{ws_url}/api/tracking/ws/{order_id}"

    try:
        async with websockets.connect(ws_url) as backend_ws:
            import asyncio

            async def client_to_backend():
                try:
                    while True:
                        data = await websocket.receive_text()
                        await backend_ws.send(data)
                except WebSocketDisconnect:
                    pass

            async def backend_to_client():
                try:
                    async for msg in backend_ws:
                        await websocket.send_text(msg)
                except Exception:
                    pass

            await asyncio.gather(client_to_backend(), backend_to_client())
    except Exception as e:
        logger.warning("WebSocket proxy error: %s", e)
        try:
            await websocket.close()
        except Exception:
            pass
