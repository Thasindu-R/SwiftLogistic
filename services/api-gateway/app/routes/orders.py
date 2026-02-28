"""
API Gateway – order route proxies.
Forwards order-related requests to the order-service.
"""

import logging

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from shared.common.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/orders", tags=["Orders (Gateway)"])

ORDER_URL = settings.ORDER_SERVICE_URL


async def _proxy(method: str, path: str, request: Request):
    url = f"{ORDER_URL}{path}"
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
            raise HTTPException(status_code=503, detail="Order service unavailable")
        except HTTPException:
            raise
        except Exception as e:
            logger.error("Order proxy error: %s", e)
            raise HTTPException(status_code=502, detail="Order service error")


@router.post("/")
async def create_order(request: Request):
    return await _proxy("POST", "/api/orders/", request)


@router.get("/")
async def list_orders(request: Request):
    return await _proxy("GET", "/api/orders/", request)


@router.get("/stats/summary")
async def order_stats(request: Request):
    return await _proxy("GET", "/api/orders/stats/summary", request)


@router.get("/{order_id}")
async def get_order(order_id: str, request: Request):
    return await _proxy("GET", f"/api/orders/{order_id}", request)


@router.patch("/{order_id}/status")
async def update_status(order_id: str, request: Request):
    return await _proxy("PATCH", f"/api/orders/{order_id}/status", request)


@router.patch("/{order_id}/assign")
async def assign_driver(order_id: str, request: Request):
    return await _proxy("PATCH", f"/api/orders/{order_id}/assign", request)


# ══════════════════════════════════════════════════════════════
# INTEGRATION ROUTES
# ══════════════════════════════════════════════════════════════

@router.get("/integration/health")
async def integration_health(request: Request):
    """Proxy to order-service integration health check."""
    return await _proxy("GET", "/api/orders/integration/health", request)


@router.post("/integration/cms/validate/{client_id}")
async def validate_client_cms(client_id: int, request: Request):
    """Proxy to CMS client validation (SOAP/XML)."""
    return await _proxy("POST", f"/api/orders/integration/cms/validate/{client_id}", request)


@router.get("/integration/cms/client/{client_id}")
async def get_client_info_cms(client_id: int, request: Request):
    """Proxy to CMS get client info (SOAP/XML)."""
    return await _proxy("GET", f"/api/orders/integration/cms/client/{client_id}", request)


@router.post("/integration/ros/optimize")
async def optimize_route_ros(request: Request):
    """Proxy to ROS route optimization (REST/JSON)."""
    return await _proxy("POST", "/api/orders/integration/ros/optimize", request)


@router.post("/integration/wms/receive/{order_id}")
async def receive_package_wms(order_id: str, request: Request):
    """Proxy to WMS package receive (TCP/IP)."""
    return await _proxy("POST", f"/api/orders/integration/wms/receive/{order_id}", request)


@router.get("/integration/wms/status/{order_id}")
async def check_wms_status(order_id: str, request: Request):
    """Proxy to WMS status check (TCP/IP)."""
    return await _proxy("GET", f"/api/orders/integration/wms/status/{order_id}", request)


@router.post("/integration/wms/load/{order_id}")
async def load_vehicle_wms(order_id: str, request: Request):
    """Proxy to WMS load vehicle (TCP/IP)."""
    return await _proxy("POST", f"/api/orders/integration/wms/load/{order_id}", request)


@router.post("/integration/full-process/{order_id}")
async def full_integration_process(order_id: str, request: Request):
    """Proxy to full integration processing."""
    return await _proxy("POST", f"/api/orders/integration/full-process/{order_id}", request)


@router.post("/integration/transform/json-to-xml")
async def transform_json_to_xml(request: Request):
    """Proxy to JSON→XML transformation."""
    return await _proxy("POST", "/api/orders/integration/transform/json-to-xml", request)


@router.post("/integration/transform/xml-to-json")
async def transform_xml_to_json(request: Request):
    """Proxy to XML→JSON transformation."""
    return await _proxy("POST", "/api/orders/integration/transform/xml-to-json", request)


# ══════════════════════════════════════════════════════════════
# ASYNC PROCESSING ROUTES
# ══════════════════════════════════════════════════════════════

@router.get("/async/health")
async def async_health(request: Request):
    """Proxy to async processing health check."""
    return await _proxy("GET", "/api/orders/async/health", request)


@router.get("/async/sagas")
async def list_sagas(request: Request):
    """Proxy to list saga records."""
    return await _proxy("GET", "/api/orders/async/sagas", request)


@router.get("/async/sagas/{saga_id}")
async def get_saga_detail(saga_id: str, request: Request):
    """Proxy to get saga detail."""
    return await _proxy("GET", f"/api/orders/async/sagas/{saga_id}", request)


@router.get("/async/events")
async def list_events(request: Request):
    """Proxy to list integration events."""
    return await _proxy("GET", "/api/orders/async/events", request)


@router.get("/async/events/stats")
async def event_stats(request: Request):
    """Proxy to get event statistics."""
    return await _proxy("GET", "/api/orders/async/events/stats", request)


@router.get("/async/audit-trail/{order_id}")
async def get_audit_trail(order_id: str, request: Request):
    """Proxy to get order audit trail."""
    return await _proxy("GET", f"/api/orders/async/audit-trail/{order_id}", request)


@router.post("/async/retry/{event_id}")
async def retry_event(event_id: str, request: Request):
    """Proxy to retry failed event."""
    return await _proxy("POST", f"/api/orders/async/retry/{event_id}", request)


@router.get("/async/dlq/stats")
async def dlq_stats(request: Request):
    """Proxy to get DLQ statistics."""
    return await _proxy("GET", "/api/orders/async/dlq/stats", request)


@router.post("/async/circuit-breaker/{system}/reset")
async def reset_circuit_breaker(system: str, request: Request):
    """Proxy to reset circuit breaker."""
    return await _proxy("POST", f"/api/orders/async/circuit-breaker/{system}/reset", request)


@router.post("/async/publish-test")
async def publish_test_message(request: Request):
    """Proxy to publish test message."""
    return await _proxy("POST", "/api/orders/async/publish-test", request)


# ══════════════════════════════════════════════════════════════
# ADMIN DASHBOARD ROUTES
# ══════════════════════════════════════════════════════════════

@router.get("/admin/dashboard")
async def admin_dashboard(request: Request):
    """Proxy to admin dashboard overview."""
    return await _proxy("GET", "/api/orders/admin/dashboard", request)


@router.get("/admin/system-status")
async def admin_system_status(request: Request):
    """Proxy to admin system status."""
    return await _proxy("GET", "/api/orders/admin/system-status", request)


@router.get("/admin/integration-status")
async def admin_integration_status(request: Request):
    """Proxy to admin integration status."""
    return await _proxy("GET", "/api/orders/admin/integration-status", request)


@router.get("/admin/failed-messages")
async def admin_failed_messages(request: Request):
    """Proxy to admin failed messages."""
    return await _proxy("GET", "/api/orders/admin/failed-messages", request)


@router.post("/admin/retry-event/{event_id}")
async def admin_retry_event(event_id: str, request: Request):
    """Proxy to admin retry event."""
    return await _proxy("POST", f"/api/orders/admin/retry-event/{event_id}", request)


@router.get("/admin/logs/integration")
async def admin_integration_logs(request: Request):
    """Proxy to admin integration logs."""
    return await _proxy("GET", "/api/orders/admin/logs/integration", request)


@router.get("/admin/logs/transactions")
async def admin_transaction_logs(request: Request):
    """Proxy to admin transaction logs."""
    return await _proxy("GET", "/api/orders/admin/logs/transactions", request)


@router.get("/admin/logs/transactions/{saga_id}")
async def admin_transaction_detail(saga_id: str, request: Request):
    """Proxy to admin transaction detail."""
    return await _proxy("GET", f"/api/orders/admin/logs/transactions/{saga_id}", request)


@router.get("/admin/logs/audit")
async def admin_audit_logs(request: Request):
    """Proxy to admin audit logs."""
    return await _proxy("GET", "/api/orders/admin/logs/audit", request)


@router.get("/admin/logs/errors/summary")
async def admin_error_summary(request: Request):
    """Proxy to admin error summary."""
    return await _proxy("GET", "/api/orders/admin/logs/errors/summary", request)


@router.get("/admin/logs/dlq")
async def admin_dlq_records(request: Request):
    """Proxy to admin DLQ records."""
    return await _proxy("GET", "/api/orders/admin/logs/dlq", request)


# ══════════════════════════════════════════════════════════════
# FILE UPLOAD ROUTES
# ══════════════════════════════════════════════════════════════

from fastapi import File, UploadFile, Form


@router.post("/{order_id}/proof-of-delivery")
async def upload_proof_of_delivery(
    order_id: str,
    request: Request,
    file: UploadFile = File(...),
):
    """Proxy to upload proof-of-delivery image."""
    import httpx
    from shared.common.config import settings
    
    # Get auth header
    auth_header = request.headers.get("authorization", "")
    headers = {"Authorization": auth_header} if auth_header else {}
    
    # Forward multipart form data
    async with httpx.AsyncClient(timeout=60.0) as client:
        files = {"file": (file.filename, await file.read(), file.content_type)}
        response = await client.post(
            f"{settings.ORDER_SERVICE_URL}/api/orders/{order_id}/proof-of-delivery",
            headers=headers,
            files=files,
        )
        return Response(
            content=response.content,
            status_code=response.status_code,
            media_type=response.headers.get("content-type", "application/json"),
        )


@router.post("/{order_id}/signature")
async def upload_signature(
    order_id: str,
    request: Request,
    signature: str = Form(...),
    recipient_name: str = Form(...),
):
    """Proxy to upload recipient signature."""
    import httpx
    from shared.common.config import settings
    
    auth_header = request.headers.get("authorization", "")
    headers = {"Authorization": auth_header} if auth_header else {}
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{settings.ORDER_SERVICE_URL}/api/orders/{order_id}/signature",
            headers=headers,
            data={"signature": signature, "recipient_name": recipient_name},
        )
        return Response(
            content=response.content,
            status_code=response.status_code,
            media_type=response.headers.get("content-type", "application/json"),
        )


@router.get("/{order_id}/files")
async def get_order_files(order_id: str, request: Request):
    """Proxy to get order files."""
    return await _proxy("GET", f"/api/orders/{order_id}/files", request)


@router.get("/files/{file_id}")
async def get_file(file_id: str, request: Request):
    """Proxy to get file content."""
    import httpx
    from shared.common.config import settings
    
    auth_header = request.headers.get("authorization", "")
    headers = {"Authorization": auth_header} if auth_header else {}
    
    # Stream file content
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(
            f"{settings.ORDER_SERVICE_URL}/api/orders/files/{file_id}",
            headers=headers,
        )
        return Response(
            content=response.content,
            status_code=response.status_code,
            media_type=response.headers.get("content-type", "application/octet-stream"),
            headers={
                "Content-Disposition": response.headers.get("content-disposition", ""),
                "Cache-Control": response.headers.get("cache-control", "private"),
            },
        )


@router.get("/files/{file_id}/thumbnail")
async def get_file_thumbnail(file_id: str, request: Request):
    """Proxy to get file thumbnail."""
    import httpx
    from shared.common.config import settings
    
    auth_header = request.headers.get("authorization", "")
    headers = {"Authorization": auth_header} if auth_header else {}
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(
            f"{settings.ORDER_SERVICE_URL}/api/orders/files/{file_id}/thumbnail",
            headers=headers,
        )
        return Response(
            content=response.content,
            status_code=response.status_code,
            media_type=response.headers.get("content-type", "image/jpeg"),
        )
