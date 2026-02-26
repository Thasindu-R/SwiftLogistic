"""
API Gateway – order route proxies.
Forwards order-related requests to the order-service.
"""

import logging

import httpx
from fastapi import APIRouter, HTTPException, Request

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
