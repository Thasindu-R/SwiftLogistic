"""
API Gateway – auth route proxies.
Forwards authentication requests to the auth-service.
"""

import logging

import httpx
from fastapi import APIRouter, HTTPException, Request

from shared.common.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/auth", tags=["Auth (Gateway)"])

AUTH_URL = settings.AUTH_SERVICE_URL


async def _proxy(method: str, path: str, request: Request):
    """Forward an HTTP request to the auth-service and return the response."""
    url = f"{AUTH_URL}{path}"
    headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length")
    }
    body = await request.body()
    params = dict(request.query_params)

    async with httpx.AsyncClient(timeout=10.0) as client:
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
            raise HTTPException(status_code=503, detail="Auth service unavailable")
        except HTTPException:
            raise
        except Exception as e:
            logger.error("Auth proxy error: %s", e)
            raise HTTPException(status_code=502, detail="Auth service error")


@router.post("/register")
async def register(request: Request):
    return await _proxy("POST", "/api/auth/register", request)


@router.post("/login")
async def login(request: Request):
    return await _proxy("POST", "/api/auth/login", request)


@router.get("/me")
async def get_profile(request: Request):
    return await _proxy("GET", "/api/auth/me", request)


@router.put("/me")
async def update_profile(request: Request):
    return await _proxy("PUT", "/api/auth/me", request)


@router.put("/me/password")
async def change_password(request: Request):
    return await _proxy("PUT", "/api/auth/me/password", request)


@router.get("/verify")
async def verify_token(request: Request):
    return await _proxy("GET", "/api/auth/verify", request)


# ── Admin user management ────────────────────────────────────
@router.get("/users")
async def list_users(request: Request):
    return await _proxy("GET", "/api/auth/users", request)


@router.post("/users")
async def create_user(request: Request):
    return await _proxy("POST", "/api/auth/users", request)


@router.get("/users/{user_id}")
async def get_user(user_id: int, request: Request):
    return await _proxy("GET", f"/api/auth/users/{user_id}", request)


@router.put("/users/{user_id}")
async def update_user(user_id: int, request: Request):
    return await _proxy("PUT", f"/api/auth/users/{user_id}", request)


@router.patch("/users/{user_id}/status")
async def toggle_user_status(user_id: int, request: Request):
    return await _proxy("PATCH", f"/api/auth/users/{user_id}/status", request)
