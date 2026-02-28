"""
Security Middleware & Rate Limiting
===================================

Advanced security features for SwiftLogistics API:
- Rate limiting per IP and per user
- Request logging and audit trail
- Token validation middleware
- CORS configuration
- Security headers
- Request ID tracking

Features:
- Sliding window rate limiting
- Configurable limits per endpoint
- IP-based and user-based throttling
- Automatic request logging to audit trail
- Security headers (HSTS, CSP, etc.)

Usage:
    from shared.common.middleware import (
        RateLimitMiddleware,
        RequestLoggingMiddleware,
        setup_security_middleware,
    )
    
    # Setup all middleware
    setup_security_middleware(app)
    
    # Or add individually
    app.add_middleware(RateLimitMiddleware)
"""

import asyncio
import hashlib
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, Optional

from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)


# ── Rate Limiting Configuration ──────────────────────────────

class RateLimitConfig:
    """Configuration for rate limiting."""
    
    # Default limits (requests per window)
    DEFAULT_RATE_LIMIT = 100  # requests
    DEFAULT_WINDOW_SECONDS = 60  # 1 minute
    
    # Limits by endpoint pattern
    ENDPOINT_LIMITS = {
        # Auth endpoints - stricter limits
        "/api/auth/login": (5, 60),  # 5 per minute
        "/api/auth/register": (3, 60),  # 3 per minute
        "/api/auth/refresh": (10, 60),  # 10 per minute
        
        # File upload - moderate limits
        "/api/orders/.*/upload": (10, 60),  # 10 per minute
        "/api/orders/.*/proof": (10, 60),
        
        # Admin endpoints - standard limits
        "/api/orders/admin/.*": (30, 60),
        
        # Tracking - higher limits for real-time
        "/api/tracking/.*": (60, 60),
        
        # WebSocket - very permissive
        "/ws/.*": (1000, 60),
    }
    
    # Limits by user role
    ROLE_MULTIPLIERS = {
        "admin": 2.0,  # Admins get 2x the limit
        "driver": 1.5,  # Drivers get 1.5x
        "client": 1.0,  # Standard limits
    }
    
    # Exempt IPs (localhost, internal networks)
    EXEMPT_IPS = {
        "127.0.0.1",
        "::1",
        "localhost",
    }
    
    # Response headers
    RATE_LIMIT_HEADER = "X-RateLimit-Limit"
    RATE_REMAINING_HEADER = "X-RateLimit-Remaining"
    RATE_RESET_HEADER = "X-RateLimit-Reset"


class RateLimiter:
    """
    Sliding window rate limiter.
    
    Uses a sliding window algorithm for accurate rate limiting
    without the burst issues of fixed windows.
    """
    
    def __init__(self):
        # Store: key -> list of (timestamp, count) tuples
        self._requests: dict[str, list[tuple[float, int]]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self.config = RateLimitConfig()
    
    def _get_key(self, identifier: str, endpoint: str) -> str:
        """Generate rate limit key."""
        # Hash the key for privacy
        raw = f"{identifier}:{endpoint}"
        return hashlib.md5(raw.encode()).hexdigest()
    
    def _get_limit_for_endpoint(self, path: str) -> tuple[int, int]:
        """Get rate limit for specific endpoint."""
        import re
        
        for pattern, limits in self.config.ENDPOINT_LIMITS.items():
            if re.match(pattern, path):
                return limits
        
        return (self.config.DEFAULT_RATE_LIMIT, self.config.DEFAULT_WINDOW_SECONDS)
    
    async def is_allowed(
        self,
        identifier: str,
        endpoint: str,
        user_role: Optional[str] = None,
    ) -> tuple[bool, int, int, int]:
        """
        Check if request is allowed under rate limit.
        
        Args:
            identifier: IP address or user ID
            endpoint: Request path
            user_role: User role for multiplier
            
        Returns:
            Tuple of (allowed, limit, remaining, reset_time)
        """
        async with self._lock:
            key = self._get_key(identifier, endpoint)
            now = time.time()
            
            # Get limits for this endpoint
            base_limit, window_seconds = self._get_limit_for_endpoint(endpoint)
            
            # Apply role multiplier
            if user_role:
                multiplier = self.config.ROLE_MULTIPLIERS.get(user_role, 1.0)
                limit = int(base_limit * multiplier)
            else:
                limit = base_limit
            
            # Clean up old entries
            window_start = now - window_seconds
            self._requests[key] = [
                (ts, count) for ts, count in self._requests[key]
                if ts > window_start
            ]
            
            # Count requests in window
            current_count = sum(count for _, count in self._requests[key])
            
            # Calculate remaining and reset time
            remaining = max(0, limit - current_count)
            
            if self._requests[key]:
                oldest = min(ts for ts, _ in self._requests[key])
                reset_time = int(oldest + window_seconds)
            else:
                reset_time = int(now + window_seconds)
            
            # Check if allowed
            if current_count >= limit:
                return (False, limit, 0, reset_time)
            
            # Record this request
            self._requests[key].append((now, 1))
            
            return (True, limit, remaining - 1, reset_time)
    
    def _cleanup(self):
        """Remove expired entries."""
        now = time.time()
        cutoff = now - 3600  # Keep last hour
        
        for key in list(self._requests.keys()):
            self._requests[key] = [
                (ts, count) for ts, count in self._requests[key]
                if ts > cutoff
            ]
            if not self._requests[key]:
                del self._requests[key]


# Global rate limiter instance
rate_limiter = RateLimiter()


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Rate limiting middleware.
    
    Limits requests per IP address and per authenticated user.
    Returns 429 Too Many Requests when limit exceeded.
    """
    
    async def dispatch(
        self,
        request: Request,
        call_next: Callable,
    ) -> Response:
        # Get client identifier
        client_ip = self._get_client_ip(request)
        
        # Skip rate limiting for exempt IPs
        if client_ip in RateLimitConfig.EXEMPT_IPS:
            return await call_next(request)
        
        # Skip WebSocket upgrade requests
        if request.headers.get("upgrade", "").lower() == "websocket":
            return await call_next(request)
        
        # Get user info if authenticated
        user_role = None
        user_id = None
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            try:
                from shared.common.security import decode_token
                token = auth_header.split(" ")[1]
                payload = decode_token(token)
                user_role = payload.get("role")
                user_id = payload.get("sub")
            except Exception:
                pass  # Not authenticated, use IP-based limiting
        
        # Use user ID if available, otherwise IP
        identifier = f"user:{user_id}" if user_id else f"ip:{client_ip}"
        
        # Check rate limit
        allowed, limit, remaining, reset_time = await rate_limiter.is_allowed(
            identifier,
            request.url.path,
            user_role,
        )
        
        if not allowed:
            logger.warning(
                "Rate limit exceeded: identifier=%s, path=%s",
                identifier, request.url.path,
            )
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": {
                        "code": "RATE_LIMIT_EXCEEDED",
                        "message": "Too many requests. Please try again later.",
                        "retry_after": reset_time - int(time.time()),
                    }
                },
                headers={
                    RateLimitConfig.RATE_LIMIT_HEADER: str(limit),
                    RateLimitConfig.RATE_REMAINING_HEADER: "0",
                    RateLimitConfig.RATE_RESET_HEADER: str(reset_time),
                    "Retry-After": str(reset_time - int(time.time())),
                },
            )
        
        # Process request
        response = await call_next(request)
        
        # Add rate limit headers
        response.headers[RateLimitConfig.RATE_LIMIT_HEADER] = str(limit)
        response.headers[RateLimitConfig.RATE_REMAINING_HEADER] = str(remaining)
        response.headers[RateLimitConfig.RATE_RESET_HEADER] = str(reset_time)
        
        return response
    
    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from request, considering proxies."""
        # Check X-Forwarded-For header (for proxied requests)
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            # Take the first IP (original client)
            return forwarded_for.split(",")[0].strip()
        
        # Check X-Real-IP header
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip.strip()
        
        # Fall back to direct client IP
        if request.client:
            return request.client.host
        
        return "unknown"


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    Request logging middleware.
    
    Logs all requests with timing and response status.
    Adds request ID for tracing.
    """
    
    async def dispatch(
        self,
        request: Request,
        call_next: Callable,
    ) -> Response:
        # Generate request ID
        request_id = str(uuid.uuid4())[:8]
        request.state.request_id = request_id
        
        # Record start time
        start_time = time.time()
        
        # Extract info
        method = request.method
        path = request.url.path
        client_ip = self._get_client_ip(request)
        
        # Get user info if available
        user_id = None
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            try:
                from shared.common.security import decode_token
                payload = decode_token(auth_header.split(" ")[1])
                user_id = payload.get("sub")
            except Exception:
                pass
        
        try:
            # Process request
            response = await call_next(request)
            
            # Calculate duration
            duration_ms = int((time.time() - start_time) * 1000)
            
            # Log request
            log_level = logging.INFO if response.status_code < 400 else logging.WARNING
            logger.log(
                log_level,
                "request_id=%s method=%s path=%s status=%d duration=%dms ip=%s user=%s",
                request_id, method, path, response.status_code,
                duration_ms, client_ip, user_id,
            )
            
            # Add request ID to response headers
            response.headers["X-Request-ID"] = request_id
            
            return response
            
        except Exception as e:
            # Log error
            duration_ms = int((time.time() - start_time) * 1000)
            logger.error(
                "request_id=%s method=%s path=%s error=%s duration=%dms ip=%s",
                request_id, method, path, str(e), duration_ms, client_ip,
            )
            raise
    
    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP from request."""
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        if request.client:
            return request.client.host
        return "unknown"


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Security headers middleware.
    
    Adds security-related HTTP headers to all responses.
    """
    
    async def dispatch(
        self,
        request: Request,
        call_next: Callable,
    ) -> Response:
        response = await call_next(request)
        
        # Security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        
        # HSTS header (only for HTTPS)
        if request.url.scheme == "https":
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        
        # Cache control for API responses
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
            response.headers["Pragma"] = "no-cache"
        
        return response


def setup_cors(app: FastAPI, allowed_origins: Optional[list[str]] = None):
    """
    Setup CORS middleware with secure defaults.
    
    Args:
        app: FastAPI application
        allowed_origins: List of allowed origins (default: localhost)
    """
    if allowed_origins is None:
        allowed_origins = [
            "http://localhost:3000",
            "http://localhost:8000",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:8000",
        ]
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
        allow_headers=[
            "Authorization",
            "Content-Type",
            "X-Request-ID",
            "Accept",
        ],
        expose_headers=[
            "X-Request-ID",
            "X-RateLimit-Limit",
            "X-RateLimit-Remaining",
            "X-RateLimit-Reset",
        ],
        max_age=3600,  # Cache preflight for 1 hour
    )
    
    logger.info("CORS middleware configured with origins: %s", allowed_origins)


def setup_security_middleware(
    app: FastAPI,
    enable_rate_limiting: bool = True,
    enable_request_logging: bool = True,
    enable_security_headers: bool = True,
    cors_origins: Optional[list[str]] = None,
):
    """
    Setup all security middleware for the application.
    
    Args:
        app: FastAPI application
        enable_rate_limiting: Enable rate limiting middleware
        enable_request_logging: Enable request logging middleware
        enable_security_headers: Enable security headers middleware
        cors_origins: List of allowed CORS origins
    """
    # Setup CORS first (outermost middleware)
    setup_cors(app, cors_origins)
    
    # Add middleware in order (last added = first executed)
    if enable_security_headers:
        app.add_middleware(SecurityHeadersMiddleware)
    
    if enable_request_logging:
        app.add_middleware(RequestLoggingMiddleware)
    
    if enable_rate_limiting:
        app.add_middleware(RateLimitMiddleware)
    
    logger.info(
        "Security middleware configured: rate_limiting=%s, logging=%s, headers=%s",
        enable_rate_limiting, enable_request_logging, enable_security_headers,
    )


# ── Token Validation Utilities ───────────────────────────────

async def validate_websocket_token(token: str) -> Optional[dict]:
    """
    Validate JWT token for WebSocket connections.
    
    Args:
        token: JWT token string
        
    Returns:
        Token payload if valid, None otherwise
    """
    try:
        from shared.common.security import decode_token
        payload = decode_token(token)
        return payload
    except Exception:
        return None


def get_token_from_query(request: Request) -> Optional[str]:
    """
    Extract token from query parameter (for WebSocket).
    
    Args:
        request: FastAPI request
        
    Returns:
        Token string or None
    """
    return request.query_params.get("token")
