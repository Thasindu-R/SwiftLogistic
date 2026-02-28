"""
ROS Client – REST/JSON Integration
==================================

Integrates with the Route Optimization System (ROS) via REST API.
Sends order details and receives optimized routes in JSON format.

Endpoints:
- POST /api/v1/routes/optimise - Request route optimization
- GET /api/v1/routes/{route_id} - Get existing route
- GET /api/v1/routes - List all routes

Usage:
    async with ROSClient("http://mock-ros:8004") as client:
        route = await client.optimise_route(order_data)
        print(route["optimised_waypoints"])
"""

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class ROSApiError(Exception):
    """Raised when ROS API call fails."""
    def __init__(self, status_code: int, message: str, details: dict | None = None):
        self.status_code = status_code
        self.message = message
        self.details = details or {}
        super().__init__(f"ROS API Error [{status_code}]: {message}")


class ROSClient:
    """
    REST client for ROS (Route Optimization System) integration.
    
    Sends order details via REST API and receives optimized 
    routes in JSON format with waypoints, distance, and ETA.
    """
    
    def __init__(self, base_url: str, timeout: float = 30.0, api_key: str | None = None):
        """
        Initialize ROS client.
        
        Args:
            base_url: ROS service URL (e.g., http://mock-ros:8004)
            timeout: Request timeout in seconds
            api_key: Optional API key for authentication
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.api_key = api_key
        self._client: httpx.AsyncClient | None = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    def _get_headers(self) -> dict[str, str]:
        """Build request headers."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Request-ID": f"swift-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers
    
    async def _request(
        self, 
        method: str, 
        endpoint: str, 
        json_data: dict | None = None
    ) -> dict[str, Any]:
        """
        Make HTTP request to ROS API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            json_data: Optional JSON body for POST/PUT
            
        Returns:
            Parsed JSON response
            
        Raises:
            ROSApiError: If request fails
        """
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()
        
        logger.debug("ROS %s %s", method, url)
        if json_data:
            logger.debug("ROS request body: %s", json_data)
        
        try:
            response = await self._client.request(
                method=method,
                url=url,
                headers=headers,
                json=json_data
            )
            
            logger.debug("ROS response status: %d", response.status_code)
            
            if response.status_code >= 400:
                try:
                    error_detail = response.json()
                except Exception:
                    error_detail = {"raw": response.text}
                
                raise ROSApiError(
                    response.status_code,
                    error_detail.get("detail", response.text),
                    error_detail
                )
            
            return response.json()
            
        except httpx.TimeoutException:
            raise ROSApiError(504, "Request timeout", {"timeout": self.timeout})
        except httpx.ConnectError as e:
            raise ROSApiError(503, f"Connection failed: {e}", {})
    
    async def optimise_route(
        self,
        order_id: str,
        pickup_address: str,
        delivery_address: str,
        vehicle_type: str = "motorcycle",
        priority: str = "normal"
    ) -> dict[str, Any]:
        """
        Request route optimization for a delivery.
        
        Args:
            order_id: Order ID for tracking
            pickup_address: Pickup location address
            delivery_address: Delivery destination address
            vehicle_type: Vehicle type (motorcycle, van, truck)
            priority: Route priority (normal, high, urgent)
            
        Returns:
            Optimized route data:
            {
                "route_id": str,
                "order_id": str,
                "pickup_address": str,
                "delivery_address": str,
                "estimated_distance_km": float,
                "estimated_duration_min": int,
                "optimised_waypoints": [
                    {"lat": float, "lng": float, "label": str}, ...
                ],
                "status": str
            }
        """
        request_data = {
            "order_id": order_id,
            "pickup_address": pickup_address,
            "delivery_address": delivery_address,
            "vehicle_type": vehicle_type,
            "priority": priority
        }
        
        logger.info(
            "ROS: Requesting route optimization for order %s (%s → %s)",
            order_id, pickup_address[:30], delivery_address[:30]
        )
        
        result = await self._request("POST", "/api/v1/routes/optimise", request_data)
        
        logger.info(
            "ROS: Route optimized for order %s - %.1f km, ~%d min",
            order_id,
            result.get("estimated_distance_km", 0),
            result.get("estimated_duration_min", 0)
        )
        
        return result
    
    async def optimise_route_from_order(self, order_data: dict) -> dict[str, Any]:
        """
        Request route optimization from order data dict.
        
        Args:
            order_data: Order dict with pickup_address, delivery_address, etc.
            
        Returns:
            Optimized route data
        """
        return await self.optimise_route(
            order_id=order_data.get("order_id", ""),
            pickup_address=order_data.get("pickup_address", ""),
            delivery_address=order_data.get("delivery_address", ""),
            vehicle_type=order_data.get("vehicle_type", "motorcycle"),
            priority=order_data.get("priority", "normal")
        )
    
    async def get_route(self, route_id: str) -> dict[str, Any]:
        """
        Get existing route by ID.
        
        Args:
            route_id: Route ID to retrieve
            
        Returns:
            Route data if found
            
        Raises:
            ROSApiError: If route not found (404)
        """
        logger.info("ROS: Getting route %s", route_id)
        return await self._request("GET", f"/api/v1/routes/{route_id}")
    
    async def list_routes(
        self, 
        limit: int = 100, 
        status: str | None = None
    ) -> list[dict[str, Any]]:
        """
        List all routes, optionally filtered.
        
        Args:
            limit: Maximum results to return
            status: Filter by status (optimised, pending, failed)
            
        Returns:
            List of route objects
        """
        logger.info("ROS: Listing routes (limit=%d)", limit)
        routes = await self._request("GET", "/api/v1/routes")
        
        if status:
            routes = [r for r in routes if r.get("status") == status]
        
        return routes[:limit]
    
    async def health_check(self) -> bool:
        """
        Check ROS service health.
        
        Returns:
            True if service is healthy
        """
        try:
            result = await self._request("GET", "/health")
            return result.get("status") == "healthy"
        except Exception as e:
            logger.warning("ROS health check failed: %s", e)
            return False


# ── Route Helper Functions ───────────────────────────────────

def format_route_for_manifest(route: dict) -> dict:
    """
    Format ROS route data for delivery manifest storage.
    
    Args:
        route: Raw route from ROS
        
    Returns:
        Formatted route_data dict for manifest
    """
    waypoints = route.get("optimised_waypoints", [])
    
    stops = []
    for idx, wp in enumerate(waypoints):
        stops.append({
            "sequence": idx + 1,
            "address": wp.get("label", f"Stop {idx + 1}"),
            "lat": wp.get("lat"),
            "lng": wp.get("lng"),
            "eta": None,  # Would be calculated based on route timing
            "distance": None
        })
    
    return {
        "route_id": route.get("route_id"),
        "total_distance_km": route.get("estimated_distance_km"),
        "total_duration_min": route.get("estimated_duration_min"),
        "stops": stops,
        "optimized_at": route.get("created_at"),
        "vehicle_type": route.get("vehicle_type", "motorcycle")
    }


def calculate_driver_route(orders: list[dict], ros_data: list[dict]) -> dict:
    """
    Combine multiple order routes into driver manifest route.
    
    Args:
        orders: List of orders for the driver
        ros_data: List of ROS route responses for those orders
        
    Returns:
        Combined route data for driver manifest
    """
    all_stops = []
    total_distance = 0.0
    total_duration = 0
    
    for route in ros_data:
        total_distance += route.get("estimated_distance_km", 0)
        total_duration += route.get("estimated_duration_min", 0)
        
        for wp in route.get("optimised_waypoints", []):
            all_stops.append({
                "address": wp.get("label"),
                "lat": wp.get("lat"),
                "lng": wp.get("lng"),
                "order_id": route.get("order_id"),
                "distance": f"{route.get('estimated_distance_km', 0):.1f} km"
            })
    
    return {
        "stops": all_stops,
        "total_distance_km": round(total_distance, 1),
        "total_duration_min": total_duration,
        "order_count": len(orders),
        "optimized": True
    }


# ── Synchronous convenience wrapper ──────────────────────────
async def get_optimised_route(
    ros_url: str, 
    order_data: dict
) -> dict[str, Any]:
    """
    Quick helper to get optimized route for an order.
    
    Args:
        ros_url: ROS service URL
        order_data: Order data dict
        
    Returns:
        Optimized route or error dict
    """
    try:
        async with ROSClient(ros_url) as client:
            return await client.optimise_route_from_order(order_data)
    except ROSApiError as e:
        logger.error("Route optimization failed: %s", e)
        return {
            "error": True,
            "message": str(e),
            "status_code": e.status_code
        }
