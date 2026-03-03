"""
Integration Orchestrator
========================

Central orchestrator for all middleware integrations.
Coordinates order transmission to CMS, ROS, and WMS systems.

Provides:
- Unified interface for multi-system integration
- Retry logic with exponential backoff
- Integration event logging
- Error handling and compensation

Usage:
    orchestrator = IntegrationOrchestrator(
        cms_url="http://mock-cms:8004",
        ros_url="http://mock-ros:8005",
        wms_host="mock-wms",
        wms_port=9000
    )
    
    result = await orchestrator.process_new_order(order_data)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from .cms_client import CMSClient, CMSSoapError
from .ros_client import ROSClient, ROSApiError
from .wms_client import WMSClient, WMSProtocolError
from .transformers import DataTransformer

logger = logging.getLogger(__name__)


class IntegrationOrchestrator:
    """
    Orchestrates multi-system integration for order processing.
    
    Coordinates calls to:
    - CMS (SOAP/XML): Client validation and billing
    - ROS (REST/JSON): Route optimization
    - WMS (TCP/IP): Warehouse package registration
    """
    
    def __init__(
        self,
        cms_url: str = "http://mock-cms:8004",
        ros_url: str = "http://mock-ros:8005",
        wms_host: str = "mock-wms",
        wms_port: int = 9000,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize integration orchestrator.
        
        Args:
            cms_url: CMS SOAP service URL
            ros_url: ROS REST API URL
            wms_host: WMS TCP server hostname
            wms_port: WMS TCP server port
            max_retries: Maximum retry attempts per system
            retry_delay: Base delay between retries (exponential backoff)
        """
        self.cms_url = cms_url
        self.ros_url = ros_url
        self.wms_host = wms_host
        self.wms_port = wms_port
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.transformer = DataTransformer()
    
    async def process_new_order(
        self,
        order_data: dict,
        skip_cms: bool = False,
        skip_ros: bool = False,
        skip_wms: bool = False
    ) -> dict[str, Any]:
        """
        Process new order through all integration systems.
        
        Steps:
        1. CMS: Validate client and create billing
        2. WMS: Register package at warehouse
        3. ROS: Request route optimization
        
        Args:
            order_data: Order data dict
            skip_cms: Skip CMS integration
            skip_ros: Skip ROS integration
            skip_wms: Skip WMS integration
            
        Returns:
            Integration result with status from all systems
        """
        order_id = order_data.get("order_id", "unknown")
        results: dict[str, Any] = {
            "order_id": order_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cms": None,
            "wms": None,
            "ros": None,
            "success": True,
            "errors": []
        }
        
        logger.info("Starting integration processing for order %s", order_id)
        
        # Step 1: CMS Integration (SOAP/XML)
        if not skip_cms:
            try:
                results["cms"] = await self._process_cms(order_data)
                logger.info("CMS integration successful for order %s", order_id)
            except Exception as e:
                results["cms"] = {"error": str(e), "success": False}
                results["errors"].append(f"CMS: {e}")
                results["success"] = False
                logger.error("CMS integration failed for order %s: %s", order_id, e)
        
        # Step 2: WMS Integration (TCP/IP)
        if not skip_wms:
            try:
                results["wms"] = await self._process_wms(order_data)
                logger.info("WMS integration successful for order %s", order_id)
            except Exception as e:
                results["wms"] = {"error": str(e), "success": False}
                results["errors"].append(f"WMS: {e}")
                # WMS failure doesn't stop the order
                logger.error("WMS integration failed for order %s: %s", order_id, e)
        
        # Step 3: ROS Integration (REST/JSON)
        if not skip_ros:
            try:
                results["ros"] = await self._process_ros(order_data)
                logger.info("ROS integration successful for order %s", order_id)
            except Exception as e:
                results["ros"] = {"error": str(e), "success": False}
                results["errors"].append(f"ROS: {e}")
                # ROS failure doesn't stop the order
                logger.error("ROS integration failed for order %s: %s", order_id, e)
        
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        
        if results["errors"]:
            logger.warning(
                "Order %s integration completed with %d error(s)",
                order_id, len(results["errors"])
            )
        else:
            logger.info("Order %s integration completed successfully", order_id)
        
        return results
    
    async def _process_cms(self, order_data: dict) -> dict[str, Any]:
        """
        Process order through CMS (SOAP integration).
        
        Converts order to SOAP XML, sends to CMS, parses response.
        """
        for attempt in range(self.max_retries):
            try:
                async with CMSClient(self.cms_url) as cms:
                    result = await cms.transmit_order(order_data)
                    return {
                        "success": True,
                        "client_validated": result.get("client_validated"),
                        "client_info": result.get("client_info"),
                        "message": result.get("message"),
                        "protocol": "SOAP/XML"
                    }
            except CMSSoapError as e:
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(
                        "CMS attempt %d failed, retrying in %.1fs: %s",
                        attempt + 1, delay, e
                    )
                    await asyncio.sleep(delay)
                else:
                    raise
            except Exception as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise CMSSoapError("Connection", str(e))
        
        raise CMSSoapError("MaxRetries", "All retry attempts failed")
    
    async def _process_wms(self, order_data: dict) -> dict[str, Any]:
        """
        Process order through WMS (TCP/IP integration).
        
        Sends TCP command, parses proprietary response.
        """
        for attempt in range(self.max_retries):
            try:
                async with WMSClient(self.wms_host, self.wms_port) as wms:
                    result = await wms.register_order(order_data)
                    return {
                        "success": result.get("success"),
                        "event": result.get("event"),
                        "package_id": result.get("package", {}).get("package_id"),
                        "status": result.get("package", {}).get("status"),
                        "protocol": "TCP/IP"
                    }
            except WMSProtocolError as e:
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(
                        "WMS attempt %d failed, retrying in %.1fs: %s",
                        attempt + 1, delay, e
                    )
                    await asyncio.sleep(delay)
                else:
                    raise
            except Exception as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise WMSProtocolError(str(e))
        
        raise WMSProtocolError("All retry attempts failed")
    
    async def _process_ros(self, order_data: dict) -> dict[str, Any]:
        """
        Process order through ROS (REST integration).
        
        Sends REST request, receives JSON route response.
        """
        for attempt in range(self.max_retries):
            try:
                async with ROSClient(self.ros_url) as ros:
                    route = await ros.optimise_route_from_order(order_data)
                    return {
                        "success": True,
                        "route_id": route.get("route_id"),
                        "distance_km": route.get("estimated_distance_km"),
                        "duration_min": route.get("estimated_duration_min"),
                        "waypoints": route.get("optimised_waypoints"),
                        "protocol": "REST/JSON"
                    }
            except ROSApiError as e:
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(
                        "ROS attempt %d failed, retrying in %.1fs: %s",
                        attempt + 1, delay, e
                    )
                    await asyncio.sleep(delay)
                else:
                    raise
            except Exception as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise ROSApiError(503, str(e))
        
        raise ROSApiError(503, "All retry attempts failed")
    
    # ══════════════════════════════════════════════════════════
    # Individual System Operations
    # ══════════════════════════════════════════════════════════
    
    async def validate_client(self, client_id: int) -> dict[str, Any]:
        """
        Validate client in CMS.
        
        Args:
            client_id: Client ID to validate
            
        Returns:
            Validation result
        """
        async with CMSClient(self.cms_url) as cms:
            return await cms.validate_client(client_id)
    
    async def get_client_info(self, client_id: int) -> dict[str, Any]:
        """
        Get client information from CMS.
        
        Args:
            client_id: Client ID
            
        Returns:
            Client info dict
        """
        async with CMSClient(self.cms_url) as cms:
            return await cms.get_client_info(client_id)
    
    async def optimize_route(
        self,
        order_id: str,
        pickup: str,
        delivery: str
    ) -> dict[str, Any]:
        """
        Get optimized route from ROS.
        
        Args:
            order_id: Order ID
            pickup: Pickup address
            delivery: Delivery address
            
        Returns:
            Optimized route data
        """
        async with ROSClient(self.ros_url) as ros:
            return await ros.optimise_route(order_id, pickup, delivery)
    
    async def check_warehouse_status(self, order_id: str) -> dict[str, Any]:
        """
        Check package status in WMS.
        
        Args:
            order_id: Order ID
            
        Returns:
            Package status from WMS
        """
        async with WMSClient(self.wms_host, self.wms_port) as wms:
            return await wms.check_status(order_id)
    
    async def load_for_delivery(self, order_id: str) -> dict[str, Any]:
        """
        Mark package as loaded in WMS.
        
        Args:
            order_id: Order ID
            
        Returns:
            Load confirmation
        """
        async with WMSClient(self.wms_host, self.wms_port) as wms:
            return await wms.load_vehicle(order_id)
    
    # ══════════════════════════════════════════════════════════
    # Health Checks
    # ══════════════════════════════════════════════════════════
    
    async def health_check_all(self) -> dict[str, Any]:
        """
        Check health of all integration systems.
        
        Returns:
            Health status for CMS, ROS, WMS
        """
        results = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cms": {"healthy": False, "protocol": "SOAP/XML"},
            "ros": {"healthy": False, "protocol": "REST/JSON"},
            "wms": {"healthy": False, "protocol": "TCP/IP"}
        }
        
        # CMS health check
        try:
            async with CMSClient(self.cms_url, timeout=5.0) as cms:
                await cms.validate_client(1)
                results["cms"]["healthy"] = True
        except Exception as e:
            results["cms"]["error"] = str(e)
        
        # ROS health check
        try:
            async with ROSClient(self.ros_url, timeout=5.0) as ros:
                results["ros"]["healthy"] = await ros.health_check()
        except Exception as e:
            results["ros"]["error"] = str(e)
        
        # WMS health check
        try:
            async with WMSClient(self.wms_host, self.wms_port, timeout=5.0) as wms:
                results["wms"]["healthy"] = True
        except Exception as e:
            results["wms"]["error"] = str(e)
        
        results["all_healthy"] = all(
            results[sys]["healthy"] for sys in ["cms", "ros", "wms"]
        )
        
        return results


# ══════════════════════════════════════════════════════════════
# Factory Function
# ══════════════════════════════════════════════════════════════

def create_orchestrator(
    cms_url: str | None = None,
    ros_url: str | None = None,
    wms_host: str | None = None,
    wms_port: int | None = None
) -> IntegrationOrchestrator:
    """
    Create integration orchestrator with configuration.
    
    Reads from environment or uses defaults.
    """
    import os
    
    return IntegrationOrchestrator(
        cms_url=cms_url or os.getenv("CMS_URL", "http://mock-cms:8004"),
        ros_url=ros_url or os.getenv("ROS_URL", "http://mock-ros:8005"),
        wms_host=wms_host or os.getenv("WMS_HOST", "mock-wms"),
        wms_port=wms_port or int(os.getenv("WMS_PORT", "9000"))
    )
