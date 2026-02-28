"""
Admin Dashboard Service – Aggregates system-wide metrics and status.

Provides comprehensive administrative oversight for SwiftLogistics including:
- Dashboard Overview (orders, deliveries, failed deliveries, system status)
- Integration Status Monitoring (CMS/ROS/WMS connectivity)
- Real-time metrics and alerts
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, Dict, Any, List

from sqlalchemy import select, func as sqlfunc, case, and_, or_, text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class SystemHealth(str, Enum):
    """System health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class IntegrationSystem(str, Enum):
    """External integration systems."""
    CMS = "cms"
    ROS = "ros"
    WMS = "wms"
    RABBITMQ = "rabbitmq"
    POSTGRES = "postgres"


@dataclass
class IntegrationStatus:
    """Status of an external integration system."""
    system: str
    status: SystemHealth
    last_check: Optional[datetime] = None
    response_time_ms: Optional[int] = None
    error_message: Optional[str] = None
    success_rate_24h: float = 0.0
    total_calls_24h: int = 0
    failed_calls_24h: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "system": self.system,
            "status": self.status.value,
            "last_check": self.last_check.isoformat() if self.last_check else None,
            "response_time_ms": self.response_time_ms,
            "error_message": self.error_message,
            "success_rate_24h": round(self.success_rate_24h, 2),
            "total_calls_24h": self.total_calls_24h,
            "failed_calls_24h": self.failed_calls_24h,
        }


@dataclass
class DeliveryMetrics:
    """Delivery performance metrics."""
    total_today: int = 0
    successful_today: int = 0
    failed_today: int = 0
    in_progress: int = 0
    pending_pickup: int = 0
    avg_delivery_time_hours: float = 0.0
    on_time_rate: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_today": self.total_today,
            "successful_today": self.successful_today,
            "failed_today": self.failed_today,
            "in_progress": self.in_progress,
            "pending_pickup": self.pending_pickup,
            "avg_delivery_time_hours": round(self.avg_delivery_time_hours, 2),
            "on_time_rate": round(self.on_time_rate, 2),
        }


@dataclass
class SystemStatus:
    """Overall system status summary."""
    overall_health: SystemHealth = SystemHealth.UNKNOWN
    services: Dict[str, SystemHealth] = field(default_factory=dict)
    integrations: Dict[str, IntegrationStatus] = field(default_factory=dict)
    queue_health: SystemHealth = SystemHealth.UNKNOWN
    database_health: SystemHealth = SystemHealth.UNKNOWN
    pending_messages: int = 0
    dlq_messages: int = 0
    active_sagas: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "overall_health": self.overall_health.value,
            "services": {k: v.value for k, v in self.services.items()},
            "integrations": {k: v.to_dict() for k, v in self.integrations.items()},
            "queue_health": self.queue_health.value,
            "database_health": self.database_health.value,
            "pending_messages": self.pending_messages,
            "dlq_messages": self.dlq_messages,
            "active_sagas": self.active_sagas,
        }


@dataclass
class DashboardOverview:
    """Complete dashboard overview for admin panel."""
    # Order statistics
    total_orders: int = 0
    orders_today: int = 0
    orders_this_week: int = 0
    orders_by_status: Dict[str, int] = field(default_factory=dict)
    
    # Delivery metrics
    active_deliveries: int = 0
    completed_deliveries: int = 0
    failed_deliveries: int = 0
    delivery_success_rate: float = 0.0
    
    # User statistics
    total_clients: int = 0
    total_drivers: int = 0
    active_drivers: int = 0
    
    # Integration statistics
    integration_success_rate: float = 0.0
    pending_integrations: int = 0
    failed_integrations: int = 0
    
    # System status
    system_status: Optional[SystemStatus] = None
    
    # Alerts
    alerts: List[Dict[str, Any]] = field(default_factory=list)
    
    # Timestamp
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "orders": {
                "total": self.total_orders,
                "today": self.orders_today,
                "this_week": self.orders_this_week,
                "by_status": self.orders_by_status,
            },
            "deliveries": {
                "active": self.active_deliveries,
                "completed": self.completed_deliveries,
                "failed": self.failed_deliveries,
                "success_rate": round(self.delivery_success_rate, 2),
            },
            "users": {
                "total_clients": self.total_clients,
                "total_drivers": self.total_drivers,
                "active_drivers": self.active_drivers,
            },
            "integrations": {
                "success_rate": round(self.integration_success_rate, 2),
                "pending": self.pending_integrations,
                "failed": self.failed_integrations,
            },
            "system_status": self.system_status.to_dict() if self.system_status else None,
            "alerts": self.alerts,
            "generated_at": self.generated_at.isoformat(),
        }


class AdminDashboardService:
    """
    Service class for administrative dashboard functionalities.
    
    Aggregates data from various services to provide:
    - Dashboard overview
    - Integration status monitoring
    - System health indicators
    - Alerts and notifications
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self._integration_clients = {}
    
    async def get_dashboard_overview(self) -> DashboardOverview:
        """Get complete dashboard overview with all metrics."""
        overview = DashboardOverview()
        
        # Get order statistics
        await self._fetch_order_stats(overview)
        
        # Get delivery metrics
        await self._fetch_delivery_metrics(overview)
        
        # Get user statistics
        await self._fetch_user_stats(overview)
        
        # Get integration statistics
        await self._fetch_integration_stats(overview)
        
        # Get system status
        overview.system_status = await self.get_system_status()
        
        # Generate alerts
        overview.alerts = await self._generate_alerts(overview)
        
        return overview
    
    async def _fetch_order_stats(self, overview: DashboardOverview) -> None:
        """Fetch order statistics from database."""
        now = datetime.now(timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = today_start - timedelta(days=today_start.weekday())
        
        # Total orders and by status
        result = await self.db.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE created_at >= :today) as today_count,
                COUNT(*) FILTER (WHERE created_at >= :week) as week_count,
                COUNT(*) FILTER (WHERE status = 'pending') as pending,
                COUNT(*) FILTER (WHERE status = 'confirmed') as confirmed,
                COUNT(*) FILTER (WHERE status = 'processing') as processing,
                COUNT(*) FILTER (WHERE status = 'in_transit') as in_transit,
                COUNT(*) FILTER (WHERE status = 'delivered') as delivered,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COUNT(*) FILTER (WHERE status = 'cancelled') as cancelled
            FROM orders
        """), {"today": today_start, "week": week_start})
        
        row = result.fetchone()
        if row:
            overview.total_orders = row.total or 0
            overview.orders_today = row.today_count or 0
            overview.orders_this_week = row.week_count or 0
            overview.orders_by_status = {
                "pending": row.pending or 0,
                "confirmed": row.confirmed or 0,
                "processing": row.processing or 0,
                "in_transit": row.in_transit or 0,
                "delivered": row.delivered or 0,
                "failed": row.failed or 0,
                "cancelled": row.cancelled or 0,
            }
    
    async def _fetch_delivery_metrics(self, overview: DashboardOverview) -> None:
        """Fetch delivery metrics from database."""
        # Active deliveries (in_transit status)
        result = await self.db.execute(text("""
            SELECT 
                COUNT(*) FILTER (WHERE status IN ('in_transit', 'processing')) as active,
                COUNT(*) FILTER (WHERE status = 'delivered') as completed,
                COUNT(*) FILTER (WHERE status = 'failed') as failed
            FROM orders
        """))
        
        row = result.fetchone()
        if row:
            overview.active_deliveries = row.active or 0
            overview.completed_deliveries = row.completed or 0
            overview.failed_deliveries = row.failed or 0
            
            total = overview.completed_deliveries + overview.failed_deliveries
            if total > 0:
                overview.delivery_success_rate = (overview.completed_deliveries / total) * 100
    
    async def _fetch_user_stats(self, overview: DashboardOverview) -> None:
        """Fetch user statistics from database."""
        result = await self.db.execute(text("""
            SELECT 
                COUNT(*) FILTER (WHERE role = 'client' AND is_active = true) as clients,
                COUNT(*) FILTER (WHERE role = 'driver') as total_drivers,
                COUNT(*) FILTER (WHERE role = 'driver' AND is_active = true) as active_drivers
            FROM users
        """))
        
        row = result.fetchone()
        if row:
            overview.total_clients = row.clients or 0
            overview.total_drivers = row.total_drivers or 0
            overview.active_drivers = row.active_drivers or 0
    
    async def _fetch_integration_stats(self, overview: DashboardOverview) -> None:
        """Fetch integration event statistics from database."""
        now = datetime.now(timezone.utc)
        day_ago = now - timedelta(hours=24)
        
        # Try enhanced integration_event_logs table first
        try:
            result = await self.db.execute(text("""
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'success') as success_count,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                    COUNT(*) as total
                FROM integration_event_logs
                WHERE created_at >= :since
            """), {"since": day_ago})
            
            row = result.fetchone()
            if row and row.total > 0:
                overview.integration_success_rate = (row.success_count / row.total) * 100
                overview.pending_integrations = row.pending_count or 0
                overview.failed_integrations = row.failed_count or 0
                return
        except Exception:
            pass
        
        # Fallback to integration_events table
        try:
            result = await self.db.execute(text("""
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'success') as success_count,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                    COUNT(*) as total
                FROM integration_events
                WHERE created_at >= :since
            """), {"since": day_ago})
            
            row = result.fetchone()
            if row and row.total > 0:
                overview.integration_success_rate = (row.success_count / row.total) * 100
                overview.pending_integrations = row.pending_count or 0
                overview.failed_integrations = row.failed_count or 0
        except Exception as e:
            logger.warning("Could not fetch integration stats: %s", e)
    
    async def get_system_status(self) -> SystemStatus:
        """Get overall system status with all service health indicators."""
        status = SystemStatus()
        
        # Check database health
        status.database_health = await self._check_database_health()
        
        # Check integration systems
        status.integrations = await self._check_integration_systems()
        
        # Check queue health
        status.queue_health = await self._check_queue_health()
        
        # Get DLQ and saga counts
        await self._fetch_queue_metrics(status)
        
        # Calculate overall health
        status.overall_health = self._calculate_overall_health(status)
        
        return status
    
    async def _check_database_health(self) -> SystemHealth:
        """Check database connectivity and health."""
        try:
            result = await self.db.execute(text("SELECT 1"))
            result.fetchone()
            return SystemHealth.HEALTHY
        except Exception as e:
            logger.error("Database health check failed: %s", e)
            return SystemHealth.UNHEALTHY
    
    async def _check_integration_systems(self) -> Dict[str, IntegrationStatus]:
        """Check health of external integration systems."""
        integrations = {}
        
        # Import integration clients and config
        try:
            from shared.common.integrations import CMSClient, ROSClient, WMSClient
            from shared.common.config import settings
            
            # Check CMS (SOAP/XML)
            cms_client = CMSClient(settings.CMS_SERVICE_URL)
            cms_status = await self._check_cms_health(cms_client)
            integrations["cms"] = cms_status
            
            # Check ROS (REST/JSON)
            ros_client = ROSClient(settings.ROS_SERVICE_URL)
            ros_status = await self._check_ros_health(ros_client)
            integrations["ros"] = ros_status
            
            # Check WMS (TCP/IP)
            wms_client = WMSClient(settings.WMS_SERVICE_HOST, settings.WMS_SERVICE_PORT)
            wms_status = await self._check_wms_health(wms_client)
            integrations["wms"] = wms_status
            
        except Exception as e:
            logger.error("Failed to check integration systems: %s", e)
            for system in ["cms", "ros", "wms"]:
                integrations[system] = IntegrationStatus(
                    system=system,
                    status=SystemHealth.UNKNOWN,
                    error_message=str(e),
                )
        
        # Get historical stats for each system
        await self._enrich_integration_stats(integrations)
        
        return integrations
    
    async def _check_cms_health(self, client) -> IntegrationStatus:
        """Check CMS (Client Management System) health via SOAP connection test."""
        import time
        start = time.time()
        
        try:
            # Use context manager to establish connection and try validate_client
            # with a test client_id to verify SOAP connectivity
            async with client:
                # Try to reach the CMS endpoint - this will verify connectivity
                # We use validate_client with ID 1 as a health check
                await client.validate_client(client_id=1)
            response_time = int((time.time() - start) * 1000)
            
            return IntegrationStatus(
                system="cms",
                status=SystemHealth.HEALTHY,
                last_check=datetime.now(timezone.utc),
                response_time_ms=response_time,
            )
        except Exception as e:
            return IntegrationStatus(
                system="cms",
                status=SystemHealth.UNHEALTHY,
                last_check=datetime.now(timezone.utc),
                error_message=str(e),
            )
    
    async def _check_ros_health(self, client) -> IntegrationStatus:
        """Check ROS (Route Optimization System) health."""
        import time
        start = time.time()
        
        try:
            await client.health_check()
            response_time = int((time.time() - start) * 1000)
            
            return IntegrationStatus(
                system="ros",
                status=SystemHealth.HEALTHY,
                last_check=datetime.now(timezone.utc),
                response_time_ms=response_time,
            )
        except Exception as e:
            return IntegrationStatus(
                system="ros",
                status=SystemHealth.UNHEALTHY,
                last_check=datetime.now(timezone.utc),
                error_message=str(e),
            )
    
    async def _check_wms_health(self, client) -> IntegrationStatus:
        """Check WMS (Warehouse Management System) health via TCP connection test."""
        import time
        start = time.time()
        
        try:
            # Use context manager to establish TCP connection
            # This will verify the WMS TCP server is reachable
            async with client:
                # Try to check status of a test order - this verifies TCP connectivity
                # Even if order doesn't exist, we'll know WMS is reachable if no connection error
                await client.check_status(order_id="health-check-test")
            response_time = int((time.time() - start) * 1000)
            
            return IntegrationStatus(
                system="wms",
                status=SystemHealth.HEALTHY,
                last_check=datetime.now(timezone.utc),
                response_time_ms=response_time,
            )
        except Exception as e:
            error_msg = str(e)
            # If error is just "order not found", WMS is actually healthy
            if "not found" in error_msg.lower() or "unknown order" in error_msg.lower():
                response_time = int((time.time() - start) * 1000)
                return IntegrationStatus(
                    system="wms",
                    status=SystemHealth.HEALTHY,
                    last_check=datetime.now(timezone.utc),
                    response_time_ms=response_time,
                )
            return IntegrationStatus(
                system="wms",
                status=SystemHealth.UNHEALTHY,
                last_check=datetime.now(timezone.utc),
                error_message=error_msg,
            )
    
    async def _check_queue_health(self) -> SystemHealth:
        """Check RabbitMQ queue health."""
        try:
            from shared.common.rabbitmq import rabbitmq_client
            
            if rabbitmq_client._connection and not rabbitmq_client._connection.is_closed:
                return SystemHealth.HEALTHY
            
            # Try to connect
            await rabbitmq_client.connect()
            return SystemHealth.HEALTHY
        except Exception as e:
            logger.error("Queue health check failed: %s", e)
            return SystemHealth.UNHEALTHY
    
    async def _enrich_integration_stats(self, integrations: Dict[str, IntegrationStatus]) -> None:
        """Add historical statistics to integration status."""
        day_ago = datetime.now(timezone.utc) - timedelta(hours=24)
        
        for system in ["cms", "ros", "wms"]:
            if system not in integrations:
                continue
            
            try:
                result = await self.db.execute(text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(*) FILTER (WHERE status = 'success') as success_count,
                        COUNT(*) FILTER (WHERE status = 'failed') as failed_count
                    FROM integration_event_logs
                    WHERE target_system = :system AND created_at >= :since
                """), {"system": system, "since": day_ago})
                
                row = result.fetchone()
                if row:
                    integrations[system].total_calls_24h = row.total or 0
                    integrations[system].failed_calls_24h = row.failed_count or 0
                    if row.total > 0:
                        integrations[system].success_rate_24h = (row.success_count / row.total) * 100
            except Exception:
                pass  # Stats not available, that's okay
    
    async def _fetch_queue_metrics(self, status: SystemStatus) -> None:
        """Fetch queue-related metrics."""
        try:
            # Get DLQ record count
            result = await self.db.execute(text("""
                SELECT COUNT(*) FILTER (WHERE processed = false) as dlq_count
                FROM dlq_records
            """))
            row = result.fetchone()
            status.dlq_messages = row.dlq_count if row else 0
        except Exception:
            pass
        
        try:
            # Get active saga count
            result = await self.db.execute(text("""
                SELECT COUNT(*) as saga_count
                FROM saga_records
                WHERE state IN ('pending', 'in_progress', 'compensating')
            """))
            row = result.fetchone()
            status.active_sagas = row.saga_count if row else 0
        except Exception:
            pass
    
    def _calculate_overall_health(self, status: SystemStatus) -> SystemHealth:
        """Calculate overall system health based on all components."""
        unhealthy_count = 0
        degraded_count = 0
        
        # Check database
        if status.database_health == SystemHealth.UNHEALTHY:
            return SystemHealth.UNHEALTHY
        elif status.database_health == SystemHealth.DEGRADED:
            degraded_count += 1
        
        # Check queue
        if status.queue_health == SystemHealth.UNHEALTHY:
            unhealthy_count += 1
        elif status.queue_health == SystemHealth.DEGRADED:
            degraded_count += 1
        
        # Check integrations
        for integration in status.integrations.values():
            if integration.status == SystemHealth.UNHEALTHY:
                unhealthy_count += 1
            elif integration.status == SystemHealth.DEGRADED:
                degraded_count += 1
        
        # Determine overall health
        if unhealthy_count >= 2:
            return SystemHealth.UNHEALTHY
        elif unhealthy_count >= 1 or degraded_count >= 2:
            return SystemHealth.DEGRADED
        else:
            return SystemHealth.HEALTHY
    
    async def _generate_alerts(self, overview: DashboardOverview) -> List[Dict[str, Any]]:
        """Generate alerts based on current system state."""
        alerts = []
        
        # High failure rate alert
        if overview.delivery_success_rate < 90 and overview.completed_deliveries + overview.failed_deliveries > 10:
            alerts.append({
                "type": "warning",
                "title": "Low Delivery Success Rate",
                "message": f"Delivery success rate is {overview.delivery_success_rate:.1f}%",
                "action": "Review failed deliveries",
            })
        
        # Failed integrations alert
        if overview.failed_integrations > 5:
            alerts.append({
                "type": "error",
                "title": "High Integration Failures",
                "message": f"{overview.failed_integrations} integration events failed in the last 24h",
                "action": "Check integration logs",
            })
        
        # System health alert
        if overview.system_status:
            if overview.system_status.overall_health == SystemHealth.UNHEALTHY:
                alerts.append({
                    "type": "critical",
                    "title": "System Health Critical",
                    "message": "One or more critical systems are down",
                    "action": "Check system status immediately",
                })
            elif overview.system_status.overall_health == SystemHealth.DEGRADED:
                alerts.append({
                    "type": "warning",
                    "title": "System Health Degraded",
                    "message": "Some systems are experiencing issues",
                    "action": "Monitor system status",
                })
            
            # DLQ alert
            if overview.system_status.dlq_messages > 10:
                alerts.append({
                    "type": "warning",
                    "title": "Messages in Dead Letter Queue",
                    "message": f"{overview.system_status.dlq_messages} messages in DLQ",
                    "action": "Review and process DLQ messages",
                })
        
        # Pending orders alert
        pending = overview.orders_by_status.get("pending", 0)
        if pending > 20:
            alerts.append({
                "type": "info",
                "title": "High Pending Orders",
                "message": f"{pending} orders awaiting processing",
                "action": "Assign drivers to pending orders",
            })
        
        return alerts
    
    async def get_failed_messages(
        self,
        system: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get failed integration messages for admin review."""
        query = """
            SELECT 
                event_id, order_id, source_system, target_system,
                event_type, status, error_message, error_code,
                retry_count, max_retries, created_at, updated_at
            FROM integration_event_logs
            WHERE status IN ('failed', 'retrying')
        """
        params = {}
        
        if system:
            query += " AND target_system = :system"
            params["system"] = system
        
        query += " ORDER BY created_at DESC LIMIT :limit"
        params["limit"] = limit
        
        try:
            result = await self.db.execute(text(query), params)
            rows = result.fetchall()
            
            return [
                {
                    "event_id": row.event_id,
                    "order_id": row.order_id,
                    "source_system": row.source_system,
                    "target_system": row.target_system,
                    "event_type": row.event_type,
                    "status": row.status,
                    "error_message": row.error_message,
                    "error_code": row.error_code,
                    "retry_count": row.retry_count,
                    "max_retries": row.max_retries,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                    "can_retry": row.retry_count < row.max_retries,
                }
                for row in rows
            ]
        except Exception as e:
            logger.error("Failed to fetch failed messages: %s", e)
            return []
    
    async def retry_failed_event(self, event_id: str) -> Dict[str, Any]:
        """
        Manually retry a failed integration event.
        
        Returns status of the retry attempt.
        """
        # Get the failed event
        result = await self.db.execute(text("""
            SELECT * FROM integration_event_logs WHERE event_id = :event_id
        """), {"event_id": event_id})
        
        event = result.fetchone()
        if not event:
            return {"success": False, "error": "Event not found"}
        
        if event.status not in ("failed", "retrying"):
            return {"success": False, "error": f"Event status is {event.status}, cannot retry"}
        
        if event.retry_count >= event.max_retries:
            return {"success": False, "error": "Event has exceeded max retries"}
        
        # Update status to retrying
        await self.db.execute(text("""
            UPDATE integration_event_logs 
            SET status = 'retrying', 
                retry_count = retry_count + 1,
                updated_at = NOW()
            WHERE event_id = :event_id
        """), {"event_id": event_id})
        
        await self.db.commit()
        
        # Trigger the actual retry based on event type
        retry_result = await self._execute_retry(event)
        
        return retry_result
    
    async def _execute_retry(self, event) -> Dict[str, Any]:
        """Execute the actual retry for a failed integration event."""
        import json
        
        try:
            from shared.common.integrations import CMSClient, ROSClient, WMSClient
            
            request_data = json.loads(event.request_data) if event.request_data else {}
            
            if event.target_system == "cms":
                client = CMSClient()
                if event.event_type == "validate_client":
                    result = await client.validate_client(request_data.get("client_id", 0))
                elif event.event_type == "get_client":
                    result = await client.get_client_info(request_data.get("client_id", 0))
                else:
                    result = {"status": "unknown_event"}
                    
            elif event.target_system == "ros":
                client = ROSClient()
                if event.event_type == "optimize_route":
                    result = await client.optimize_route(
                        request_data.get("pickup", {}),
                        request_data.get("destinations", []),
                    )
                else:
                    result = {"status": "unknown_event"}
                    
            elif event.target_system == "wms":
                client = WMSClient()
                if event.event_type == "receive_package":
                    result = await client.receive_package(request_data.get("order_data", {}))
                elif event.event_type == "check_status":
                    result = await client.check_status(request_data.get("order_id", ""))
                else:
                    result = {"status": "unknown_event"}
            else:
                return {"success": False, "error": f"Unknown target system: {event.target_system}"}
            
            # Update event as successful
            await self.db.execute(text("""
                UPDATE integration_event_logs 
                SET status = 'success',
                    response_data = :response,
                    updated_at = NOW()
                WHERE event_id = :event_id
            """), {"event_id": event.event_id, "response": json.dumps(result)})
            
            await self.db.commit()
            
            return {
                "success": True,
                "event_id": event.event_id,
                "result": result,
            }
            
        except Exception as e:
            # Update event with new error
            await self.db.execute(text("""
                UPDATE integration_event_logs 
                SET status = 'failed',
                    error_message = :error,
                    updated_at = NOW()
                WHERE event_id = :event_id
            """), {"event_id": event.event_id, "error": str(e)})
            
            await self.db.commit()
            
            return {
                "success": False,
                "event_id": event.event_id,
                "error": str(e),
            }
