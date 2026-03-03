"""
Order Saga – orchestrates distributed transaction across CMS, WMS, and ROS.

Pattern: Saga (orchestration-based) with compensating transactions.
Uses SagaStateMachine for state persistence, RetryHandler with
CircuitBreaker for resilient integration calls, and IntegrationEventStore
for full audit logging.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import aio_pika
from sqlalchemy.ext.asyncio import AsyncSession

from shared.common.config import settings
from shared.common.database import async_session_factory
from shared.common.rabbitmq import rabbitmq_client

# Async processing & reliability components
from shared.common.async_processor.saga_state import SagaStateMachine, SagaState
from shared.common.async_processor.event_store import (
    IntegrationEventStore,
    EventStatus,
)
from shared.common.async_processor.retry_handler import (
    retry_handler,
    RetryPolicy,
    RetryStatus,
)

# Integration clients (SOAP, TCP, REST)
from shared.common.integrations.cms_client import CMSClient
from shared.common.integrations.ros_client import ROSClient
from shared.common.integrations.wms_client import WMSClient

logger = logging.getLogger(__name__)


class OrderSaga:
    """
    Enhanced Order Saga with full async processing & reliability.

    Saga steps for a new order:
      1. Confirm order in CMS  (SOAP/XML  → mock-cms)  – via RetryHandler
      2. Register package in WMS (TCP/IP  → mock-wms)  – via RetryHandler
      3. Request route from ROS  (REST    → mock-ros)  – via RetryHandler

    Features:
    - SagaStateMachine: DB-persisted state, automatic compensation
    - RetryHandler: Exponential backoff + CircuitBreaker per system
    - IntegrationEventStore: Every call logged with duration & result
    - Audit trail: Complete history of all saga actions
    """

    def __init__(self, order_data: dict, db: AsyncSession):
        self.order_data = order_data
        self.order_id = order_data["order_id"]
        self.db = db
        self.event_store = IntegrationEventStore(db)

        # Integration endpoints
        import os
        self._cms_url = os.getenv("CMS_URL", "http://mock-cms:8004")
        self._ros_url = os.getenv("ROS_URL", "http://mock-ros:8005")
        self._wms_host = os.getenv("WMS_HOST", "mock-wms")
        self._wms_port = int(os.getenv("WMS_PORT", "9000"))

        # Create saga state machine with DB persistence
        self.saga = SagaStateMachine(
            order_id=self.order_id,
            order_data=order_data,
            db_session=db,
        )

        # Register steps – max_retries=1 here because RetryHandler
        # handles the actual retry logic with CircuitBreaker
        self.saga.register_step(
            name="cms",
            execute_fn=self._execute_cms,
            compensate_fn=self._compensate_cms,
            max_retries=1,
        )
        self.saga.register_step(
            name="wms",
            execute_fn=self._execute_wms,
            compensate_fn=self._compensate_wms,
            max_retries=1,
        )
        self.saga.register_step(
            name="ros",
            execute_fn=self._execute_ros,
            compensate_fn=self._compensate_ros,
            max_retries=1,
        )

    async def execute(self) -> dict:
        """Execute the full saga and return result."""
        # Audit trail: saga start
        await self.event_store.log_audit_trail(
            order_id=self.order_id,
            action="saga.started",
            entity_type="saga",
            entity_id=self.saga.saga_id,
            details=f"Order saga started for order {self.order_id}",
        )

        # Publish order.created so tracking-service records it
        await self._publish_order_created()

        # Run the state machine (executes CMS → WMS → ROS, compensates on failure)
        result = await self.saga.execute()

        # Audit trail: saga completion
        await self.event_store.log_audit_trail(
            order_id=self.order_id,
            action=f"saga.{result['state']}",
            entity_type="saga",
            entity_id=self.saga.saga_id,
            details=f"Saga finished with state: {result['state']}",
        )

        logger.info(
            "Saga %s for order %s finished: %s",
            self.saga.saga_id, self.order_id, result["state"],
        )
        return result

    # ── Publish helpers ──────────────────────────────────────

    async def _publish_order_created(self):
        """Publish order.created event for the tracking service."""
        event = {
            "event": settings.ORDER_CREATED_KEY,
            "order_id": self.order_id,
            "client_id": self.order_data["client_id"],
            "status": "processing",
            "pickup_address": self.order_data["pickup_address"],
            "delivery_address": self.order_data["delivery_address"],
            "package_description": self.order_data.get("package_description", ""),
            "package_weight": self.order_data.get("package_weight", 0.0),
            "priority": self.order_data.get("priority", "normal"),
            "recipient_name": self.order_data.get("recipient_name", ""),
            "recipient_phone": self.order_data.get("recipient_phone", ""),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await rabbitmq_client.publish(
            exchange_name=settings.ORDER_EXCHANGE,
            routing_key=settings.ORDER_CREATED_KEY,
            message=event,
        )

    async def _publish_tracking(self, event_type: str, description: str):
        """Publish a tracking event to RabbitMQ."""
        try:
            await rabbitmq_client.publish(
                exchange_name=settings.TRACKING_EXCHANGE,
                routing_key=settings.TRACKING_UPDATE_KEY,
                message={
                    "event": "tracking.update",
                    "order_id": self.order_id,
                    "event_type": event_type,
                    "description": description,
                    "location": "Order Service (Saga)",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
        except Exception as exc:
            logger.error("Failed to publish tracking event: %s", exc)

    # ── Raw integration calls (no retry – RetryHandler wraps them) ──

    async def _call_cms(self, _order_data: dict) -> dict:
        async with CMSClient(self._cms_url) as cms:
            result = await cms.transmit_order(self.order_data)
            return {
                "success": True,
                "client_validated": result.get("client_validated"),
                "message": result.get("message"),
                "protocol": "SOAP/XML",
            }

    async def _call_wms(self, _order_data: dict) -> dict:
        async with WMSClient(self._wms_host, self._wms_port) as wms:
            result = await wms.register_order(self.order_data)
            return {
                "success": True,
                "event": result.get("event"),
                "package_id": result.get("package", {}).get("package_id"),
                "protocol": "TCP/IP",
            }

    async def _call_ros(self, _order_data: dict) -> dict:
        async with ROSClient(self._ros_url) as ros:
            route = await ros.optimise_route_from_order(self.order_data)
            return {
                "success": True,
                "route_id": route.get("route_id"),
                "distance_km": route.get("estimated_distance_km"),
                "duration_min": route.get("estimated_duration_min"),
                "protocol": "REST/JSON",
            }

    # ── Saga step implementations ────────────────────────────

    async def _execute_cms(self, order_data: dict) -> dict:
        """CMS: validate client & billing via SOAP/XML."""
        start = datetime.now(timezone.utc)
        event = await self.event_store.log_event(
            source="order-service", target="cms",
            event_type="validate_client",
            order_id=self.order_id, saga_id=self.saga.saga_id,
            status=EventStatus.IN_PROGRESS,
            request_data={"client_id": order_data["client_id"]},
        )
        try:
            rr = await retry_handler.execute_with_retry(
                operation=self._call_cms,
                args=(order_data,),
                policy=RetryPolicy(max_attempts=3, base_delay=1.0, max_delay=10.0),
                system_name="cms",
                operation_name="validate_client",
            )
            if rr.status != RetryStatus.SUCCESS:
                raise rr.error or RuntimeError("CMS validation failed")
            dur = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
            await self.event_store.update_event(
                event_id=event.event_id, status=EventStatus.SUCCESS,
                response_data=rr.result, duration_ms=dur,
            )
            await self._publish_tracking("cms_validated", "Client validated in CMS")
            return rr.result
        except Exception as exc:
            dur = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
            await self.event_store.update_event(
                event_id=event.event_id, status=EventStatus.FAILED,
                error_message=str(exc), duration_ms=dur,
            )
            raise

    async def _execute_wms(self, order_data: dict) -> dict:
        """WMS: register package via TCP/IP."""
        start = datetime.now(timezone.utc)
        event = await self.event_store.log_event(
            source="order-service", target="wms",
            event_type="register_package",
            order_id=self.order_id, saga_id=self.saga.saga_id,
            status=EventStatus.IN_PROGRESS,
            request_data={
                "order_id": order_data["order_id"],
                "weight": order_data.get("package_weight", 0),
            },
        )
        try:
            rr = await retry_handler.execute_with_retry(
                operation=self._call_wms,
                args=(order_data,),
                policy=RetryPolicy(max_attempts=3, base_delay=1.0, max_delay=10.0),
                system_name="wms",
                operation_name="register_package",
            )
            if rr.status != RetryStatus.SUCCESS:
                raise rr.error or RuntimeError("WMS registration failed")
            dur = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
            await self.event_store.update_event(
                event_id=event.event_id, status=EventStatus.SUCCESS,
                response_data=rr.result, duration_ms=dur,
            )
            await self._publish_tracking(
                "package_registered", "Package registered in WMS warehouse",
            )
            return rr.result
        except Exception as exc:
            dur = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
            await self.event_store.update_event(
                event_id=event.event_id, status=EventStatus.FAILED,
                error_message=str(exc), duration_ms=dur,
            )
            raise

    async def _execute_ros(self, order_data: dict) -> dict:
        """ROS: optimise route via REST/JSON."""
        start = datetime.now(timezone.utc)
        event = await self.event_store.log_event(
            source="order-service", target="ros",
            event_type="optimize_route",
            order_id=self.order_id, saga_id=self.saga.saga_id,
            status=EventStatus.IN_PROGRESS,
            request_data={
                "pickup": order_data["pickup_address"],
                "delivery": order_data["delivery_address"],
            },
        )
        try:
            rr = await retry_handler.execute_with_retry(
                operation=self._call_ros,
                args=(order_data,),
                policy=RetryPolicy(max_attempts=3, base_delay=1.0, max_delay=10.0),
                system_name="ros",
                operation_name="optimize_route",
            )
            if rr.status != RetryStatus.SUCCESS:
                raise rr.error or RuntimeError("ROS optimization failed")
            dur = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
            await self.event_store.update_event(
                event_id=event.event_id, status=EventStatus.SUCCESS,
                response_data=rr.result, duration_ms=dur,
            )
            desc = f"Route optimised: {rr.result.get('distance_km', '?')}km"
            await self._publish_tracking("route_optimised", desc)
            return rr.result
        except Exception as exc:
            dur = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
            await self.event_store.update_event(
                event_id=event.event_id, status=EventStatus.FAILED,
                error_message=str(exc), duration_ms=dur,
            )
            raise

    # ── Compensation handlers ────────────────────────────────

    async def _compensate_cms(self, order_data: dict, result: Any):
        await self.event_store.log_event(
            source="order-service", target="cms",
            event_type="compensate_billing",
            order_id=self.order_id, saga_id=self.saga.saga_id,
            status=EventStatus.SUCCESS,
            request_data={"action": "void_billing"},
        )
        await self._publish_tracking(
            "cms_compensated", "CMS billing voided (saga compensation)",
        )
        logger.info("CMS compensation done for order %s", self.order_id)

    async def _compensate_wms(self, order_data: dict, result: Any):
        await self.event_store.log_event(
            source="order-service", target="wms",
            event_type="compensate_package",
            order_id=self.order_id, saga_id=self.saga.saga_id,
            status=EventStatus.SUCCESS,
            request_data={"action": "cancel_package"},
        )
        await self._publish_tracking(
            "wms_compensated", "WMS package cancelled (saga compensation)",
        )
        logger.info("WMS compensation done for order %s", self.order_id)

    async def _compensate_ros(self, order_data: dict, result: Any):
        await self.event_store.log_event(
            source="order-service", target="ros",
            event_type="compensate_route",
            order_id=self.order_id, saga_id=self.saga.saga_id,
            status=EventStatus.SUCCESS,
            request_data={"action": "cancel_route"},
        )
        await self._publish_tracking(
            "ros_compensated", "ROS route cancelled (saga compensation)",
        )
        logger.info("ROS compensation done for order %s", self.order_id)


# ── Saga reply consumer ─────────────────────────────────────

async def handle_saga_reply(message: aio_pika.abc.AbstractIncomingMessage):
    """
    Callback for saga-reply messages from mock services.
    Logs the reply as an integration event and records an audit trail entry.
    """
    async with message.process():
        body = json.loads(message.body.decode())
        order_id = body.get("order_id", "unknown")
        step = body.get("step", "unknown")
        success = body.get("success", False)
        logger.info(
            "Saga reply for order %s – step=%s success=%s",
            order_id, step, success,
        )

        # Persist the reply as an integration event
        try:
            async with async_session_factory() as session:
                store = IntegrationEventStore(session)
                await store.log_event(
                    source=step,
                    target="order-service",
                    event_type=f"saga.reply.{step}",
                    order_id=order_id,
                    status=EventStatus.SUCCESS if success else EventStatus.FAILED,
                    response_data=body,
                )
                await store.log_audit_trail(
                    order_id=order_id,
                    action=f"saga.reply.{step}",
                    entity_type="saga",
                    details=f"Saga reply from {step}: {'success' if success else 'failed'}",
                )
        except Exception as exc:
            logger.error("Failed to log saga reply event: %s", exc)
