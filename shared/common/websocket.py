"""
WebSocket Manager – Real-Time Communication
============================================

Provides WebSocket connection management for live updates:
- Order status changes
- Tracking updates (driver location, delivery progress)
- System notifications
- Integration status alerts

Features:
- Connection pooling by user/order
- Broadcast to specific channels
- Heartbeat/ping-pong for connection health
- Automatic reconnection handling
- Message queuing for offline clients

Usage:
    from shared.common.websocket import ws_manager
    
    # In FastAPI route
    @app.websocket("/ws/{client_id}")
    async def websocket_endpoint(websocket: WebSocket, client_id: str):
        await ws_manager.connect(websocket, client_id)
        try:
            while True:
                data = await websocket.receive_text()
                await ws_manager.handle_message(client_id, data)
        except WebSocketDisconnect:
            ws_manager.disconnect(client_id)
    
    # Broadcast update
    await ws_manager.broadcast_order_update(order_id, status_data)
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional
from dataclasses import dataclass, field

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class MessageType(str, Enum):
    """WebSocket message types."""
    # Connection management
    CONNECT = "connect"
    DISCONNECT = "disconnect"
    PING = "ping"
    PONG = "pong"
    ERROR = "error"
    
    # Order updates
    ORDER_CREATED = "order_created"
    ORDER_STATUS_CHANGED = "order_status_changed"
    ORDER_ASSIGNED = "order_assigned"
    
    # Tracking updates
    TRACKING_UPDATE = "tracking_update"
    LOCATION_UPDATE = "location_update"
    ETA_UPDATE = "eta_update"
    
    # Delivery updates
    DELIVERY_STARTED = "delivery_started"
    DELIVERY_COMPLETED = "delivery_completed"
    PROOF_UPLOADED = "proof_uploaded"
    
    # System notifications
    SYSTEM_ALERT = "system_alert"
    INTEGRATION_STATUS = "integration_status"
    
    # Subscriptions
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"


class ChannelType(str, Enum):
    """Channel types for message routing."""
    USER = "user"           # Messages for specific user
    ORDER = "order"         # Messages for specific order
    DRIVER = "driver"       # Messages for specific driver
    ADMIN = "admin"         # Admin-only messages
    BROADCAST = "broadcast" # All connected clients


@dataclass
class WebSocketMessage:
    """Standardized WebSocket message format."""
    type: MessageType
    payload: dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    channel: Optional[str] = None
    sender: Optional[str] = None
    
    def to_json(self) -> str:
        """Serialize message to JSON string."""
        return json.dumps({
            "type": self.type.value if isinstance(self.type, MessageType) else self.type,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "channel": self.channel,
            "sender": self.sender,
        })
    
    @classmethod
    def from_json(cls, data: str) -> "WebSocketMessage":
        """Deserialize message from JSON string."""
        parsed = json.loads(data)
        return cls(
            type=MessageType(parsed.get("type", "error")),
            payload=parsed.get("payload", {}),
            timestamp=parsed.get("timestamp", datetime.now(timezone.utc).isoformat()),
            channel=parsed.get("channel"),
            sender=parsed.get("sender"),
        )


@dataclass
class ConnectionInfo:
    """Information about a WebSocket connection."""
    websocket: WebSocket
    client_id: str
    user_id: Optional[int] = None
    user_role: Optional[str] = None
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_activity: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    subscriptions: set = field(default_factory=set)
    
    def update_activity(self):
        """Update last activity timestamp."""
        self.last_activity = datetime.now(timezone.utc)


class WebSocketManager:
    """
    Manages WebSocket connections for real-time updates.
    
    Supports multiple connection patterns:
    - User-based: Each user gets their own connection
    - Order-based: Subscribe to specific order updates
    - Role-based: Admin vs client vs driver channels
    - Broadcast: System-wide announcements
    """
    
    def __init__(self):
        # Active connections: client_id -> ConnectionInfo
        self._connections: dict[str, ConnectionInfo] = {}
        
        # Channel subscriptions: channel_name -> set of client_ids
        self._channels: dict[str, set[str]] = {}
        
        # Message handlers: MessageType -> handler function
        self._handlers: dict[MessageType, Callable] = {}
        
        # Pending messages for offline users: user_id -> list of messages
        self._pending_messages: dict[int, list[WebSocketMessage]] = {}
        
        # Connection lock for thread safety
        self._lock = asyncio.Lock()
        
        # Heartbeat interval (seconds)
        self._heartbeat_interval = 30
        
        # Register default handlers
        self._register_default_handlers()
    
    def _register_default_handlers(self):
        """Register default message handlers."""
        self._handlers[MessageType.PING] = self._handle_ping
        self._handlers[MessageType.SUBSCRIBE] = self._handle_subscribe
        self._handlers[MessageType.UNSUBSCRIBE] = self._handle_unsubscribe
    
    async def connect(
        self,
        websocket: WebSocket,
        client_id: str,
        user_id: Optional[int] = None,
        user_role: Optional[str] = None,
    ) -> bool:
        """
        Accept and register a new WebSocket connection.
        
        Args:
            websocket: FastAPI WebSocket instance
            client_id: Unique client identifier (e.g., session ID)
            user_id: Authenticated user ID (if available)
            user_role: User role (client/driver/admin)
            
        Returns:
            True if connection successful
        """
        try:
            await websocket.accept()
            
            async with self._lock:
                # Store connection info
                conn_info = ConnectionInfo(
                    websocket=websocket,
                    client_id=client_id,
                    user_id=user_id,
                    user_role=user_role,
                )
                self._connections[client_id] = conn_info
                
                # Auto-subscribe to user channel
                if user_id:
                    user_channel = f"user:{user_id}"
                    await self._subscribe_to_channel(client_id, user_channel)
                
                # Auto-subscribe to role channel
                if user_role:
                    role_channel = f"role:{user_role}"
                    await self._subscribe_to_channel(client_id, role_channel)
            
            logger.info(
                "WebSocket connected: client_id=%s, user_id=%s, role=%s",
                client_id, user_id, user_role
            )
            
            # Send connection confirmation
            await self.send_to_client(client_id, WebSocketMessage(
                type=MessageType.CONNECT,
                payload={
                    "status": "connected",
                    "client_id": client_id,
                    "user_id": user_id,
                    "role": user_role,
                },
            ))
            
            # Deliver any pending messages
            if user_id and user_id in self._pending_messages:
                await self._deliver_pending_messages(client_id, user_id)
            
            return True
            
        except Exception as e:
            logger.error("WebSocket connection failed: %s", e)
            return False
    
    def disconnect(self, client_id: str):
        """
        Remove a WebSocket connection.
        
        Args:
            client_id: Client identifier to disconnect
        """
        if client_id in self._connections:
            conn_info = self._connections[client_id]
            
            # Remove from all subscribed channels
            for channel in list(conn_info.subscriptions):
                if channel in self._channels:
                    self._channels[channel].discard(client_id)
                    if not self._channels[channel]:
                        del self._channels[channel]
            
            # Remove connection
            del self._connections[client_id]
            
            logger.info("WebSocket disconnected: client_id=%s", client_id)
    
    async def handle_message(self, client_id: str, raw_data: str):
        """
        Process incoming WebSocket message.
        
        Args:
            client_id: Source client identifier
            raw_data: Raw JSON message string
        """
        try:
            message = WebSocketMessage.from_json(raw_data)
            message.sender = client_id
            
            # Update connection activity
            if client_id in self._connections:
                self._connections[client_id].update_activity()
            
            # Route to handler
            handler = self._handlers.get(message.type)
            if handler:
                await handler(client_id, message)
            else:
                logger.warning("No handler for message type: %s", message.type)
                
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON message from %s: %s", client_id, e)
            await self.send_error(client_id, "Invalid message format")
        except Exception as e:
            logger.error("Error handling message from %s: %s", client_id, e)
            await self.send_error(client_id, str(e))
    
    async def send_to_client(self, client_id: str, message: WebSocketMessage) -> bool:
        """
        Send message to specific client.
        
        Args:
            client_id: Target client identifier
            message: Message to send
            
        Returns:
            True if sent successfully
        """
        if client_id not in self._connections:
            logger.warning("Client not connected: %s", client_id)
            return False
        
        try:
            websocket = self._connections[client_id].websocket
            await websocket.send_text(message.to_json())
            return True
        except Exception as e:
            logger.error("Failed to send to client %s: %s", client_id, e)
            self.disconnect(client_id)
            return False
    
    async def send_to_user(self, user_id: int, message: WebSocketMessage) -> int:
        """
        Send message to all connections for a user.
        
        Args:
            user_id: Target user ID
            message: Message to send
            
        Returns:
            Number of clients that received the message
        """
        channel = f"user:{user_id}"
        sent_count = await self.send_to_channel(channel, message)
        
        # Queue message if user is offline
        if sent_count == 0:
            self._queue_pending_message(user_id, message)
        
        return sent_count
    
    async def send_to_channel(self, channel: str, message: WebSocketMessage) -> int:
        """
        Send message to all clients subscribed to a channel.
        
        Args:
            channel: Channel name
            message: Message to send
            
        Returns:
            Number of clients that received the message
        """
        message.channel = channel
        sent_count = 0
        
        if channel not in self._channels:
            return 0
        
        for client_id in list(self._channels[channel]):
            if await self.send_to_client(client_id, message):
                sent_count += 1
        
        return sent_count
    
    async def broadcast(self, message: WebSocketMessage) -> int:
        """
        Broadcast message to all connected clients.
        
        Args:
            message: Message to broadcast
            
        Returns:
            Number of clients that received the message
        """
        message.channel = "broadcast"
        sent_count = 0
        
        for client_id in list(self._connections.keys()):
            if await self.send_to_client(client_id, message):
                sent_count += 1
        
        return sent_count
    
    async def broadcast_to_role(self, role: str, message: WebSocketMessage) -> int:
        """
        Broadcast message to all clients with specific role.
        
        Args:
            role: Target role (client/driver/admin)
            message: Message to send
            
        Returns:
            Number of clients that received the message
        """
        channel = f"role:{role}"
        return await self.send_to_channel(channel, message)
    
    # ── Order-specific methods ───────────────────────────────
    
    async def broadcast_order_update(
        self,
        order_id: str,
        status: str,
        details: Optional[dict] = None,
        client_id: Optional[int] = None,
        driver_id: Optional[int] = None,
    ):
        """
        Broadcast order status update to relevant parties.
        
        Args:
            order_id: Order UUID
            status: New order status
            details: Additional update details
            client_id: Order owner's user ID
            driver_id: Assigned driver's user ID
        """
        message = WebSocketMessage(
            type=MessageType.ORDER_STATUS_CHANGED,
            payload={
                "order_id": order_id,
                "status": status,
                "details": details or {},
            },
        )
        
        # Send to order channel (anyone subscribed to this order)
        order_channel = f"order:{order_id}"
        await self.send_to_channel(order_channel, message)
        
        # Send to order owner
        if client_id:
            await self.send_to_user(client_id, message)
        
        # Send to assigned driver
        if driver_id:
            await self.send_to_user(driver_id, message)
        
        # Notify admins
        await self.broadcast_to_role("admin", message)
    
    async def broadcast_tracking_update(
        self,
        order_id: str,
        event_type: str,
        location: Optional[dict] = None,
        description: Optional[str] = None,
        eta: Optional[str] = None,
        client_id: Optional[int] = None,
    ):
        """
        Broadcast real-time tracking update.
        
        Args:
            order_id: Order UUID
            event_type: Type of tracking event
            location: GPS coordinates {lat, lng}
            description: Event description
            eta: Estimated time of arrival
            client_id: Order owner to notify
        """
        message = WebSocketMessage(
            type=MessageType.TRACKING_UPDATE,
            payload={
                "order_id": order_id,
                "event_type": event_type,
                "location": location,
                "description": description,
                "eta": eta,
            },
        )
        
        # Send to order channel
        order_channel = f"order:{order_id}"
        await self.send_to_channel(order_channel, message)
        
        # Send to order owner
        if client_id:
            await self.send_to_user(client_id, message)
    
    async def broadcast_location_update(
        self,
        driver_id: int,
        order_id: str,
        latitude: float,
        longitude: float,
        speed: Optional[float] = None,
        heading: Optional[float] = None,
    ):
        """
        Broadcast driver location update for live tracking.
        
        Args:
            driver_id: Driver user ID
            order_id: Current delivery order
            latitude: GPS latitude
            longitude: GPS longitude
            speed: Current speed (km/h)
            heading: Direction in degrees
        """
        message = WebSocketMessage(
            type=MessageType.LOCATION_UPDATE,
            payload={
                "driver_id": driver_id,
                "order_id": order_id,
                "location": {
                    "lat": latitude,
                    "lng": longitude,
                },
                "speed": speed,
                "heading": heading,
            },
        )
        
        # Send to order subscribers
        order_channel = f"order:{order_id}"
        await self.send_to_channel(order_channel, message)
    
    async def notify_delivery_completed(
        self,
        order_id: str,
        client_id: int,
        proof_url: Optional[str] = None,
        signature_url: Optional[str] = None,
        delivered_at: Optional[str] = None,
    ):
        """
        Notify that delivery has been completed.
        
        Args:
            order_id: Order UUID
            client_id: Order owner's user ID
            proof_url: URL to proof-of-delivery image
            signature_url: URL to signature image
            delivered_at: Delivery timestamp
        """
        message = WebSocketMessage(
            type=MessageType.DELIVERY_COMPLETED,
            payload={
                "order_id": order_id,
                "proof_url": proof_url,
                "signature_url": signature_url,
                "delivered_at": delivered_at or datetime.now(timezone.utc).isoformat(),
            },
        )
        
        await self.send_to_user(client_id, message)
        
        # Also send to order channel
        order_channel = f"order:{order_id}"
        await self.send_to_channel(order_channel, message)
    
    # ── Subscription handlers ────────────────────────────────
    
    async def _subscribe_to_channel(self, client_id: str, channel: str):
        """Subscribe client to a channel."""
        if channel not in self._channels:
            self._channels[channel] = set()
        self._channels[channel].add(client_id)
        
        if client_id in self._connections:
            self._connections[client_id].subscriptions.add(channel)
    
    async def _unsubscribe_from_channel(self, client_id: str, channel: str):
        """Unsubscribe client from a channel."""
        if channel in self._channels:
            self._channels[channel].discard(client_id)
            if not self._channels[channel]:
                del self._channels[channel]
        
        if client_id in self._connections:
            self._connections[client_id].subscriptions.discard(channel)
    
    async def subscribe_to_order(self, client_id: str, order_id: str):
        """Subscribe client to order updates."""
        channel = f"order:{order_id}"
        await self._subscribe_to_channel(client_id, channel)
        logger.info("Client %s subscribed to order %s", client_id, order_id)
    
    async def unsubscribe_from_order(self, client_id: str, order_id: str):
        """Unsubscribe client from order updates."""
        channel = f"order:{order_id}"
        await self._unsubscribe_from_channel(client_id, channel)
        logger.info("Client %s unsubscribed from order %s", client_id, order_id)
    
    # ── Default message handlers ─────────────────────────────
    
    async def _handle_ping(self, client_id: str, message: WebSocketMessage):
        """Handle ping message with pong response."""
        await self.send_to_client(client_id, WebSocketMessage(
            type=MessageType.PONG,
            payload={"ping_time": message.payload.get("time")},
        ))
    
    async def _handle_subscribe(self, client_id: str, message: WebSocketMessage):
        """Handle subscription request."""
        channel = message.payload.get("channel")
        if channel:
            await self._subscribe_to_channel(client_id, channel)
            await self.send_to_client(client_id, WebSocketMessage(
                type=MessageType.SUBSCRIBE,
                payload={"channel": channel, "status": "subscribed"},
            ))
    
    async def _handle_unsubscribe(self, client_id: str, message: WebSocketMessage):
        """Handle unsubscription request."""
        channel = message.payload.get("channel")
        if channel:
            await self._unsubscribe_from_channel(client_id, channel)
            await self.send_to_client(client_id, WebSocketMessage(
                type=MessageType.UNSUBSCRIBE,
                payload={"channel": channel, "status": "unsubscribed"},
            ))
    
    # ── Utility methods ──────────────────────────────────────
    
    async def send_error(self, client_id: str, error_message: str):
        """Send error message to client."""
        await self.send_to_client(client_id, WebSocketMessage(
            type=MessageType.ERROR,
            payload={"error": error_message},
        ))
    
    def _queue_pending_message(self, user_id: int, message: WebSocketMessage):
        """Queue message for offline user."""
        if user_id not in self._pending_messages:
            self._pending_messages[user_id] = []
        
        # Limit pending messages per user
        if len(self._pending_messages[user_id]) < 100:
            self._pending_messages[user_id].append(message)
    
    async def _deliver_pending_messages(self, client_id: str, user_id: int):
        """Deliver pending messages to reconnected user."""
        if user_id not in self._pending_messages:
            return
        
        messages = self._pending_messages.pop(user_id)
        for message in messages:
            await self.send_to_client(client_id, message)
        
        logger.info("Delivered %d pending messages to user %d", len(messages), user_id)
    
    def get_connection_count(self) -> int:
        """Get total number of active connections."""
        return len(self._connections)
    
    def get_channel_count(self) -> int:
        """Get total number of active channels."""
        return len(self._channels)
    
    def get_stats(self) -> dict:
        """Get WebSocket manager statistics."""
        return {
            "active_connections": len(self._connections),
            "active_channels": len(self._channels),
            "pending_messages": sum(len(m) for m in self._pending_messages.values()),
            "connections_by_role": self._count_by_role(),
        }
    
    def _count_by_role(self) -> dict[str, int]:
        """Count connections by user role."""
        counts: dict[str, int] = {}
        for conn_info in self._connections.values():
            role = conn_info.user_role or "anonymous"
            counts[role] = counts.get(role, 0) + 1
        return counts
    
    def is_user_online(self, user_id: int) -> bool:
        """Check if a user has an active connection."""
        channel = f"user:{user_id}"
        return channel in self._channels and len(self._channels[channel]) > 0
    
    async def send_system_alert(
        self,
        alert_type: str,
        title: str,
        message: str,
        severity: str = "info",
        target_roles: Optional[list[str]] = None,
    ):
        """
        Send system alert to specified roles.
        
        Args:
            alert_type: Type of alert (error/warning/info/success)
            title: Alert title
            message: Alert message
            severity: Alert severity level
            target_roles: List of roles to notify (default: admin only)
        """
        alert_msg = WebSocketMessage(
            type=MessageType.SYSTEM_ALERT,
            payload={
                "alert_type": alert_type,
                "title": title,
                "message": message,
                "severity": severity,
            },
        )
        
        roles = target_roles or ["admin"]
        for role in roles:
            await self.broadcast_to_role(role, alert_msg)


# Global WebSocket manager instance
ws_manager = WebSocketManager()
