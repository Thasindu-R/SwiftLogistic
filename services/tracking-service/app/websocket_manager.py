"""
WebSocket connection manager for real-time tracking updates.
Clients subscribe to order updates; when a tracking event arrives
via RabbitMQ the manager broadcasts it to all interested connections.
"""

import json
import logging
from collections import defaultdict

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class WebSocketManager:
    """
    Manages WebSocket connections grouped by order_id.
    Supports broadcasting a tracking event to all clients
    watching a specific order.
    """

    def __init__(self):
        # order_id → set of active WebSocket connections
        self._connections: dict[str, set[WebSocket]] = defaultdict(set)

    async def connect(self, websocket: WebSocket, order_id: str):
        await websocket.accept()
        self._connections[order_id].add(websocket)
        logger.info("WS connected for order %s (total=%d)", order_id, len(self._connections[order_id]))

    def disconnect(self, websocket: WebSocket, order_id: str):
        self._connections[order_id].discard(websocket)
        if not self._connections[order_id]:
            del self._connections[order_id]
        logger.info("WS disconnected for order %s", order_id)

    async def broadcast(self, order_id: str, data: dict):
        """Send a JSON message to every client watching `order_id`."""
        if order_id not in self._connections:
            return
        dead: list[WebSocket] = []
        for ws in self._connections[order_id]:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._connections[order_id].discard(ws)

    async def broadcast_all(self, data: dict):
        """Send a JSON message to every connected client."""
        dead_pairs: list[tuple[str, WebSocket]] = []
        for order_id, sockets in self._connections.items():
            for ws in sockets:
                try:
                    await ws.send_json(data)
                except Exception:
                    dead_pairs.append((order_id, ws))
        for oid, ws in dead_pairs:
            self._connections[oid].discard(ws)


# Singleton
ws_manager = WebSocketManager()
