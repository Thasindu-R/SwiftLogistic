"""
WebSocket connection manager for real-time tracking updates.

Two layers:
  1. Per-order channels  – `/ws/{order_id}` (existing)
  2. Global user channel – `/ws/global?token=…`  (new)
     Subscribes clients to channels based on role/user-id so that
     every relevant event is pushed automatically.
"""

import json
import logging
from collections import defaultdict

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class WebSocketManager:
    """
    Manages WebSocket connections in two dimensions:

    *  _connections[order_id]  – per-order watchers (existing)
    *  _user_connections[user_id] – per-user global sockets
    *  _role_connections[role]    – role-based broadcast
    """

    def __init__(self):
        # Per-order sockets
        self._connections: dict[str, set[WebSocket]] = defaultdict(set)
        # Global user sockets  (user_id → set[ws])
        self._user_connections: dict[int, set[WebSocket]] = defaultdict(set)
        # Role-based sockets   (role → set[ws])
        self._role_connections: dict[str, set[WebSocket]] = defaultdict(set)
        # Reverse lookup: ws → (user_id, role)
        self._ws_meta: dict[WebSocket, tuple[int, str]] = {}

    # ── per-order (unchanged) ────────────────────────────────
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

    # ── global user channel ──────────────────────────────────
    async def connect_global(self, websocket: WebSocket, user_id: int, role: str):
        """Register a global (user-level) WebSocket."""
        await websocket.accept()
        self._user_connections[user_id].add(websocket)
        self._role_connections[role].add(websocket)
        self._ws_meta[websocket] = (user_id, role)
        logger.info("Global WS connected user=%d role=%s", user_id, role)

    def disconnect_global(self, websocket: WebSocket):
        meta = self._ws_meta.pop(websocket, None)
        if not meta:
            return
        user_id, role = meta
        self._user_connections[user_id].discard(websocket)
        if not self._user_connections[user_id]:
            del self._user_connections[user_id]
        self._role_connections[role].discard(websocket)
        if not self._role_connections[role]:
            del self._role_connections[role]
        logger.info("Global WS disconnected user=%d role=%s", user_id, role)

    async def send_to_user(self, user_id: int, data: dict):
        """Push to every global socket owned by this user."""
        dead: list[WebSocket] = []
        for ws in self._user_connections.get(user_id, set()):
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect_global(ws)

    async def send_to_role(self, role: str, data: dict):
        """Push to every global socket belonging to users with this role."""
        dead: list[WebSocket] = []
        for ws in self._role_connections.get(role, set()):
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect_global(ws)

    async def broadcast_global(self, data: dict):
        """Push to every global socket regardless of role."""
        dead: list[WebSocket] = []
        for ws in list(self._ws_meta.keys()):
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect_global(ws)


# Singleton
ws_manager = WebSocketManager()
