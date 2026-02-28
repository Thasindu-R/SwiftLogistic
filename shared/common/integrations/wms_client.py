"""
WMS Client – TCP/IP Integration
===============================

Integrates with the Warehouse Management System (WMS) via TCP/IP protocol.
Uses proprietary JSON-over-TCP protocol with newline-delimited messages.

Protocol Format:
- Request:  {"command": "COMMAND_NAME", "order_id": "...", ...}\n
- Response: {"status": "OK/ERROR", "event": "EVENT_NAME", ...}\n

Commands:
- RECEIVE_PACKAGE: Register package arrival at warehouse
- CHECK_STATUS: Check current package status
- LOAD_VEHICLE: Mark package as loaded onto vehicle

Events:
- PACKAGE_RECEIVED: Package arrived at warehouse
- PACKAGE_READY: Package processed and ready
- PACKAGE_LOADED: Package loaded onto delivery vehicle
- STATUS: Current package status response

Usage:
    async with WMSClient("mock-wms", 9000) as client:
        result = await client.receive_package(order_id, weight, description)
        status = await client.check_status(order_id)
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

logger = logging.getLogger(__name__)


class WMSProtocolError(Exception):
    """Raised when WMS protocol communication fails."""
    def __init__(self, message: str, details: dict | None = None):
        self.message = message
        self.details = details or {}
        super().__init__(message)


class WMSClient:
    """
    TCP client for WMS (Warehouse Management System) integration.
    
    Communicates using proprietary TCP protocol with JSON messages.
    Handles connection management, message serialization, and response parsing.
    """
    
    def __init__(
        self, 
        host: str, 
        port: int = 9000, 
        timeout: float = 30.0,
        reconnect_attempts: int = 3
    ):
        """
        Initialize WMS TCP client.
        
        Args:
            host: WMS server hostname
            port: WMS server port (default 9000)
            timeout: Read/write timeout in seconds
            reconnect_attempts: Number of reconnect attempts on failure
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.reconnect_attempts = reconnect_attempts
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = False
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
    
    async def connect(self) -> None:
        """
        Establish TCP connection to WMS server.
        
        Raises:
            WMSProtocolError: If connection fails
        """
        for attempt in range(self.reconnect_attempts):
            try:
                logger.info(
                    "WMS: Connecting to %s:%d (attempt %d/%d)",
                    self.host, self.port, attempt + 1, self.reconnect_attempts
                )
                
                self._reader, self._writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=self.timeout
                )
                self._connected = True
                logger.info("WMS: Connected successfully")
                return
                
            except asyncio.TimeoutError:
                logger.warning("WMS: Connection timeout (attempt %d)", attempt + 1)
            except ConnectionRefusedError:
                logger.warning("WMS: Connection refused (attempt %d)", attempt + 1)
            except Exception as e:
                logger.warning("WMS: Connection error (attempt %d): %s", attempt + 1, e)
            
            if attempt < self.reconnect_attempts - 1:
                await asyncio.sleep(1.0 * (attempt + 1))
        
        raise WMSProtocolError(
            f"Failed to connect to WMS at {self.host}:{self.port}",
            {"attempts": self.reconnect_attempts}
        )
    
    async def disconnect(self) -> None:
        """Close TCP connection."""
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception as e:
                logger.debug("WMS disconnect error: %s", e)
        
        self._reader = None
        self._writer = None
        self._connected = False
        logger.info("WMS: Disconnected")
    
    async def _send_command(self, command: str, **kwargs) -> dict[str, Any]:
        """
        Send command to WMS and receive response.
        
        Args:
            command: WMS command name
            **kwargs: Additional command parameters
            
        Returns:
            Parsed JSON response dict
            
        Raises:
            WMSProtocolError: If communication fails
        """
        if not self._connected or not self._writer or not self._reader:
            await self.connect()
        
        # Build message
        message = {"command": command, **kwargs}
        message_bytes = (json.dumps(message) + "\n").encode("utf-8")
        
        logger.debug("WMS TX: %s", message)
        
        try:
            # Send
            self._writer.write(message_bytes)
            await asyncio.wait_for(
                self._writer.drain(),
                timeout=self.timeout
            )
            
            # Receive response
            response_line = await asyncio.wait_for(
                self._reader.readline(),
                timeout=self.timeout
            )
            
            if not response_line:
                raise WMSProtocolError("Empty response from WMS")
            
            response = json.loads(response_line.decode("utf-8").strip())
            logger.debug("WMS RX: %s", response)
            
            if response.get("status") == "ERROR":
                raise WMSProtocolError(
                    response.get("message", "Unknown error"),
                    response
                )
            
            return response
            
        except asyncio.TimeoutError:
            await self.disconnect()
            raise WMSProtocolError("WMS response timeout", {"timeout": self.timeout})
        except json.JSONDecodeError as e:
            raise WMSProtocolError(f"Invalid JSON response: {e}", {})
    
    async def receive_package(
        self,
        order_id: str,
        weight: float = 0,
        description: str = ""
    ) -> dict[str, Any]:
        """
        Register package arrival at warehouse.
        
        Args:
            order_id: Order ID for the package
            weight: Package weight in kg
            description: Package description
            
        Returns:
            Response with package details:
            {
                "status": "OK",
                "event": "PACKAGE_RECEIVED",
                "package": {
                    "order_id": str,
                    "package_id": str,
                    "status": "received",
                    "received_at": str,
                    ...
                }
            }
        """
        logger.info("WMS: Receiving package for order %s", order_id)
        
        result = await self._send_command(
            "RECEIVE_PACKAGE",
            order_id=order_id,
            weight=weight,
            description=description
        )
        
        logger.info(
            "WMS: Package received - order=%s package_id=%s",
            order_id, result.get("package", {}).get("package_id")
        )
        
        return result
    
    async def check_status(self, order_id: str) -> dict[str, Any]:
        """
        Check current package status in warehouse.
        
        Args:
            order_id: Order ID to check
            
        Returns:
            Response with current package status
        """
        logger.info("WMS: Checking status for order %s", order_id)
        
        result = await self._send_command("CHECK_STATUS", order_id=order_id)
        
        status = result.get("package", {}).get("status", "unknown")
        logger.info("WMS: Order %s status: %s", order_id, status)
        
        return result
    
    async def load_vehicle(self, order_id: str, vehicle_id: str = "") -> dict[str, Any]:
        """
        Mark package as loaded onto delivery vehicle.
        
        Args:
            order_id: Order ID of package to load
            vehicle_id: Optional vehicle identifier
            
        Returns:
            Response confirming package load
        """
        logger.info("WMS: Loading package for order %s onto vehicle", order_id)
        
        result = await self._send_command(
            "LOAD_VEHICLE",
            order_id=order_id,
            vehicle_id=vehicle_id
        )
        
        logger.info("WMS: Package loaded for order %s", order_id)
        
        return result
    
    async def register_order(self, order_data: dict) -> dict[str, Any]:
        """
        Full order registration in WMS:
        1. Receive package
        
        Args:
            order_data: Order data dict
            
        Returns:
            Combined result of WMS operations
        """
        receive_result = await self.receive_package(
            order_id=order_data.get("order_id", ""),
            weight=order_data.get("package_weight", 0),
            description=order_data.get("package_description", "")
        )
        
        return {
            "success": receive_result.get("status") == "OK",
            "event": receive_result.get("event"),
            "package": receive_result.get("package"),
            "order_id": order_data.get("order_id")
        }


class WMSStatusListener:
    """
    Persistent listener for WMS status updates.
    
    Maintains TCP connection and processes incoming messages.
    Used for real-time warehouse status tracking.
    """
    
    def __init__(
        self,
        host: str,
        port: int = 9000,
        on_package_received: Callable[[dict], Coroutine] | None = None,
        on_package_ready: Callable[[dict], Coroutine] | None = None,
        on_package_loaded: Callable[[dict], Coroutine] | None = None,
        on_error: Callable[[Exception], Coroutine] | None = None
    ):
        """
        Initialize WMS status listener.
        
        Args:
            host: WMS server hostname
            port: WMS server port
            on_package_received: Callback for PACKAGE_RECEIVED events
            on_package_ready: Callback for PACKAGE_READY events
            on_package_loaded: Callback for PACKAGE_LOADED events
            on_error: Callback for errors
        """
        self.host = host
        self.port = port
        self._callbacks = {
            "PACKAGE_RECEIVED": on_package_received,
            "PACKAGE_READY": on_package_ready,
            "PACKAGE_LOADED": on_package_loaded,
        }
        self._on_error = on_error
        self._running = False
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
    
    async def start(self) -> None:
        """Start listening for WMS status updates."""
        self._running = True
        logger.info("WMS Listener: Starting on %s:%d", self.host, self.port)
        
        while self._running:
            try:
                # Connect
                self._reader, self._writer = await asyncio.open_connection(
                    self.host, self.port
                )
                logger.info("WMS Listener: Connected")
                
                # Read messages
                while self._running:
                    line = await self._reader.readline()
                    if not line:
                        break
                    
                    await self._process_message(line.decode("utf-8").strip())
                    
            except Exception as e:
                logger.error("WMS Listener error: %s", e)
                if self._on_error:
                    try:
                        await self._on_error(e)
                    except Exception:
                        pass
                
                if self._running:
                    await asyncio.sleep(5.0)  # Reconnect delay
    
    async def stop(self) -> None:
        """Stop the listener."""
        self._running = False
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()
        logger.info("WMS Listener: Stopped")
    
    async def _process_message(self, raw: str) -> None:
        """Process incoming WMS message."""
        try:
            message = json.loads(raw)
            event_type = message.get("event")
            
            logger.debug("WMS Listener received: %s", event_type)
            
            callback = self._callbacks.get(event_type)
            if callback:
                await callback(message)
                
        except json.JSONDecodeError:
            logger.warning("WMS Listener: Invalid JSON: %s", raw)


# ── Message Parsing Utilities ────────────────────────────────

def parse_wms_message(raw: str) -> dict[str, Any]:
    """
    Parse raw WMS TCP message.
    
    Args:
        raw: Raw message string (JSON newline-delimited)
        
    Returns:
        Parsed message dict
        
    Raises:
        WMSProtocolError: If parsing fails
    """
    try:
        return json.loads(raw.strip())
    except json.JSONDecodeError as e:
        raise WMSProtocolError(f"Invalid WMS message format: {e}", {"raw": raw})


def build_wms_command(command: str, **kwargs) -> bytes:
    """
    Build WMS TCP command message.
    
    Args:
        command: Command name
        **kwargs: Command parameters
        
    Returns:
        Encoded message bytes ready to send
    """
    message = {"command": command, **kwargs}
    return (json.dumps(message) + "\n").encode("utf-8")


def extract_package_info(response: dict) -> dict[str, Any]:
    """
    Extract package information from WMS response.
    
    Args:
        response: WMS response dict
        
    Returns:
        Normalized package info dict
    """
    package = response.get("package", {})
    return {
        "order_id": package.get("order_id"),
        "package_id": package.get("package_id"),
        "status": package.get("status"),
        "weight": package.get("weight"),
        "description": package.get("description"),
        "received_at": package.get("received_at"),
        "loaded_at": package.get("loaded_at"),
        "wms_event": response.get("event")
    }


# ── Synchronous convenience wrapper ──────────────────────────
async def send_wms_command(
    host: str,
    port: int,
    command: str,
    **kwargs
) -> dict[str, Any]:
    """
    Quick helper to send single WMS command.
    
    Args:
        host: WMS hostname
        port: WMS port
        command: Command name
        **kwargs: Command parameters
        
    Returns:
        WMS response dict
    """
    async with WMSClient(host, port) as client:
        return await client._send_command(command, **kwargs)
