"""
Mock Warehouse Management System (WMS) – Proprietary TCP/IP messaging.

Simulates the WMS described in the SwiftLogistics scenario.
Runs TWO servers simultaneously:
  1. A raw TCP socket server on port 9000 for proprietary messaging.
  2. A lightweight FastAPI HTTP server on port 8006 for health checks.

Also consumes order.created events from RabbitMQ and publishes
wms.package.* tracking events.

TCP Protocol (JSON-over-TCP, newline-delimited):
  → Client sends:  {"command": "RECEIVE_PACKAGE", "order_id": "...", ...}\n
  ← Server replies: {"status": "OK", "event": "PACKAGE_RECEIVED", ...}\n

  Commands: RECEIVE_PACKAGE, CHECK_STATUS, LOAD_VEHICLE
  Events:   PACKAGE_RECEIVED, PACKAGE_READY, PACKAGE_LOADED
"""

import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import aio_pika
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("mock-wms")

RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"
TCP_PORT = 9000


async def connect_rabbitmq_with_retry(
    url: str,
    *,
    retries: int = 30,
    delay_seconds: float = 2.0,
) -> aio_pika.abc.AbstractRobustConnection:
    """Connect to RabbitMQ with retry to avoid startup race conditions."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return await aio_pika.connect_robust(url)
        except Exception as e:
            last_error = e
            logger.warning(
                "RabbitMQ not ready (attempt %d/%d): %s",
                attempt,
                retries,
                e,
            )
            await asyncio.sleep(delay_seconds)
    raise RuntimeError(f"Failed to connect to RabbitMQ after {retries} attempts: {last_error}")

# ── In-memory warehouse ──────────────────────────────────────
warehouse: dict[str, dict] = {}  # order_id → package info


# ── Publish tracking event helper ────────────────────────────
async def publish_tracking(order_id: str, event_type: str, description: str):
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        channel = await connection.channel()
        exchange = await channel.declare_exchange("swifttrack.tracking", aio_pika.ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(
                body=json.dumps({
                    "event": "tracking.update",
                    "order_id": order_id,
                    "event_type": event_type,
                    "description": description,
                    "location": "Warehouse",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }).encode(),
                content_type="application/json",
            ),
            routing_key="tracking.update",
        )
        await connection.close()
    except Exception:
        logger.exception("Failed to publish tracking event")


# ── TCP handler ──────────────────────────────────────────────
async def handle_tcp_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    logger.info("TCP client connected: %s", addr)

    try:
        while True:
            data = await reader.readline()
            if not data:
                break

            try:
                msg = json.loads(data.decode().strip())
            except json.JSONDecodeError:
                response = {"status": "ERROR", "message": "Invalid JSON"}
                writer.write((json.dumps(response) + "\n").encode())
                await writer.drain()
                continue

            command = msg.get("command", "").upper()
            order_id = msg.get("order_id", "")

            if command == "RECEIVE_PACKAGE":
                package = {
                    "order_id": order_id,
                    "package_id": str(uuid.uuid4()),
                    "status": "received",
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "weight": msg.get("weight", 0),
                    "description": msg.get("description", ""),
                }
                warehouse[order_id] = package
                response = {"status": "OK", "event": "PACKAGE_RECEIVED", "package": package}
                await publish_tracking(order_id, "package_received", "Package received at warehouse")

            elif command == "CHECK_STATUS":
                package = warehouse.get(order_id)
                if package:
                    response = {"status": "OK", "event": "STATUS", "package": package}
                else:
                    response = {"status": "ERROR", "message": "Package not found"}

            elif command == "LOAD_VEHICLE":
                package = warehouse.get(order_id)
                if package:
                    package["status"] = "loaded"
                    package["loaded_at"] = datetime.now(timezone.utc).isoformat()
                    response = {"status": "OK", "event": "PACKAGE_LOADED", "package": package}
                    await publish_tracking(order_id, "package_loaded", "Package loaded onto delivery vehicle")
                else:
                    response = {"status": "ERROR", "message": "Package not found"}

            else:
                response = {"status": "ERROR", "message": f"Unknown command: {command}"}

            writer.write((json.dumps(response) + "\n").encode())
            await writer.drain()
            logger.info("TCP %s → %s for order %s", command, response.get("status"), order_id)

    except asyncio.IncompleteReadError:
        pass
    except Exception:
        logger.exception("TCP handler error")
    finally:
        writer.close()
        await writer.wait_closed()
        logger.info("TCP client disconnected: %s", addr)


# ── RabbitMQ consumer ────────────────────────────────────────
async def on_order_created(message: aio_pika.abc.AbstractIncomingMessage):
    """Automatically receive package into warehouse when order is created."""
    async with message.process():
        try:
            body = json.loads(message.body.decode())
            order_id = body.get("order_id")
            logger.info("WMS received order.created: %s", order_id)

            package = {
                "order_id": order_id,
                "package_id": str(uuid.uuid4()),
                "status": "received",
                "received_at": datetime.now(timezone.utc).isoformat(),
                "weight": body.get("package_weight", 0),
                "description": body.get("package_description", ""),
            }
            warehouse[order_id] = package

            await publish_tracking(order_id, "package_received", "Package received at warehouse via WMS")

            # Saga reply
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            channel = await connection.channel()
            oex = await channel.declare_exchange("swifttrack.orders", aio_pika.ExchangeType.TOPIC, durable=True)
            await oex.publish(
                aio_pika.Message(
                    body=json.dumps({
                        "event": "wms.package.received",
                        "order_id": order_id,
                        "step": "wms",
                        "success": True,
                        "package": package,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }).encode(),
                    content_type="application/json",
                ),
                routing_key="wms.package",
            )
            await connection.close()
            logger.info("WMS package received for order %s", order_id)
        except Exception:
            logger.exception("WMS processing error")


# ── Application ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.info("Mock WMS starting …")

    # Start TCP server
    tcp_server = await asyncio.start_server(handle_tcp_client, "0.0.0.0", TCP_PORT)
    logger.info("WMS TCP server listening on port %d", TCP_PORT)

    # Connect to RabbitMQ
    try:
        connection = await connect_rabbitmq_with_retry(RABBITMQ_URL)
        channel = await connection.channel()
        exchange = await channel.declare_exchange("swifttrack.orders", aio_pika.ExchangeType.TOPIC, durable=True)
        queue = await channel.declare_queue("order.wms", durable=True)
        await queue.bind(exchange, routing_key="order.created")
        await queue.consume(on_order_created)
        logger.info("WMS consuming from order.wms queue.")
        application.state.rmq_connection = connection
    except Exception:
        logger.exception("Failed to connect to RabbitMQ")

    yield

    tcp_server.close()
    await tcp_server.wait_closed()
    if hasattr(application.state, "rmq_connection"):
        await application.state.rmq_connection.close()
    logger.info("Mock WMS shutting down.")


app = FastAPI(title="Mock WMS (TCP/IP)", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/api/wms/packages")
async def list_packages():
    return list(warehouse.values())


@app.get("/api/wms/packages/{order_id}")
async def get_package(order_id: str):
    pkg = warehouse.get(order_id)
    if not pkg:
        return {"error": "Package not found"}
    return pkg


@app.get("/")
async def root():
    return {"service": "mock-wms", "protocol": "TCP/IP + HTTP", "tcp_port": TCP_PORT, "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy"}
