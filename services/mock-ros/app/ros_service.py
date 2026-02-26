"""
Mock Route Optimisation System (ROS) – RESTful JSON API.

Simulates the cloud-based third-party ROS described in the scenario.
Provides REST endpoints for route optimisation and also consumes
order.created events from RabbitMQ to asynchronously compute routes.
"""

import json
import logging
import random
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import aio_pika
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("mock-ros")

RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"

# ── In-memory route store ────────────────────────────────────
routes_db: dict[str, dict] = {}


# ── Request / Response models ────────────────────────────────
class RouteRequest(BaseModel):
    order_id: str
    pickup_address: str
    delivery_address: str
    vehicle_type: str = Field(default="motorcycle", example="motorcycle")


class RouteResponse(BaseModel):
    route_id: str
    order_id: str
    pickup_address: str
    delivery_address: str
    estimated_distance_km: float
    estimated_duration_min: int
    optimised_waypoints: list[dict]
    status: str


# ── Route optimisation logic (mock) ─────────────────────────
def _compute_route(order_id: str, pickup: str, delivery: str) -> dict:
    """Generate a fake optimised route."""
    route_id = str(uuid.uuid4())
    distance = round(random.uniform(2.0, 35.0), 1)
    duration = int(distance * random.uniform(2.5, 4.0))

    route = {
        "route_id": route_id,
        "order_id": order_id,
        "pickup_address": pickup,
        "delivery_address": delivery,
        "estimated_distance_km": distance,
        "estimated_duration_min": duration,
        "optimised_waypoints": [
            {"lat": 6.9271 + random.uniform(-0.05, 0.05), "lng": 79.8612 + random.uniform(-0.05, 0.05), "label": "Start"},
            {"lat": 6.9271 + random.uniform(-0.05, 0.05), "lng": 79.8612 + random.uniform(-0.05, 0.05), "label": "Waypoint 1"},
            {"lat": 6.9271 + random.uniform(-0.05, 0.05), "lng": 79.8612 + random.uniform(-0.05, 0.05), "label": "Destination"},
        ],
        "status": "optimised",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    routes_db[route_id] = route
    return route


# ── RabbitMQ consumer ────────────────────────────────────────
async def on_order_created(message: aio_pika.abc.AbstractIncomingMessage):
    async with message.process():
        try:
            body = json.loads(message.body.decode())
            order_id = body.get("order_id")
            pickup = body.get("pickup_address", "")
            delivery = body.get("delivery_address", "")
            logger.info("ROS received order.created: %s", order_id)

            route = _compute_route(order_id, pickup, delivery)

            # Publish route optimised event
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            channel = await connection.channel()

            # Tracking update
            tex = await channel.declare_exchange("swifttrack.tracking", aio_pika.ExchangeType.TOPIC, durable=True)
            await tex.publish(
                aio_pika.Message(
                    body=json.dumps({
                        "event": "tracking.update",
                        "order_id": order_id,
                        "event_type": "route_optimised",
                        "description": f"Route optimised: {route['estimated_distance_km']}km, ~{route['estimated_duration_min']}min",
                        "location": "ROS System",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }).encode(),
                    content_type="application/json",
                ),
                routing_key="tracking.update",
            )

            # Saga reply
            oex = await channel.declare_exchange("swifttrack.orders", aio_pika.ExchangeType.TOPIC, durable=True)
            await oex.publish(
                aio_pika.Message(
                    body=json.dumps({
                        "event": "ros.route.optimised",
                        "order_id": order_id,
                        "step": "ros",
                        "success": True,
                        "route": route,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }).encode(),
                    content_type="application/json",
                ),
                routing_key="ros.route",
            )
            await connection.close()
            logger.info("Route optimised for order %s: %s", order_id, route["route_id"])
        except Exception:
            logger.exception("ROS processing error")


# ── Lifespan ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.info("Mock ROS starting …")
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        channel = await connection.channel()
        exchange = await channel.declare_exchange("swifttrack.orders", aio_pika.ExchangeType.TOPIC, durable=True)
        queue = await channel.declare_queue("order.ros", durable=True)
        await queue.bind(exchange, routing_key="order.created")
        await queue.consume(on_order_created)
        logger.info("ROS consuming from order.ros queue.")
        application.state.rmq_connection = connection
    except Exception:
        logger.exception("Failed to connect to RabbitMQ")
    yield
    if hasattr(application.state, "rmq_connection"):
        await application.state.rmq_connection.close()
    logger.info("Mock ROS shutting down.")


app = FastAPI(title="Mock ROS (REST/JSON)", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── REST endpoints ───────────────────────────────────────────
@app.post("/api/v1/routes/optimise", response_model=RouteResponse)
async def optimise_route(payload: RouteRequest):
    """Synchronous route optimisation endpoint."""
    route = _compute_route(payload.order_id, payload.pickup_address, payload.delivery_address)
    logger.info("Route computed via REST for order %s", payload.order_id)
    return route


@app.get("/api/v1/routes/{route_id}", response_model=RouteResponse)
async def get_route(route_id: str):
    route = routes_db.get(route_id)
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")
    return route


@app.get("/api/v1/routes")
async def list_routes():
    return list(routes_db.values())


@app.get("/")
async def root():
    return {"service": "mock-ros", "protocol": "REST/JSON", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy"}
