"""
Mock Client Management System (CMS) – SOAP/XML API.

Simulates the legacy on-premise CMS described in the SwiftLogistics scenario.
Exposes SOAP-style endpoints that accept XML requests and return XML responses
wrapped in SOAP envelopes.

Also consumes order.created events from RabbitMQ and publishes
cms.billing.confirmed / cms.billing.failed events.
"""

import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import aio_pika
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("mock-cms")

# ── In-memory data stores ────────────────────────────────────
clients_db: dict[int, dict] = {
    1: {"client_id": 1, "name": "ABC Online Store", "contract": "GOLD", "active": True},
    2: {"client_id": 2, "name": "XYZ Retail", "contract": "SILVER", "active": True},
    3: {"client_id": 3, "name": "Demo Client", "contract": "BRONZE", "active": True},
}
billing_records: list[dict] = []

RABBITMQ_URL = "amqp://guest:guest@rabbitmq:5672/"


# ── SOAP helpers ─────────────────────────────────────────────
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
CMS_NS = "http://swiftlogistics.lk/cms"


def soap_envelope(body_xml: str) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="{SOAP_NS}" xmlns:cms="{CMS_NS}">
  <soap:Header/>
  <soap:Body>
    {body_xml}
  </soap:Body>
</soap:Envelope>"""


def soap_fault(code: str, message: str) -> str:
    return soap_envelope(
        f"""<soap:Fault>
      <faultcode>{code}</faultcode>
      <faultstring>{message}</faultstring>
    </soap:Fault>"""
    )


# ── RabbitMQ consumer ────────────────────────────────────────
async def on_order_created(message: aio_pika.abc.AbstractIncomingMessage):
    """Process new order: validate client → create billing → publish result."""
    async with message.process():
        try:
            body = json.loads(message.body.decode())
            order_id = body.get("order_id")
            client_id = body.get("client_id")
            logger.info("CMS received order.created: order=%s client=%s", order_id, client_id)

            # Validate client exists (simulated)
            client = clients_db.get(client_id)
            success = client is not None and client.get("active", False)

            if success:
                billing = {
                    "billing_id": str(uuid.uuid4()),
                    "order_id": order_id,
                    "client_id": client_id,
                    "amount": round(body.get("package_weight", 1.0) * 150.0, 2),
                    "currency": "LKR",
                    "status": "invoiced",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                billing_records.append(billing)

            # Publish reply
            reply = {
                "event": "cms.billing.confirmed" if success else "cms.billing.failed",
                "order_id": order_id,
                "client_id": client_id,
                "step": "cms",
                "success": success,
                "detail": billing if success else {"reason": "Client not found or inactive"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            channel = await connection.channel()
            exchange = await channel.declare_exchange("swifttrack.orders", aio_pika.ExchangeType.TOPIC, durable=True)
            await exchange.publish(
                aio_pika.Message(body=json.dumps(reply).encode(), content_type="application/json"),
                routing_key="cms.billing",
            )
            await connection.close()

            # Also publish tracking event
            connection2 = await aio_pika.connect_robust(RABBITMQ_URL)
            channel2 = await connection2.channel()
            exchange2 = await channel2.declare_exchange("swifttrack.tracking", aio_pika.ExchangeType.TOPIC, durable=True)
            await exchange2.publish(
                aio_pika.Message(
                    body=json.dumps({
                        "event": "tracking.update",
                        "order_id": order_id,
                        "event_type": "cms_confirmed" if success else "cms_failed",
                        "description": f"CMS billing {'confirmed' if success else 'failed'} for client {client_id}",
                        "location": "CMS System",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }).encode(),
                    content_type="application/json",
                ),
                routing_key="tracking.update",
            )
            await connection2.close()

            logger.info("CMS billing %s for order %s", "confirmed" if success else "failed", order_id)
        except Exception:
            logger.exception("CMS processing error")


# ── Lifespan ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.info("Mock CMS starting …")
    # Connect to RabbitMQ and consume order.created events
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        channel = await connection.channel()
        exchange = await channel.declare_exchange("swifttrack.orders", aio_pika.ExchangeType.TOPIC, durable=True)
        queue = await channel.declare_queue("order.cms", durable=True)
        await queue.bind(exchange, routing_key="order.created")
        await queue.consume(on_order_created)
        logger.info("CMS consuming from order.cms queue.")
        application.state.rmq_connection = connection
    except Exception:
        logger.exception("Failed to connect to RabbitMQ")

    yield

    if hasattr(application.state, "rmq_connection"):
        await application.state.rmq_connection.close()
    logger.info("Mock CMS shutting down.")


app = FastAPI(title="Mock CMS (SOAP/XML)", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── SOAP endpoint ────────────────────────────────────────────
@app.post("/soap/cms")
async def soap_endpoint(request: Request):
    """
    SOAP-style endpoint. Parses the XML body to determine the operation:
      - GetClientInfo
      - CreateBilling
      - ValidateClient
    """
    raw = await request.body()
    content_type = request.headers.get("content-type", "")

    if "xml" not in content_type and "text" not in content_type:
        return Response(
            content=soap_fault("Client", "Content-Type must be text/xml"),
            media_type="text/xml",
            status_code=400,
        )

    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return Response(
            content=soap_fault("Client", "Malformed XML"),
            media_type="text/xml",
            status_code=400,
        )

    # Find operation element inside Body
    body_el = root.find(f".//{{{SOAP_NS}}}Body")
    if body_el is None:
        body_el = root.find(".//Body")
    if body_el is None:
        # Try without namespace
        for child in root:
            if "Body" in child.tag:
                body_el = child
                break

    if body_el is None:
        return Response(
            content=soap_fault("Client", "Missing SOAP Body"),
            media_type="text/xml",
            status_code=400,
        )

    # Determine operation from first child of Body
    operation_el = list(body_el)[0] if len(list(body_el)) > 0 else None
    if operation_el is None:
        return Response(
            content=soap_fault("Client", "No operation in SOAP Body"),
            media_type="text/xml",
            status_code=400,
        )

    op_tag = operation_el.tag.split("}")[-1] if "}" in operation_el.tag else operation_el.tag

    if op_tag == "GetClientInfo":
        cid_el = operation_el.find("ClientId") or operation_el.find(f"{{{CMS_NS}}}ClientId")
        if cid_el is None:
            return Response(content=soap_fault("Client", "Missing ClientId"), media_type="text/xml")
        client = clients_db.get(int(cid_el.text or "0"))
        if not client:
            return Response(content=soap_fault("Client", "Client not found"), media_type="text/xml")
        resp_xml = soap_envelope(f"""<cms:GetClientInfoResponse>
      <cms:ClientId>{client['client_id']}</cms:ClientId>
      <cms:Name>{client['name']}</cms:Name>
      <cms:Contract>{client['contract']}</cms:Contract>
      <cms:Active>{str(client['active']).lower()}</cms:Active>
    </cms:GetClientInfoResponse>""")
        return Response(content=resp_xml, media_type="text/xml")

    elif op_tag == "ValidateClient":
        cid_el = operation_el.find("ClientId") or operation_el.find(f"{{{CMS_NS}}}ClientId")
        client = clients_db.get(int(cid_el.text or "0")) if cid_el is not None else None
        valid = client is not None and client.get("active", False)
        resp_xml = soap_envelope(f"""<cms:ValidateClientResponse>
      <cms:Valid>{str(valid).lower()}</cms:Valid>
    </cms:ValidateClientResponse>""")
        return Response(content=resp_xml, media_type="text/xml")

    else:
        return Response(
            content=soap_fault("Client", f"Unknown operation: {op_tag}"),
            media_type="text/xml",
            status_code=400,
        )


# ── REST convenience endpoints (for debugging / demo) ───────
@app.get("/api/cms/clients")
async def list_clients():
    return list(clients_db.values())


@app.get("/api/cms/billing")
async def list_billing():
    return billing_records


@app.get("/")
async def root():
    return {"service": "mock-cms", "protocol": "SOAP/XML", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy"}
