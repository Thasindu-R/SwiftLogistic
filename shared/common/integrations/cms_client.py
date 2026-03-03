"""
CMS Client – SOAP/XML Integration
=================================

Integrates with the Client Management System (CMS) via SOAP protocol.
Handles XML conversion, SOAP envelope creation, and response parsing.

SOAP Operations:
- ValidateClient: Check if client exists and is active
- GetClientInfo: Retrieve client details
- CreateBilling: Create billing record for order

Usage:
    async with CMSClient("http://mock-cms:8004") as client:
        result = await client.validate_client(client_id=1)
        billing = await client.create_billing(order_data)
"""

import logging
from typing import Any
from xml.etree import ElementTree as ET

import httpx

from .transformers import DataTransformer

logger = logging.getLogger(__name__)


class CMSSoapError(Exception):
    """Raised when CMS SOAP call fails."""
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"SOAP Fault [{code}]: {message}")


class CMSClient:
    """
    SOAP client for CMS (Client Management System) integration.
    
    Converts internal JSON order data to SOAP XML format,
    sends requests, and parses XML responses back to dicts.
    """
    
    # XML Namespaces
    SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
    CMS_NS = "http://swiftlogistics.lk/cms"
    
    def __init__(self, base_url: str, timeout: float = 30.0):
        """
        Initialize CMS client.
        
        Args:
            base_url: CMS service URL (e.g., http://mock-cms:8005)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._client:
            await self._client.aclose()
            self._client = None
    
    def _build_soap_envelope(self, operation: str, body_xml: str) -> str:
        """
        Build complete SOAP envelope with the given body content.
        
        Args:
            operation: SOAP operation name
            body_xml: XML content for SOAP Body
            
        Returns:
            Complete SOAP XML envelope string
        """
        return f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="{self.SOAP_NS}" xmlns:cms="{self.CMS_NS}">
  <soap:Header/>
  <soap:Body>
    {body_xml}
  </soap:Body>
</soap:Envelope>"""
    
    def _parse_soap_response(self, xml_str: str) -> ET.Element:
        """
        Parse SOAP response and check for faults.
        
        Args:
            xml_str: Raw XML response string
            
        Returns:
            Body element containing response data
            
        Raises:
            CMSSoapError: If SOAP fault detected
        """
        root = ET.fromstring(xml_str)
        
        # Find Body element (with or without namespace)
        body = root.find(f".//{{{self.SOAP_NS}}}Body")
        if body is None:
            body = root.find(".//Body")
        if body is None:
            for child in root:
                if "Body" in child.tag:
                    body = child
                    break
        
        if body is None:
            raise CMSSoapError("Client", "Missing SOAP Body in response")
        
        # Check for SOAP Fault
        fault = body.find(f".//{{{self.SOAP_NS}}}Fault")
        if fault is None:
            fault = body.find(".//Fault")
        
        if fault is not None:
            code_el = fault.find("faultcode")
            msg_el = fault.find("faultstring")
            raise CMSSoapError(
                code_el.text if code_el is not None else "Unknown",
                msg_el.text if msg_el is not None else "Unknown error"
            )
        
        return body
    
    async def _send_soap_request(self, xml_body: str) -> ET.Element:
        """
        Send SOAP request to CMS and return parsed response.
        
        Args:
            xml_body: Complete SOAP XML request
            
        Returns:
            Parsed response body element
        """
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": ""
        }
        
        logger.debug("Sending SOAP request to CMS:\n%s", xml_body)
        
        response = await self._client.post(
            f"{self.base_url}/soap/cms",
            content=xml_body,
            headers=headers
        )
        
        logger.debug("CMS response status: %d", response.status_code)
        logger.debug("CMS response body:\n%s", response.text)
        
        if response.status_code >= 400:
            raise CMSSoapError("HTTP", f"HTTP {response.status_code}: {response.text}")
        
        return self._parse_soap_response(response.text)
    
    async def validate_client(self, client_id: int) -> dict[str, Any]:
        """
        Validate if a client exists and is active in CMS.
        
        Args:
            client_id: Client ID to validate
            
        Returns:
            Dict with validation result: {"valid": bool, "client_id": int}
        """
        body_xml = f"""<cms:ValidateClient>
      <ClientId>{client_id}</ClientId>
    </cms:ValidateClient>"""
        
        envelope = self._build_soap_envelope("ValidateClient", body_xml)
        
        logger.info("CMS: Validating client %d", client_id)
        response_body = await self._send_soap_request(envelope)
        
        # Parse response
        valid_el = response_body.find(f".//{{{self.CMS_NS}}}Valid")
        if valid_el is None:
            valid_el = response_body.find(".//Valid")
        
        valid = valid_el is not None and valid_el.text and valid_el.text.lower() == "true"
        
        result = {"valid": valid, "client_id": client_id}
        logger.info("CMS: Client %d validation result: %s", client_id, valid)
        
        return result
    
    async def get_client_info(self, client_id: int) -> dict[str, Any]:
        """
        Get detailed client information from CMS.
        
        Args:
            client_id: Client ID to query
            
        Returns:
            Dict with client info: {client_id, name, contract, active}
        """
        body_xml = f"""<cms:GetClientInfo>
      <ClientId>{client_id}</ClientId>
    </cms:GetClientInfo>"""
        
        envelope = self._build_soap_envelope("GetClientInfo", body_xml)
        
        logger.info("CMS: Getting info for client %d", client_id)
        response_body = await self._send_soap_request(envelope)
        
        # Parse response elements
        def get_text(tag: str) -> str | None:
            el = response_body.find(f".//{{{self.CMS_NS}}}{tag}")
            if el is None:
                el = response_body.find(f".//{tag}")
            return el.text if el is not None else None
        
        result = {
            "client_id": int(get_text("ClientId") or client_id),
            "name": get_text("Name"),
            "contract": get_text("Contract"),
            "active": get_text("Active") == "true"
        }
        
        logger.info("CMS: Retrieved client info for %d: %s", client_id, result.get("name"))
        return result
    
    async def create_billing(self, order_data: dict) -> dict[str, Any]:
        """
        Create billing record in CMS for an order.
        
        Args:
            order_data: Order dict containing order_id, client_id, package_weight
            
        Returns:
            Dict with billing confirmation
        """
        body_xml = f"""<cms:CreateBilling>
      <OrderId>{order_data.get('order_id', '')}</OrderId>
      <ClientId>{order_data.get('client_id', 0)}</ClientId>
      <Amount>{order_data.get('package_weight', 1.0) * 150.0}</Amount>
      <Currency>LKR</Currency>
      <Description>Delivery service for order {order_data.get('order_id', '')}</Description>
    </cms:CreateBilling>"""
        
        envelope = self._build_soap_envelope("CreateBilling", body_xml)
        
        logger.info("CMS: Creating billing for order %s", order_data.get("order_id"))
        response_body = await self._send_soap_request(envelope)
        
        # Parse billing response
        def get_text(tag: str) -> str | None:
            el = response_body.find(f".//{{{self.CMS_NS}}}{tag}")
            if el is None:
                el = response_body.find(f".//{tag}")
            return el.text if el is not None else None
        
        result = {
            "billing_id": get_text("BillingId"),
            "status": get_text("Status") or "confirmed",
            "order_id": order_data.get("order_id"),
            "amount": get_text("Amount"),
        }
        
        logger.info("CMS: Billing created for order %s: %s", 
                   order_data.get("order_id"), result.get("billing_id"))
        return result
    
    async def transmit_order(self, order_data: dict) -> dict[str, Any]:
        """
        Full order transmission to CMS:
        1. Validate client
        2. Create billing record
        
        Args:
            order_data: Complete order data dict
            
        Returns:
            Dict with transmission result including validation and billing
            
        Raises:
            CMSSoapError: If validation fails or client inactive
        """
        client_id = order_data.get("client_id", 0)
        
        # Step 1: Validate client
        validation = await self.validate_client(client_id)
        if not validation.get("valid"):
            raise CMSSoapError("Business", f"Client {client_id} not found or inactive")
        
        # Step 2: Get client info for logging
        client_info = await self.get_client_info(client_id)
        
        # Step 3: Create billing (this is handled via RabbitMQ in real flow)
        # Return combined result
        result = {
            "success": True,
            "client_validated": True,
            "client_info": client_info,
            "order_id": order_data.get("order_id"),
            "message": f"Order transmitted to CMS for client '{client_info.get('name')}'"
        }
        
        logger.info("CMS: Order %s fully transmitted", order_data.get("order_id"))
        return result


# ── Synchronous convenience wrapper ──────────────────────────
async def validate_client_for_order(cms_url: str, client_id: int) -> bool:
    """
    Quick helper to validate a client exists.
    
    Args:
        cms_url: CMS service URL
        client_id: Client ID to validate
        
    Returns:
        True if client valid, False otherwise
    """
    try:
        async with CMSClient(cms_url) as client:
            result = await client.validate_client(client_id)
            return result.get("valid", False)
    except Exception as e:
        logger.error("Client validation failed: %s", e)
        return False
