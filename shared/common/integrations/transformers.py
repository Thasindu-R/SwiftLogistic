"""
Data Format Transformers
========================

Utilities for converting between different data formats used by
integration systems:

- JSON ↔ XML: For CMS SOAP integration
- TCP Messages ↔ JSON: For WMS proprietary protocol
- Internal Model Normalization: Consistent data structure

Maintains consistent internal data model across all transformations.

Usage:
    transformer = DataTransformer()
    
    # JSON to XML
    xml_str = transformer.json_to_xml(order_dict, root_tag="Order")
    
    # XML to JSON
    json_dict = transformer.xml_to_json(xml_string)
    
    # Normalize from different sources
    normalized = transformer.normalize_order(external_data, source="cms")
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Central transformer for all data format conversions.
    
    Handles:
    - JSON ↔ XML conversions
    - TCP message ↔ JSON conversions
    - Order data normalization from different sources
    """
    
    # Standard field mappings for normalization
    ORDER_FIELD_MAP = {
        # CMS field names → internal names
        "OrderId": "order_id",
        "orderId": "order_id",
        "order-id": "order_id",
        "ClientId": "client_id",
        "clientId": "client_id",
        "client-id": "client_id",
        "PickupAddress": "pickup_address",
        "pickupAddress": "pickup_address",
        "pickup-address": "pickup_address",
        "DeliveryAddress": "delivery_address",
        "deliveryAddress": "delivery_address",
        "delivery-address": "delivery_address",
        "RecipientName": "recipient_name",
        "recipientName": "recipient_name",
        "recipient-name": "recipient_name",
        "RecipientPhone": "recipient_phone",
        "recipientPhone": "recipient_phone",
        "recipient-phone": "recipient_phone",
        "PackageWeight": "package_weight",
        "packageWeight": "package_weight",
        "package-weight": "package_weight",
        "PackageDescription": "package_description",
        "packageDescription": "package_description",
        "package-description": "package_description",
    }
    
    def __init__(self, default_namespace: str | None = None):
        """
        Initialize transformer.
        
        Args:
            default_namespace: Optional default XML namespace
        """
        self.default_namespace = default_namespace
    
    # ══════════════════════════════════════════════════════════
    # JSON ↔ XML Conversions
    # ══════════════════════════════════════════════════════════
    
    def json_to_xml(
        self,
        data: dict | list,
        root_tag: str = "root",
        namespace: str | None = None,
        pretty: bool = True
    ) -> str:
        """
        Convert JSON/dict to XML string.
        
        Args:
            data: Dict or list to convert
            root_tag: Root element tag name
            namespace: Optional XML namespace
            pretty: Whether to indent output
            
        Returns:
            XML string representation
        """
        ns = namespace or self.default_namespace
        ns_prefix = f"{{{ns}}}" if ns else ""
        
        root = ET.Element(f"{ns_prefix}{root_tag}")
        if ns:
            root.set("xmlns", ns)
        
        self._dict_to_xml_element(data, root, ns_prefix)
        
        xml_str = ET.tostring(root, encoding="unicode")
        
        if pretty:
            xml_str = self._prettify_xml(xml_str)
        
        return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'
    
    def _dict_to_xml_element(
        self,
        data: Any,
        parent: ET.Element,
        ns_prefix: str = ""
    ) -> None:
        """Recursively convert dict/list to XML elements."""
        if isinstance(data, dict):
            for key, value in data.items():
                # Convert snake_case to PascalCase for XML
                xml_key = self._to_pascal_case(key)
                child = ET.SubElement(parent, f"{ns_prefix}{xml_key}")
                
                if isinstance(value, (dict, list)):
                    self._dict_to_xml_element(value, child, ns_prefix)
                elif value is not None:
                    child.text = str(value)
                    
        elif isinstance(data, list):
            for idx, item in enumerate(data):
                item_tag = parent.tag.rstrip("s") if parent.tag.endswith("s") else "item"
                child = ET.SubElement(parent, item_tag)
                
                if isinstance(item, (dict, list)):
                    self._dict_to_xml_element(item, child, ns_prefix)
                else:
                    child.text = str(item) if item is not None else ""
        else:
            parent.text = str(data) if data is not None else ""
    
    def xml_to_json(
        self,
        xml_str: str,
        remove_namespace: bool = True
    ) -> dict[str, Any]:
        """
        Convert XML string to JSON/dict.
        
        Args:
            xml_str: XML string to parse
            remove_namespace: Strip XML namespaces from keys
            
        Returns:
            Dictionary representation of XML
        """
        root = ET.fromstring(xml_str)
        return self._xml_element_to_dict(root, remove_namespace)
    
    def _xml_element_to_dict(
        self,
        element: ET.Element,
        remove_namespace: bool = True
    ) -> dict[str, Any]:
        """Recursively convert XML element to dict."""
        result: dict[str, Any] = {}
        
        # Get tag name (strip namespace if requested)
        tag = element.tag
        if remove_namespace and "}" in tag:
            tag = tag.split("}")[1]
        
        # Handle attributes
        if element.attrib:
            result["@attributes"] = dict(element.attrib)
        
        # Handle children
        children = list(element)
        if children:
            child_dict: dict[str, Any] = {}
            
            for child in children:
                child_tag = child.tag
                if remove_namespace and "}" in child_tag:
                    child_tag = child_tag.split("}")[1]
                
                # Convert tag to snake_case for JSON
                json_key = self._to_snake_case(child_tag)
                child_value = self._xml_element_to_dict(child, remove_namespace)
                
                # Handle repeated elements as lists
                if json_key in child_dict:
                    if not isinstance(child_dict[json_key], list):
                        child_dict[json_key] = [child_dict[json_key]]
                    child_dict[json_key].append(child_value)
                else:
                    child_dict[json_key] = child_value
            
            result.update(child_dict)
        
        # Handle text content
        elif element.text and element.text.strip():
            text = element.text.strip()
            # Try to convert to appropriate type
            return self._parse_value(text)
        
        return result if result else ""
    
    # ══════════════════════════════════════════════════════════
    # TCP Messages ↔ JSON Conversions
    # ══════════════════════════════════════════════════════════
    
    def tcp_to_json(self, tcp_message: str | bytes) -> dict[str, Any]:
        """
        Parse TCP message (JSON-over-TCP) to dict.
        
        WMS uses newline-delimited JSON over TCP.
        
        Args:
            tcp_message: Raw TCP message (string or bytes)
            
        Returns:
            Parsed message dict
        """
        if isinstance(tcp_message, bytes):
            tcp_message = tcp_message.decode("utf-8")
        
        # Strip newline and whitespace
        tcp_message = tcp_message.strip()
        
        try:
            return json.loads(tcp_message)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse TCP message: %s", e)
            # Try to extract what we can
            return self._parse_proprietary_tcp(tcp_message)
    
    def json_to_tcp(self, data: dict) -> bytes:
        """
        Convert dict to TCP message format.
        
        Args:
            data: Dict to convert
            
        Returns:
            Bytes ready to send over TCP (newline-terminated)
        """
        return (json.dumps(data, separators=(",", ":")) + "\n").encode("utf-8")
    
    def _parse_proprietary_tcp(self, message: str) -> dict[str, Any]:
        """
        Parse proprietary TCP message format (fallback).
        
        Handles legacy formats like:
        - "COMMAND:ORDER_ID:STATUS:DATA"
        - "KEY=VALUE;KEY=VALUE"
        """
        result: dict[str, Any] = {"raw": message}
        
        # Try colon-delimited format
        if ":" in message and not message.startswith("{"):
            parts = message.split(":")
            if len(parts) >= 2:
                result["command"] = parts[0]
                result["order_id"] = parts[1]
                if len(parts) >= 3:
                    result["status"] = parts[2]
                if len(parts) >= 4:
                    result["data"] = ":".join(parts[3:])
        
        # Try key=value format
        elif "=" in message:
            for pair in message.split(";"):
                if "=" in pair:
                    key, value = pair.split("=", 1)
                    result[key.strip().lower()] = value.strip()
        
        return result
    
    # ══════════════════════════════════════════════════════════
    # Order Data Normalization
    # ══════════════════════════════════════════════════════════
    
    def normalize_order(
        self,
        data: dict,
        source: str = "internal"
    ) -> dict[str, Any]:
        """
        Normalize order data from different sources to internal model.
        
        Args:
            data: Raw order data
            source: Data source (internal, cms, ros, wms)
            
        Returns:
            Normalized order dict with consistent field names
        """
        normalized = {}
        
        for key, value in data.items():
            # Map external field names to internal names
            internal_key = self.ORDER_FIELD_MAP.get(key, key)
            internal_key = self._to_snake_case(internal_key)
            normalized[internal_key] = value
        
        # Ensure required fields have defaults
        defaults = {
            "order_id": None,
            "client_id": None,
            "status": "pending",
            "pickup_address": "",
            "delivery_address": "",
            "recipient_name": "",
            "recipient_phone": "",
            "package_weight": 0.0,
            "package_description": "",
            "priority": "normal",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        
        for field, default in defaults.items():
            if field not in normalized or normalized[field] is None:
                normalized[field] = default
        
        # Type conversions
        if "package_weight" in normalized:
            try:
                normalized["package_weight"] = float(normalized["package_weight"])
            except (ValueError, TypeError):
                normalized["package_weight"] = 0.0
        
        if "client_id" in normalized and normalized["client_id"]:
            try:
                normalized["client_id"] = int(normalized["client_id"])
            except (ValueError, TypeError):
                pass
        
        normalized["_source"] = source
        normalized["_normalized_at"] = datetime.now(timezone.utc).isoformat()
        
        return normalized
    
    def denormalize_for_cms(self, order: dict) -> dict[str, Any]:
        """
        Convert internal order to CMS format (PascalCase).
        
        Args:
            order: Internal order dict
            
        Returns:
            Dict with CMS-style field names
        """
        return {
            "OrderId": order.get("order_id"),
            "ClientId": order.get("client_id"),
            "Status": order.get("status"),
            "PickupAddress": order.get("pickup_address"),
            "DeliveryAddress": order.get("delivery_address"),
            "RecipientName": order.get("recipient_name"),
            "RecipientPhone": order.get("recipient_phone"),
            "PackageWeight": order.get("package_weight"),
            "PackageDescription": order.get("package_description"),
            "Priority": order.get("priority"),
        }
    
    def denormalize_for_ros(self, order: dict) -> dict[str, Any]:
        """
        Convert internal order to ROS format (camelCase).
        
        Args:
            order: Internal order dict
            
        Returns:
            Dict with ROS-style field names
        """
        return {
            "orderId": order.get("order_id"),
            "pickupAddress": order.get("pickup_address"),
            "deliveryAddress": order.get("delivery_address"),
            "vehicleType": order.get("vehicle_type", "motorcycle"),
            "priority": order.get("priority"),
        }
    
    def denormalize_for_wms(self, order: dict) -> dict[str, Any]:
        """
        Convert internal order to WMS format.
        
        Args:
            order: Internal order dict
            
        Returns:
            Dict for WMS TCP command
        """
        return {
            "order_id": order.get("order_id"),
            "weight": order.get("package_weight", 0),
            "description": order.get("package_description", ""),
        }
    
    # ══════════════════════════════════════════════════════════
    # SOAP Envelope Helpers
    # ══════════════════════════════════════════════════════════
    
    def build_soap_envelope(
        self,
        operation: str,
        body_data: dict,
        namespace: str = "http://swiftlogistics.lk/cms"
    ) -> str:
        """
        Build complete SOAP envelope for CMS request.
        
        Args:
            operation: SOAP operation name
            body_data: Operation parameters as dict
            namespace: CMS namespace
            
        Returns:
            Complete SOAP XML string
        """
        soap_ns = "http://schemas.xmlsoap.org/soap/envelope/"
        
        # Build body content
        body_elements = []
        for key, value in body_data.items():
            xml_key = self._to_pascal_case(key)
            body_elements.append(f"      <{xml_key}>{value}</{xml_key}>")
        
        body_content = "\n".join(body_elements)
        
        return f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="{soap_ns}" xmlns:cms="{namespace}">
  <soap:Header/>
  <soap:Body>
    <cms:{operation}>
{body_content}
    </cms:{operation}>
  </soap:Body>
</soap:Envelope>"""
    
    def parse_soap_response(
        self,
        xml_str: str,
        expected_operation: str | None = None
    ) -> dict[str, Any]:
        """
        Parse SOAP response envelope.
        
        Args:
            xml_str: SOAP XML response
            expected_operation: Expected operation response name
            
        Returns:
            Parsed body content as dict
            
        Raises:
            ValueError: If SOAP fault detected
        """
        result = self.xml_to_json(xml_str)
        
        # Navigate to body content
        body = result.get("body", result)
        
        # Check for fault
        if "fault" in body:
            fault = body["fault"]
            raise ValueError(f"SOAP Fault: {fault}")
        
        return body
    
    # ══════════════════════════════════════════════════════════
    # Utility Methods
    # ══════════════════════════════════════════════════════════
    
    @staticmethod
    def _to_snake_case(s: str) -> str:
        """Convert PascalCase or camelCase to snake_case."""
        s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
        s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
        return s.lower().replace("-", "_")
    
    @staticmethod
    def _to_pascal_case(s: str) -> str:
        """Convert snake_case to PascalCase."""
        return "".join(word.capitalize() for word in s.split("_"))
    
    @staticmethod
    def _to_camel_case(s: str) -> str:
        """Convert snake_case to camelCase."""
        pascal = DataTransformer._to_pascal_case(s)
        return pascal[0].lower() + pascal[1:] if pascal else ""
    
    @staticmethod
    def _parse_value(text: str) -> Any:
        """Parse string to appropriate Python type."""
        # Boolean
        if text.lower() in ("true", "false"):
            return text.lower() == "true"
        
        # Integer
        try:
            return int(text)
        except ValueError:
            pass
        
        # Float
        try:
            return float(text)
        except ValueError:
            pass
        
        return text
    
    @staticmethod
    def _prettify_xml(xml_str: str) -> str:
        """Add indentation to XML string."""
        try:
            import xml.dom.minidom
            dom = xml.dom.minidom.parseString(xml_str)
            pretty = dom.toprettyxml(indent="  ")
            # Remove extra declaration if present
            lines = pretty.split("\n")
            if lines[0].startswith("<?xml"):
                lines = lines[1:]
            return "\n".join(line for line in lines if line.strip())
        except Exception:
            return xml_str


# ══════════════════════════════════════════════════════════════
# Convenience Functions
# ══════════════════════════════════════════════════════════════

_transformer = DataTransformer()


def json_to_xml(data: dict, root_tag: str = "root") -> str:
    """Quick JSON to XML conversion."""
    return _transformer.json_to_xml(data, root_tag)


def xml_to_json(xml_str: str) -> dict:
    """Quick XML to JSON conversion."""
    return _transformer.xml_to_json(xml_str)


def normalize_order(data: dict, source: str = "internal") -> dict:
    """Quick order normalization."""
    return _transformer.normalize_order(data, source)


def tcp_to_json(message: str | bytes) -> dict:
    """Quick TCP message to JSON conversion."""
    return _transformer.tcp_to_json(message)


def json_to_tcp(data: dict) -> bytes:
    """Quick JSON to TCP message conversion."""
    return _transformer.json_to_tcp(data)
