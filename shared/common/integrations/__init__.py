"""
Middleware Integration Clients
==============================

This package provides clients for external system integration:
- CMS (Client Management System) - SOAP/XML
- ROS (Route Optimization System) - REST/JSON
- WMS (Warehouse Management System) - TCP/IP

Data transformation utilities for format conversion.
"""

from .cms_client import CMSClient, CMSSoapError
from .ros_client import ROSClient, ROSApiError
from .wms_client import WMSClient, WMSProtocolError
from .transformers import DataTransformer, json_to_xml, xml_to_json, normalize_order
from .orchestrator import IntegrationOrchestrator, create_orchestrator

__all__ = [
    "CMSClient",
    "CMSSoapError",
    "ROSClient",
    "ROSApiError",
    "WMSClient",
    "WMSProtocolError",
    "DataTransformer",
    "IntegrationOrchestrator",
    "create_orchestrator",
    "json_to_xml",
    "xml_to_json",
    "normalize_order",
]
