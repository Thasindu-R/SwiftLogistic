"""
Input Validation Utilities
==========================

Comprehensive input validation for SwiftLogistics:
- Field validators (email, phone, coordinates, etc.)
- Business rule validators (order data, addresses)
- Sanitization utilities
- Custom Pydantic validators

Features:
- Prevent SQL injection and XSS
- Validate geographic coordinates
- Email and phone format validation
- Order weight and dimension limits
- Address validation
- Status transition validation

Usage:
    from shared.common.validators import (
        validate_email,
        validate_phone,
        validate_coordinates,
        sanitize_string,
        OrderValidator,
    )
    
    # Single field validation
    if validate_email(email):
        # Valid
    
    # Using with Pydantic models
    class OrderCreate(BaseModel):
        email: str
        
        @validator('email')
        def validate_email_field(cls, v):
            return validate_email(v, raise_error=True)
"""

import html
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from .errors import (
    ValidationError,
    InvalidFormatError,
    MissingFieldError,
    ValueOutOfRangeError,
    InvalidStatusTransitionError,
)

logger = logging.getLogger(__name__)


# ── Regex Patterns ───────────────────────────────────────────

EMAIL_PATTERN = re.compile(
    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
)

PHONE_PATTERN = re.compile(
    r"^\+?[0-9]{10,15}$"
)

# UUID v4 pattern
UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# Alpha-numeric with spaces and basic punctuation
SAFE_TEXT_PATTERN = re.compile(r"^[\w\s.,'\-#/()]+$", re.UNICODE)

# Postal/ZIP code patterns
POSTAL_CODE_PATTERNS = {
    "LK": re.compile(r"^\d{5}$"),  # Sri Lanka
    "US": re.compile(r"^\d{5}(-\d{4})?$"),  # USA
    "UK": re.compile(r"^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$", re.IGNORECASE),  # UK
    "DEFAULT": re.compile(r"^[\w\s-]{3,10}$"),
}


# ── Constants ────────────────────────────────────────────────

# Order constraints
MIN_PACKAGE_WEIGHT_KG = 0.01
MAX_PACKAGE_WEIGHT_KG = 1000.0
MIN_PACKAGE_DIMENSION_CM = 1.0
MAX_PACKAGE_DIMENSION_CM = 500.0

# Coordinate bounds (Sri Lanka focus, but allow worldwide)
MIN_LATITUDE = -90.0
MAX_LATITUDE = 90.0
MIN_LONGITUDE = -180.0
MAX_LONGITUDE = 180.0

# Sri Lanka approximate bounds
SL_MIN_LAT = 5.9
SL_MAX_LAT = 9.9
SL_MIN_LNG = 79.6
SL_MAX_LNG = 81.9

# Address constraints
MIN_ADDRESS_LENGTH = 10
MAX_ADDRESS_LENGTH = 500

# Name constraints
MIN_NAME_LENGTH = 2
MAX_NAME_LENGTH = 100

# Description constraints
MAX_DESCRIPTION_LENGTH = 2000
MAX_NOTES_LENGTH = 1000

# Valid order statuses and transitions
ORDER_STATUSES = {
    "pending",
    "confirmed",
    "processing",
    "in_transit",
    "delivered",
    "failed",
    "cancelled",
}

VALID_STATUS_TRANSITIONS = {
    "pending": {"confirmed", "cancelled"},
    "confirmed": {"processing", "cancelled"},
    "processing": {"in_transit", "failed", "cancelled"},
    "in_transit": {"delivered", "failed"},
    "delivered": set(),  # Terminal state
    "failed": {"pending"},  # Can retry
    "cancelled": set(),  # Terminal state
}

# Valid order priorities
ORDER_PRIORITIES = {"low", "normal", "high", "urgent"}


# ── Basic Field Validators ───────────────────────────────────

def validate_required(value: Any, field_name: str) -> Any:
    """
    Validate that a value is not None or empty.
    
    Args:
        value: Value to check
        field_name: Field name for error message
        
    Returns:
        The value if valid
        
    Raises:
        MissingFieldError: If value is None or empty
    """
    if value is None:
        raise MissingFieldError(field_name)
    
    if isinstance(value, str) and not value.strip():
        raise MissingFieldError(field_name, f"Field '{field_name}' cannot be empty")
    
    return value


def validate_email(
    email: str,
    field_name: str = "email",
    raise_error: bool = True,
) -> Union[str, bool]:
    """
    Validate email format.
    
    Args:
        email: Email address to validate
        field_name: Field name for error message
        raise_error: Whether to raise exception on failure
        
    Returns:
        Normalized email (lowercase) if valid, or False if invalid and raise_error=False
    """
    if not email or not isinstance(email, str):
        if raise_error:
            raise MissingFieldError(field_name)
        return False
    
    email = email.strip().lower()
    
    if not EMAIL_PATTERN.match(email):
        if raise_error:
            raise InvalidFormatError(
                field_name,
                "valid email address (e.g., user@example.com)",
                email,
            )
        return False
    
    return email


def validate_phone(
    phone: str,
    field_name: str = "phone",
    raise_error: bool = True,
) -> Union[str, bool]:
    """
    Validate phone number format.
    
    Args:
        phone: Phone number to validate
        field_name: Field name for error message
        raise_error: Whether to raise exception on failure
        
    Returns:
        Normalized phone (digits only) if valid, or False if invalid
    """
    if not phone or not isinstance(phone, str):
        if raise_error:
            raise MissingFieldError(field_name)
        return False
    
    # Remove common separators
    normalized = re.sub(r"[\s\-().]+", "", phone.strip())
    
    if not PHONE_PATTERN.match(normalized):
        if raise_error:
            raise InvalidFormatError(
                field_name,
                "valid phone number (10-15 digits, optional + prefix)",
                phone,
            )
        return False
    
    return normalized


def validate_uuid(
    value: str,
    field_name: str = "id",
    raise_error: bool = True,
) -> Union[str, bool]:
    """
    Validate UUID v4 format.
    
    Args:
        value: UUID string to validate
        field_name: Field name for error message
        raise_error: Whether to raise exception on failure
        
    Returns:
        Lowercase UUID if valid, or False if invalid
    """
    if not value or not isinstance(value, str):
        if raise_error:
            raise MissingFieldError(field_name)
        return False
    
    value = value.strip().lower()
    
    if not UUID_PATTERN.match(value):
        if raise_error:
            raise InvalidFormatError(field_name, "valid UUID v4", value)
        return False
    
    return value


def validate_coordinates(
    latitude: float,
    longitude: float,
    field_name: str = "coordinates",
    sri_lanka_only: bool = False,
    raise_error: bool = True,
) -> Union[tuple[float, float], bool]:
    """
    Validate geographic coordinates.
    
    Args:
        latitude: Latitude value
        longitude: Longitude value
        field_name: Field name for error message
        sri_lanka_only: Restrict to Sri Lanka bounds
        raise_error: Whether to raise exception on failure
        
    Returns:
        Tuple of (lat, lng) if valid, or False if invalid
    """
    try:
        lat = float(latitude)
        lng = float(longitude)
    except (TypeError, ValueError):
        if raise_error:
            raise InvalidFormatError(field_name, "numeric coordinates", f"({latitude}, {longitude})")
        return False
    
    # Check global bounds
    if not (MIN_LATITUDE <= lat <= MAX_LATITUDE):
        if raise_error:
            raise ValueOutOfRangeError(
                f"{field_name}.latitude", lat,
                MIN_LATITUDE, MAX_LATITUDE,
            )
        return False
    
    if not (MIN_LONGITUDE <= lng <= MAX_LONGITUDE):
        if raise_error:
            raise ValueOutOfRangeError(
                f"{field_name}.longitude", lng,
                MIN_LONGITUDE, MAX_LONGITUDE,
            )
        return False
    
    # Check Sri Lanka bounds if required
    if sri_lanka_only:
        if not (SL_MIN_LAT <= lat <= SL_MAX_LAT and SL_MIN_LNG <= lng <= SL_MAX_LNG):
            if raise_error:
                raise ValueOutOfRangeError(
                    field_name, f"({lat}, {lng})",
                    f"({SL_MIN_LAT}, {SL_MIN_LNG})",
                    f"({SL_MAX_LAT}, {SL_MAX_LNG})",
                )
            return False
    
    return (lat, lng)


def validate_string_length(
    value: str,
    field_name: str,
    min_length: int = 0,
    max_length: int = 1000,
    raise_error: bool = True,
) -> Union[str, bool]:
    """
    Validate string length.
    
    Args:
        value: String to validate
        field_name: Field name for error message
        min_length: Minimum allowed length
        max_length: Maximum allowed length
        raise_error: Whether to raise exception on failure
        
    Returns:
        Stripped string if valid, or False if invalid
    """
    if not isinstance(value, str):
        if raise_error:
            raise InvalidFormatError(field_name, "string", type(value).__name__)
        return False
    
    value = value.strip()
    length = len(value)
    
    if length < min_length or length > max_length:
        if raise_error:
            raise ValueOutOfRangeError(
                f"{field_name} length", length,
                min_length, max_length,
            )
        return False
    
    return value


def validate_numeric_range(
    value: Union[int, float, Decimal],
    field_name: str,
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None,
    raise_error: bool = True,
) -> Union[float, bool]:
    """
    Validate numeric value is within range.
    
    Args:
        value: Number to validate
        field_name: Field name for error message
        min_value: Minimum allowed value
        max_value: Maximum allowed value
        raise_error: Whether to raise exception on failure
        
    Returns:
        Float value if valid, or False if invalid
    """
    try:
        num_value = float(value)
    except (TypeError, ValueError):
        if raise_error:
            raise InvalidFormatError(field_name, "numeric value", str(value))
        return False
    
    if min_value is not None and num_value < min_value:
        if raise_error:
            raise ValueOutOfRangeError(field_name, num_value, min_value=min_value)
        return False
    
    if max_value is not None and num_value > max_value:
        if raise_error:
            raise ValueOutOfRangeError(field_name, num_value, max_value=max_value)
        return False
    
    return num_value


def validate_enum_value(
    value: str,
    field_name: str,
    allowed_values: set[str],
    raise_error: bool = True,
) -> Union[str, bool]:
    """
    Validate value is in allowed set.
    
    Args:
        value: Value to validate
        field_name: Field name for error message
        allowed_values: Set of allowed values
        raise_error: Whether to raise exception on failure
        
    Returns:
        Lowercase value if valid, or False if invalid
    """
    if not isinstance(value, str):
        if raise_error:
            raise InvalidFormatError(field_name, "string", type(value).__name__)
        return False
    
    value = value.strip().lower()
    
    if value not in allowed_values:
        if raise_error:
            raise ValidationError(
                f"Invalid value for '{field_name}'. Must be one of: {', '.join(sorted(allowed_values))}",
                field=field_name,
                value=value,
            )
        return False
    
    return value


# ── Sanitization Utilities ───────────────────────────────────

def sanitize_string(
    value: str,
    max_length: int = 1000,
    allow_html: bool = False,
    strip_control_chars: bool = True,
) -> str:
    """
    Sanitize string input to prevent XSS and injection attacks.
    
    Args:
        value: String to sanitize
        max_length: Maximum allowed length
        allow_html: Whether to allow HTML (default: escape it)
        strip_control_chars: Remove control characters
        
    Returns:
        Sanitized string
    """
    if not isinstance(value, str):
        return ""
    
    # Trim to max length
    value = value[:max_length]
    
    # Strip control characters (except newlines and tabs)
    if strip_control_chars:
        value = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", value)
    
    # Escape HTML if not allowed
    if not allow_html:
        value = html.escape(value)
    
    return value.strip()


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename to prevent path traversal attacks.
    
    Args:
        filename: Original filename
        
    Returns:
        Safe filename
    """
    # Remove path components
    filename = filename.replace("\\", "/").split("/")[-1]
    
    # Remove dangerous characters
    filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", filename)
    
    # Limit length
    name, ext = filename.rsplit(".", 1) if "." in filename else (filename, "")
    name = name[:100]
    
    if ext:
        return f"{name}.{ext[:10]}"
    return name


def sanitize_sql_identifier(identifier: str) -> str:
    """
    Sanitize SQL identifier (table/column names).
    WARNING: This is for logging only. Use parameterized queries for actual SQL.
    
    Args:
        identifier: SQL identifier to sanitize
        
    Returns:
        Safe identifier (alphanumeric + underscore only)
    """
    return re.sub(r"[^\w]", "_", identifier)[:64]


# ── Business Rule Validators ─────────────────────────────────

def validate_order_status_transition(
    current_status: str,
    new_status: str,
    raise_error: bool = True,
) -> bool:
    """
    Validate order status transition is allowed.
    
    Args:
        current_status: Current order status
        new_status: Target status
        raise_error: Whether to raise exception on failure
        
    Returns:
        True if transition is valid
    """
    current = current_status.lower()
    new = new_status.lower()
    
    if current not in VALID_STATUS_TRANSITIONS:
        if raise_error:
            raise ValidationError(
                f"Unknown current status: {current}",
                field="status",
            )
        return False
    
    allowed = VALID_STATUS_TRANSITIONS[current]
    
    if new not in allowed:
        if raise_error:
            raise InvalidStatusTransitionError(
                current, new, list(allowed),
            )
        return False
    
    return True


def validate_address(
    address: str,
    field_name: str = "address",
    raise_error: bool = True,
) -> Union[str, bool]:
    """
    Validate address string.
    
    Args:
        address: Address to validate
        field_name: Field name for error message
        raise_error: Whether to raise exception on failure
        
    Returns:
        Sanitized address if valid, or False if invalid
    """
    if not address or not isinstance(address, str):
        if raise_error:
            raise MissingFieldError(field_name)
        return False
    
    address = sanitize_string(address, max_length=MAX_ADDRESS_LENGTH)
    
    if len(address) < MIN_ADDRESS_LENGTH:
        if raise_error:
            raise ValueOutOfRangeError(
                f"{field_name} length", len(address),
                min_value=MIN_ADDRESS_LENGTH,
            )
        return False
    
    return address


def validate_package_weight(
    weight: Union[int, float],
    field_name: str = "package_weight",
    raise_error: bool = True,
) -> Union[float, bool]:
    """
    Validate package weight is within limits.
    
    Args:
        weight: Weight in kilograms
        field_name: Field name for error message
        raise_error: Whether to raise exception on failure
        
    Returns:
        Float weight if valid, or False if invalid
    """
    return validate_numeric_range(
        weight, field_name,
        min_value=MIN_PACKAGE_WEIGHT_KG,
        max_value=MAX_PACKAGE_WEIGHT_KG,
        raise_error=raise_error,
    )


def validate_package_dimensions(
    length: float,
    width: float,
    height: float,
    raise_error: bool = True,
) -> Union[tuple[float, float, float], bool]:
    """
    Validate package dimensions.
    
    Args:
        length: Length in cm
        width: Width in cm
        height: Height in cm
        raise_error: Whether to raise exception on failure
        
    Returns:
        Tuple of (length, width, height) if valid, or False if invalid
    """
    dims = []
    for name, value in [("length", length), ("width", width), ("height", height)]:
        result = validate_numeric_range(
            value, f"package_{name}",
            min_value=MIN_PACKAGE_DIMENSION_CM,
            max_value=MAX_PACKAGE_DIMENSION_CM,
            raise_error=raise_error,
        )
        if result is False:
            return False
        dims.append(result)
    
    return tuple(dims)


# ── Pydantic Validator Mixins ────────────────────────────────

class EmailMixin:
    """Mixin for email validation in Pydantic models."""
    
    @field_validator("email", mode="before")
    @classmethod
    def validate_email_field(cls, v):
        if v is None:
            return v
        return validate_email(v, "email", raise_error=True)


class PhoneMixin:
    """Mixin for phone validation in Pydantic models."""
    
    @field_validator("phone", mode="before")
    @classmethod
    def validate_phone_field(cls, v):
        if v is None:
            return v
        return validate_phone(v, "phone", raise_error=True)


class AddressMixin:
    """Mixin for address validation in Pydantic models."""
    
    @field_validator("pickup_address", "delivery_address", mode="before")
    @classmethod
    def validate_address_field(cls, v, info):
        if v is None:
            return v
        return validate_address(v, info.field_name, raise_error=True)


# ── Pydantic Models with Validation ──────────────────────────

class ValidatedOrderCreate(BaseModel):
    """Order creation with comprehensive validation."""
    
    pickup_address: str = Field(..., min_length=10, max_length=500)
    delivery_address: str = Field(..., min_length=10, max_length=500)
    package_description: str = Field(default="", max_length=2000)
    package_weight: float = Field(default=1.0, ge=0.01, le=1000.0)
    priority: str = Field(default="normal")
    recipient_name: str = Field(default="", max_length=100)
    recipient_phone: str = Field(default="")
    notes: str = Field(default="", max_length=1000)
    
    @field_validator("priority")
    @classmethod
    def validate_priority(cls, v):
        return validate_enum_value(v, "priority", ORDER_PRIORITIES)
    
    @field_validator("recipient_phone")
    @classmethod
    def validate_recipient_phone(cls, v):
        if not v:
            return ""
        return validate_phone(v, "recipient_phone", raise_error=True)
    
    @field_validator("pickup_address", "delivery_address")
    @classmethod
    def sanitize_addresses(cls, v):
        if v:
            return sanitize_string(v, max_length=500)
        return v
    
    @field_validator("package_description", "notes")
    @classmethod
    def sanitize_text_fields(cls, v):
        if v:
            return sanitize_string(v, max_length=2000)
        return v


class ValidatedLocationUpdate(BaseModel):
    """Location update with coordinate validation."""
    
    order_id: str
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    speed: Optional[float] = Field(default=None, ge=0, le=300)
    heading: Optional[float] = Field(default=None, ge=0, lt=360)
    accuracy: Optional[float] = Field(default=None, ge=0, le=1000)
    
    @field_validator("order_id")
    @classmethod
    def validate_order_id(cls, v):
        return validate_uuid(v, "order_id")


class ValidatedUserRegistration(BaseModel):
    """User registration with validation."""
    
    username: str = Field(..., min_length=3, max_length=50)
    email: str
    password: str = Field(..., min_length=8, max_length=100)
    full_name: str = Field(default="", max_length=100)
    phone: str = Field(default="")
    role: str = Field(default="client")
    
    @field_validator("email")
    @classmethod
    def validate_email(cls, v):
        return validate_email(v)
    
    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        if not v:
            return ""
        return validate_phone(v, raise_error=True)
    
    @field_validator("role")
    @classmethod
    def validate_role(cls, v):
        allowed = {"client", "driver", "admin"}
        return validate_enum_value(v, "role", allowed)
    
    @field_validator("username")
    @classmethod
    def validate_username(cls, v):
        # Only alphanumeric and underscore
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", v):
            raise ValidationError(
                "Username must start with a letter and contain only letters, numbers, and underscores",
                field="username",
            )
        return v.lower()
    
    @field_validator("password")
    @classmethod
    def validate_password(cls, v):
        # Check password strength
        if len(v) < 8:
            raise ValidationError("Password must be at least 8 characters", field="password")
        if not re.search(r"[A-Z]", v):
            raise ValidationError("Password must contain at least one uppercase letter", field="password")
        if not re.search(r"[a-z]", v):
            raise ValidationError("Password must contain at least one lowercase letter", field="password")
        if not re.search(r"\d", v):
            raise ValidationError("Password must contain at least one digit", field="password")
        return v


# ── Validation Utility Functions ─────────────────────────────

def validate_order_data(data: dict) -> dict:
    """
    Validate and sanitize order creation data.
    
    Args:
        data: Raw order data dictionary
        
    Returns:
        Validated and sanitized data
        
    Raises:
        ValidationError: If data is invalid
    """
    validated = ValidatedOrderCreate(**data)
    return validated.model_dump()


def validate_file_upload(
    filename: str,
    file_size: int,
    content_type: str,
    allowed_types: Optional[list[str]] = None,
    max_size_mb: float = 10.0,
) -> dict:
    """
    Validate file upload parameters.
    
    Args:
        filename: Original filename
        file_size: File size in bytes
        content_type: MIME type
        allowed_types: List of allowed MIME types
        max_size_mb: Maximum file size in MB
        
    Returns:
        Dict with validation results and safe filename
        
    Raises:
        FileTooLargeError, InvalidFileTypeError
    """
    from .errors import FileTooLargeError, InvalidFileTypeError
    
    # Default allowed types for images
    if allowed_types is None:
        allowed_types = [
            "image/jpeg",
            "image/png",
            "image/gif",
            "image/webp",
        ]
    
    max_size_bytes = int(max_size_mb * 1024 * 1024)
    
    if file_size > max_size_bytes:
        raise FileTooLargeError(file_size, max_size_bytes)
    
    if content_type not in allowed_types:
        raise InvalidFileTypeError(content_type, allowed_types)
    
    safe_filename = sanitize_filename(filename)
    
    return {
        "original_filename": filename,
        "safe_filename": safe_filename,
        "file_size": file_size,
        "content_type": content_type,
    }
