"""
Error Handling & Custom Exceptions
"""

import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Optional
from enum import Enum

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError as PydanticValidationError

logger = logging.getLogger(__name__)


class ErrorCode(str, Enum):
    """Standardized error codes for API responses."""
    # Validation errors (4xx)
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INVALID_INPUT = "INVALID_INPUT"
    MISSING_FIELD = "MISSING_FIELD"
    INVALID_FORMAT = "INVALID_FORMAT"
    VALUE_OUT_OF_RANGE = "VALUE_OUT_OF_RANGE"
    
    # Authentication errors
    AUTHENTICATION_REQUIRED = "AUTHENTICATION_REQUIRED"
    INVALID_CREDENTIALS = "INVALID_CREDENTIALS"
    TOKEN_EXPIRED = "TOKEN_EXPIRED"
    TOKEN_INVALID = "TOKEN_INVALID"
    
    # Authorization errors
    PERMISSION_DENIED = "PERMISSION_DENIED"
    INSUFFICIENT_ROLE = "INSUFFICIENT_ROLE"
    RESOURCE_ACCESS_DENIED = "RESOURCE_ACCESS_DENIED"
    
    # Resource errors
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
    RESOURCE_CONFLICT = "RESOURCE_CONFLICT"
    RESOURCE_ALREADY_EXISTS = "RESOURCE_ALREADY_EXISTS"
    
    # Business logic errors
    ORDER_NOT_ASSIGNABLE = "ORDER_NOT_ASSIGNABLE"
    DELIVERY_NOT_COMPLETABLE = "DELIVERY_NOT_COMPLETABLE"
    INVALID_STATUS_TRANSITION = "INVALID_STATUS_TRANSITION"
    
    # Integration errors
    INTEGRATION_ERROR = "INTEGRATION_ERROR"
    CMS_ERROR = "CMS_ERROR"
    WMS_ERROR = "WMS_ERROR"
    ROS_ERROR = "ROS_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    
    # File errors
    FILE_UPLOAD_ERROR = "FILE_UPLOAD_ERROR"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    FILE_TOO_LARGE = "FILE_TOO_LARGE"
    INVALID_FILE_TYPE = "INVALID_FILE_TYPE"
    
    # Server errors
    INTERNAL_ERROR = "INTERNAL_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    QUEUE_ERROR = "QUEUE_ERROR"


class ErrorResponse:
    """Standardized error response format."""
    
    def __init__(
        self,
        code: ErrorCode,
        message: str,
        details: Optional[dict[str, Any]] = None,
        field: Optional[str] = None,
        trace_id: Optional[str] = None,
    ):
        self.code = code
        self.message = message
        self.details = details or {}
        self.field = field
        self.trace_id = trace_id or self._generate_trace_id()
        self.timestamp = datetime.now(timezone.utc).isoformat()
    
    def _generate_trace_id(self) -> str:
        """Generate unique trace ID for error tracking."""
        import uuid
        return str(uuid.uuid4())[:8]
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON response."""
        response = {
            "error": {
                "code": self.code.value if isinstance(self.code, ErrorCode) else self.code,
                "message": self.message,
                "timestamp": self.timestamp,
                "trace_id": self.trace_id,
            }
        }
        
        if self.field:
            response["error"]["field"] = self.field
        
        if self.details:
            response["error"]["details"] = self.details
        
        return response


# ── Custom Exception Classes ─────────────────────────────────

class SwiftLogisticsError(Exception):
    """Base exception for all SwiftLogistics errors."""
    
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    error_code: ErrorCode = ErrorCode.INTERNAL_ERROR
    
    def __init__(
        self,
        message: str,
        details: Optional[dict[str, Any]] = None,
        field: Optional[str] = None,
    ):
        self.message = message
        self.details = details or {}
        self.field = field
        super().__init__(message)
    
    def to_response(self) -> ErrorResponse:
        """Convert exception to error response."""
        return ErrorResponse(
            code=self.error_code,
            message=self.message,
            details=self.details,
            field=self.field,
        )


class ValidationError(SwiftLogisticsError):
    """Raised when input validation fails."""
    status_code = status.HTTP_400_BAD_REQUEST
    error_code = ErrorCode.VALIDATION_ERROR
    
    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        value: Optional[Any] = None,
        constraint: Optional[str] = None,
    ):
        details = {}
        if value is not None:
            details["value"] = str(value)[:100]  # Truncate long values
        if constraint:
            details["constraint"] = constraint
        
        super().__init__(message, details=details, field=field)


class InvalidInputError(ValidationError):
    """Raised when input data is invalid."""
    error_code = ErrorCode.INVALID_INPUT


class MissingFieldError(ValidationError):
    """Raised when a required field is missing."""
    error_code = ErrorCode.MISSING_FIELD
    
    def __init__(self, field: str, message: Optional[str] = None):
        super().__init__(
            message or f"Required field '{field}' is missing",
            field=field,
        )


class InvalidFormatError(ValidationError):
    """Raised when data format is invalid."""
    error_code = ErrorCode.INVALID_FORMAT
    
    def __init__(
        self,
        field: str,
        expected_format: str,
        value: Optional[str] = None,
    ):
        super().__init__(
            f"Invalid format for '{field}'. Expected: {expected_format}",
            field=field,
            value=value,
            constraint=expected_format,
        )


class ValueOutOfRangeError(ValidationError):
    """Raised when value is out of allowed range."""
    error_code = ErrorCode.VALUE_OUT_OF_RANGE
    
    def __init__(
        self,
        field: str,
        value: Any,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None,
    ):
        constraint = ""
        if min_value is not None and max_value is not None:
            constraint = f"between {min_value} and {max_value}"
        elif min_value is not None:
            constraint = f"at least {min_value}"
        elif max_value is not None:
            constraint = f"at most {max_value}"
        
        super().__init__(
            f"Value for '{field}' is out of range. Must be {constraint}",
            field=field,
            value=value,
            constraint=constraint,
        )


class AuthenticationError(SwiftLogisticsError):
    """Raised when authentication fails."""
    status_code = status.HTTP_401_UNAUTHORIZED
    error_code = ErrorCode.AUTHENTICATION_REQUIRED


class InvalidCredentialsError(AuthenticationError):
    """Raised when credentials are invalid."""
    error_code = ErrorCode.INVALID_CREDENTIALS
    
    def __init__(self, message: str = "Invalid username or password"):
        super().__init__(message)


class TokenExpiredError(AuthenticationError):
    """Raised when token has expired."""
    error_code = ErrorCode.TOKEN_EXPIRED
    
    def __init__(self, message: str = "Authentication token has expired"):
        super().__init__(message)


class TokenInvalidError(AuthenticationError):
    """Raised when token is invalid."""
    error_code = ErrorCode.TOKEN_INVALID
    
    def __init__(self, message: str = "Invalid authentication token"):
        super().__init__(message)


class AuthorizationError(SwiftLogisticsError):
    """Raised when user lacks required permissions."""
    status_code = status.HTTP_403_FORBIDDEN
    error_code = ErrorCode.PERMISSION_DENIED


class InsufficientRoleError(AuthorizationError):
    """Raised when user's role is insufficient."""
    error_code = ErrorCode.INSUFFICIENT_ROLE
    
    def __init__(self, required_role: str, user_role: str):
        super().__init__(
            f"This action requires '{required_role}' role, but you have '{user_role}'",
            details={"required_role": required_role, "user_role": user_role},
        )


class ResourceAccessDeniedError(AuthorizationError):
    """Raised when user cannot access a specific resource."""
    error_code = ErrorCode.RESOURCE_ACCESS_DENIED
    
    def __init__(self, resource_type: str, resource_id: str):
        super().__init__(
            f"You do not have access to this {resource_type}",
            details={"resource_type": resource_type, "resource_id": resource_id},
        )


class NotFoundError(SwiftLogisticsError):
    """Raised when a resource is not found."""
    status_code = status.HTTP_404_NOT_FOUND
    error_code = ErrorCode.RESOURCE_NOT_FOUND
    
    def __init__(self, resource_type: str, resource_id: Optional[str] = None):
        message = f"{resource_type} not found"
        if resource_id:
            message = f"{resource_type} with ID '{resource_id}' not found"
        
        super().__init__(
            message,
            details={"resource_type": resource_type, "resource_id": resource_id},
        )


class ConflictError(SwiftLogisticsError):
    """Raised when there's a resource conflict."""
    status_code = status.HTTP_409_CONFLICT
    error_code = ErrorCode.RESOURCE_CONFLICT


class ResourceAlreadyExistsError(ConflictError):
    """Raised when resource already exists."""
    error_code = ErrorCode.RESOURCE_ALREADY_EXISTS
    
    def __init__(self, resource_type: str, identifier: str):
        super().__init__(
            f"{resource_type} with this identifier already exists",
            details={"resource_type": resource_type, "identifier": identifier},
        )


class InvalidStatusTransitionError(ConflictError):
    """Raised when status transition is invalid."""
    error_code = ErrorCode.INVALID_STATUS_TRANSITION
    
    def __init__(self, current_status: str, target_status: str, allowed: list[str]):
        super().__init__(
            f"Cannot transition from '{current_status}' to '{target_status}'",
            details={
                "current_status": current_status,
                "target_status": target_status,
                "allowed_transitions": allowed,
            },
        )


class BusinessLogicError(SwiftLogisticsError):
    """Raised for business logic violations."""
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY


class OrderNotAssignableError(BusinessLogicError):
    """Raised when order cannot be assigned."""
    error_code = ErrorCode.ORDER_NOT_ASSIGNABLE
    
    def __init__(self, order_id: str, reason: str):
        super().__init__(
            f"Order cannot be assigned: {reason}",
            details={"order_id": order_id, "reason": reason},
        )


class DeliveryNotCompletableError(BusinessLogicError):
    """Raised when delivery cannot be completed."""
    error_code = ErrorCode.DELIVERY_NOT_COMPLETABLE
    
    def __init__(self, order_id: str, reason: str):
        super().__init__(
            f"Delivery cannot be completed: {reason}",
            details={"order_id": order_id, "reason": reason},
        )


class IntegrationError(SwiftLogisticsError):
    """Raised when external integration fails."""
    status_code = status.HTTP_502_BAD_GATEWAY
    error_code = ErrorCode.INTEGRATION_ERROR
    
    def __init__(
        self,
        system: str,
        message: str,
        original_error: Optional[str] = None,
    ):
        super().__init__(
            f"Integration with {system} failed: {message}",
            details={
                "system": system,
                "original_error": original_error,
            },
        )


class CMSIntegrationError(IntegrationError):
    """Raised when CMS integration fails."""
    error_code = ErrorCode.CMS_ERROR
    
    def __init__(self, message: str, original_error: Optional[str] = None):
        super().__init__("CMS", message, original_error)


class WMSIntegrationError(IntegrationError):
    """Raised when WMS integration fails."""
    error_code = ErrorCode.WMS_ERROR
    
    def __init__(self, message: str, original_error: Optional[str] = None):
        super().__init__("WMS", message, original_error)


class ROSIntegrationError(IntegrationError):
    """Raised when ROS integration fails."""
    error_code = ErrorCode.ROS_ERROR
    
    def __init__(self, message: str, original_error: Optional[str] = None):
        super().__init__("ROS", message, original_error)


class ServiceUnavailableError(SwiftLogisticsError):
    """Raised when a service is temporarily unavailable."""
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    error_code = ErrorCode.SERVICE_UNAVAILABLE
    
    def __init__(self, service: str, retry_after: Optional[int] = None):
        details = {"service": service}
        if retry_after:
            details["retry_after_seconds"] = retry_after
        
        super().__init__(
            f"Service '{service}' is temporarily unavailable",
            details=details,
        )


class FileError(SwiftLogisticsError):
    """Base class for file-related errors."""
    status_code = status.HTTP_400_BAD_REQUEST
    error_code = ErrorCode.FILE_UPLOAD_ERROR


class FileNotFoundError(FileError):
    """Raised when file is not found."""
    status_code = status.HTTP_404_NOT_FOUND
    error_code = ErrorCode.FILE_NOT_FOUND
    
    def __init__(self, file_id: str):
        super().__init__(
            f"File not found",
            details={"file_id": file_id},
        )


class FileTooLargeError(FileError):
    """Raised when file exceeds size limit."""
    error_code = ErrorCode.FILE_TOO_LARGE
    
    def __init__(self, file_size: int, max_size: int):
        super().__init__(
            f"File size ({file_size} bytes) exceeds maximum allowed ({max_size} bytes)",
            details={"file_size": file_size, "max_size": max_size},
        )


class InvalidFileTypeError(FileError):
    """Raised when file type is not allowed."""
    error_code = ErrorCode.INVALID_FILE_TYPE
    
    def __init__(self, file_type: str, allowed_types: list[str]):
        super().__init__(
            f"File type '{file_type}' is not allowed",
            details={"file_type": file_type, "allowed_types": allowed_types},
        )


class DatabaseError(SwiftLogisticsError):
    """Raised when database operation fails."""
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    error_code = ErrorCode.DATABASE_ERROR
    
    def __init__(self, message: str = "Database operation failed"):
        super().__init__(message)


class QueueError(SwiftLogisticsError):
    """Raised when message queue operation fails."""
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    error_code = ErrorCode.QUEUE_ERROR
    
    def __init__(self, message: str = "Message queue operation failed"):
        super().__init__(message)


# ── Exception Handlers ───────────────────────────────────────

async def swiftlogistics_exception_handler(
    request: Request,
    exc: SwiftLogisticsError,
) -> JSONResponse:
    """Handle SwiftLogistics custom exceptions."""
    error_response = exc.to_response()
    
    # Log the error
    logger.error(
        "SwiftLogistics error: code=%s, message=%s, trace_id=%s, path=%s",
        error_response.code,
        exc.message,
        error_response.trace_id,
        request.url.path,
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content=error_response.to_dict(),
    )


async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    """Handle Pydantic/FastAPI validation errors."""
    errors = exc.errors()
    
    # Extract field-level errors
    field_errors = []
    for error in errors:
        field_path = ".".join(str(loc) for loc in error["loc"] if loc != "body")
        field_errors.append({
            "field": field_path,
            "message": error["msg"],
            "type": error["type"],
        })
    
    error_response = ErrorResponse(
        code=ErrorCode.VALIDATION_ERROR,
        message="Request validation failed",
        details={"errors": field_errors},
    )
    
    logger.warning(
        "Validation error: path=%s, errors=%s",
        request.url.path,
        field_errors,
    )
    
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=error_response.to_dict(),
    )


async def pydantic_validation_handler(
    request: Request,
    exc: PydanticValidationError,
) -> JSONResponse:
    """Handle Pydantic validation errors."""
    errors = exc.errors()
    
    field_errors = []
    for error in errors:
        field_path = ".".join(str(loc) for loc in error["loc"])
        field_errors.append({
            "field": field_path,
            "message": error["msg"],
            "type": error["type"],
        })
    
    error_response = ErrorResponse(
        code=ErrorCode.VALIDATION_ERROR,
        message="Data validation failed",
        details={"errors": field_errors},
    )
    
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=error_response.to_dict(),
    )


async def generic_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Handle unexpected exceptions."""
    # Generate trace ID for tracking
    import uuid
    trace_id = str(uuid.uuid4())[:8]
    
    # Log full traceback for debugging
    logger.error(
        "Unhandled exception: trace_id=%s, path=%s, error=%s\n%s",
        trace_id,
        request.url.path,
        str(exc),
        traceback.format_exc(),
    )
    
    error_response = ErrorResponse(
        code=ErrorCode.INTERNAL_ERROR,
        message="An unexpected error occurred",
        trace_id=trace_id,
    )
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=error_response.to_dict(),
    )


def register_exception_handlers(app: FastAPI):
    """
    Register all exception handlers with FastAPI application.
    
    Args:
        app: FastAPI application instance
    """
    # Custom SwiftLogistics exceptions
    app.add_exception_handler(SwiftLogisticsError, swiftlogistics_exception_handler)
    
    # FastAPI validation exceptions
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    
    # Pydantic validation exceptions
    app.add_exception_handler(PydanticValidationError, pydantic_validation_handler)
    
    # Catch-all for unexpected exceptions
    app.add_exception_handler(Exception, generic_exception_handler)
    
    logger.info("Exception handlers registered")
