"""
Retry Handler – Manages retry logic for CMS/ROS/WMS communications.

Features:
- Configurable retry policies with exponential backoff
- Circuit breaker pattern for system protection
- Move permanently failed messages to Dead Letter Queue
- Retry state persistence for recovery

Usage:
    retry_handler = RetryHandler()
    
    result = await retry_handler.execute_with_retry(
        operation=cms_client.validate_client,
        args=(client_id,),
        policy=RetryPolicy(max_attempts=3, base_delay=1.0)
    )
    
    if result.status == RetryStatus.EXHAUSTED:
        await retry_handler.send_to_dlq(result)
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger(__name__)


class RetryStatus(Enum):
    """Retry operation status."""
    SUCCESS = "success"
    RETRYING = "retrying"
    EXHAUSTED = "exhausted"  # All retries failed
    CIRCUIT_OPEN = "circuit_open"  # Circuit breaker tripped


@dataclass
class RetryPolicy:
    """
    Retry policy configuration.
    
    Attributes:
        max_attempts: Maximum number of attempts (including initial)
        base_delay: Base delay between retries in seconds
        max_delay: Maximum delay cap
        exponential_base: Base for exponential backoff (default 2)
        jitter: Add random jitter to delays to prevent thundering herd
        retryable_exceptions: Exception types that should trigger retry
    """
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: tuple = (Exception,)
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given attempt number (0-indexed)."""
        import random
        
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay
        )
        
        if self.jitter:
            # Add ±25% jitter
            jitter_range = delay * 0.25
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)


@dataclass
class RetryResult:
    """Result of a retry operation."""
    status: RetryStatus
    attempts: int
    result: Any = None
    error: Exception | None = None
    errors: list[tuple[int, Exception]] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    total_delay: float = 0.0
    
    # Metadata for DLQ
    operation_name: str = ""
    operation_args: tuple = ()
    operation_kwargs: dict = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            "status": self.status.value,
            "attempts": self.attempts,
            "result": str(self.result) if self.result else None,
            "error": str(self.error) if self.error else None,
            "error_history": [
                {"attempt": a, "error": str(e)} for a, e in self.errors
            ],
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "total_delay": self.total_delay,
            "operation_name": self.operation_name,
        }


class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures.
    
    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Too many failures, requests are blocked
    - HALF_OPEN: Testing if system has recovered
    """
    
    class State(Enum):
        CLOSED = "closed"
        OPEN = "open"
        HALF_OPEN = "half_open"
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 3,
    ):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before trying half-open state
            half_open_max_calls: Max test calls in half-open state
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        
        self._state = self.State.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: datetime | None = None
        self._half_open_calls = 0
    
    @property
    def state(self) -> State:
        """Get current circuit state, auto-transitioning if needed."""
        if self._state == self.State.OPEN and self._last_failure_time:
            elapsed = (datetime.now(timezone.utc) - self._last_failure_time).total_seconds()
            if elapsed >= self.recovery_timeout:
                self._state = self.State.HALF_OPEN
                self._half_open_calls = 0
                logger.info("Circuit breaker transitioning to HALF_OPEN")
        return self._state
    
    def can_execute(self) -> bool:
        """Check if request can pass through."""
        current_state = self.state
        
        if current_state == self.State.CLOSED:
            return True
        elif current_state == self.State.HALF_OPEN:
            return self._half_open_calls < self.half_open_max_calls
        else:  # OPEN
            return False
    
    def record_success(self) -> None:
        """Record successful call."""
        if self._state == self.State.HALF_OPEN:
            self._success_count += 1
            self._half_open_calls += 1
            
            if self._success_count >= self.half_open_max_calls:
                # Recovery successful, close circuit
                self._state = self.State.CLOSED
                self._failure_count = 0
                self._success_count = 0
                logger.info("Circuit breaker CLOSED after successful recovery")
        else:
            # In closed state, reset failure count on success
            self._failure_count = max(0, self._failure_count - 1)
    
    def record_failure(self) -> None:
        """Record failed call."""
        self._failure_count += 1
        self._last_failure_time = datetime.now(timezone.utc)
        
        if self._state == self.State.HALF_OPEN:
            # Failed during recovery, re-open circuit
            self._state = self.State.OPEN
            self._success_count = 0
            logger.warning("Circuit breaker re-OPENED during half-open test")
        elif self._failure_count >= self.failure_threshold:
            self._state = self.State.OPEN
            logger.warning(
                "Circuit breaker OPENED after %d failures",
                self._failure_count
            )
    
    def get_status(self) -> dict:
        """Get circuit breaker status."""
        return {
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            "last_failure": self._last_failure_time.isoformat() if self._last_failure_time else None,
        }


class RetryHandler:
    """
    Manages retry logic with circuit breaker pattern.
    
    Supports:
    - Configurable retry policies per operation
    - Exponential backoff with jitter
    - Circuit breaker for system protection
    - Retry history tracking
    """
    
    def __init__(
        self,
        default_policy: RetryPolicy | None = None,
        enable_circuit_breaker: bool = True,
    ):
        """
        Initialize retry handler.
        
        Args:
            default_policy: Default retry policy for all operations
            enable_circuit_breaker: Enable circuit breaker pattern
        """
        self.default_policy = default_policy or RetryPolicy()
        self.enable_circuit_breaker = enable_circuit_breaker
        
        # Circuit breakers per target system
        self._circuit_breakers: dict[str, CircuitBreaker] = {}
        
        # Retry history for monitoring
        self._retry_history: list[RetryResult] = []
        self._max_history = 1000
    
    def get_circuit_breaker(self, system_name: str) -> CircuitBreaker:
        """Get or create circuit breaker for a system."""
        if system_name not in self._circuit_breakers:
            self._circuit_breakers[system_name] = CircuitBreaker()
        return self._circuit_breakers[system_name]
    
    async def execute_with_retry(
        self,
        operation: Callable[..., Coroutine[Any, Any, Any]],
        args: tuple = (),
        kwargs: dict | None = None,
        policy: RetryPolicy | None = None,
        system_name: str = "default",
        operation_name: str | None = None,
    ) -> RetryResult:
        """
        Execute an async operation with retry logic.
        
        Args:
            operation: Async function to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            policy: Retry policy (uses default if not provided)
            system_name: Target system name (for circuit breaker)
            operation_name: Operation name for logging
            
        Returns:
            RetryResult with operation outcome
        """
        kwargs = kwargs or {}
        policy = policy or self.default_policy
        op_name = operation_name or getattr(operation, "__name__", "unknown")
        
        result = RetryResult(
            status=RetryStatus.RETRYING,
            attempts=0,
            operation_name=op_name,
            operation_args=args,
            operation_kwargs=kwargs,
        )
        
        # Check circuit breaker
        if self.enable_circuit_breaker:
            circuit_breaker = self.get_circuit_breaker(system_name)
            if not circuit_breaker.can_execute():
                result.status = RetryStatus.CIRCUIT_OPEN
                result.error = RuntimeError(f"Circuit breaker is OPEN for {system_name}")
                result.completed_at = datetime.now(timezone.utc)
                logger.warning("Request blocked by circuit breaker for %s", system_name)
                self._add_to_history(result)
                return result
        
        # Execute with retries
        for attempt in range(policy.max_attempts):
            result.attempts = attempt + 1
            
            try:
                logger.debug(
                    "Executing %s (attempt %d/%d) for %s",
                    op_name, attempt + 1, policy.max_attempts, system_name
                )
                
                result.result = await operation(*args, **kwargs)
                result.status = RetryStatus.SUCCESS
                result.completed_at = datetime.now(timezone.utc)
                
                if self.enable_circuit_breaker:
                    circuit_breaker.record_success()
                
                logger.info(
                    "%s succeeded on attempt %d for %s",
                    op_name, attempt + 1, system_name
                )
                
                self._add_to_history(result)
                return result
                
            except policy.retryable_exceptions as e:
                result.errors.append((attempt + 1, e))
                result.error = e
                
                if self.enable_circuit_breaker:
                    circuit_breaker.record_failure()
                
                if attempt < policy.max_attempts - 1:
                    delay = policy.calculate_delay(attempt)
                    result.total_delay += delay
                    
                    logger.warning(
                        "%s failed (attempt %d/%d) for %s: %s. Retrying in %.2fs...",
                        op_name, attempt + 1, policy.max_attempts, system_name, e, delay
                    )
                    
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        "%s exhausted all %d attempts for %s: %s",
                        op_name, policy.max_attempts, system_name, e
                    )
        
        # All retries exhausted
        result.status = RetryStatus.EXHAUSTED
        result.completed_at = datetime.now(timezone.utc)
        
        self._add_to_history(result)
        return result
    
    def _add_to_history(self, result: RetryResult) -> None:
        """Add result to history, maintaining max size."""
        self._retry_history.append(result)
        if len(self._retry_history) > self._max_history:
            self._retry_history = self._retry_history[-self._max_history:]
    
    def get_retry_stats(self, system_name: str | None = None) -> dict:
        """Get retry statistics."""
        if system_name:
            history = [r for r in self._retry_history if system_name in r.operation_name]
        else:
            history = self._retry_history
        
        if not history:
            return {"total": 0, "success": 0, "exhausted": 0, "circuit_open": 0}
        
        return {
            "total": len(history),
            "success": sum(1 for r in history if r.status == RetryStatus.SUCCESS),
            "exhausted": sum(1 for r in history if r.status == RetryStatus.EXHAUSTED),
            "circuit_open": sum(1 for r in history if r.status == RetryStatus.CIRCUIT_OPEN),
            "avg_attempts": sum(r.attempts for r in history) / len(history),
            "avg_delay": sum(r.total_delay for r in history) / len(history),
        }
    
    def get_circuit_breaker_status(self, system_name: str | None = None) -> dict:
        """Get circuit breaker status for all or specific systems."""
        if system_name:
            if system_name in self._circuit_breakers:
                return {system_name: self._circuit_breakers[system_name].get_status()}
            return {system_name: {"state": "not_initialized"}}
        
        return {
            name: cb.get_status()
            for name, cb in self._circuit_breakers.items()
        }
    
    async def reset_circuit_breaker(self, system_name: str) -> None:
        """Manually reset a circuit breaker."""
        if system_name in self._circuit_breakers:
            self._circuit_breakers[system_name] = CircuitBreaker()
            logger.info("Circuit breaker reset for %s", system_name)


# Singleton instance
retry_handler = RetryHandler()


# Utility functions for common patterns
async def with_retry(
    func: Callable[..., Coroutine],
    *args,
    max_attempts: int = 3,
    system_name: str = "default",
    **kwargs
) -> Any:
    """
    Convenience wrapper for executing a function with retry.
    
    Usage:
        result = await with_retry(
            cms_client.validate_client,
            client_id,
            max_attempts=3,
            system_name="CMS"
        )
    """
    policy = RetryPolicy(max_attempts=max_attempts)
    result = await retry_handler.execute_with_retry(
        operation=func,
        args=args,
        kwargs=kwargs,
        policy=policy,
        system_name=system_name,
    )
    
    if result.status == RetryStatus.SUCCESS:
        return result.result
    else:
        raise result.error or RuntimeError(f"Retry failed: {result.status.value}")
