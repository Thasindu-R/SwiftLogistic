"""
Async Processing & Reliability Module

Provides:
- Message queue handling with RabbitMQ
- Retry mechanism with Dead Letter Queue
- Distributed transaction (Saga) handling
- Event logging and audit trail
- Failure recovery mechanism
"""

from .queue_manager import (
    QueueManager,
    MessagePriority,
    DLQEntry,
)
from .retry_handler import (
    RetryHandler,
    RetryPolicy,
    RetryResult,
)
from .saga_state import (
    SagaStateMachine,
    SagaState,
    SagaStep,
    SagaStepStatus,
)
from .message_processor import (
    AsyncMessageProcessor,
    MessageHandler,
    ProcessingResult,
)
from .recovery_service import (
    FailureRecoveryService,
    RecoveryStatus,
)
from .event_store import (
    IntegrationEventStore,
    EventStatus,
    AuditTrailEntry,
)

__all__ = [
    # Queue Manager
    "QueueManager",
    "MessagePriority",
    "DLQEntry",
    # Retry Handler
    "RetryHandler",
    "RetryPolicy",
    "RetryResult",
    # Saga State Machine
    "SagaStateMachine",
    "SagaState",
    "SagaStep",
    "SagaStepStatus",
    # Message Processor
    "AsyncMessageProcessor",
    "MessageHandler",
    "ProcessingResult",
    # Recovery Service
    "FailureRecoveryService",
    "RecoveryStatus",
    # Event Store
    "IntegrationEventStore",
    "EventStatus",
    "AuditTrailEntry",
]
