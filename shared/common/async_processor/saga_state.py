"""
Saga State Machine – Distributed Transaction Handling.

Ensures order consistency across CMS, ROS, and WMS with:
- State machine for saga lifecycle
- Compensating transactions on failure
- Status history tracking
- Persistence for recovery

Usage:
    saga = SagaStateMachine(order_id="ORD-001", order_data={...})
    
    # Execute each step
    await saga.execute_step("cms", cms_operation)
    await saga.execute_step("wms", wms_operation)
    await saga.execute_step("ros", ros_operation)
    
    # If any step fails, compensation runs automatically
    if saga.state == SagaState.COMPENSATING:
        await saga.complete_compensation()
"""

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Coroutine, Optional

from sqlalchemy import Column, DateTime, Integer, String, Text, Boolean, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.common.database import Base

logger = logging.getLogger(__name__)


class SagaState(Enum):
    """Saga lifecycle states."""
    PENDING = "pending"           # Not started
    IN_PROGRESS = "in_progress"   # Executing steps
    COMPLETED = "completed"       # All steps successful
    COMPENSATING = "compensating" # Running compensation
    COMPENSATED = "compensated"   # Compensation complete
    FAILED = "failed"             # Failed and cannot compensate


class SagaStepStatus(Enum):
    """Individual step status."""
    PENDING = "pending"
    EXECUTING = "executing"
    SUCCESS = "success"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"


@dataclass
class SagaStep:
    """
    Represents a single step in the saga.
    
    Attributes:
        name: Step identifier (e.g., "cms", "wms", "ros")
        order: Execution order (lower = earlier)
        status: Current step status
        execute_fn: Async function to execute the step
        compensate_fn: Async function to undo the step
        result: Result from execution
        error: Error if step failed
        started_at: When step started
        completed_at: When step finished
        retry_count: Number of retries attempted
    """
    name: str
    order: int
    status: SagaStepStatus = SagaStepStatus.PENDING
    execute_fn: Callable[..., Coroutine] | None = None
    compensate_fn: Callable[..., Coroutine] | None = None
    result: Any = None
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    retry_count: int = 0
    max_retries: int = 3
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "order": self.order,
            "status": self.status.value,
            "error": self.error,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "retry_count": self.retry_count,
        }


# Database model for saga state persistence
class SagaRecord(Base):
    """Persistent saga record for recovery."""
    __tablename__ = "saga_records"
    
    id = Column(Integer, primary_key=True, index=True)
    saga_id = Column(String(36), unique=True, nullable=False, index=True)
    order_id = Column(String(36), nullable=False, index=True)
    state = Column(String(20), nullable=False, default="pending")
    steps_json = Column(Text, nullable=False, default="[]")
    order_data_json = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class SagaStatusHistory(Base):
    """History of saga state transitions."""
    __tablename__ = "saga_status_history"
    
    id = Column(Integer, primary_key=True, index=True)
    saga_id = Column(String(36), nullable=False, index=True)
    order_id = Column(String(36), nullable=False, index=True)
    step_name = Column(String(30), nullable=True)
    from_state = Column(String(20), nullable=True)
    to_state = Column(String(20), nullable=False)
    details = Column(Text, nullable=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class SagaStateMachine:
    """
    Orchestrates distributed transactions across CMS, ROS, and WMS.
    
    Features:
    - Automatic step execution with retry
    - Compensating transactions on failure
    - State persistence for recovery
    - Complete audit trail
    """
    
    # Standard step order
    STEP_ORDER = {
        "cms": 1,    # First: Validate client
        "wms": 2,    # Second: Register in warehouse
        "ros": 3,    # Third: Optimize route
    }
    
    def __init__(
        self,
        order_id: str,
        order_data: dict,
        saga_id: str | None = None,
        db_session: AsyncSession | None = None,
    ):
        """
        Initialize saga state machine.
        
        Args:
            order_id: Order identifier
            order_data: Order data for processing
            saga_id: Existing saga ID (for recovery)
            db_session: Database session for persistence
        """
        self.saga_id = saga_id or str(uuid.uuid4())
        self.order_id = order_id
        self.order_data = order_data
        self.db = db_session
        
        self._state = SagaState.PENDING
        self._steps: dict[str, SagaStep] = {}
        self._status_history: list[dict] = []
        self._started_at: datetime | None = None
        self._completed_at: datetime | None = None
    
    @property
    def state(self) -> SagaState:
        return self._state
    
    @property
    def steps(self) -> list[SagaStep]:
        """Get steps sorted by execution order."""
        return sorted(self._steps.values(), key=lambda s: s.order)
    
    def register_step(
        self,
        name: str,
        execute_fn: Callable[..., Coroutine],
        compensate_fn: Callable[..., Coroutine] | None = None,
        order: int | None = None,
        max_retries: int = 3,
    ) -> None:
        """
        Register a step in the saga.
        
        Args:
            name: Step name (e.g., "cms", "wms", "ros")
            execute_fn: Async function to execute
            compensate_fn: Async function to compensate (undo)
            order: Execution order (uses default if not provided)
            max_retries: Maximum retry attempts
        """
        step_order = order if order is not None else self.STEP_ORDER.get(name, 99)
        
        self._steps[name] = SagaStep(
            name=name,
            order=step_order,
            execute_fn=execute_fn,
            compensate_fn=compensate_fn,
            max_retries=max_retries,
        )
        
        logger.debug("Registered saga step: %s (order=%d)", name, step_order)
    
    async def execute(self) -> dict:
        """
        Execute all registered steps in order.
        
        Returns:
            Saga execution result
        """
        if not self._steps:
            raise ValueError("No steps registered in saga")
        
        self._started_at = datetime.now(timezone.utc)
        await self._transition_state(SagaState.IN_PROGRESS)
        await self._persist_state()
        
        logger.info("Starting saga %s for order %s", self.saga_id, self.order_id)
        
        completed_steps: list[str] = []
        
        for step in self.steps:
            try:
                await self._execute_step(step)
                completed_steps.append(step.name)
            except Exception as e:
                logger.error(
                    "Saga step %s failed for order %s: %s",
                    step.name, self.order_id, e
                )
                
                # Start compensation for completed steps
                await self._compensate(completed_steps, step.name, str(e))
                break
        
        if self._state == SagaState.IN_PROGRESS:
            await self._transition_state(SagaState.COMPLETED)
        
        self._completed_at = datetime.now(timezone.utc)
        await self._persist_state()
        
        return self.get_result()
    
    async def _execute_step(self, step: SagaStep) -> Any:
        """Execute a single saga step with retry."""
        step.status = SagaStepStatus.EXECUTING
        step.started_at = datetime.now(timezone.utc)
        
        await self._record_history(
            step_name=step.name,
            to_state="executing",
            details=f"Starting step {step.name}"
        )
        
        last_error: Exception | None = None
        
        for attempt in range(step.max_retries):
            step.retry_count = attempt
            
            try:
                if step.execute_fn:
                    step.result = await step.execute_fn(self.order_data)
                
                step.status = SagaStepStatus.SUCCESS
                step.completed_at = datetime.now(timezone.utc)
                
                await self._record_history(
                    step_name=step.name,
                    from_state="executing",
                    to_state="success",
                    details=f"Step {step.name} completed successfully"
                )
                
                logger.info(
                    "Saga step %s succeeded (attempt %d) for order %s",
                    step.name, attempt + 1, self.order_id
                )
                
                return step.result
                
            except Exception as e:
                last_error = e
                step.error = str(e)
                
                if attempt < step.max_retries - 1:
                    delay = 2 ** attempt  # Exponential backoff
                    logger.warning(
                        "Saga step %s failed (attempt %d/%d) for order %s: %s. Retrying in %ds...",
                        step.name, attempt + 1, step.max_retries, self.order_id, e, delay
                    )
                    await asyncio.sleep(delay)
        
        # All retries exhausted
        step.status = SagaStepStatus.FAILED
        step.completed_at = datetime.now(timezone.utc)
        
        await self._record_history(
            step_name=step.name,
            from_state="executing",
            to_state="failed",
            details=f"Step {step.name} failed after {step.max_retries} attempts: {last_error}"
        )
        
        raise last_error or RuntimeError(f"Step {step.name} failed")
    
    async def _compensate(
        self,
        completed_steps: list[str],
        failed_step: str,
        error: str,
    ) -> None:
        """
        Run compensating transactions for completed steps.
        
        Compensation runs in reverse order of execution.
        """
        await self._transition_state(SagaState.COMPENSATING)
        
        logger.info(
            "Starting compensation for saga %s (failed at %s): %s",
            self.saga_id, failed_step, error
        )
        
        # Compensate in reverse order
        for step_name in reversed(completed_steps):
            step = self._steps.get(step_name)
            if not step or not step.compensate_fn:
                logger.warning("No compensation function for step %s", step_name)
                continue
            
            step.status = SagaStepStatus.COMPENSATING
            
            await self._record_history(
                step_name=step_name,
                to_state="compensating",
                details=f"Starting compensation for {step_name}"
            )
            
            try:
                await step.compensate_fn(self.order_data, step.result)
                step.status = SagaStepStatus.COMPENSATED
                
                await self._record_history(
                    step_name=step_name,
                    from_state="compensating",
                    to_state="compensated",
                    details=f"Compensation completed for {step_name}"
                )
                
                logger.info("Compensated step %s for saga %s", step_name, self.saga_id)
                
            except Exception as e:
                logger.error(
                    "Compensation failed for step %s in saga %s: %s",
                    step_name, self.saga_id, e
                )
                # Continue compensating other steps
        
        await self._transition_state(SagaState.COMPENSATED)
    
    async def _transition_state(self, new_state: SagaState) -> None:
        """Transition saga to new state."""
        old_state = self._state
        self._state = new_state
        
        await self._record_history(
            from_state=old_state.value,
            to_state=new_state.value,
            details=f"Saga state transition: {old_state.value} → {new_state.value}"
        )
        
        logger.debug(
            "Saga %s state: %s → %s",
            self.saga_id, old_state.value, new_state.value
        )
    
    async def _record_history(
        self,
        to_state: str,
        from_state: str | None = None,
        step_name: str | None = None,
        details: str | None = None,
    ) -> None:
        """Record state transition in history."""
        entry = {
            "saga_id": self.saga_id,
            "order_id": self.order_id,
            "step_name": step_name,
            "from_state": from_state,
            "to_state": to_state,
            "details": details,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        
        self._status_history.append(entry)
        
        # Persist to database if session available
        if self.db:
            history_record = SagaStatusHistory(
                saga_id=self.saga_id,
                order_id=self.order_id,
                step_name=step_name,
                from_state=from_state,
                to_state=to_state,
                details=details,
            )
            self.db.add(history_record)
            await self.db.commit()
    
    async def _persist_state(self) -> None:
        """Persist saga state to database."""
        if not self.db:
            return
        
        # Check if record exists
        result = await self.db.execute(
            select(SagaRecord).where(SagaRecord.saga_id == self.saga_id)
        )
        record = result.scalar_one_or_none()
        
        steps_json = json.dumps([s.to_dict() for s in self.steps])
        order_data_json = json.dumps(self.order_data)
        
        if record:
            record.state = self._state.value
            record.steps_json = steps_json
            record.completed_at = self._completed_at
            record.updated_at = datetime.now(timezone.utc)
        else:
            record = SagaRecord(
                saga_id=self.saga_id,
                order_id=self.order_id,
                state=self._state.value,
                steps_json=steps_json,
                order_data_json=order_data_json,
                started_at=self._started_at,
            )
            self.db.add(record)
        
        await self.db.commit()
    
    @classmethod
    async def load_from_db(
        cls,
        saga_id: str,
        db: AsyncSession,
    ) -> Optional["SagaStateMachine"]:
        """
        Load saga state from database for recovery.
        
        Args:
            saga_id: Saga identifier
            db: Database session
            
        Returns:
            Loaded SagaStateMachine or None if not found
        """
        result = await db.execute(
            select(SagaRecord).where(SagaRecord.saga_id == saga_id)
        )
        record = result.scalar_one_or_none()
        
        if not record:
            return None
        
        order_data = json.loads(record.order_data_json) if record.order_data_json else {}
        
        saga = cls(
            saga_id=record.saga_id,
            order_id=record.order_id,
            order_data=order_data,
            db_session=db,
        )
        
        saga._state = SagaState(record.state)
        saga._started_at = record.started_at
        saga._completed_at = record.completed_at
        
        # Load steps
        steps_data = json.loads(record.steps_json)
        for step_data in steps_data:
            saga._steps[step_data["name"]] = SagaStep(
                name=step_data["name"],
                order=step_data["order"],
                status=SagaStepStatus(step_data["status"]),
                error=step_data.get("error"),
                retry_count=step_data.get("retry_count", 0),
            )
        
        logger.info("Loaded saga %s from database (state=%s)", saga_id, saga._state.value)
        
        return saga
    
    @classmethod
    async def get_incomplete_sagas(
        cls,
        db: AsyncSession,
    ) -> list["SagaStateMachine"]:
        """
        Get all incomplete sagas for recovery.
        
        Returns:
            List of incomplete sagas
        """
        result = await db.execute(
            select(SagaRecord).where(
                SagaRecord.state.in_(["pending", "in_progress", "compensating"])
            )
        )
        records = result.scalars().all()
        
        sagas = []
        for record in records:
            saga = await cls.load_from_db(record.saga_id, db)
            if saga:
                sagas.append(saga)
        
        logger.info("Found %d incomplete sagas for recovery", len(sagas))
        
        return sagas
    
    def get_result(self) -> dict:
        """Get saga execution result."""
        return {
            "saga_id": self.saga_id,
            "order_id": self.order_id,
            "state": self._state.value,
            "steps": [s.to_dict() for s in self.steps],
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "completed_at": self._completed_at.isoformat() if self._completed_at else None,
            "success": self._state == SagaState.COMPLETED,
        }
    
    def get_history(self) -> list[dict]:
        """Get status history."""
        return self._status_history.copy()
