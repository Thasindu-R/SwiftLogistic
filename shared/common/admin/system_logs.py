"""
System Logs Service – Provides access to system logs for admin monitoring.

Enables viewing of:
- Integration errors
- Transaction history
- Audit trail
- System events
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, Dict, Any, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class LogLevel(str, Enum):
    """Log severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class LogCategory(str, Enum):
    """Log categories for filtering."""
    INTEGRATION = "integration"
    TRANSACTION = "transaction"
    SAGA = "saga"
    ORDER = "order"
    DELIVERY = "delivery"
    AUTHENTICATION = "authentication"
    SYSTEM = "system"


@dataclass
class LogFilter:
    """Filter criteria for log queries."""
    level: Optional[LogLevel] = None
    category: Optional[LogCategory] = None
    source_system: Optional[str] = None
    target_system: Optional[str] = None
    order_id: Optional[str] = None
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    status: Optional[str] = None
    search_text: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "level": self.level.value if self.level else None,
            "category": self.category.value if self.category else None,
            "source_system": self.source_system,
            "target_system": self.target_system,
            "order_id": self.order_id,
            "since": self.since.isoformat() if self.since else None,
            "until": self.until.isoformat() if self.until else None,
            "status": self.status,
            "search_text": self.search_text,
        }


@dataclass
class SystemLogEntry:
    """A system log entry."""
    id: str
    timestamp: datetime
    level: LogLevel
    category: LogCategory
    source: str
    message: str
    details: Optional[Dict[str, Any]] = None
    order_id: Optional[str] = None
    user_id: Optional[str] = None
    error_code: Optional[str] = None
    stack_trace: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "level": self.level.value,
            "category": self.category.value,
            "source": self.source,
            "message": self.message,
            "details": self.details,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "error_code": self.error_code,
            "stack_trace": self.stack_trace,
        }


class SystemLogService:
    """
    Service for viewing and filtering system logs.
    
    Provides access to:
    - Integration event logs
    - Transaction history (sagas)
    - Audit trail
    - Error logs
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_integration_logs(
        self,
        filter: Optional[LogFilter] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Get integration event logs with filtering.
        
        Returns integration events from CMS, ROS, WMS communications.
        """
        query = """
            SELECT 
                event_id, correlation_id, order_id, saga_id,
                source_system, target_system, event_type,
                status, severity, error_message, error_code,
                retry_count, max_retries, duration_ms,
                request_data, response_data,
                created_at, updated_at
            FROM integration_event_logs
            WHERE 1=1
        """
        count_query = "SELECT COUNT(*) FROM integration_event_logs WHERE 1=1"
        params: Dict[str, Any] = {}
        
        # Apply filters
        if filter:
            if filter.source_system:
                query += " AND source_system = :source"
                count_query += " AND source_system = :source"
                params["source"] = filter.source_system
            
            if filter.target_system:
                query += " AND target_system = :target"
                count_query += " AND target_system = :target"
                params["target"] = filter.target_system
            
            if filter.order_id:
                query += " AND order_id = :order_id"
                count_query += " AND order_id = :order_id"
                params["order_id"] = filter.order_id
            
            if filter.status:
                query += " AND status = :status"
                count_query += " AND status = :status"
                params["status"] = filter.status
            
            if filter.level:
                query += " AND severity = :severity"
                count_query += " AND severity = :severity"
                params["severity"] = filter.level.value
            
            if filter.since:
                query += " AND created_at >= :since"
                count_query += " AND created_at >= :since"
                params["since"] = filter.since
            
            if filter.until:
                query += " AND created_at <= :until"
                count_query += " AND created_at <= :until"
                params["until"] = filter.until
            
            if filter.search_text:
                query += " AND (error_message ILIKE :search OR event_type ILIKE :search)"
                count_query += " AND (error_message ILIKE :search OR event_type ILIKE :search)"
                params["search"] = f"%{filter.search_text}%"
        
        # Get total count
        try:
            result = await self.db.execute(text(count_query), params)
            total = result.scalar() or 0
        except Exception:
            total = 0
        
        # Add pagination and ordering
        query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset
        
        try:
            result = await self.db.execute(text(query), params)
            rows = result.fetchall()
            
            logs = [
                {
                    "event_id": row.event_id,
                    "correlation_id": row.correlation_id,
                    "order_id": row.order_id,
                    "saga_id": row.saga_id,
                    "source_system": row.source_system,
                    "target_system": row.target_system,
                    "event_type": row.event_type,
                    "status": row.status,
                    "severity": row.severity,
                    "error_message": row.error_message,
                    "error_code": row.error_code,
                    "retry_count": row.retry_count,
                    "max_retries": row.max_retries,
                    "duration_ms": row.duration_ms,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                }
                for row in rows
            ]
            
            return {
                "total": total,
                "limit": limit,
                "offset": offset,
                "logs": logs,
            }
        except Exception as e:
            logger.error("Failed to fetch integration logs: %s", e)
            return {"total": 0, "limit": limit, "offset": offset, "logs": [], "error": str(e)}
    
    async def get_transaction_history(
        self,
        order_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Get transaction (saga) history for monitoring distributed transactions.
        
        Shows saga executions, their states, and any compensation activities.
        """
        query = """
            SELECT 
                sr.saga_id, sr.order_id, sr.state,
                sr.steps_json, sr.error_message,
                sr.started_at, sr.completed_at, sr.updated_at,
                (
                    SELECT COUNT(*) FROM saga_status_history ssh 
                    WHERE ssh.saga_id = sr.saga_id
                ) as transition_count
            FROM saga_records sr
            WHERE 1=1
        """
        count_query = "SELECT COUNT(*) FROM saga_records WHERE 1=1"
        params: Dict[str, Any] = {}
        
        if order_id:
            query += " AND sr.order_id = :order_id"
            count_query += " AND order_id = :order_id"
            params["order_id"] = order_id
        
        if state:
            query += " AND sr.state = :state"
            count_query += " AND state = :state"
            params["state"] = state
        
        # Get total count
        try:
            result = await self.db.execute(text(count_query), params)
            total = result.scalar() or 0
        except Exception:
            total = 0
        
        query += " ORDER BY sr.started_at DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset
        
        try:
            result = await self.db.execute(text(query), params)
            rows = result.fetchall()
            
            import json
            
            transactions = []
            for row in rows:
                steps = []
                try:
                    steps = json.loads(row.steps_json) if row.steps_json else []
                except Exception:
                    pass
                
                transactions.append({
                    "saga_id": row.saga_id,
                    "order_id": row.order_id,
                    "state": row.state,
                    "steps": steps,
                    "error_message": row.error_message,
                    "started_at": row.started_at.isoformat() if row.started_at else None,
                    "completed_at": row.completed_at.isoformat() if row.completed_at else None,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                    "transition_count": row.transition_count or 0,
                })
            
            return {
                "total": total,
                "limit": limit,
                "offset": offset,
                "transactions": transactions,
            }
        except Exception as e:
            logger.error("Failed to fetch transaction history: %s", e)
            return {"total": 0, "limit": limit, "offset": offset, "transactions": [], "error": str(e)}
    
    async def get_transaction_detail(self, saga_id: str) -> Dict[str, Any]:
        """Get detailed transaction information including state history."""
        # Get saga record
        try:
            result = await self.db.execute(text("""
                SELECT * FROM saga_records WHERE saga_id = :saga_id
            """), {"saga_id": saga_id})
            
            saga = result.fetchone()
            if not saga:
                return {"error": "Transaction not found"}
            
            import json
            
            # Get state history
            history_result = await self.db.execute(text("""
                SELECT * FROM saga_status_history 
                WHERE saga_id = :saga_id 
                ORDER BY timestamp ASC
            """), {"saga_id": saga_id})
            
            history_rows = history_result.fetchall()
            
            history = [
                {
                    "step_name": h.step_name,
                    "from_state": h.from_state,
                    "to_state": h.to_state,
                    "details": h.details,
                    "timestamp": h.timestamp.isoformat() if h.timestamp else None,
                }
                for h in history_rows
            ]
            
            steps = []
            try:
                steps = json.loads(saga.steps_json) if saga.steps_json else []
            except Exception:
                pass
            
            return {
                "saga_id": saga.saga_id,
                "order_id": saga.order_id,
                "state": saga.state,
                "steps": steps,
                "error_message": saga.error_message,
                "started_at": saga.started_at.isoformat() if saga.started_at else None,
                "completed_at": saga.completed_at.isoformat() if saga.completed_at else None,
                "history": history,
            }
        except Exception as e:
            logger.error("Failed to fetch transaction detail: %s", e)
            return {"error": str(e)}
    
    async def get_audit_trail(
        self,
        order_id: Optional[str] = None,
        entity_type: Optional[str] = None,
        action: Optional[str] = None,
        actor_type: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Get audit trail logs for compliance and debugging.
        
        Shows all actions taken on entities with actor information.
        """
        query = """
            SELECT 
                trail_id, order_id, actor_type, actor_id, actor_name,
                action, entity_type, entity_id,
                old_value, new_value, details,
                ip_address, user_agent, timestamp
            FROM audit_trail_logs
            WHERE 1=1
        """
        count_query = "SELECT COUNT(*) FROM audit_trail_logs WHERE 1=1"
        params: Dict[str, Any] = {}
        
        if order_id:
            query += " AND order_id = :order_id"
            count_query += " AND order_id = :order_id"
            params["order_id"] = order_id
        
        if entity_type:
            query += " AND entity_type = :entity_type"
            count_query += " AND entity_type = :entity_type"
            params["entity_type"] = entity_type
        
        if action:
            query += " AND action = :action"
            count_query += " AND action = :action"
            params["action"] = action
        
        if actor_type:
            query += " AND actor_type = :actor_type"
            count_query += " AND actor_type = :actor_type"
            params["actor_type"] = actor_type
        
        if since:
            query += " AND timestamp >= :since"
            count_query += " AND timestamp >= :since"
            params["since"] = since
        
        # Get total count
        try:
            result = await self.db.execute(text(count_query), params)
            total = result.scalar() or 0
        except Exception:
            total = 0
        
        query += " ORDER BY timestamp DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset
        
        try:
            result = await self.db.execute(text(query), params)
            rows = result.fetchall()
            
            audit_entries = [
                {
                    "trail_id": row.trail_id,
                    "order_id": row.order_id,
                    "actor_type": row.actor_type,
                    "actor_id": row.actor_id,
                    "actor_name": row.actor_name,
                    "action": row.action,
                    "entity_type": row.entity_type,
                    "entity_id": row.entity_id,
                    "old_value": row.old_value,
                    "new_value": row.new_value,
                    "details": row.details,
                    "ip_address": row.ip_address,
                    "user_agent": row.user_agent,
                    "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                }
                for row in rows
            ]
            
            return {
                "total": total,
                "limit": limit,
                "offset": offset,
                "audit_trail": audit_entries,
            }
        except Exception as e:
            logger.error("Failed to fetch audit trail: %s", e)
            return {"total": 0, "limit": limit, "offset": offset, "audit_trail": [], "error": str(e)}
    
    async def get_error_summary(
        self,
        hours: int = 24,
    ) -> Dict[str, Any]:
        """
        Get summary of errors in the last N hours.
        
        Aggregates errors by type, source, and severity.
        """
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        summary = {
            "period_hours": hours,
            "since": since.isoformat(),
            "total_errors": 0,
            "by_source": {},
            "by_target": {},
            "by_type": {},
            "by_severity": {},
            "recent_errors": [],
        }
        
        try:
            # Get error counts by source
            result = await self.db.execute(text("""
                SELECT source_system, COUNT(*) as count
                FROM integration_event_logs
                WHERE status = 'failed' AND created_at >= :since
                GROUP BY source_system
            """), {"since": since})
            
            for row in result.fetchall():
                summary["by_source"][row.source_system] = row.count
            
            # Get error counts by target
            result = await self.db.execute(text("""
                SELECT target_system, COUNT(*) as count
                FROM integration_event_logs
                WHERE status = 'failed' AND created_at >= :since
                GROUP BY target_system
            """), {"since": since})
            
            for row in result.fetchall():
                summary["by_target"][row.target_system] = row.count
            
            # Get error counts by type
            result = await self.db.execute(text("""
                SELECT event_type, COUNT(*) as count
                FROM integration_event_logs
                WHERE status = 'failed' AND created_at >= :since
                GROUP BY event_type
                ORDER BY count DESC
                LIMIT 10
            """), {"since": since})
            
            for row in result.fetchall():
                summary["by_type"][row.event_type] = row.count
            
            # Get error counts by severity
            result = await self.db.execute(text("""
                SELECT severity, COUNT(*) as count
                FROM integration_event_logs
                WHERE status = 'failed' AND created_at >= :since
                GROUP BY severity
            """), {"since": since})
            
            for row in result.fetchall():
                summary["by_severity"][row.severity] = row.count
            
            # Get total error count
            result = await self.db.execute(text("""
                SELECT COUNT(*) FROM integration_event_logs
                WHERE status = 'failed' AND created_at >= :since
            """), {"since": since})
            
            summary["total_errors"] = result.scalar() or 0
            
            # Get recent errors
            result = await self.db.execute(text("""
                SELECT event_id, order_id, target_system, event_type, 
                       error_message, severity, created_at
                FROM integration_event_logs
                WHERE status = 'failed' AND created_at >= :since
                ORDER BY created_at DESC
                LIMIT 10
            """), {"since": since})
            
            summary["recent_errors"] = [
                {
                    "event_id": row.event_id,
                    "order_id": row.order_id,
                    "target_system": row.target_system,
                    "event_type": row.event_type,
                    "error_message": row.error_message,
                    "severity": row.severity,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                }
                for row in result.fetchall()
            ]
            
        except Exception as e:
            logger.error("Failed to generate error summary: %s", e)
            summary["error"] = str(e)
        
        return summary
    
    async def get_dlq_records(
        self,
        queue: Optional[str] = None,
        processed: Optional[bool] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Get Dead Letter Queue records for admin review.
        
        Shows messages that failed processing repeatedly.
        """
        query = """
            SELECT 
                message_id, original_queue, original_exchange,
                original_routing_key, payload_json, error_reason,
                retry_count, max_retries, first_failure_at,
                last_failure_at, processed, processed_at, created_at
            FROM dlq_records
            WHERE 1=1
        """
        count_query = "SELECT COUNT(*) FROM dlq_records WHERE 1=1"
        params: Dict[str, Any] = {}
        
        if queue:
            query += " AND original_queue = :queue"
            count_query += " AND original_queue = :queue"
            params["queue"] = queue
        
        if processed is not None:
            query += " AND processed = :processed"
            count_query += " AND processed = :processed"
            params["processed"] = processed
        
        # Get total count
        try:
            result = await self.db.execute(text(count_query), params)
            total = result.scalar() or 0
        except Exception:
            total = 0
        
        query += " ORDER BY last_failure_at DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset
        
        try:
            result = await self.db.execute(text(query), params)
            rows = result.fetchall()
            
            records = [
                {
                    "message_id": row.message_id,
                    "original_queue": row.original_queue,
                    "original_exchange": row.original_exchange,
                    "original_routing_key": row.original_routing_key,
                    "payload": row.payload_json,
                    "error_reason": row.error_reason,
                    "retry_count": row.retry_count,
                    "max_retries": row.max_retries,
                    "first_failure_at": row.first_failure_at.isoformat() if row.first_failure_at else None,
                    "last_failure_at": row.last_failure_at.isoformat() if row.last_failure_at else None,
                    "processed": row.processed,
                    "processed_at": row.processed_at.isoformat() if row.processed_at else None,
                }
                for row in rows
            ]
            
            return {
                "total": total,
                "limit": limit,
                "offset": offset,
                "records": records,
            }
        except Exception as e:
            logger.error("Failed to fetch DLQ records: %s", e)
            return {"total": 0, "limit": limit, "offset": offset, "records": [], "error": str(e)}
