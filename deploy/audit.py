"""
Structured audit logging and event tracking for the Canary Deployment Simulator.

Provides data models and collection mechanisms for concurrency-safe,
JSON-serializable deployment events.
"""

from __future__ import annotations

import datetime
import json
import os
import threading
from enum import Enum
from typing import Any, Dict

from logging_config import get_logger

logger = get_logger(__name__)


class DeploymentEventType(str, Enum):
    """Supported event types for canary deployment tracking."""
    DEPLOYMENT_START = "deployment_start"
    STAGE_TRANSITION = "stage_transition"
    HEALTH_CHECK = "health_check"
    ROLLBACK_START = "rollback_start"
    ROLLBACK_COMPLETE = "rollback_complete"
    ABORT_RECEIVED = "abort_received"
    DEPLOYMENT_COMPLETED = "deployment_completed"
    DEPLOYMENT_FAILED = "deployment_failed"


class DeploymentEvent:
    """A single structured deployment event."""

    def __init__(
        self,
        event_type: DeploymentEventType,
        deployment_id: str,
        timestamp: datetime.datetime | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        self.event_type = event_type
        self.deployment_id = deployment_id
        # Use UTC timestamp to preserve consistent time formatting
        self.timestamp = timestamp or datetime.datetime.now(datetime.timezone.utc)
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert the event to a JSON-compatible dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type.value,
            "deployment_id": self.deployment_id,
            "details": self.details,
        }

    def __repr__(self) -> str:
        return (
            f"DeploymentEvent(type={self.event_type.value}, "
            f"id={self.deployment_id}, time={self.timestamp.isoformat()})"
        )


class AuditLogger:
    """A thread-safe audit log recorder.

    Maintains a list of logged events in memory and optionally appends
    them as JSON lines to a file on disk.
    """

    def __init__(self, file_path: str | None = None) -> None:
        self.file_path = file_path
        self._events: list[DeploymentEvent] = []
        self._lock = threading.Lock()

    def log(self, event: DeploymentEvent) -> None:
        """Record a deployment event. Concurrency-safe and exception-safe.

        Args:
            event: The DeploymentEvent to log.
        """
        with self._lock:
            self._events.append(event)

        if self.file_path:
            # Concurrency-safe file write
            with self._lock:
                try:
                    dirname = os.path.dirname(self.file_path)
                    if dirname:
                        os.makedirs(dirname, exist_ok=True)
                    with open(self.file_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(event.to_dict()) + "\n")
                except Exception as exc:
                    logger.error(
                        "Exception while writing to audit log file '%s': %s",
                        self.file_path,
                        exc,
                    )

    def get_events(self) -> list[DeploymentEvent]:
        """Retrieve a copy of all recorded events in memory.

        Returns:
            List of recorded DeploymentEvent instances.
        """
        with self._lock:
            return list(self._events)

    def clear(self) -> None:
        """Clear all in-memory events."""
        with self._lock:
            self._events.clear()
