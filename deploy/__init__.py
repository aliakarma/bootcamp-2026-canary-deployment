"""
Deployment coordination package for the Canary Deployment Simulator.

Re-exports the key public symbols::

    from deploy import DeploymentEngine, DeploymentConfig
    from deploy import DeploymentState, DeploymentStatus, StageResult
"""

from deploy.config import DeploymentConfig
from deploy.state import DeploymentState, DeploymentStatus, StageResult
from deploy.engine import DeploymentEngine
from deploy.rollback import (
    save_deployment_state,
    load_deployment_state,
    validate_rollback_consistency,
    RollbackConsistencyError,
)
from deploy.abort_listener import ConsoleAbortListener
from deploy.audit import AuditLogger, DeploymentEvent, DeploymentEventType

__all__ = [
    "DeploymentConfig",
    "DeploymentEngine",
    "DeploymentState",
    "DeploymentStatus",
    "StageResult",
    "save_deployment_state",
    "load_deployment_state",
    "validate_rollback_consistency",
    "RollbackConsistencyError",
    "ConsoleAbortListener",
    "AuditLogger",
    "DeploymentEvent",
    "DeploymentEventType",
]
