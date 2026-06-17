"""
Deployment coordination package for the Canary Deployment Simulator.

Re-exports the key public symbols::

    from deploy import DeploymentEngine, DeploymentConfig
    from deploy import DeploymentState, DeploymentStatus, StageResult
"""

from deploy.config import DeploymentConfig
from deploy.state import DeploymentState, DeploymentStatus, StageResult
from deploy.engine import DeploymentEngine

__all__ = [
    "DeploymentConfig",
    "DeploymentEngine",
    "DeploymentState",
    "DeploymentStatus",
    "StageResult",
]
