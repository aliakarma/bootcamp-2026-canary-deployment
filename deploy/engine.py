"""
Deployment Coordination Engine for the Canary Deployment Simulator.

Orchestrates a staged canary rollout across a simulated server cluster:

1. Selects servers for each stage based on percentage targets
2. Updates servers to the target version in batches
3. Waits between stages (interruptible via abort event)
4. Invokes optional health-check callbacks after each stage
5. Tracks full deployment state with per-stage granularity

Usage::

    from cluster import generate_cluster, ClusterState
    from deploy import DeploymentEngine, DeploymentConfig

    state = ClusterState(generate_cluster(size=30))
    config = DeploymentConfig(target_version="2.0.0")
    engine = DeploymentEngine(state)
    result = engine.deploy(config)
"""

from __future__ import annotations

import math
import time
import uuid
from datetime import datetime
from typing import Any

from logging_config import get_logger
from cluster.models import ServerStatus
from cluster.state import ClusterState
from deploy.config import DeploymentConfig
from deploy.state import DeploymentState, DeploymentStatus, StageResult

logger = get_logger(__name__)


class DeploymentEngine:
    """Coordinates a staged canary deployment across a cluster.

    The engine is stateless between deployments — each call to
    :meth:`deploy` creates a fresh :class:`DeploymentState` and
    returns it when the deployment completes (or fails/aborts).

    Args:
        cluster_state: The :class:`ClusterState` to deploy into.
    """

    def __init__(self, cluster_state: ClusterState) -> None:
        self._cluster = cluster_state
        self._current_deployment: DeploymentState | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def current_deployment(self) -> DeploymentState | None:
        """Return the deployment state of the currently active (or most
        recent) deployment, or ``None`` if no deployment has been run."""
        return self._current_deployment

    def rollback(self, deployment: DeploymentState, force: bool = False) -> list[str]:
        """Execute a rollback on a previously run deployment.

        Validates cluster consistency before executing.

        Args:
            deployment: The DeploymentState representing the rollout to revert.
            force: If True, bypass consistency checks and revert matching nodes anyway.

        Returns:
            List of successfully rolled back server IDs.
        """
        from deploy.rollback import rollback as run_rollback
        return run_rollback(self._cluster, deployment, force=force)

    def deploy(self, config: DeploymentConfig) -> DeploymentState:
        """Execute a full canary deployment.

        This method runs synchronously through all stages defined in
        *config*.  Between stages it sleeps for
        ``config.stage_delay_seconds`` (interruptible by
        ``config.abort_event``).

        Args:
            config: A :class:`DeploymentConfig` describing the rollout
                parameters.

        Returns:
            The final :class:`DeploymentState` after deployment
            completes, rolls back, or is aborted.
        """
        deployment = self._init_deployment(config)
        self._current_deployment = deployment

        logger.info("=" * 60)
        logger.info(
            "DEPLOYMENT STARTED: %s -> %s  (ID: %s)",
            deployment.source_version,
            deployment.target_version,
            deployment.deployment_id,
        )
        logger.info("  Stages: %s", config)
        logger.info("  Total servers: %d", deployment.total_servers)
        logger.info("=" * 60)

        deployment.status = DeploymentStatus.IN_PROGRESS

        try:
            for stage_idx, target_pct in enumerate(config.stages):
                # ----------------------------------------------------------
                # Check for abort before starting each stage
                # ----------------------------------------------------------
                if self._is_aborted(config):
                    self._handle_abort(deployment, "Abort signal received before stage start")
                    return deployment

                # ----------------------------------------------------------
                # Execute stage
                # ----------------------------------------------------------
                stage_result = self._execute_stage(
                    deployment, config, stage_idx, target_pct
                )
                deployment.stages.append(stage_result)

                if stage_result.error:
                    # Stage failed — check if due to abort
                    logger.error(
                        "Stage %d FAILED: %s", stage_idx, stage_result.error
                    )
                    if self._is_aborted(config):
                        self._handle_abort(
                            deployment,
                            f"Abort signal received mid-stage: {stage_result.error}",
                        )
                    else:
                        self._handle_rollback(
                            deployment,
                            f"Stage {stage_idx} failed: {stage_result.error}",
                        )
                    return deployment

                # ----------------------------------------------------------
                # Post-stage health check
                # ----------------------------------------------------------
                health_passed = self._run_health_check(
                    config, stage_idx, target_pct
                )
                stage_result.health_check_passed = health_passed

                if not health_passed:
                    # Health check failed — handle retries
                    retries_remaining = config.max_retries_per_stage
                    while retries_remaining > 0 and not health_passed:
                        logger.warning(
                            "Health check FAILED for stage %d (%d%%). "
                            "Retrying (%d retries left)...",
                            stage_idx, target_pct, retries_remaining,
                        )
                        time.sleep(config.health_check_interval)
                        health_passed = self._run_health_check(
                            config, stage_idx, target_pct
                        )
                        retries_remaining -= 1

                    if not health_passed:
                        stage_result.health_check_passed = False
                        logger.error(
                            "Health check FAILED for stage %d (%d%%) "
                            "after all retries. Initiating rollback.",
                            stage_idx, target_pct,
                        )
                        self._handle_rollback(
                            deployment,
                            f"Health check failed at stage {stage_idx} ({target_pct}%)",
                        )
                        return deployment

                    stage_result.health_check_passed = True

                # ----------------------------------------------------------
                # Stage complete callback
                # ----------------------------------------------------------
                if config.on_stage_complete:
                    try:
                        config.on_stage_complete(
                            stage_idx, target_pct, len(stage_result.servers_updated)
                        )
                    except Exception as exc:
                        logger.warning(
                            "on_stage_complete callback raised: %s", exc
                        )

                logger.info(
                    "Stage %d COMPLETE: %d%% deployed (%d/%d servers updated)",
                    stage_idx,
                    target_pct,
                    len(deployment.servers_updated),
                    deployment.total_servers,
                )

                # ----------------------------------------------------------
                # Inter-stage delay (unless this is the final stage)
                # ----------------------------------------------------------
                if stage_idx < len(config.stages) - 1:
                    deployment.status = DeploymentStatus.PAUSED
                    if not self._wait_between_stages(config, deployment):
                        # Aborted during wait
                        self._handle_abort(
                            deployment, "Abort signal received during inter-stage wait"
                        )
                        return deployment
                    deployment.status = DeploymentStatus.IN_PROGRESS

            # ----------------------------------------------------------
            # All stages completed successfully
            # ----------------------------------------------------------
            deployment.mark_completed()
            logger.info("=" * 60)
            logger.info(
                "DEPLOYMENT COMPLETED SUCCESSFULLY: %s -> %s",
                deployment.source_version,
                deployment.target_version,
            )
            logger.info(
                "  Duration: %.1fs | Servers updated: %d/%d",
                deployment.duration_seconds,
                len(deployment.servers_updated),
                deployment.total_servers,
            )
            logger.info("=" * 60)

            # Mark all updated servers as HEALTHY
            for server_id in deployment.servers_updated:
                self._cluster.update_server_status(server_id, ServerStatus.HEALTHY)

        except Exception as exc:
            logger.exception("Unexpected error during deployment: %s", exc)
            deployment.mark_failed(f"Unexpected error: {exc}")

        return deployment

    # ------------------------------------------------------------------
    # Stage execution
    # ------------------------------------------------------------------

    def _execute_stage(
        self,
        deployment: DeploymentState,
        config: DeploymentConfig,
        stage_idx: int,
        target_pct: int,
    ) -> StageResult:
        """Execute a single rollout stage.

        Determines which servers to update to reach *target_pct*, then
        updates them via the cluster state manager.
        """
        stage = StageResult(
            stage_index=stage_idx,
            target_percentage=target_pct,
            servers_total=deployment.total_servers,
            started_at=datetime.now(),
        )
        deployment.current_stage_index = stage_idx

        logger.info("-" * 50)
        logger.info(
            "STAGE %d: Rolling out to %d%% of cluster...",
            stage_idx, target_pct,
        )

        # Calculate how many servers should be updated cumulatively
        target_count = math.ceil(
            deployment.total_servers * target_pct / 100
        )
        already_updated = len(deployment.servers_updated)
        servers_needed = target_count - already_updated

        if servers_needed <= 0:
            logger.info(
                "  Stage %d: Already at %d%% — no additional servers needed",
                stage_idx, target_pct,
            )
            stage.completed_at = datetime.now()
            stage.duration_seconds = (
                stage.completed_at - stage.started_at
            ).total_seconds()
            return stage

        # Select servers to update (from pending pool)
        servers_to_update = self._select_servers_for_stage(
            deployment, servers_needed
        )

        logger.info(
            "  Updating %d servers (cumulative: %d -> %d of %d)",
            len(servers_to_update),
            already_updated,
            already_updated + len(servers_to_update),
            deployment.total_servers,
        )

        # Update each server
        for server_id in servers_to_update:
            # Check for abort mid-stage
            if self._is_aborted(config):
                stage.error = "Abort signal received mid-stage"
                break

            success = self._cluster.update_server_version(
                server_id, config.target_version
            )
            if success:
                deployment.servers_updated.add(server_id)
                deployment.servers_pending.discard(server_id)
                stage.servers_updated.append(server_id)
            else:
                logger.warning(
                    "  Failed to update server %s — skipping", server_id
                )

        # Check if the required number of servers was successfully updated
        # (Only flag as error if we weren't aborted mid-stage)
        if not stage.error and len(stage.servers_updated) < servers_needed:
            stage.error = (
                f"Under-provisioned update: target required updating {servers_needed} servers, "
                f"but only {len(stage.servers_updated)} were successfully updated."
            )

        stage.completed_at = datetime.now()
        stage.duration_seconds = (
            stage.completed_at - stage.started_at
        ).total_seconds()

        return stage

    def _select_servers_for_stage(
        self,
        deployment: DeploymentState,
        count: int,
    ) -> list[str]:
        """Select servers from the pending pool for the next stage.

        Distributes selection across regions for maximum coverage.
        Only selects servers that are in an updatable state.
        """
        pending_servers = [
            s for s in self._cluster.servers
            if s.id in deployment.servers_pending and s.is_updatable
        ]

        # Sort by region to ensure cross-region distribution
        pending_servers.sort(key=lambda s: (s.region, s.id))

        # Round-robin across regions for balanced distribution
        by_region: dict[str, list[str]] = {}
        for s in pending_servers:
            by_region.setdefault(s.region, []).append(s.id)

        selected: list[str] = []
        regions = list(by_region.keys())
        region_idx = 0

        while len(selected) < count and any(by_region.values()):
            region = regions[region_idx % len(regions)]
            if by_region[region]:
                selected.append(by_region[region].pop(0))
            region_idx += 1

            # Remove exhausted regions
            regions = [r for r in regions if by_region.get(r)]
            if not regions:
                break

        return selected[:count]

    # ------------------------------------------------------------------
    # Health check integration
    # ------------------------------------------------------------------

    def _run_health_check(
        self,
        config: DeploymentConfig,
        stage_idx: int,
        target_pct: int,
    ) -> bool:
        """Run the health-check function if configured.

        Returns ``True`` if no health-check is configured or if the
        check passes.
        """
        if config.health_check_fn is None:
            logger.debug(
                "  No health check configured — assuming stage %d passed",
                stage_idx,
            )
            return True

        logger.info("  Running health check for stage %d (%d%%)...", stage_idx, target_pct)
        try:
            result = config.health_check_fn(self._cluster)
            status = "PASSED" if result else "FAILED"
            logger.info("  Health check %s for stage %d", status, stage_idx)
            return bool(result)
        except Exception as exc:
            logger.error(
                "  Health check raised exception: %s — treating as failure",
                exc,
            )
            return False

    # ------------------------------------------------------------------
    # Timing / inter-stage wait
    # ------------------------------------------------------------------

    def _wait_between_stages(
        self,
        config: DeploymentConfig,
        deployment: DeploymentState,
    ) -> bool:
        """Wait between stages, checking for abort periodically.

        Returns ``True`` if the wait completed normally, ``False`` if
        an abort signal was received.
        """
        delay = config.stage_delay_seconds
        if delay <= 0:
            return True

        logger.info(
            "  Waiting %.1fs before next stage (progress: %.1f%%)...",
            delay,
            deployment.progress_percentage,
        )

        if config.abort_event is not None:
            # Use the event's wait() — returns True if the event is set
            aborted = config.abort_event.wait(timeout=delay)
            return not aborted
        else:
            # Simple sleep, but in small increments to stay responsive
            elapsed = 0.0
            increment = min(config.health_check_interval, delay)
            while elapsed < delay:
                time.sleep(increment)
                elapsed += increment
            return True

    # ------------------------------------------------------------------
    # Abort handling
    # ------------------------------------------------------------------

    def _is_aborted(self, config: DeploymentConfig) -> bool:
        """Check whether the abort event has been signalled."""
        if config.abort_event is not None:
            return config.abort_event.is_set()
        return False

    def _handle_abort(
        self, deployment: DeploymentState, reason: str
    ) -> None:
        """Handle an abort signal: log, mark state, trigger rollback."""
        logger.warning("DEPLOYMENT ABORTED: %s", reason)
        deployment.mark_aborted(reason)
        self._rollback_updated_servers(deployment)

    # ------------------------------------------------------------------
    # Rollback
    # ------------------------------------------------------------------

    def _handle_rollback(
        self, deployment: DeploymentState, reason: str
    ) -> None:
        """Initiate a rollback of all servers updated so far."""
        logger.warning("INITIATING ROLLBACK: %s", reason)
        deployment.mark_rolling_back()
        deployment.error_message = reason
        self._rollback_updated_servers(deployment)
        deployment.mark_rolled_back()
        logger.info(
            "ROLLBACK COMPLETE: %d servers reverted to %s",
            len(deployment.servers_updated),
            deployment.source_version,
        )

    def _rollback_updated_servers(self, deployment: DeploymentState) -> None:
        """Revert all servers that were updated during this deployment."""
        for server_id in list(deployment.servers_updated):
            success = self._cluster.rollback_server(server_id)
            if success:
                logger.info("  Rolled back server %s", server_id)
            else:
                logger.error(
                    "  Failed to rollback server %s", server_id
                )

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _init_deployment(self, config: DeploymentConfig) -> DeploymentState:
        """Create and initialise a new DeploymentState."""
        # Determine source version from the majority of servers
        summary = self._cluster.get_deployment_summary()
        versions = summary["versions"]
        source_version = max(versions, key=lambda v: versions[v])

        # Get all server IDs
        all_server_ids = {s.id for s in self._cluster.servers}

        deployment = DeploymentState(
            deployment_id=str(uuid.uuid4())[:8],
            target_version=config.target_version,
            source_version=source_version,
            total_servers=self._cluster.size,
            servers_pending=set(all_server_ids),
        )

        return deployment
