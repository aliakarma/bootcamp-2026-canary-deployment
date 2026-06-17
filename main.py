"""
Canary Deployment Simulator — Entry Point

Initialises the logging system, generates a simulated cluster,
displays the initial cluster status report, and runs a staged
canary deployment.
"""

from logging_config import get_logger
from cluster import generate_cluster, ClusterState, inspect_cluster
from deploy import (
    DeploymentEngine,
    DeploymentConfig,
    save_deployment_state,
    load_deployment_state,
    validate_rollback_consistency,
    RollbackConsistencyError,
)
from health import HealthThresholds, create_health_check_fn, inject_failures

logger = get_logger(__name__)


def main() -> None:
    """Run the canary deployment simulation."""
    logger.info("=" * 60)
    logger.info("  CANARY DEPLOYMENT SIMULATOR")
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Phase 1: Generate cluster
    # ------------------------------------------------------------------
    logger.info("Phase 1: Generating simulated cluster ...")
    servers = generate_cluster()
    state = ClusterState(servers)

    # ------------------------------------------------------------------
    # Phase 2: Inspect initial state
    # ------------------------------------------------------------------
    logger.info("Phase 2: Inspecting initial cluster state ...")
    inspect_cluster(state)

    # ------------------------------------------------------------------
    # Phase 3: Staged canary deployment
    # ------------------------------------------------------------------
    logger.info("Phase 3: Starting healthy canary deployment with health check hook ...")

    # Define standard health thresholds
    thresholds = HealthThresholds()
    health_check = create_health_check_fn(thresholds)

    config = DeploymentConfig(
        target_version="2.0.0",
        stages=[10, 25, 50, 75, 100],
        stage_delay_seconds=1.0,   # Shortened for demo
        health_check_fn=health_check,
    )

    engine = DeploymentEngine(state)
    result = engine.deploy(config)

    # ------------------------------------------------------------------
    # Phase 3b: Inspect post-deployment state
    # ------------------------------------------------------------------
    logger.info("Post-deployment cluster state:")
    inspect_cluster(state)

    logger.info(
        "Deployment result: %s (%.1fs, %d/%d servers)",
        result.status.value,
        result.duration_seconds,
        len(result.servers_updated),
        result.total_servers,
    )

    # ------------------------------------------------------------------
    # Phase 4: Staged canary deployment with Failure Injection
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("Phase 4: Running failure injection and rollback demonstration")
    logger.info("=" * 60)

    # Generate a fresh cluster for the failure scenario
    logger.info("Generating a fresh cluster state ...")
    failing_servers = generate_cluster(size=20, seed=42)
    failing_state = ClusterState(failing_servers)

    logger.info("Initial status of fresh cluster:")
    inspect_cluster(failing_state)

    # Configure zero degraded servers threshold to abort immediately on any failure
    strict_thresholds = HealthThresholds(max_degraded_server_percentage=0.0)
    base_health_fn = create_health_check_fn(strict_thresholds)

    call_count = 0

    def failure_injection_hook(cs: ClusterState) -> bool:
        nonlocal call_count
        # Inject failures right after Stage 1 completes (25% progress)
        if call_count == 1:
            logger.warning("!!! CHAOS INJECTION: Spiking latency & degrading target-version servers !!!")
            inject_failures(
                cs,
                target_version="2.0.0",
                failure_rate=0.4,
                failure_type="degrade",
                seed=42,
            )
        call_count += 1
        return base_health_fn(cs)

    failing_config = DeploymentConfig(
        target_version="2.0.0",
        stages=[10, 25, 50, 75, 100],
        stage_delay_seconds=1.0,
        health_check_interval=0.5,
        health_check_fn=failure_injection_hook,
        max_retries_per_stage=0,  # Fail immediately to trigger rollback
    )

    logger.info("Starting deployment with failure hook active ...")
    failing_engine = DeploymentEngine(failing_state)
    failing_result = failing_engine.deploy(failing_config)

    # Inspect final state to show everything is rolled back to v1.0.0
    logger.info("Final cluster state after failure detection & automatic rollback:")
    inspect_cluster(failing_state)

    logger.info(
        "Failing deployment result: %s (%.1fs, %d/%d servers updated, final error: %s)",
        failing_result.status.value,
        failing_result.duration_seconds,
        len(failing_result.servers_updated),
        failing_result.total_servers,
        failing_result.error_message,
    )

    # ------------------------------------------------------------------
    # Phase 5: Manual Rollback System Demonstration
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("Phase 5: Running manual rollback system demonstration")
    logger.info("=" * 60)

    # 1. Store/Save deployment state of the healthy Phase 3 run
    state_file = "logs/phase3_deployment_state.json"
    logger.info("Saving healthy deployment state to disk: %s", state_file)
    save_deployment_state(result, state_file)

    # 2. Restore/Load the deployment state from disk
    logger.info("Restoring deployment state from disk ...")
    loaded_state = load_deployment_state(state_file)
    logger.info(
        "Successfully loaded deployment state. ID: %s, Target Version: %s, Updated Servers: %d",
        loaded_state.deployment_id,
        loaded_state.target_version,
        len(loaded_state.servers_updated),
    )

    # 3. Modify a server's version manually in the cluster to simulate a configuration drift/inconsistency
    drift_server_id = list(loaded_state.servers_updated)[0]
    drift_server = state.get_server(drift_server_id)
    if drift_server:
        logger.info("Simulating configuration drift by changing %s version manually to '2.1.0'", drift_server_id)
        # Directly modify current_version under state's lock to simulate drift
        with state._lock:
            drift_server.current_version = "2.1.0"

    # 4. Validate rollback consistency
    logger.info("Running consistency check on the cluster state ...")
    inconsistencies = validate_rollback_consistency(state, loaded_state)
    logger.warning("Inconsistencies detected: %s", inconsistencies)

    # 5. Attempt consistent rollback (should fail due to drift)
    logger.info("Attempting to run a consistent rollback (force=False) ...")
    try:
        engine.rollback(loaded_state, force=False)
    except RollbackConsistencyError as exc:
        logger.error("Rollback aborted! Consistency check failed: %s", exc)
        logger.error("Detailed server mismatches: %s", exc.errors)

    # 6. Run forced rollback (force=True) to override drift and successfully revert all updated nodes
    logger.info("Executing forced rollback (force=True) to revert all nodes ...")
    rolled_back_servers = engine.rollback(loaded_state, force=True)

    # 7. Print final cluster state after manual rollback
    logger.info("Final cluster state after manual rollback:")
    inspect_cluster(state)

    logger.info(
        "Manual rollback completed: reverted %d servers back to version %s",
        len(rolled_back_servers),
        loaded_state.source_version,
    )


if __name__ == "__main__":
    main()
