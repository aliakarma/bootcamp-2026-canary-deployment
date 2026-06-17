"""
Canary Deployment Simulator — Entry Point

Initialises the logging system, generates a simulated cluster,
displays the initial cluster status report, and runs a staged
canary deployment.
"""

from logging_config import get_logger
from cluster import generate_cluster, ClusterState, inspect_cluster
from deploy import DeploymentEngine, DeploymentConfig
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


if __name__ == "__main__":
    main()
