"""
Canary Deployment Simulator — Entry Point

Initialises the logging system, generates a simulated cluster,
displays the initial cluster status report, and runs a staged
canary deployment.
"""

from logging_config import get_logger
from cluster import generate_cluster, ClusterState, inspect_cluster
from deploy import DeploymentEngine, DeploymentConfig

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
    logger.info("Phase 3: Starting canary deployment ...")

    config = DeploymentConfig(
        target_version="2.0.0",
        stages=[10, 25, 50, 75, 100],
        stage_delay_seconds=2.0,   # Shortened for demo (production: 30-300s)
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
    # Future phases (to be implemented in subsequent sections):
    #   Phase 4: Health analysis
    #   Phase 5: Rollback (if needed)
    #   Phase 6: Async abort listener
    # ------------------------------------------------------------------
    logger.info("Simulation entry-point complete. Further phases pending.")


if __name__ == "__main__":
    main()
