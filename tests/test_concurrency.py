"""
Concurrency stress and torture tests for the Canary Deployment Simulator.
Verifies thread-safety, lock correctness, and race-condition safety.
"""

from __future__ import annotations

import threading
import time

import pytest

from cluster.generator import generate_cluster
from cluster.state import ClusterState
from deploy.audit import AuditLogger, DeploymentEvent, DeploymentEventType
from deploy.config import DeploymentConfig
from deploy.engine import DeploymentEngine
from deploy.state import DeploymentStatus
from governance import GovernanceCoordinator


class TestConcurrencyStress:
    """Stress tests asserting system stability under concurrent loads."""

    @pytest.fixture
    def cluster_state(self) -> ClusterState:
        return ClusterState(generate_cluster(size=20, seed=42))

    def test_rapid_audit_log_burst(self) -> None:
        """Stress log method of AuditLogger with rapid parallel event streams."""
        logger = AuditLogger()
        num_threads = 20
        events_per_thread = 100
        threads = []

        def worker(thread_idx: int) -> None:
            for i in range(events_per_thread):
                evt = DeploymentEvent(
                    event_type=DeploymentEventType.HEALTH_CHECK,
                    deployment_id=f"dep-{thread_idx}",
                    details={"index": i, "thread": thread_idx},
                )
                logger.log(evt)

        for idx in range(num_threads):
            t = threading.Thread(target=worker, args=(idx,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        all_events = logger.get_events()
        assert len(all_events) == num_threads * events_per_thread

    def test_simultaneous_rollback_triggers(self, cluster_state: ClusterState) -> None:
        """Trigger concurrent rollbacks on the same engine to ensure thread safety."""
        engine = DeploymentEngine(cluster_state)
        config = DeploymentConfig(target_version="2.0.0", stages=[100], stage_delay_seconds=0.0)
        dep_state = engine._init_deployment(config)
        engine._current_deployment = dep_state
        engine._current_config = config

        # Mark some nodes updated
        servers = cluster_state.servers
        for s in servers[:5]:
            cluster_state.update_server_version(s.id, "2.0.0")
            dep_state.servers_updated.add(s.id)

        num_threads = 10
        errors = []

        def trigger_rollback() -> None:
            try:
                # Concurrent rollback calls
                engine.rollback(dep_state, force=True)
            except Exception as e:
                errors.append(e)

        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=trigger_rollback)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Should complete without crashing (no deadlocks or unhandled concurrency failures)
        assert len(errors) == 0
        # All servers should be rolled back to version 1.0.0
        for s in cluster_state.servers[:5]:
            assert s.current_version == "1.0.0"

    def test_repeated_abort_spam(self, cluster_state: ClusterState) -> None:
        """Spam abort_event trigger concurrently while a deployment is running."""
        engine = DeploymentEngine(cluster_state)
        abort_event = threading.Event()

        # Simple slow health check to ensure deployment is in progress
        def slow_health(cs: ClusterState) -> bool:
            time.sleep(0.05)
            return True

        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[20, 50, 100],
            stage_delay_seconds=0.2,
            abort_event=abort_event,
            health_check_fn=slow_health,
        )

        def deploy_worker() -> None:
            engine.deploy(config)

        t_deploy = threading.Thread(target=deploy_worker)
        t_deploy.start()

        # Spam set and clear abort event concurrently
        def spam_abort() -> None:
            for _ in range(50):
                abort_event.set()
                time.sleep(0.005)
                abort_event.clear()
                time.sleep(0.005)

        t_spam = threading.Thread(target=spam_abort)
        t_spam.start()

        t_spam.join()
        abort_event.set()  # Make sure it's fully aborted
        t_deploy.join()

        # Deployment should gracefully enter aborted/failed state
        res = engine.current_deployment
        assert res is not None
        assert res.status in (DeploymentStatus.FAILED, DeploymentStatus.ABORTED)

    def test_governance_during_concurrent_changes(self, cluster_state: ClusterState) -> None:
        """Trigger random cluster state modifications while governance evaluation runs."""
        coordinator = GovernanceCoordinator()
        config = DeploymentConfig(
            target_version="2.0.0",
            stages=[50, 100],
            stage_delay_seconds=0.01,
            governance_coordinator=coordinator,
        )
        engine = DeploymentEngine(cluster_state)
        dep_state = engine._init_deployment(config)

        stop_threads = threading.Event()

        def cluster_mutator() -> None:
            # Randomly mutate server status to cause risk calculations to fluctuate
            servers = cluster_state.servers
            import random

            from cluster.models import ServerStatus

            while not stop_threads.is_set():
                s = random.choice(servers)
                status = random.choice(
                    [ServerStatus.HEALTHY, ServerStatus.DEGRADED, ServerStatus.FAILED]
                )
                cluster_state.update_server_status(s.id, status)
                time.sleep(0.002)

        t_mutator = threading.Thread(target=cluster_mutator)
        t_mutator.start()

        # Run several coordinator check runs
        try:
            for _ in range(20):
                coordinator.evaluate_start(cluster_state, dep_state)
                coordinator.evaluate_stage_start(cluster_state, dep_state, 0, 50)
                coordinator.evaluate_stage_complete(cluster_state, dep_state, 0, 50)
                time.sleep(0.005)
        finally:
            stop_threads.set()
            t_mutator.join()
