"""
Event Replay Engine for timeline reconstruction, causality tracing, and log verification.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Set, Tuple

from logging_config import get_logger

logger = get_logger(__name__)


class EventReplayEngine:
    """Replays structured deployment audit logs to verify causality, timelines, and logic."""

    def __init__(self) -> None:
        self.replayed_events: List[Dict[str, Any]] = []

    def load_audit_trail(self, filepath: str) -> List[Dict[str, Any]]:
        """Load audit trail events from a JSONL file."""
        events = []
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    stripped = line.strip()
                    if stripped:
                        events.append(json.loads(stripped))
        except Exception as exc:
            logger.error("Failed to load audit trail file %s: %s", filepath, exc)
        return events

    def reconstruct_timeline(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sort events deterministically by timestamp to reconstruct execution order."""
        # Standardise ISO timestamps for sorting
        return sorted(events, key=lambda e: e.get("timestamp", ""))

    def build_causality_graph(self, events: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Build a parent-to-child causality map of event UUIDs.

        Returns:
            A dictionary mapping parent_event_id -> list of child_event_ids.
        """
        graph: Dict[str, List[str]] = {}
        for ev in events:
            parent = ev.get("parent_event_id")
            event_id = ev.get("event_id")
            if event_id:
                if parent:
                    graph.setdefault(parent, []).append(event_id)
                else:
                    graph.setdefault("ROOT", []).append(event_id)
        return graph

    def verify_event_lineage(self, events: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        """Validate parent-child event linkages to detect trace gaps, missing parents, or cycles.

        Returns:
            A tuple of (is_valid, list_of_error_messages).
        """
        errors: List[str] = []
        known_ids: Set[str] = {e["event_id"] for e in events if "event_id" in e}

        for ev in events:
            event_id = ev.get("event_id")
            parent_id = ev.get("parent_event_id")

            if not event_id:
                errors.append("Malformed event: missing 'event_id'.")
                continue

            if parent_id and parent_id not in known_ids:
                errors.append(
                    f"Causality gap: Event {event_id} references missing parent {parent_id}."
                )

            if parent_id and parent_id == event_id:
                errors.append(f"Causality corruption: Event {event_id} is its own parent.")

        # Reconstruct path traversal to detect cycles
        graph = self.build_causality_graph(events)
        visited = set()
        path = set()

        def dfs(node: str) -> bool:
            if node in path:
                errors.append(f"Causality loop detected: cycle involving event '{node}'")
                return False
            if node in visited:
                return True

            path.add(node)
            for child in graph.get(node, []):
                if not dfs(child):
                    return False
            path.remove(node)
            visited.add(node)
            return True

        roots = graph.get("ROOT", [])
        for r in roots:
            dfs(r)

        return (len(errors) == 0, errors)

    def reconstruct_state_at_step(
        self, events: List[Dict[str, Any]], step_event_id: str
    ) -> Dict[str, Any]:
        """Reconstruct a virtual view of the cluster state at a specific step in the audit trail.

        Iterates sequentially over events up to `step_event_id` and aggregates transitions.
        """
        timeline = self.reconstruct_timeline(events)

        # Virtual model
        virtual_servers: Dict[str, Dict[str, Any]] = {}
        deployment_status = "pending"
        target_version = None
        source_version = None

        for ev in timeline:
            evt_type = ev.get("event_type")
            details = ev.get("details", {})

            if evt_type == "deployment_start":
                target_version = details.get("target_version")
                source_version = details.get("source_version")
                deployment_status = "in_progress"

            elif evt_type == "stage_transition":
                servers_updated = details.get("servers_updated", [])
                for s_id in servers_updated:
                    virtual_servers[s_id] = {"version": target_version, "status": "healthy"}

            elif evt_type == "health_check":
                # If health check failed in details, update virtual server status
                if details.get("status") == "fail":
                    # Mark updated servers as degraded for simulated failures
                    for s_id, s_data in virtual_servers.items():
                        if s_data["version"] == target_version:
                            s_data["status"] = "degraded"

            elif evt_type == "rollback_complete":
                # Rollback reverted servers
                servers_reverted = details.get("servers_rolled_back", [])
                for s_id in servers_reverted:
                    if s_id in virtual_servers:
                        virtual_servers[s_id] = {"version": source_version, "status": "healthy"}
                deployment_status = "rolled_back"

            elif evt_type == "deployment_completed":
                deployment_status = "completed"

            elif evt_type == "rollback_initiated":
                deployment_status = "rolling_back"

            elif evt_type == "deployment_failed":
                deployment_status = "failed"

            if ev.get("event_id") == step_event_id:
                break

        return {
            "deployment_status": deployment_status,
            "target_version": target_version,
            "source_version": source_version,
            "servers": virtual_servers,
        }
