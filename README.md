# Canary Deployment Simulator

A production-grade simulation of a **canary deployment strategy** with staged rollouts, health monitoring, automated rollback, and async abort capabilities.

## Overview

This project simulates deploying a new software version across a cluster of servers using a canary deployment pattern. Instead of updating all servers at once, it rolls out changes in controlled stages — monitoring health metrics at each step and automatically rolling back if failures are detected.

### Key Features

- **Cluster Simulation** — Generates a realistic cluster of 20–50 servers across multiple regions
- **Staged Rollout Engine** — Percentage-based deployment with configurable timing
- **Health Analysis** — Simulated monitoring with configurable failure injection
- **Automated Rollback** — Reverts failed deployments to the previous stable version
- **Async Abort** — Real-time `ABORT` command listener with thread-safe interruption
- **Comprehensive Logging** — Centralized dual-output (console + file) runtime log trail
- **Structured Audit Logging** — Concurrency-safe event records streamed as JSON Lines (JSONL) to disk for operational traceability

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      main.py                            │
│              (Orchestration Entry-Point)                │
├───────────────────┬───────────────────┬─────────────────┤
│     cluster/      │      deploy/      │     health/     │
│                   │                   │                 │
│  • models.py      │  • engine.py      │  • analyzer.py  │
│  • generator.py   │  • config.py      │  • metrics.py   │
│  • state.py       │  • state.py       │  • thresholds.py│
│  • inspector.py   │  • rollback.py    │  • failure_     │
│                   │  • abort_         │    injection.py │
│                   │    listener.py    │                 │
│                   │  • audit.py       │                 │
└───────────────────┴───────────────────┴─────────────────┘
```

## Setup

### Prerequisites

- Python 3.10+

### Installation

```bash
# Clone the repository
git clone https://github.com/aliakarma/bootcamp-2026-canary-deployment.git
cd bootcamp-2026-canary-deployment

# Create and activate virtual environment
python -m venv venv

# Windows
.\venv\Scripts\activate

# macOS / Linux
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

To run the simulator, execute the main entry point:
```bash
python main.py
```

### Simulating a Manual Abort

During a deployment, the simulator performs staged updates with a short delay between stages. An asynchronous listener thread runs in the background monitoring standard input.

To trigger an emergency abort and rollback the current deployment:
1. Run the simulator (`python main.py`).
2. While the stages are rolling out (e.g., `Waiting 1.0s before next stage...`), type `abort` (case-insensitive) in the terminal and press **Enter**.
3. The background thread will intercept the input, signal the deployment's abort event, interrupt any current delay sleep, halt the rollout, and automatically rollback any updated servers back to their pre-deployment versions.

### Structured Event Log Auditing

When the simulator runs, it records operational lifecycle transitions as structured JSON Events. By default, these are appended to `logs/audit_trail.jsonl` (JSON Lines format).

Supported Event Types:
* `deployment_start` - Rollout initiation details (target, source, stages, total nodes).
* `stage_transition` - Progressive stage rollouts showing which server IDs were updated.
* `health_check` - Latency/error evaluations mapping check outcomes (`pass`/`fail`) and retries.
* `abort_received` - Trace of terminal emergency console signals.
* `rollback_start` - Reversion initialization logging node targets.
* `rollback_complete` - Rollback final states confirming reverted nodes.
* `deployment_completed` - Successful canary run summary.
* `deployment_failed` - Terminated canary run with details.

Example event output:
```json
{
  "timestamp": "2026-06-17T17:43:39.330392+00:00",
  "event_type": "stage_transition",
  "deployment_id": "432fb86f",
  "details": {
    "stage_index": 0,
    "target_percentage": 10,
    "servers_updated": ["server-003", "server-002", "server-004"]
  }
}
```

## Running Tests

To run the comprehensive suite of 134 automated unit tests:
```bash
pytest tests/ -v
```

## Project Structure

```
bootcamp-2026-canary-deployment/
├── main.py                 # Entry-point and orchestration
├── logging_config.py       # Centralised logging setup
├── requirements.txt        # Project dependencies
├── cluster/                # Cluster simulation module
│   ├── __init__.py         # Package exports
│   ├── models.py           # Server data model
│   ├── generator.py        # Cluster generation
│   ├── state.py            # State tracking & version management
│   └── inspector.py        # Cluster inspection utility
├── deploy/                 # Deployment coordinator & engine
│   ├── __init__.py         # Package exports
│   ├── config.py           # Deployment configuration settings
│   ├── engine.py           # Staged rollout execution
│   ├── rollback.py         # Rollback & serialization/deserialization logic
│   ├── state.py            # Rollout & stage state models
│   ├── abort_listener.py   # Asynchronous console abort listener
│   └── audit.py            # Structured event audit logging & repository
├── health/                 # Health check & monitoring module
│   ├── __init__.py         # Package exports
│   ├── analyzer.py         # Health evaluation engine
│   ├── failure_injection.py# Fault injection framework
│   ├── metrics.py          # System latency & error metric models
│   └── thresholds.py       # Pass/fail boundary thresholds
├── tests/                  # Test suites
│   ├── __init__.py         # Test setup
│   ├── test_cluster.py     # Cluster generator & state tests
│   ├── test_deploy.py      # Staged rollout engine tests
│   ├── test_health.py      # Health analyzer & thresholds tests
│   ├── test_rollback.py    # Rollback validation & recovery tests
│   ├── test_abort.py       # Async console abort listener tests
│   └── test_audit.py       # Structured audit event logging tests
└── logs/                   # Centralized log outputs (canary logs & audit trails)
```

## License

This project is part of the 2026 Bootcamp program.
