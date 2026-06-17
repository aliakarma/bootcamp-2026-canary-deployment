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
- **Comprehensive Logging** — Timestamped, dual-output (console + file) audit trail

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      main.py                            │
│              (Orchestration Entry-Point)                │
├──────────┬──────────┬──────────┬──────────┬─────────────┤
│ cluster/ │ deploy/  │ health/  │rollback/ │   abort/    │
│          │          │          │          │             │
│ models   │ engine   │ analyze  │ restore  │ listener    │
│ generator│ stages   │ metrics  │ history  │ interrupt   │
│ state    │ tracking │ failures │ validate │ sync        │
│ inspector│          │          │          │             │
└──────────┴──────────┴──────────┴──────────┴─────────────┘
```

## Setup

### Prerequisites

- Python 3.10+

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/bootcamp-2026-canary-deployment.git
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

```bash
python main.py
```

## Running Tests

```bash
pytest tests/ -v
```

## Project Structure

```
├── main.py                 # Entry-point and orchestration
├── logging_config.py       # Centralised logging setup
├── requirements.txt        # Project dependencies
├── cluster/                # Cluster simulation module
│   ├── models.py           # Server data model
│   ├── generator.py        # Cluster generation
│   ├── state.py            # State tracking & version management
│   └── inspector.py        # Cluster inspection utility
├── tests/                  # Unit tests
│   └── test_cluster.py     # Cluster module tests
└── logs/                   # Runtime log output
```

## License

This project is part of the 2026 Bootcamp program.
