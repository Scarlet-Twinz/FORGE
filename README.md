#  FORGE

**Distributed build and task execution engine in Rust.**

FORGE is a dependency-aware execution system for workloads that need more than a single process. It models tasks as a DAG, schedules runnable work, coordinates workers over TCP, persists execution state, handles failures and timeouts, and provides artifact, cache, journal, and metrics primitives.

## Execution Model

```text
Client / CLI
     │
     ▼
 Task Graph
     │
     ▼
Coordinator
  │      │
  ▼      ▼
Worker A Worker B
  │      │
  └──┬───┘
     ▼
Execution / Results
     │
 ┌───┼──────────┐
 ▼   ▼          ▼
Store Cache   Journal
 │     │          │
 └─────┴────┬─────┘
             ▼
          Metrics
```

## Scheduling

- Dependency-aware DAG execution
- Explicit task lifecycle states
- Deterministic task ordering
- Runnable-task scheduling with `VecDeque`
- Blocked-dependency propagation
- Cycle detection

## Distributed Workers

Workers communicate with the coordinator over a versioned binary TCP protocol.

- Worker registration
- Heartbeat/liveness probing
- Stale-worker detection
- Healthy-worker selection
- Round-robin distribution
- Configurable retries
- Communication failure handling
- Task-command failure handling without unnecessarily ejecting a healthy worker

The distinction between **worker failure** and **task failure** is intentional: a command returning a failure does not automatically mean the worker itself is unhealthy.

## Worker Runtime

- Child-process execution
- Shared concurrency limits
- Per-task execution timeouts
- Process termination on timeout
- Explicit task-result reporting
- Concurrent TCP connections
- Windows process-tree termination for timed-out child processes

## Wire Protocol

```text
+--------+---------+------+----------------+
| MAGIC  | VERSION | KIND | PAYLOAD LENGTH |
+--------+---------+------+----------------+
                         │
                         ▼
                      PAYLOAD
```

Frames validate magic bytes, protocol version, message kind, payload length, UTF-8 data, and exact payload consumption.

## Persistence, Artifacts & Cache

FORGE separates execution state from artifact storage.

- Atomic file-backed job records
- Reopen/recovery of persisted job state
- Content-addressed artifacts using SHA-256
- Atomic artifact writes
- Deduplication
- Persistent cache-key → artifact-hash index
- Deterministic cache ordering
- Append-only execution journal
- `sync_data()` durability before journal append completion
- Journal replay with record validation

### Cache boundary

The current cache key is derived from the task command and configured timeout. A cache hit can avoid contacting a worker when its referenced artifact remains available.

The current cache artifact is a **cache stamp** derived from the task command; it does not yet restore the original stdout/stderr payload. That limitation is intentional and documented rather than hidden behind a broader claim of build-cache completeness.

### Journal boundary

The execution journal records task lifecycle boundaries and can replay validated records after reopening. It is a durability primitive, **not a complete crash-recovery state machine**.

## Metrics

The metrics layer records execution count, outcomes, execution duration, configured workers, and latest healthy-worker count, and renders deterministic Prometheus-style text.

It is a metrics layer, not a Prometheus server.

## Workspace Structure

```text
FORGE/
├── crates/
│   ├── forge-core/        # graph, scheduler, task states
│   ├── forge-worker/      # task execution and TCP worker
│   ├── forge-protocol/    # coordinator/worker protocol
│   ├── forge-store/       # state, artifacts, cache, journal
│   ├── forge-cli/         # local execution CLI
│   ├── forge-coordinator/ # distributed execution and health
│   ├── forge-cache/       # cache-aware execution
│   └── forge-metrics/     # metrics and text rendering
├── Cargo.toml
└── README.md
```

## Local Development

Prerequisite: Rust/Cargo.

```bash
git clone https://github.com/Scarlet-Twinz/FORGE.git
cd FORGE
cargo run -p forge-cli
cargo test --workspace
```

The CLI currently executes a small dependency chain locally and persists job state under the system temporary directory.

## Engineering Focus

FORGE explores the boundaries involved in distributed execution:

- DAG scheduling;
- deterministic execution order;
- worker coordination and liveness;
- versioned network protocols;
- timeout propagation;
- task/worker failure classification;
- durable state and journaling;
- content-addressed artifacts;
- cache identity; and
- execution metrics.

## Current State

**Core execution path implemented and tested.**

Implemented: task graph/scheduling, distributed worker execution, worker health/heartbeats, retries, task timeouts, binary TCP protocol, persistent job state, content-addressed artifacts, cache indexing, execution journaling, metrics, and local CLI execution.

Separate future layers include richer CLI commands, benchmark/fault-injection suites, full crash recovery, and restoration of actual task output from the cache.

## License

MIT

## Author

**Anthony Emmanuella Mmasinachi**

Full-stack and systems engineer focused on distributed systems, backend infrastructure, networking, databases, AI integration, and systems programming.

## Project Links

- **Repository:** https://github.com/Scarlet-Twinz/FORGE
- **Author:** Anthony Emmanuella Mmasinachi
- **GitHub:** https://github.com/Scarlet-Twinz
