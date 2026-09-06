# FORGE

**Distributed build and task execution engine.**

FORGE is a Rust-based execution system for dependency-aware workloads. It models tasks as a DAG, schedules runnable work, coordinates workers over TCP, persists execution state, handles worker and task failures, and provides artifact, cache, journal, and metrics primitives.

## Architecture

```text
Client / CLI
     |
     v
 Task Graph
     |
     v
 Coordinator
     |
     +-------------------+
     |                   |
     v                   v
Worker A              Worker B
     |                   |
     +---------+---------+
               |
               v
        Store / Artifacts
               |
          +----+----+
          |         |
          v         v
        Cache     Journal
          |         |
          +----+----+
               |
               v
            Metrics
```

## Features

### Task graph and scheduling

- Dependency-aware task graph
- Explicit task lifecycle states
- Deterministic task ordering
- Runnable-task scheduling with `VecDeque`
- Blocked-dependency propagation
- Cycle detection in the graph implementation

### Distributed execution

- Coordinator-to-worker execution over TCP
- Dependency-aware distributed DAG execution
- Worker registration and heartbeat probing
- Stale-worker detection
- Round-robin selection across healthy workers
- Configurable task retry attempts
- Worker communication failure handling
- Command failure handling without unnecessarily ejecting a healthy worker

### Worker runtime

- Child-process task execution
- Shared concurrency limits across connection threads
- Per-task execution timeouts
- Timeout cancellation at the process boundary
- Explicit task-result reporting with exit code, stdout, stderr, and timeout state
- Concurrent TCP connections

### Wire protocol

FORGE uses a versioned binary framing protocol between the coordinator and workers.

```text
+--------+---------+------+----------------+
| MAGIC  | VERSION | KIND | PAYLOAD LENGTH |
+--------+---------+------+----------------+
                         |
                         v
                      PAYLOAD
```

The protocol validates magic bytes, protocol versions, message kinds, payload sizes, UTF-8 data, and exact payload consumption. Task requests carry timeout metadata; task results carry execution outcome and process output; heartbeat messages identify workers and provide liveness information.

### Persistence and storage

- Atomic file-backed job records
- Reopen and recover persisted job state
- Content-addressed binary artifact storage
- SHA-256 artifact identities
- Atomic artifact writes and content deduplication
- Persistent cache-key to artifact-hash index
- Deterministic cache index ordering
- Append-only execution journal with fsynced records
- Journal replay with record validation

### Cache

The cache layer derives a deterministic key from the task command and configured timeout, then maps that key to a content-addressed artifact.

```text
Task command + timeout
          |
          v
       SHA-256
          |
          v
      CacheStore
          |
          v
     artifact hash
          |
          v
    ArtifactStore
```

A cache hit can satisfy a previously successful task without contacting a worker when the cache entry and referenced artifact are still present. The current cache artifact is a cache stamp derived from the task command; it does not yet restore the task's original stdout/stderr.

### Execution journal

The journal provides an append-only durability primitive for task lifecycle boundaries.

```text
TaskStarted
     |
     v
execution
     |
     +------> TaskSucceeded
     |
     +------> TaskFailed
```

Journal appends use `sync_data()` before returning, and records can be replayed after reopening the journal. The journaled coordinator records lifecycle boundaries around distributed execution. It is not a complete crash-recovery state machine.

### Metrics

The metrics layer records execution count, task outcomes, execution duration, configured workers, and the latest healthy-worker count. It renders deterministic Prometheus-style text for scraping or diagnostics.

It is a metrics layer, not a Prometheus server.

## Repository Structure

```text
FORGE/
├── crates/
│   ├── forge-core/        # Task graph, scheduler, task states
│   ├── forge-worker/      # Task execution runtime and TCP worker
│   ├── forge-protocol/    # Binary coordinator/worker protocol
│   ├── forge-store/       # Job state, artifacts, cache index, journal
│   ├── forge-cli/         # Local execution CLI
│   ├── forge-coordinator/ # Distributed execution and worker health
│   ├── forge-cache/       # Cache-aware distributed execution
│   └── forge-metrics/     # Execution metrics and text rendering
├── Cargo.toml
└── README.md
```

## Getting Started

### Prerequisites

- Rust toolchain with Cargo

Verify the installation:

```bash
rustc --version
cargo --version
```

### Clone

```bash
git clone https://github.com/Scarlet-Twinz/FORGE.git
cd FORGE
```

### Run the local CLI

```bash
cargo run -p forge-cli
```

The current CLI executes a small dependency chain locally and persists job state under the system temporary directory.

### Run the tests

```bash
cargo test --workspace
```

The workspace includes unit and integration-oriented coverage across task scheduling, protocol encoding/decoding, worker execution, timeout handling, worker health, retries, persistence, artifact storage, caching, journaling, and metrics.

## Engineering Notes

FORGE is structured as a Cargo workspace so the execution model, worker runtime, protocol, persistence layer, coordinator, cache, metrics, and CLI can evolve independently.

The implementation emphasizes deterministic behavior and explicit failure handling. Worker communication failures are treated differently from command failures: communication failures can make a worker unhealthy, while a normal task failure does not automatically remove a responsive worker from the registry.

Timeouts are carried from the coordinator through the wire protocol to the worker, where the child process is terminated when the execution deadline is reached. On Windows, process-tree termination is used so child processes do not remain attached to the worker's pipes.

## Current State

The core execution path is implemented and tested, including:

- task graph and scheduling;
- distributed worker execution;
- worker health and heartbeats;
- retries and task timeouts;
- binary TCP protocol;
- persistent job state;
- content-addressed artifacts;
- persistent cache indexing;
- execution journaling;
- execution metrics;
- local CLI execution.

The remaining larger engineering layers are separate from the current implementation: richer CLI commands, benchmark suites, fault-injection scenarios, full crash recovery, and caching/restoration of actual task output.

## Author

**Anthony Emmanuella Mmasinachi**

Software developer focused on systems engineering, backend infrastructure, APIs, distributed systems, databases, automation, and practical software architecture.

**GitHub:** https://github.com/Scarlet-Twinz

## License

MIT
