# FORGE

A systems-oriented distributed build and task execution engine.

FORGE models dependency-aware workloads, schedules executable tasks, coordinates workers over a framed TCP protocol, persists execution state, and records deterministic results. The project is designed as infrastructure rather than a conventional web application.

## Architecture

```text
Client / CLI
     |
     v
 Job Definition
     |
     v
  DAG Builder
     |
     v
 Distributed Scheduler
     |
     +-------------------+
     |                   |
     v                   v
Worker A              Worker B
     |                   |
     +---------+---------+
               |
               v
         Results / Store
               |
               v
        Artifact / Cache
```

## Current Implementation

- **Task graph** — dependency-aware tasks with deterministic ordering and blocked-dependency propagation.
- **Local scheduler** — deterministic runnable-task dispatch.
- **Distributed executor** — executes runnable DAG tasks through remote workers and advances the graph only after task results are received.
- **Worker health registry** — probes configured workers with heartbeats, records worker identity and last-seen time, expires stale workers, and prevents unhealthy workers from receiving scheduled work.
- **Retry policy** — configurable maximum task attempts for failed commands, task timeouts, or worker communication failures.
- **Task timeouts** — coordinator-configured execution deadlines are carried over the wire to workers and enforced at the child-process boundary.
- **Timeout cancellation** — a worker terminates an over-deadline child process, returns an explicit timed-out result, releases its concurrency slot, and keeps the worker itself healthy.
- **Worker runtime** — executes tasks as isolated child processes with a shared concurrency limit across worker connection threads.
- **Wire protocol** — versioned binary frames with structured task requests, task results, timeout metadata, and heartbeats.
- **TCP worker service** — workers accept concurrent task connections over localhost/network TCP.
- **Coordinator client** — submits tasks to remote workers, validates returned results, and probes worker heartbeats.
- **Persistence** — atomic file-backed job records that survive process reopen.
- **CLI** — local execution path plus a coordinator executable for remote worker execution.

## Engineering Direction

The system is built from the execution layer upward. Correctness, deterministic scheduling, explicit state transitions, protocol validation, process isolation, concurrency control, and failure handling take priority over presentation.

The distributed layer is intentionally built in validated increments. The current implementation has a real coordinator-to-worker TCP path, heartbeat-based worker health, concurrent worker connections, dependency-aware distributed execution, configurable retries, and timeout-triggered process termination. Artifact caching, durable event/WAL semantics, richer CLI commands, explicit user cancellation messages, metrics, and benchmarks remain separate engineering layers and will be added only when implemented and tested.

## Repository Structure

```text
forge/
├── crates/
│   ├── forge-core/        # Task graph, scheduler, state machine
│   ├── forge-worker/      # Worker execution runtime + TCP service
│   ├── forge-protocol/    # Versioned client/worker wire protocol
│   ├── forge-store/       # Persistent job state
│   ├── forge-coordinator/ # Distributed worker client + DAG executor
│   └── forge-cli/         # Local execution CLI
├── tests/                 # Integration and fault tests
├── benches/               # Performance benchmarks
├── docs/                  # Architecture and protocol notes
└── README.md
```

## Status

The execution core, local scheduling, persistence foundation, binary protocol, concurrent TCP workers, heartbeat-based worker health, distributed DAG execution, retry foundation, and timeout-triggered process cancellation are implemented. The project is still under active systems-engineering development and is intentionally focused on infrastructure depth rather than a web frontend.

## License

MIT
