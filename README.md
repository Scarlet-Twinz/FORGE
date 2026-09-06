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
- **Artifact store** — content-addressed binary artifacts keyed by SHA-256, stored atomically and deduplicated by content.
- **Cache index** — persistent task/cache-key to artifact-hash mappings with deterministic on-disk ordering and validation of stored hashes.
- **Execution cache** — cache-aware distributed execution can satisfy previously successful tasks without contacting a worker when the command and timeout identity match and the backing artifact still exists.
- **CLI** — local execution path plus a coordinator executable for remote worker execution.

## Artifact and Cache Model

FORGE separates immutable artifact data from cache metadata:

```text
Task identity
     |
     v
   SHA-256
     |
     v
 CacheStore ──────> task-key -> artifact-hash
                           |
                           v
                    ArtifactStore
                           |
                           v
                    objects/<hash>
```

A cache entry is valid only while its referenced content-addressed object exists. Cache identity currently includes the command and configured task timeout, giving the execution cache a deterministic versioned key. The cache layer is deliberately separate from the core scheduler so storage policy can evolve without coupling the task graph to filesystem details.

## Engineering Direction

The system is built from the execution layer upward. Correctness, deterministic scheduling, explicit state transitions, protocol validation, process isolation, concurrency control, and failure handling take priority over presentation.

The distributed layer is intentionally built in validated increments. The current implementation has a real coordinator-to-worker TCP path, heartbeat-based worker health, concurrent worker connections, dependency-aware distributed execution, configurable retries, timeout-triggered process termination, content-addressed artifact storage, a persistent cache index, and cache-aware distributed execution. Durable event/WAL semantics, richer CLI commands, explicit user cancellation messages, metrics, benchmarks, and fault-injection scenarios remain separate engineering layers and will be added only when implemented and tested.

## Repository Structure

```text
forge/
├── crates/
│   ├── forge-core/        # Task graph, scheduler, state machine
│   ├── forge-worker/      # Worker execution runtime + TCP service
│   ├── forge-protocol/    # Versioned client/worker wire protocol
│   ├── forge-store/       # Persistent state, artifacts, and cache index
│   ├── forge-coordinator/ # Distributed worker client + DAG executor
│   ├── forge-cache/       # Cache-aware distributed execution
│   └── forge-cli/         # Local execution CLI
├── tests/                 # Integration and fault tests
├── benches/               # Performance benchmarks
├── docs/                  # Architecture and protocol notes
└── README.md
```

## Status

The execution core, local scheduling, persistence foundation, binary protocol, concurrent TCP workers, heartbeat-based worker health, distributed DAG execution, retry foundation, timeout-triggered process cancellation, content-addressed artifact storage, persistent cache index, and cache-aware distributed execution are implemented. The project is still under active systems-engineering development and is intentionally focused on infrastructure depth rather than a web frontend.

## License

MIT
