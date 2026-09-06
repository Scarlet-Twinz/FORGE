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
 Scheduler
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
- **Scheduler** — deterministic runnable-task dispatch.
- **Worker runtime** — executes tasks as isolated child processes.
- **Wire protocol** — versioned binary frames with structured task requests, task results, and heartbeats.
- **TCP worker service** — workers can receive task requests from a coordinator over localhost/network TCP.
- **Coordinator client** — submits a task to a remote worker and validates the returned result.
- **Persistence** — atomic file-backed job records that survive process reopen.
- **CLI** — local execution path plus a coordinator executable for remote worker execution.

## Engineering Direction

The system is built from the execution layer upward. Correctness, deterministic scheduling, explicit state transitions, protocol validation, process isolation, and failure handling take priority over presentation.

The distributed layer is being introduced incrementally: local execution is already exercised by the CLI, while worker/coordinator communication is now covered by a real TCP protocol path and an integration test. More advanced scheduling, retries, worker health tracking, artifact caching, and fault-injection tests will be added only when implemented and validated.

## Repository Structure

```text
forge/
├── crates/
│   ├── forge-core/        # Task graph, scheduler, state machine
│   ├── forge-worker/      # Worker execution runtime + TCP service
│   ├── forge-protocol/    # Versioned client/worker wire protocol
│   ├── forge-store/       # Persistent job state
│   ├── forge-coordinator/ # Remote worker client
│   └── forge-cli/         # Local execution CLI
├── tests/                 # Integration and fault tests
├── benches/               # Performance benchmarks
├── docs/                  # Architecture and protocol notes
└── README.md
```

## Status

Core scheduling, local execution, persistence, and the first coordinator-to-worker TCP execution path are implemented. The project remains intentionally focused on infrastructure depth rather than a web frontend.

## License

MIT
