# FORGE

A systems-oriented distributed build and task execution engine.

FORGE accepts a dependency-aware workload, schedules executable tasks, coordinates workers, persists execution state, and records deterministic results. The project is designed as an infrastructure system rather than a dashboard application.

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
   / | \
  v  v  v
 W1 W2 W3
  \  |  /
   \ | /
    Results
      |
      v
 Artifact / Cache Store
```

## Core Components

- **Job graph** — dependency-aware task representation.
- **Scheduler** — selects runnable work and tracks execution state.
- **Workers** — execute isolated tasks and report results.
- **Persistence** — preserves job and task state across process restarts.
- **Artifact store** — records build outputs and reusable results.
- **Cache** — avoids repeating deterministic work.
- **Transport** — worker/client communication over a defined network protocol.
- **Fault handling** — retries, timeouts, and worker failure detection.
- **Observability** — structured logs, metrics, and execution statistics.
- **CLI** — submit workloads and inspect execution results without requiring a web UI.

## Engineering Goals

FORGE is intentionally built from the execution layer upward. The implementation prioritizes correctness, concurrency, deterministic behavior, failure handling, and measurable performance.

The initial implementation will establish a complete single-process execution engine before distributed worker coordination is introduced. Each layer is independently testable so the final system is not dependent on unfinished infrastructure.

## Planned Repository Structure

```text
forge/
├── crates/
│   ├── forge-core/       # Task graph, scheduler, state machine
│   ├── forge-worker/     # Worker execution runtime
│   ├── forge-protocol/   # Client/worker wire protocol
│   ├── forge-store/      # Persistence and artifact storage
│   └── forge-cli/        # Command-line interface
├── tests/                # Integration and fault tests
├── benches/              # Performance benchmarks
├── docs/                 # Architecture and protocol notes
└── README.md
```

## Status

The repository has been initialized for implementation. Source modules will be added incrementally with tests and validation at each layer.

## License

MIT
