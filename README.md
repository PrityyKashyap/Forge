# FORGE

A distributed key-value database, built incrementally, from scratch, in Java.

FORGE is a learning-by-building project: every component (storage engine, WAL,
networking, partitioning, replication, failure detection) is implemented by hand
rather than pulled in as a library, so that each concept is actually understood,
not just wired together.

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) — system design, components, repo layout
- [DESIGN.md](docs/DESIGN.md) — implementation roadmap, phases, prerequisites
- [BENCHMARKS.md](docs/BENCHMARKS.md) — real, measured performance results (filled in as phases land)
- [PROGRESS.md](PROGRESS.md) — session-by-session log of what's done, what's next
- `tests/` — integration/system-level tests that span multiple modules

## Status

Phase 0 complete: Maven multi-module skeleton builds and tests pass. No
database functionality yet — Phase 1 not started.
See [PROGRESS.md](PROGRESS.md) for the current state.

## Building

Requires Java 21+ (JDK 25 recommended) and Maven.

```bash
mvn clean install
```

This builds all six modules (`forge-common`, `forge-storage`, `forge-server`,
`forge-client`, `forge-cluster`, `forge-bench`) and runs each module's tests.
