# kronos

A resource-constrained distributed job scheduler in Go + gRPC.

Workers register their capabilities. Jobs declare their requirements. Kronos matches them.

## Services

- **scheduler** — accepts jobs, tracks workers, matches constraints, detects failures
- **worker** — registers capabilities, heartbeats, executes jobs
- **client** — submits jobs with requirements

## Status

In progress.
