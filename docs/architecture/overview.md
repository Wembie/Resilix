# Resilix Architecture Overview

## Mission

Resilix is a production-grade Redis SDK platform for backend systems that need a consistent contract for resilience, observability, and future multi-language support.

The current implementation focuses on Go first while preparing the repository, contracts, examples, and operational model for a future Python SDK. Consumers install the Go library directly from `github.com/Wembie/Resilix/sdk/go`.

## Monorepo Architecture

```text
resilix/
|-- sdk/
|   |-- go/         # primary SDK implementation
|   `-- python/     # architectural placeholder
|-- examples/       # runnable sample applications
|-- benchmarks/     # repeatable performance suites
|-- docs/           # architecture, ADRs, operational guides
|-- scripts/        # dev and CI helper scripts
|-- deploy/         # runtime deployment assets
|-- internal/       # shared monorepo templates and conventions
|-- proto/          # future contracts for service/event boundaries
|-- tests/          # cross-module integration and contract suites
|-- docker/         # local runtime containers
`-- observability/  # optional local provisioning for maintainers and integration work
```

## Go SDK Layering

The Go SDK follows a pragmatic clean and hexagonal structure:

1. Public API layer
   `sdk/go/*.go`
   Exposes the idiomatic client, service domains, middleware, hooks, errors, and configuration options.

2. Application orchestration layer
   `sdk/go/client.go`, `sdk/go/pipeline.go`
   Coordinates command execution, inflight admission, hooks, observability, retry policy, and error normalization.

3. Internal contracts
   `sdk/go/internal/contract`
   Defines the backend port and shared transport-neutral types used by the client and the driver adapter.

4. Infrastructure adapters
   `sdk/go/internal/driver/goredis`
   Implements the backend contract using `go-redis/v9`.

5. Cross-cutting runtime controls
   `sdk/go/internal/{retry,breaker,backoff,telemetry}`
   Encapsulates policies for resilience and observability.

6. Config composition
   `sdk/go/config`
   Loads and merges file config, env vars and runtime overrides.

## Domain Modules

The public client is split by Redis capability domains instead of exposing one oversized surface:

- `KV()`
- `Hash()`
- `List()`
- `Set()`
- `SortedSet()`
- `PubSub()`
- `Stream()`
- `Script()`
- `Bulk()`

This keeps the API navigable, makes behavior easier to test, and gives room for future language parity.

## Execution Flow

For every command, Resilix executes the following chain:

1. Validate inputs
2. Build operation metadata
3. Run before hooks
4. Acquire inflight admission token
5. Start trace span and metrics timer
6. Execute through retry executor
7. Use circuit breaker feedback to record success/failure
8. Emit slow query logs and metrics
9. Run after hooks
10. Normalize and wrap errors

## Resilience Model

- Retry policy is configurable per client.
- Exponential backoff and jitter are internalized in a dedicated package.
- Circuit breaker is driver-agnostic and concurrency-safe.
- Backpressure is implemented with inflight admission control.
- Pooling, reconnect and low-level failover handling are delegated to `go-redis` universal client semantics.

## Observability Model

- OpenTelemetry tracing via `trace.Tracer`
- Prometheus metrics via explicit counters, histograms, gauges
- Structured logging through `log/slog`
- Correlation IDs propagated via context
- Slow query detection in the telemetry recorder

## Security Model

- TLS is supported at the client option layer.
- Username/password auth and ACL-compatible credentials are supported.
- Secure defaults prioritize bounded timeouts and explicit connection settings.
- Secret injection is expected through env vars, sealed files or external secret systems.

## Scalability Path

Resilix is designed to evolve in three dimensions:

1. More adapters
   Additional Redis drivers, cluster-specialized adapters, and mock/test backends can implement the internal contract.

2. More SDKs
   Python can mirror the domain services and cross-cutting semantics while staying idiomatic.

3. Service platform integration
   `proto/`, `deploy/`, and observability assets prepare the repo for sidecars, control planes, or event-driven utility services later.
