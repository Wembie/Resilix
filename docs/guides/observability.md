# Observability Guide

## Telemetry Surfaces

Resilix emits three telemetry classes:

- Traces: one client span per Redis operation
- Metrics: operation totals, durations, inflight count, slow query count
- Logs: structured slow-query warnings with correlation IDs

## Integration Points

- OpenTelemetry tracer injection via `Options.Observability.Tracer`
- Prometheus registry injection via `Options.Observability.Registry`
- `log/slog` logger injection via `Options.Observability.Logger`

## Recommended Dashboard Signals

- p50, p95, p99 latency by operation
- error rate by operation
- slow query count by operation
- inflight pressure
- breaker state transitions
- Redis pool misses and timeouts

## Jaeger / Grafana / Prometheus

The repository includes:

- `docker-compose.yml`
- `observability/prometheus.yml`
- `observability/grafana/*`

These are optional maintainer assets for a local telemetry sandbox and can be adapted to Kubernetes later. They are not required by applications that simply import the SDK.
