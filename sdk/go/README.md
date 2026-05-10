# Resilix Go SDK

The Go SDK is the primary implementation of Resilix.

Install:

```bash
go get -v github.com/Wembie/Resilix/sdk/go
```

Highlights:

- Domain-oriented Redis API
- Middleware and hooks
- Retries with exponential backoff and jitter
- Circuit breaker and inflight backpressure
- OpenTelemetry tracing and Prometheus metrics
- Config loaders for YAML, JSON, ENV and runtime overrides
- Version sourced from `sdk/go/VERSION`

See the repository docs for architecture and operational guides.
