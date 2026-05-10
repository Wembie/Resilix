# Migration Guide

## From raw go-redis to Resilix

1. Replace direct driver initialization with `resilix.New`.
2. Move retry logic out of application code.
3. Inject tracing and logging through `Options.Observability`.
4. Replace driver calls with domain services such as `KV()`, `Hash()`, or `Stream()`.
5. Move ad hoc bulk loops into `Bulk()`.

## Why migrate

- consistent resilience behavior
- unified instrumentation
- typed error model
- room for cross-team standards and plugins
