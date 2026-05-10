# Performance Guide

## Hot Path Design

Resilix optimizes the hot path with:

- reusable internal policy objects
- bounded inflight concurrency
- centralized execution chain
- thin driver translation over `go-redis`

## Practical Tuning Knobs

- `PoolSize`
- `MinIdleConns`
- `ReadTimeout`
- `WriteTimeout`
- `MaxInflight`
- benchmark batch sizes

## Benchmark Workflow

- place repeatable workloads in `benchmarks/go`
- run `make bench`
- store profiler outputs in CI artifacts for regressions

## Future Optimizations

- command object pooling
- optional bytes-oriented APIs for hot paths
- adaptive batch splitting
- sampling-aware telemetry for extreme throughput scenarios
