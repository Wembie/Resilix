# Resilience Guide

## Default Policy

Resilix defaults to bounded retries, exponential backoff, jitter, and a circuit breaker. The defaults are intentionally conservative to avoid cascading retries.

## Retry Strategy

- Max attempts: 3
- Base delay: 25ms
- Max delay: 750ms
- Multiplier: 2
- Jitter: 20%

## Circuit Breaker Strategy

- Failure threshold: 5
- Open timeout: 5s
- Half-open requests: 1

## Backpressure

Backpressure is applied through an inflight admission controller. By default, Resilix uses a concurrency gate based on `MaxInflight`.

Advanced teams can replace it with a custom `AdmissionController` for:

- global rate limiting
- tenant-aware quotas
- adaptive shed load
- priority lanes

## Operational Guidance

- Keep caller timeouts shorter than your outer service SLA.
- Avoid aggressive retry counts for write-heavy or fan-out workloads.
- Track breaker state in dashboards.
- Partition large bulk operations.
