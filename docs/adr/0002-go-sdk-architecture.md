# ADR 0002: Go SDK Architecture

## Status

Accepted

## Context

The Go implementation must be production-ready, extensible, and testable without locking the public API directly to one Redis driver.

## Decision

Use a public service-oriented API backed by an internal backend contract and a `go-redis` adapter.

## Rationale

- Keeps the public API clean and domain-focused
- Preserves adapter swap flexibility
- Allows resilience and observability to wrap execution centrally
- Makes future contract testing against multiple adapters feasible

## Consequences

Positive:

- Strong separation of concerns
- Easier middleware and hook insertion
- Reduced blast radius when driver details change

Negative:

- Some duplication between public types and internal command translation
- Pipelines require an explicit builder abstraction instead of exposing the raw driver
