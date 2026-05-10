# ADR 0001: Monorepo and SDK Strategy

## Status

Accepted

## Context

Resilix needs to ship a primary Go SDK immediately while keeping a credible path for Python and future platform tooling. Splitting repositories too early would increase coordination cost, duplicate documentation, and fragment versioning policy.

## Decision

Adopt a single monorepo with language-specific SDK folders and shared operational assets.

## Consequences

Positive:

- Shared CI/CD, release, docs and observability patterns
- Easier cross-language parity tracking
- Centralized ADRs and operational standards

Trade-offs:

- More discipline needed around module boundaries
- Workspace and dependency management become more important

Mitigations:

- Go workspace with module-local `replace` directives
- Explicit folder conventions
- ADR-driven evolution
