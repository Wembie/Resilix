# Resilix

Resilix is an enterprise-grade Redis SDK monorepo focused on resilient backend systems, high throughput workloads, rich observability, and production-safe developer experience.

Current priority:

- Go SDK: active implementation
- Python SDK: architectural preparation only

## Design Principles

- Clean and hexagonal architecture
- Strong domain modularity
- Concurrency safety and backpressure awareness
- Resilience by default
- First-class observability
- Secure configuration and transport
- Predictable DX for application teams

## Monorepo Layout

```text
.
|-- sdk/
|   |-- go/
|   `-- python/
|-- examples/
|-- benchmarks/
|-- docs/
|-- scripts/
|-- deploy/
|-- internal/
|-- proto/
|-- tests/
|-- docker/
`-- observability/
```

## Quick Start

1. Start Redis plus observability:

   ```bash
   make docker-up
   ```

2. Run Go tests:

   ```bash
   make test
   ```

3. Run the CLI health probe:

   ```bash
   go run ./sdk/go/cmd/resilixctl --config ./docs/sample-config.yaml
   ```

## Repository Standards

- Conventional Commits
- Semantic Release
- Pre-commit hooks
- GitHub Actions matrix testing
- Security and dependency scanning
- Benchmarks and observability assets

## Documentation

- Architecture: [docs/architecture/overview.md](docs/architecture/overview.md)
- ADRs: [docs/adr](docs/adr)
- Guides: [docs/guides](docs/guides)
