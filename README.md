# Resilix

Resilix is an enterprise-grade Redis SDK monorepo focused on resilient backend systems, high throughput workloads, rich observability, and production-safe developer experience.

Current priority:

- Go SDK: active implementation
- Python SDK: architectural preparation only

## Install

For Go consumers, the public module path is:

```bash
go get -v github.com/Wembie/Resilix/sdk/go
```

Docker is not required to consume the library. The Docker and observability assets in this repository are only for local integration, debugging, and maintainers of Resilix itself.

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

1. Install the Go SDK:

   ```bash
   go get -v github.com/Wembie/Resilix/sdk/go
   ```

2. Run Go tests:

   ```bash
   make test
   ```

3. Optional: start local Redis plus observability for integration work:

   ```bash
   make docker-up
   ```

4. Run the CLI health probe:

   ```bash
   go run ./sdk/go/cmd/resilixctl --config ./docs/sample-config.yaml
   ```

## Repository Standards

- Conventional Commits
- Per-SDK VERSION files
- Pre-commit hooks
- GitHub Actions with separated Go and Python validation
- Security and dependency scanning
- Benchmarks and observability assets

## Documentation

- Architecture: [docs/architecture/overview.md](docs/architecture/overview.md)
- ADRs: [docs/adr](docs/adr)
- Guides: [docs/guides](docs/guides)
