SHELL := /bin/sh

GO_MODULE := ./sdk/go
GO_EXAMPLES := ./examples/go
GO_TESTS := ./tests/go
GO_BENCH := ./benchmarks/go
PYTHON_SDK := ./sdk/python

.PHONY: bootstrap fmt fmt-go lint lint-go lint-python test test-go test-unit test-integration race fuzz bench docker-up docker-down docs precommit security

bootstrap:
	cd $(GO_MODULE) && go mod tidy
	cd $(GO_EXAMPLES) && go mod tidy
	cd $(GO_TESTS) && go mod tidy
	cd $(GO_BENCH) && go mod tidy

fmt:
	$(MAKE) fmt-go

fmt-go:
	cd $(GO_MODULE) && gofmt -w .
	cd $(GO_EXAMPLES) && gofmt -w .
	cd $(GO_TESTS) && gofmt -w .
	cd $(GO_BENCH) && gofmt -w .

lint:
	$(MAKE) lint-go
	$(MAKE) lint-python

lint-go:
	golangci-lint run ./sdk/go/...
	golangci-lint run ./examples/go/...
	golangci-lint run ./tests/go/...
	golangci-lint run ./benchmarks/go/...

lint-python:
	python -m ruff check $(PYTHON_SDK)/src
	python -m black --check $(PYTHON_SDK)/src
	python -m flake8 $(PYTHON_SDK)/src
	python -m mypy $(PYTHON_SDK)/src

test: test-go

test-go: test-unit test-integration

test-unit:
	cd $(GO_MODULE) && go test -cover ./...

test-integration:
	cd $(GO_TESTS) && go test -tags=integration ./...

race:
	cd $(GO_MODULE) && go test -race ./...

fuzz:
	cd $(GO_MODULE) && go test -fuzz=Fuzz -fuzztime=10s ./...

bench:
	cd $(GO_BENCH) && go test -bench=. -benchmem ./...

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down --remove-orphans

docs:
	@echo "Documentation lives under ./docs"

precommit:
	pre-commit run --all-files

security:
	govulncheck ./sdk/go/...
