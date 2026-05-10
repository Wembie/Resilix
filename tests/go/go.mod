module github.com/resilix/resilix/tests/go

go 1.24.2

require (
  github.com/resilix/resilix/sdk/go v0.0.0
  github.com/testcontainers/testcontainers-go v0.38.0
)

replace github.com/resilix/resilix/sdk/go => ../../sdk/go
