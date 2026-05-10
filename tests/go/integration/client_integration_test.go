//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	resilix "github.com/Wembie/Resilix/sdk/go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestKVAndPipeline(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7.4-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	defer container.Terminate(ctx)

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}

	options := resilix.DefaultOptions()
	options.Addrs = []string{host + ":" + port.Port()}

	client, err := resilix.New(options)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer client.Close()

	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := client.KV().Set(timeoutCtx, "resilix:test", "value", time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}

	value, err := client.KV().Get(timeoutCtx, "resilix:test")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if value != "value" {
		t.Fatalf("unexpected value: %s", value)
	}

	results, err := client.Pipeline(timeoutCtx, func(builder resilix.PipelineBuilder) {
		builder.Incr("resilix:counter")
		builder.Incr("resilix:counter")
	})
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("unexpected pipeline result count: %d", len(results))
	}
}
