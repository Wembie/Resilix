package main

import (
	"context"
	"fmt"
	"log"
	"time"

	resilix "github.com/resilix/resilix/sdk/go"
)

func main() {
	client, err := resilix.New(resilix.DefaultOptions())
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = client.Stream().CreateGroup(ctx, "resilix:stream", "workers", "$")

	id, err := client.Stream().Add(ctx, "resilix:stream", map[string]any{
		"event": "created",
		"ts":    time.Now().UTC().Format(time.RFC3339),
	}, 1000, true)
	if err != nil {
		log.Fatal(err)
	}

	messages, err := client.Stream().ReadGroup(ctx, resilix.StreamReadRequest{
		Group:    "workers",
		Consumer: "example-consumer",
		Streams:  []string{"resilix:stream"},
		IDs:      []string{">"},
		Count:    1,
		Block:    time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("published=%s entries=%d\n", id, len(messages))
}
