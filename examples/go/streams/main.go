package main

import (
	"context"
	"fmt"
	"log"
	"time"

	resilix "github.com/Wembie/Resilix/sdk/go"
)

func main() {
	client, err := resilix.New(resilix.DefaultOptions())
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("close client: %v", closeErr)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Stream().CreateGroup(ctx, "resilix:stream", "workers", "$"); err != nil {
		log.Fatal(err)
	}

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
