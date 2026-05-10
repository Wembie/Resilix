package main

import (
	"context"
	"fmt"
	"log"
	"time"

	resilix "github.com/Wembie/Resilix/sdk/go"
)

func main() {
	middleware := func(next resilix.Handler) resilix.Handler {
		return func(ctx context.Context, operation resilix.Operation) (any, error) {
			started := time.Now()
			result, err := next(ctx, operation)
			fmt.Printf("operation=%s duration=%s err=%v\n", operation.Name, time.Since(started), err)
			return result, err
		}
	}

	options := resilix.DefaultOptions()
	options.Middlewares = []resilix.Middleware{middleware}

	client, err := resilix.New(options)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("close client: %v", closeErr)
		}
	}()

	ctx := context.Background()
	if err := client.KV().Set(ctx, "resilix:mw", "ok", time.Minute); err != nil {
		log.Printf("set value: %v", err)
		return
	}
}
