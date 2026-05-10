package main

import (
	"context"
	"fmt"
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
		panic(err)
	}
	defer client.Close()

	ctx := context.Background()
	_ = client.KV().Set(ctx, "resilix:mw", "ok", time.Minute)
}
