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
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.KV().Set(ctx, "resilix:example", "hello", time.Minute); err != nil {
		log.Fatal(err)
	}

	value, err := client.KV().Get(ctx, "resilix:example")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(value)
}
