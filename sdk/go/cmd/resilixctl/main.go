package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	resilix "github.com/Wembie/Resilix/sdk/go"
	"github.com/Wembie/Resilix/sdk/go/config"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		configPath  = flag.String("config", "", "Path to YAML or JSON configuration")
		addr        = flag.String("addr", "127.0.0.1:6379", "Redis address override")
		timeout     = flag.Duration("timeout", 2*time.Second, "Health check timeout")
		showVersion = flag.Bool("version", false, "Print the Resilix Go SDK version")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println(resilix.Version)
		return 0
	}

	options := resilix.DefaultOptions()
	options.Addrs = []string{*addr}

	if *configPath != "" {
		loaded, err := config.LoadOptions(*configPath, "RESILIX", nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config error: %v\n", err)
			return 1
		}
		options = loaded
	}

	client, err := resilix.New(options)
	if err != nil {
		fmt.Fprintf(os.Stderr, "client error: %v\n", err)
		return 1
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "close error: %v\n", closeErr)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	status, err := client.Health(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "health error: %v\n", err)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if encodeErr := encoder.Encode(status); encodeErr != nil {
		fmt.Fprintf(os.Stderr, "encode error: %v\n", encodeErr)
		return 1
	}

	if err != nil {
		return 1
	}

	return 0
}
