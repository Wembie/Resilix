package benchmarks

import (
	"context"
	"testing"
	"time"

	resilix "github.com/Wembie/Resilix/sdk/go"
)

func BenchmarkSetGet(b *testing.B) {
	client, err := resilix.New(resilix.DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if pingErr := client.Ping(ctx); pingErr != nil {
		b.Skipf("redis unavailable: %v", pingErr)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench:key"
		if err := client.KV().Set(ctx, key, i, time.Minute); err != nil {
			b.Fatal(err)
		}
		if _, err := client.KV().Get(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
}
