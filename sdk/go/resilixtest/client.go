package resilixtest

import (
	"testing"

	resilix "github.com/Wembie/Resilix/sdk/go"
)

// NewClient returns a *resilix.RedisClient backed by mock.
// Registers t.Cleanup to close the client when the test finishes.
func NewClient(t *testing.T, mock *MockBackend) *resilix.RedisClient {
	t.Helper()
	opts := resilix.DefaultOptions()
	opts.Backend = mock
	client, err := resilix.New(opts)
	if err != nil {
		t.Fatalf("resilixtest.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
