package resilix

import (
	"context"
	"testing"
)

func TestCorrelationIDRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := WithCorrelationID(context.Background(), "corr-123")
	if got := CorrelationIDFromContext(ctx); got != "corr-123" {
		t.Fatalf("unexpected correlation id: %s", got)
	}
}
