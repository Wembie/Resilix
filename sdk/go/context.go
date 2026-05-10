package resilix

import (
	"context"

	internaltelemetry "github.com/resilix/resilix/sdk/go/internal/telemetry"
)

func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return internaltelemetry.WithCorrelationID(ctx, correlationID)
}

func CorrelationIDFromContext(ctx context.Context) string {
	return internaltelemetry.CorrelationIDFromContext(ctx)
}
