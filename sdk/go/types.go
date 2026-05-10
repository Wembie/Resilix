package resilix

import (
	"context"
	"time"

	"github.com/resilix/resilix/sdk/go/internal/contract"
)

type (
	ZMember            = contract.ZMember
	PubSubMessage      = contract.Message
	Subscription       = contract.Subscription
	StreamEntry        = contract.StreamEntry
	StreamReadRequest  = contract.StreamReadRequest
	StreamReadResponse = contract.StreamReadResponse
	CommandResult      = contract.CommandResult
	PoolStats          = contract.PoolStats
)

type HealthStatus struct {
	Status       string
	Timestamp    time.Time
	Pool         PoolStats
	BreakerState string
}

type Client interface {
	KV() KV
	Hash() Hash
	List() List
	Set() SetStore
	SortedSet() SortedSetStore
	PubSub() PubSub
	Stream() Stream
	Script() Script
	Bulk() Bulk
	Pipeline(ctx context.Context, fn func(PipelineBuilder)) ([]CommandResult, error)
	Transaction(ctx context.Context, watchKeys []string, fn func(PipelineBuilder)) ([]CommandResult, error)
	Scan(ctx context.Context, pattern string, count int64, fn func(string) error) error
	Ping(ctx context.Context) error
	Health(ctx context.Context) (HealthStatus, error)
	Close() error
}
