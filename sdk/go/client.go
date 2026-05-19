package resilix

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Wembie/Resilix/sdk/go/internal/backoff"
	"github.com/Wembie/Resilix/sdk/go/internal/breaker"
	"github.com/Wembie/Resilix/sdk/go/internal/contract"
	goredisdriver "github.com/Wembie/Resilix/sdk/go/internal/driver/goredis"
	"github.com/Wembie/Resilix/sdk/go/internal/retry"
	internaltelemetry "github.com/Wembie/Resilix/sdk/go/internal/telemetry"
)

var ErrClientClosed = errors.New("resilix: client is closed")

type RedisClient struct {
	backend     contract.Backend
	executor    *retry.Executor
	recorder    *internaltelemetry.Recorder
	hooks       HookSet
	middlewares []Middleware
	admission   AdmissionController
	breaker     *breaker.Breaker
	mu          sync.RWMutex
	closed      bool
}

func New(options Options) (*RedisClient, error) {
	for _, plugin := range options.Plugins {
		if err := plugin.Apply(&options); err != nil {
			return nil, err
		}
	}

	if err := options.Normalize(); err != nil {
		return nil, err
	}

	recorder := internaltelemetry.New(
		options.Name,
		options.Observability.Logger,
		options.Observability.Tracer,
		options.Observability.Registry,
		options.Observability.SlowQueryThreshold,
	)

	circuitBreaker := breaker.New(breaker.Config{
		FailureThreshold:    options.CircuitBreaker.FailureThreshold,
		OpenTimeout:         options.CircuitBreaker.OpenTimeout,
		HalfOpenMaxRequests: options.CircuitBreaker.HalfOpenMaxRequests,
		OnStateChange: func(_, to breaker.State) {
			recorder.RecordBreakerState(string(to))
		},
	})

	executor := retry.New(
		retry.Policy{
			MaxAttempts: options.Retry.MaxAttempts,
			Classifier:  options.Retry.Classifier,
		},
		backoff.New(options.Retry.BaseDelay, options.Retry.MaxDelay, options.Retry.Multiplier, options.Retry.Jitter),
		circuitBreaker,
	)

	limiter := options.RateLimiter
	if limiter == nil {
		limiter = newInflightGate(options.MaxInflight)
	}

	var backend contract.Backend
	if options.Backend != nil {
		backend = options.Backend
	} else {
		backend = goredisdriver.New(options.universalOptions())
	}

	return &RedisClient{
		backend:     backend,
		executor:    executor,
		recorder:    recorder,
		hooks:       options.Hooks,
		middlewares: options.Middlewares,
		admission:   limiter,
		breaker:     circuitBreaker,
	}, nil
}

func (c *RedisClient) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return c.backend.Close()
}

func (c *RedisClient) KV() KV {
	return kvService{client: c}
}

func (c *RedisClient) Hash() Hash {
	return hashService{client: c}
}

func (c *RedisClient) List() List {
	return listService{client: c}
}

func (c *RedisClient) Set() SetStore {
	return setService{client: c}
}

func (c *RedisClient) SortedSet() SortedSetStore {
	return sortedSetService{client: c}
}

func (c *RedisClient) PubSub() PubSub {
	return pubsubService{client: c}
}

func (c *RedisClient) Stream() Stream {
	return streamService{client: c}
}

func (c *RedisClient) Script() Script {
	return scriptService{client: c}
}

func (c *RedisClient) Bulk() Bulk {
	return bulkService{client: c}
}

func (c *RedisClient) Geo() Geo {
	return geoService{client: c}
}

func (c *RedisClient) HyperLogLog() HyperLogLog {
	return hllService{client: c}
}

func (c *RedisClient) Bits() BitOps {
	return bitService{client: c}
}

func (c *RedisClient) Ping(ctx context.Context) error {
	_, err := c.execute(ctx, Operation{Name: "PING", Kind: "control"}, func(inner context.Context) (any, error) {
		return nil, c.backend.Ping(inner)
	})
	return err
}

func (c *RedisClient) Health(ctx context.Context) (HealthStatus, error) {
	pool := c.backend.PoolStats()
	c.recorder.RecordPoolStats(pool.Hits, pool.Misses, pool.Timeouts, pool.TotalConns)

	if err := c.Ping(ctx); err != nil {
		return HealthStatus{
			Status:       "degraded",
			Timestamp:    time.Now().UTC(),
			Pool:         pool,
			BreakerState: string(c.breaker.State()),
		}, err
	}

	return HealthStatus{
		Status:       "ok",
		Timestamp:    time.Now().UTC(),
		Pool:         pool,
		BreakerState: string(c.breaker.State()),
	}, nil
}

func (c *RedisClient) Scan(ctx context.Context, pattern string, count int64, fn func(string) error) error {
	var cursor uint64

	for {
		result, err := c.execute(ctx, Operation{
			Name: "SCAN",
			Kind: "scan",
			Attributes: map[string]string{
				"pattern": pattern,
			},
		}, func(inner context.Context) (any, error) {
			keys, next, scanErr := c.backend.Scan(inner, cursor, pattern, count)
			return struct {
				keys []string
				next uint64
			}{keys: keys, next: next}, scanErr
		})
		if err != nil {
			return err
		}

		batch := result.(struct {
			keys []string
			next uint64
		})

		for _, key := range batch.keys {
			if callbackErr := fn(key); callbackErr != nil {
				return callbackErr
			}
		}

		cursor = batch.next
		if cursor == 0 {
			return nil
		}
	}
}

func (c *RedisClient) execute(ctx context.Context, operation Operation, action func(context.Context) (any, error)) (any, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, ErrClientClosed
	}
	defer c.mu.RUnlock()

	terminal := func(current context.Context, op Operation) (any, error) {
		for _, hook := range c.hooks.BeforeExecute {
			if err := hook(current, op); err != nil {
				return nil, err
			}
		}

		release, err := c.admission.Acquire(current, op)
		if err != nil {
			return nil, err
		}
		defer release()

		observed, finish := c.recorder.Begin(current, op.Name, op.Kind, CorrelationIDFromContext(current))
		started := time.Now()
		result, err := c.executor.Do(observed, action)
		duration := time.Since(started)
		finish(err)

		for _, hook := range c.hooks.AfterExecute {
			hook(observed, op, duration, err)
		}

		return result, normalizePublicError(op, err)
	}

	result, err := chain(c.middlewares, terminal)(ctx, operation)
	if err != nil {
		return nil, wrapOperationError(operation.Name, operation.Keys, err)
	}
	return result, nil
}

func normalizePublicError(operation Operation, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, contract.ErrKeyNotFound) {
		return ErrKeyNotFound
	}
	return wrapOperationError(operation.Name, operation.Keys, err)
}

type inflightGate struct {
	slots chan struct{}
}

func newInflightGate(limit int) *inflightGate {
	return &inflightGate{slots: make(chan struct{}, limit)}
}

func (g *inflightGate) Acquire(ctx context.Context, _ Operation) (ReleaseFunc, error) {
	select {
	case g.slots <- struct{}{}:
		return func() {
			<-g.slots
		}, nil
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, ctx.Err()
		}
		return nil, ErrBackpressure
	}
}
