package retry

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/resilix/resilix/sdk/go/internal/backoff"
	"github.com/resilix/resilix/sdk/go/internal/breaker"
	"github.com/resilix/resilix/sdk/go/internal/contract"
)

type Classifier func(error) bool

type Policy struct {
	MaxAttempts int
	Classifier  Classifier
}

type Executor struct {
	policy  Policy
	breaker *breaker.Breaker
	backoff *backoff.Strategy
}

func New(policy Policy, strategy *backoff.Strategy, cb *breaker.Breaker) *Executor {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 3
	}
	if policy.Classifier == nil {
		policy.Classifier = DefaultClassifier
	}

	return &Executor{
		policy:  policy,
		breaker: cb,
		backoff: strategy,
	}
}

func (e *Executor) Do(ctx context.Context, fn func(context.Context) (any, error)) (any, error) {
	if e.breaker != nil {
		if err := e.breaker.Allow(); err != nil {
			return nil, err
		}
	}

	var lastErr error

	for attempt := 1; attempt <= e.policy.MaxAttempts; attempt++ {
		result, err := fn(ctx)
		if err == nil {
			if e.breaker != nil {
				e.breaker.RecordSuccess()
			}
			return result, nil
		}

		lastErr = err
		if !e.policy.Classifier(err) || attempt == e.policy.MaxAttempts {
			if e.breaker != nil {
				e.breaker.RecordFailure()
			}
			return nil, lastErr
		}

		timer := time.NewTimer(e.backoff.Duration(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	if e.breaker != nil {
		e.breaker.RecordFailure()
	}

	return nil, lastErr
}

func DefaultClassifier(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, contract.ErrKeyNotFound) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "connection") ||
		strings.Contains(lower, "timeout") ||
		strings.Contains(lower, "broken pipe") ||
		strings.Contains(lower, "reset by peer")
}
