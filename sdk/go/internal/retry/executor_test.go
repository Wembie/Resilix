package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/resilix/resilix/sdk/go/internal/backoff"
	"github.com/resilix/resilix/sdk/go/internal/breaker"
)

func TestExecutorRetriesRetryableErrors(t *testing.T) {
	t.Parallel()

	attempts := 0
	executor := New(
		Policy{
			MaxAttempts: 3,
			Classifier: func(err error) bool {
				return errors.Is(err, context.DeadlineExceeded)
			},
		},
		backoff.New(time.Millisecond, time.Millisecond, 1, 0),
		breaker.New(breaker.Config{FailureThreshold: 10, OpenTimeout: time.Second, HalfOpenMaxRequests: 1}),
	)

	_, err := executor.Do(context.Background(), func(context.Context) (any, error) {
		attempts++
		if attempts < 3 {
			return nil, context.DeadlineExceeded
		}
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestExecutorStopsOnNonRetryableError(t *testing.T) {
	t.Parallel()

	attempts := 0
	executor := New(
		Policy{
			MaxAttempts: 3,
			Classifier:  func(error) bool { return false },
		},
		backoff.New(time.Millisecond, time.Millisecond, 1, 0),
		breaker.New(breaker.Config{FailureThreshold: 10, OpenTimeout: time.Second, HalfOpenMaxRequests: 1}),
	)

	expected := errors.New("fatal")
	_, err := executor.Do(context.Background(), func(context.Context) (any, error) {
		attempts++
		return nil, expected
	})
	if !errors.Is(err, expected) {
		t.Fatalf("expected fatal error, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}
