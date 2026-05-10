package breaker

import (
	"testing"
	"time"
)

func TestBreakerOpensAfterThreshold(t *testing.T) {
	t.Parallel()

	cb := New(Config{FailureThreshold: 2, OpenTimeout: 20 * time.Millisecond, HalfOpenMaxRequests: 1})
	cb.RecordFailure()
	cb.RecordFailure()

	if state := cb.State(); state != StateOpen {
		t.Fatalf("expected open state, got %s", state)
	}
	if err := cb.Allow(); err != ErrOpen {
		t.Fatalf("expected open error, got %v", err)
	}
}

func TestBreakerHalfOpenThenClosedOnSuccess(t *testing.T) {
	t.Parallel()

	cb := New(Config{FailureThreshold: 1, OpenTimeout: 5 * time.Millisecond, HalfOpenMaxRequests: 1})
	cb.RecordFailure()
	time.Sleep(10 * time.Millisecond)

	if err := cb.Allow(); err != nil {
		t.Fatalf("expected allow after timeout, got %v", err)
	}
	if state := cb.State(); state != StateHalfOpen {
		t.Fatalf("expected half-open state, got %s", state)
	}

	cb.RecordSuccess()
	if state := cb.State(); state != StateClosed {
		t.Fatalf("expected closed state, got %s", state)
	}
}
