package backoff

import (
	"testing"
	"time"
)

func TestDurationCapsAtMax(t *testing.T) {
	t.Parallel()

	strategy := New(10*time.Millisecond, 40*time.Millisecond, 2, 0)
	if got := strategy.Duration(5); got != 40*time.Millisecond {
		t.Fatalf("expected cap at 40ms, got %s", got)
	}
}

func TestDurationWithJitterIsNonNegative(t *testing.T) {
	t.Parallel()

	strategy := New(time.Millisecond, 10*time.Millisecond, 2, 0.9)
	for i := 1; i < 20; i++ {
		if got := strategy.Duration(i); got < 0 {
			t.Fatalf("expected non-negative duration, got %s", got)
		}
	}
}
