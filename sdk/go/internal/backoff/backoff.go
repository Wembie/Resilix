package backoff

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

type Strategy struct {
	base       time.Duration
	max        time.Duration
	multiplier float64
	jitter     float64
	source     *rand.Rand
	mu         sync.Mutex
}

func New(base, max time.Duration, multiplier, jitter float64) *Strategy {
	if base <= 0 {
		base = 25 * time.Millisecond
	}
	if max <= 0 {
		max = time.Second
	}
	if multiplier < 1 {
		multiplier = 2
	}
	if jitter < 0 {
		jitter = 0
	}

	return &Strategy{
		base:       base,
		max:        max,
		multiplier: multiplier,
		jitter:     jitter,
		source:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *Strategy) Duration(attempt int) time.Duration {
	if attempt <= 1 {
		return s.applyJitter(s.base)
	}

	next := float64(s.base) * math.Pow(s.multiplier, float64(attempt-1))
	if next > float64(s.max) {
		next = float64(s.max)
	}

	return s.applyJitter(time.Duration(next))
}

func (s *Strategy) applyJitter(delay time.Duration) time.Duration {
	if s.jitter == 0 {
		return delay
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	spread := (s.source.Float64()*2 - 1) * s.jitter
	adjusted := float64(delay) * (1 + spread)
	if adjusted < 0 {
		return 0
	}

	return time.Duration(adjusted)
}
