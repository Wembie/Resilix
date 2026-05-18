package breaker

import (
	"errors"
	"sync"
	"time"
)

var ErrOpen = errors.New("resilix: circuit breaker is open")

type State string

const (
	StateClosed   State = "closed"
	StateOpen     State = "open"
	StateHalfOpen State = "half_open"
)

type Config struct {
	FailureThreshold    int64
	OpenTimeout         time.Duration
	HalfOpenMaxRequests int64
	OnStateChange       func(from, to State)
}

type Breaker struct {
	cfg              Config
	mu               sync.Mutex
	state            State
	failures         int64
	openedAt         time.Time
	halfOpenRequests int64
	onStateChange    func(from, to State)
}

func New(cfg Config) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 5 * time.Second
	}
	if cfg.HalfOpenMaxRequests <= 0 {
		cfg.HalfOpenMaxRequests = 1
	}

	return &Breaker{
		cfg:           cfg,
		state:         StateClosed,
		onStateChange: cfg.OnStateChange,
	}
}

func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return nil
	case StateOpen:
		if time.Since(b.openedAt) >= b.cfg.OpenTimeout {
			b.state = StateHalfOpen
			b.halfOpenRequests = 0
			b.notifyStateChange(StateOpen, StateHalfOpen)
		} else {
			return ErrOpen
		}
	}

	if b.state == StateHalfOpen {
		if b.halfOpenRequests >= b.cfg.HalfOpenMaxRequests {
			return ErrOpen
		}
		b.halfOpenRequests++
	}

	return nil
}

func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failures = 0
	b.halfOpenRequests = 0
	if b.state == StateHalfOpen {
		b.state = StateClosed
		b.notifyStateChange(StateHalfOpen, StateClosed)
	} else {
		b.state = StateClosed
	}
}

func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == StateHalfOpen {
		b.state = StateOpen
		b.openedAt = time.Now()
		b.halfOpenRequests = 0
		b.notifyStateChange(StateHalfOpen, StateOpen)
		return
	}

	b.failures++
	if b.failures >= b.cfg.FailureThreshold {
		b.state = StateOpen
		b.openedAt = time.Now()
		b.notifyStateChange(StateClosed, StateOpen)
	}
}

func (b *Breaker) notifyStateChange(from, to State) {
	if b.onStateChange != nil {
		go b.onStateChange(from, to)
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
