// Package lock provides a distributed lock implementation using the Redlock algorithm.
//
// Usage:
//
//	locker := lock.New(client1, client2, client3)
//	token, err := locker.Acquire(ctx, "resource", 10*time.Second)
//	if err != nil { ... }
//	defer locker.Release(ctx, "resource", token)
package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

)

var (
	// ErrNotAcquired is returned when the lock cannot be acquired within the retry budget.
	ErrNotAcquired = errors.New("resilix/lock: could not acquire lock")
	// ErrNotOwner is returned when releasing a lock the caller does not own.
	ErrNotOwner = errors.New("resilix/lock: token mismatch — not the lock owner")
)

const (
	// clockDriftFactor is the assumed max clock drift between nodes (10%).
	clockDriftFactor = 0.01
	// minNodes is the minimum quorum size for a Redlock cluster.
	minNodes = 1
)

// Backend is the minimal interface a node backend must implement for Redlock.
// contract.Backend satisfies this interface.
type Backend interface {
	Set(ctx context.Context, key string, value any, ttlSeconds float64) error
	Get(ctx context.Context, key string) (string, error)
	Del(ctx context.Context, keys ...string) (int64, error)
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
}

// Locker implements the Redlock algorithm across N independent Redis nodes.
// A lock is considered acquired when it succeeds on a strict majority (N/2 + 1).
type Locker struct {
	nodes    []Backend
	quorum   int
	retries  int
	retryGap time.Duration
}

// New creates a Locker using the provided backends as independent Redis nodes.
// For single-node setups pass one backend — it still works but loses fault-tolerance.
func New(nodes ...Backend) *Locker {
	q := len(nodes)/2 + 1
	if q < minNodes {
		q = minNodes
	}
	return &Locker{
		nodes:    nodes,
		quorum:   q,
		retries:  3,
		retryGap: 200 * time.Millisecond,
	}
}

// WithRetries sets how many acquisition attempts to make before giving up.
func (l *Locker) WithRetries(n int) *Locker { l.retries = n; return l }

// WithRetryGap sets the wait between acquisition attempts.
func (l *Locker) WithRetryGap(d time.Duration) *Locker { l.retryGap = d; return l }

// Acquire attempts to lock key for ttl duration. Returns the token required to release.
// The token is a random nonce; keep it to release the lock later.
// If the lock cannot be obtained within the retry budget, returns ErrNotAcquired.
func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration) (string, error) {
	token, err := randomToken()
	if err != nil {
		return "", err
	}

	for attempt := 0; attempt < l.retries; attempt++ {
		start := time.Now()
		acquired := l.tryAcquire(ctx, key, token, ttl)
		elapsed := time.Since(start)

		validity := ttl - elapsed - driftFor(ttl)
		if acquired >= l.quorum && validity > 0 {
			return token, nil
		}

		// Rollback: release on nodes where we succeeded.
		l.release(ctx, key, token)

		if attempt < l.retries-1 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(l.retryGap):
			}
		}
	}
	return "", ErrNotAcquired
}

// Release unlocks key on all nodes. Returns ErrNotOwner if the token does not match
// (lock expired and was reacquired by another caller).
func (l *Locker) Release(ctx context.Context, key, token string) error {
	released := l.release(ctx, key, token)
	if released < l.quorum {
		return ErrNotOwner
	}
	return nil
}

// Extend renews the lock TTL if the caller still owns it.
// Returns ErrNotOwner if the lock has expired or been taken by another caller.
func (l *Locker) Extend(ctx context.Context, key, token string, ttl time.Duration) error {
	extended := 0
	for _, node := range l.nodes {
		if extendOnNode(ctx, node, key, token, ttl) {
			extended++
		}
	}
	if extended < l.quorum {
		return ErrNotOwner
	}
	return nil
}

// --- internals ---

func (l *Locker) tryAcquire(ctx context.Context, key, token string, ttl time.Duration) int {
	acquired := 0
	ttlSeconds := ttl.Seconds()
	for _, node := range l.nodes {
		if setNX(ctx, node, key, token, ttlSeconds) {
			acquired++
		}
	}
	return acquired
}

func (l *Locker) release(ctx context.Context, key, token string) int {
	released := 0
	for _, node := range l.nodes {
		if releaseOnNode(ctx, node, key, token) {
			released++
		}
	}
	return released
}

// setNX sets key=token with ttl only if key does not exist (SET NX EX).
// Uses Eval to perform the operation atomically.
const setNXScript = `
if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
  return 1
else
  return 0
end`

func setNX(ctx context.Context, node Backend, key, token string, ttlSeconds float64) bool {
	ttlMS := int64(ttlSeconds * 1000)
	result, err := node.Eval(ctx, setNXScript, []string{key}, token, ttlMS)
	if err != nil {
		return false
	}
	n, ok := result.(int64)
	return ok && n == 1
}

// releaseOnNode deletes the key only if the token matches (atomic compare-and-delete).
const releaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end`

func releaseOnNode(ctx context.Context, node Backend, key, token string) bool {
	result, err := node.Eval(ctx, releaseScript, []string{key}, token)
	if err != nil {
		return false
	}
	n, ok := result.(int64)
	return ok && n == 1
}

// extendOnNode renews TTL only if the caller still owns the lock.
const extendScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end`

func extendOnNode(ctx context.Context, node Backend, key, token string, ttl time.Duration) bool {
	ttlMS := ttl.Milliseconds()
	result, err := node.Eval(ctx, extendScript, []string{key}, token, ttlMS)
	if err != nil {
		return false
	}
	n, ok := result.(int64)
	return ok && n == 1
}

func randomToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func driftFor(ttl time.Duration) time.Duration {
	return time.Duration(float64(ttl)*clockDriftFactor) + 2*time.Millisecond
}
