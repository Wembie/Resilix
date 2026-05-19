package resilix_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	resilix "github.com/Wembie/Resilix/sdk/go"
	"github.com/Wembie/Resilix/sdk/go/resilixtest"
)

// ============================================================
// Middleware chain
// ============================================================

func TestMiddleware_ChainOrder(t *testing.T) {
	var order []string
	mu := &sync.Mutex{}

	record := func(label string) resilix.Middleware {
		return func(next resilix.Handler) resilix.Handler {
			return func(ctx context.Context, op resilix.Operation) (any, error) {
				mu.Lock()
				order = append(order, label+":before")
				mu.Unlock()
				result, err := next(ctx, op)
				mu.Lock()
				order = append(order, label+":after")
				mu.Unlock()
				return result, err
			}
		}
	}

	mock := resilixtest.NewMockBackend()
	mock.SetKV("k", "v")

	opts := resilix.DefaultOptions()
	opts.Backend = mock
	opts.Middlewares = []resilix.Middleware{record("m1"), record("m2")}
	client, err := resilix.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.KV().Get(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{"m1:before", "m2:before", "m2:after", "m1:after"}
	if len(order) != len(want) {
		t.Fatalf("order len %d want %d: %v", len(order), len(want), order)
	}
	for i, v := range want {
		if order[i] != v {
			t.Fatalf("order[%d] = %q want %q", i, order[i], v)
		}
	}
}

func TestMiddleware_CanAbort(t *testing.T) {
	boom := errors.New("middleware abort")
	abort := func(next resilix.Handler) resilix.Handler {
		return func(ctx context.Context, op resilix.Operation) (any, error) {
			return nil, boom
		}
	}

	mock := resilixtest.NewMockBackend()
	opts := resilix.DefaultOptions()
	opts.Backend = mock
	opts.Middlewares = []resilix.Middleware{abort}
	client, err := resilix.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.KV().Get(context.Background(), "k")
	if !errors.Is(err, boom) {
		t.Fatalf("want middleware error, got %v", err)
	}
}

// ============================================================
// Before/After hooks
// ============================================================

func TestHooks_BeforeExecute_Fired(t *testing.T) {
	var ops []string
	mu := &sync.Mutex{}

	mock := resilixtest.NewMockBackend()
	mock.SetKV("k", "v")

	opts := resilix.DefaultOptions()
	opts.Backend = mock
	opts.Hooks = resilix.HookSet{
		BeforeExecute: []resilix.BeforeExecuteHook{
			func(_ context.Context, op resilix.Operation) error {
				mu.Lock()
				ops = append(ops, op.Name)
				mu.Unlock()
				return nil
			},
		},
	}
	client, err := resilix.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.KV().Get(context.Background(), "k")

	mu.Lock()
	defer mu.Unlock()
	if len(ops) == 0 || ops[0] != "GET" {
		t.Fatalf("want [GET], got %v", ops)
	}
}

func TestHooks_BeforeExecute_CanAbort(t *testing.T) {
	boom := errors.New("hook abort")
	mock := resilixtest.NewMockBackend()
	opts := resilix.DefaultOptions()
	opts.Backend = mock
	opts.Hooks = resilix.HookSet{
		BeforeExecute: []resilix.BeforeExecuteHook{
			func(_ context.Context, _ resilix.Operation) error { return boom },
		},
	}
	client, err := resilix.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.KV().Get(context.Background(), "k")
	if !errors.Is(err, boom) {
		t.Fatalf("want hook error, got %v", err)
	}
}

func TestHooks_AfterExecute_Fired(t *testing.T) {
	var durations []time.Duration
	mu := &sync.Mutex{}

	mock := resilixtest.NewMockBackend()
	mock.SetKV("k", "v")

	opts := resilix.DefaultOptions()
	opts.Backend = mock
	opts.Hooks = resilix.HookSet{
		AfterExecute: []resilix.AfterExecuteHook{
			func(_ context.Context, _ resilix.Operation, d time.Duration, _ error) {
				mu.Lock()
				durations = append(durations, d)
				mu.Unlock()
			},
		},
	}
	client, err := resilix.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.KV().Get(context.Background(), "k")

	mu.Lock()
	defer mu.Unlock()
	if len(durations) == 0 {
		t.Fatal("AfterExecute hook not fired")
	}
	if durations[0] < 0 {
		t.Fatalf("negative duration: %v", durations[0])
	}
}

// ============================================================
// Admission controller (backpressure)
// ============================================================

func TestAdmissionController_LimitsInflight(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{}, 10)

	mock := resilixtest.NewMockBackend()
	// Slow backend: blocks until released
	_ = mock

	opts := resilix.DefaultOptions()
	opts.Backend = &slowBackend{MockBackend: mock, block: block}
	opts.MaxInflight = 2
	// no retry to keep test fast
	opts.Retry.MaxAttempts = 1

	client, err := resilix.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			client.KV().Get(ctx, "k")
		}()
	}

	// Wait for 2 inflight to start
	<-started
	<-started

	// 3rd goroutine should hit backpressure or context deadline
	ctxShort, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelShort()
	_, err = client.KV().Get(ctxShort, "k")
	if err == nil {
		t.Fatal("want backpressure or deadline error for 3rd inflight over limit=2")
	}

	close(block)
	wg.Wait()
}

// slowBackend wraps a MockBackend and blocks Get until block is closed.
type slowBackend struct {
	*resilixtest.MockBackend
	block <-chan struct{}
}

func (s *slowBackend) Get(ctx context.Context, key string) (string, error) {
	select {
	case <-s.block:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return s.MockBackend.Get(ctx, key)
}

// ============================================================
// Graceful shutdown drain
// ============================================================

func TestClose_DrainInflight(t *testing.T) {
	released := make(chan struct{})
	mock := resilixtest.NewMockBackend()

	opts := resilix.DefaultOptions()
	opts.Backend = &blockingBackend{MockBackend: mock, released: released}
	opts.Retry.MaxAttempts = 1

	client, err := resilix.New(opts)
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		close(started)
		_, err := client.KV().Get(context.Background(), "k")
		done <- err
	}()

	<-started
	// Small sleep to let the goroutine enter execute()
	time.Sleep(10 * time.Millisecond)

	closeErr := make(chan error, 1)
	go func() {
		closeErr <- client.Close()
	}()

	// Unblock the inflight
	close(released)

	select {
	case err := <-done:
		_ = err // may be ErrKeyNotFound or nil
	case <-time.After(time.Second):
		t.Fatal("inflight goroutine did not finish")
	}

	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after drain")
	}
}

func TestClose_RejectsNewOps(t *testing.T) {
	mock := resilixtest.NewMockBackend()
	client := resilixtest.NewClient(t, mock)
	client.Close()

	_, err := client.KV().Get(context.Background(), "k")
	if !errors.Is(err, resilix.ErrClientClosed) {
		t.Fatalf("want ErrClientClosed, got %v", err)
	}
}

// blockingBackend blocks Get until released is closed.
type blockingBackend struct {
	*resilixtest.MockBackend
	released <-chan struct{}
}

func (b *blockingBackend) Get(ctx context.Context, key string) (string, error) {
	select {
	case <-b.released:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return b.MockBackend.Get(ctx, key)
}

// ============================================================
// Configurable retry classifier
// ============================================================

func TestRetryClassifier_Custom(t *testing.T) {
	customErr := errors.New("custom-retryable")
	mock := resilixtest.NewMockBackend()

	// Inject error twice — with default classifier it won't retry (not a network error).
	// With custom classifier, it should retry and eventually fail after MaxAttempts.
	attempts := 0
	mock.InjectError("Get", customErr)
	mock.InjectError("Get", customErr)
	mock.InjectError("Get", customErr)

	opts := resilix.DefaultOptions()
	opts.Backend = mock
	opts.Retry.MaxAttempts = 3
	opts.Retry.BaseDelay = time.Millisecond
	opts.Retry.Classifier = func(err error) bool {
		return errors.Is(err, customErr)
	}

	client, err := resilix.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.KV().Get(context.Background(), "k")
	if err == nil {
		t.Fatal("want error after all retries exhausted")
	}

	// Should have tried 3 times
	if got := mock.CallCount("Get"); got != 3 {
		attempts = got
		t.Fatalf("want 3 attempts with custom classifier, got %d", attempts)
	}
}

// ============================================================
// Error types
// ============================================================

func TestOperationError_Wrapped(t *testing.T) {
	mock := resilixtest.NewMockBackend()
	boom := errors.New("driver failure")
	mock.InjectError("Get", boom)

	client := resilixtest.NewClient(t, mock)
	_, err := client.KV().Get(context.Background(), "k")

	var opErr resilix.OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("want OperationError, got %T: %v", err, err)
	}
	if opErr.Operation != "GET" {
		t.Fatalf("want operation GET, got %q", opErr.Operation)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("want wrapped cause %v", boom)
	}
}

func TestValidationError_EmptyKey(t *testing.T) {
	_, client := setup(t)
	_, err := client.KV().Get(context.Background(), "")
	var valErr resilix.ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("want ValidationError, got %T: %v", err, err)
	}
}
