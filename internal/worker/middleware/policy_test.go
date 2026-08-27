package middleware

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/task-processing/internal/domain/job"
)

func executionContext() *job.ExecutionContext {
	return job.NewExecutionContext(context.Background(), &job.Execution{FunctionKey: "billing.charge", FunctionVersion: "v1"}, nil)
}

func TestCircuitBreakerFailsFastAndRecoversAfterProbe(t *testing.T) {
	breaker := NewCircuitBreaker(CircuitBreakerConfig{ConsecutiveFailures: 2, OpenFor: 10 * time.Millisecond, HalfOpenMax: 1})
	failing := breaker.Middleware()(func(*job.ExecutionContext) error { return errors.New("dependency down") })
	for range 2 {
		if err := failing(executionContext()); err == nil {
			t.Fatal("expected handler failure")
		}
	}
	err := failing(executionContext())
	var classified *job.ClassifiedError
	if !errors.As(err, &classified) || classified.Code != "CIRCUIT_OPEN" {
		t.Fatalf("err=%v", err)
	}
	time.Sleep(15 * time.Millisecond)
	if err = breaker.Middleware()(func(*job.ExecutionContext) error { return nil })(executionContext()); err != nil {
		t.Fatalf("half-open success should close breaker: %v", err)
	}
}

func TestCircuitBreakerStaleSuccessCannotCloseNewerOpenGeneration(t *testing.T) {
	breaker := NewCircuitBreaker(CircuitBreakerConfig{ConsecutiveFailures: 1, OpenFor: time.Hour})
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	handler := breaker.Middleware()(func(*job.ExecutionContext) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return nil
		}
		return errors.New("dependency down")
	})
	done := make(chan error, 1)
	go func() { done <- handler(executionContext()) }()
	<-started
	if err := handler(executionContext()); err == nil {
		t.Fatal("expected failure")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("stale success err=%v", err)
	}
	err := handler(executionContext())
	var classified *job.ClassifiedError
	if !errors.As(err, &classified) || classified.Code != "CIRCUIT_OPEN" {
		t.Fatalf("breaker was incorrectly closed: %v", err)
	}
}

func TestBulkheadNeverWaitsAndReleasesCapacity(t *testing.T) {
	bulkhead := NewBulkhead(BulkheadConfig{Limit: 1})
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var enteredOnce sync.Once
	var doneOnce sync.Once
	handler := bulkhead.Middleware()(func(*job.ExecutionContext) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		doneOnce.Do(func() { close(done) })
		return nil
	})
	go func() { _ = handler(executionContext()) }()
	<-entered
	start := time.Now()
	err := handler(executionContext())
	if time.Since(start) > 20*time.Millisecond {
		t.Fatal("bulkhead waited instead of shedding")
	}
	var classified *job.ClassifiedError
	if !errors.As(err, &classified) || classified.Code != "BULKHEAD_FULL" || classified.Class != job.RateLimited {
		t.Fatalf("err=%v", err)
	}
	close(release)
	<-done
	if err = handler(executionContext()); err != nil {
		t.Fatalf("capacity was not released: %v", err)
	}
}

func TestBulkheadConcurrencyNeverExceedsLimit(t *testing.T) {
	bulkhead := NewBulkhead(BulkheadConfig{Limit: 3})
	var current, maximum int
	var mu sync.Mutex
	handler := bulkhead.Middleware()(func(*job.ExecutionContext) error {
		mu.Lock()
		current++
		if current > maximum {
			maximum = current
		}
		mu.Unlock()
		time.Sleep(time.Millisecond)
		mu.Lock()
		current--
		mu.Unlock()
		return nil
	})
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = handler(executionContext()) }()
	}
	wg.Wait()
	if maximum > 3 {
		t.Fatalf("maximum=%d", maximum)
	}
}

func TestDeadlineGuardDoesNotInvokeHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond)
	called := false
	err := DeadlineGuard(time.Second)(func(*job.ExecutionContext) error { called = true; return nil })(job.NewExecutionContext(ctx, &job.Execution{}, nil))
	if called || err == nil {
		t.Fatalf("called=%t err=%v", called, err)
	}
}
