package middleware

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/task-processing/internal/domain/job"
)

// Timeout returns a middleware that only narrows an existing handler budget.
// It never creates a goroutine or waits for a handler after cancellation; a
// handler that ignores context cancellation is still bounded by the worker's
// lease fencing and terminal-write conditions.
func Timeout(limit time.Duration) Middleware {
	return func(next job.Handler) job.Handler {
		return func(execution *job.ExecutionContext) error {
			if limit <= 0 {
				return next(execution)
			}
			if deadline, ok := execution.Deadline(); ok && time.Until(deadline) <= limit {
				return next(execution)
			}
			ctx, cancel := context.WithTimeout(execution.Context, limit)
			defer cancel()
			child := *execution
			child.Context = ctx
			err := next(&child)
			if err == nil && ctx.Err() != nil {
				return &job.ClassifiedError{Class: job.Timeout, Code: "MIDDLEWARE_TIMEOUT", Err: ctx.Err()}
			}
			return err
		}
	}
}

// DeadlineGuard sheds work that no longer has enough useful execution budget.
// It is intentionally a local, lock-free admission guard; global concurrency
// and token-bucket admission remain authoritative in PostgreSQL.
func DeadlineGuard(minimum time.Duration) Middleware {
	return func(next job.Handler) job.Handler {
		return func(execution *job.ExecutionContext) error {
			if minimum > 0 {
				if deadline, ok := execution.Deadline(); ok && time.Until(deadline) < minimum {
					return &job.ClassifiedError{Class: job.Timeout, Code: "DEADLINE_BUDGET_EXHAUSTED", Err: context.DeadlineExceeded}
				}
			}
			return next(execution)
		}
	}
}

type CircuitBreakerConfig struct {
	// ConsecutiveFailures is the number of eligible failures before opening.
	ConsecutiveFailures int
	OpenFor             time.Duration
	HalfOpenMax         int
	// MaxKeys bounds memory if function keys are created dynamically. New keys
	// beyond the limit fail open (the job is still allowed) rather than turning
	// a metadata flood into an availability incident.
	MaxKeys int
	// TripOn optionally narrows which classified errors count. It must be pure
	// and non-blocking; panics are treated as false.
	TripOn func(*job.ClassifiedError) bool
}

type breakerMode int32

const (
	breakerClosed breakerMode = iota
	breakerOpen
	breakerHalfOpen
)

type breakerState struct {
	mode        atomic.Int32
	failures    atomic.Int32
	openUntilNS atomic.Int64
	probes      atomic.Int32
	epoch       atomic.Uint64
}

type breakerPermit struct {
	mode  breakerMode
	epoch uint64
}

// CircuitBreaker is process-local by design. It protects a single worker from
// hammering a dependency. It never claims to be a distributed breaker; that
// would require a separately governed shared state service and can otherwise
// become a hot global lock.
type CircuitBreaker struct {
	config CircuitBreakerConfig
	states sync.Map // map[string]*breakerState
	keys   atomic.Int64
}

func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	if config.ConsecutiveFailures < 1 {
		config.ConsecutiveFailures = 5
	}
	if config.OpenFor <= 0 {
		config.OpenFor = 30 * time.Second
	}
	if config.HalfOpenMax < 1 {
		config.HalfOpenMax = 1
	}
	if config.MaxKeys < 1 {
		config.MaxKeys = 10_000
	}
	return &CircuitBreaker{config: config}
}

func (b *CircuitBreaker) Middleware() Middleware {
	return func(next job.Handler) job.Handler {
		return func(execution *job.ExecutionContext) error {
			state := b.stateFor(key(execution))
			permit, allowed := b.allow(state, time.Now().UnixNano())
			if state == nil || allowed {
				err := next(execution)
				if state != nil {
					b.record(state, permit, err)
				}
				return err
			}
			return &job.ClassifiedError{Class: job.DependencyUnavailable, Code: "CIRCUIT_OPEN", Err: ErrCircuitOpen}
		}
	}
}

func (b *CircuitBreaker) stateFor(name string) *breakerState {
	if name == "" || len(name) > 512 {
		return nil
	}
	if existing, ok := b.states.Load(name); ok {
		return existing.(*breakerState)
	}
	if b.keys.Load() >= int64(b.config.MaxKeys) {
		return nil
	}
	candidate := &breakerState{}
	actual, loaded := b.states.LoadOrStore(name, candidate)
	if loaded {
		return actual.(*breakerState)
	}
	if b.keys.Add(1) > int64(b.config.MaxKeys) {
		b.states.Delete(name)
		b.keys.Add(-1)
		return nil
	}
	return candidate
}

func (b *CircuitBreaker) allow(state *breakerState, now int64) (breakerPermit, bool) {
	if state == nil {
		return breakerPermit{}, true
	}
	mode := breakerMode(state.mode.Load())
	if mode == breakerClosed {
		return breakerPermit{mode: breakerClosed, epoch: state.epoch.Load()}, true
	}
	if mode == breakerOpen {
		if now < state.openUntilNS.Load() {
			return breakerPermit{}, false
		}
		if state.mode.CompareAndSwap(int32(breakerOpen), int32(breakerHalfOpen)) {
			epoch := state.epoch.Add(1)
			state.probes.Store(1)
			return breakerPermit{mode: breakerHalfOpen, epoch: epoch}, true
		}
		mode = breakerMode(state.mode.Load())
	}
	if mode != breakerHalfOpen {
		if mode == breakerClosed {
			return breakerPermit{mode: breakerClosed, epoch: state.epoch.Load()}, true
		}
		return breakerPermit{}, false
	}
	for {
		probes := state.probes.Load()
		if probes >= int32(b.config.HalfOpenMax) {
			return breakerPermit{}, false
		}
		if state.probes.CompareAndSwap(probes, probes+1) {
			return breakerPermit{mode: breakerHalfOpen, epoch: state.epoch.Load()}, true
		}
	}
}

func (b *CircuitBreaker) record(state *breakerState, permit breakerPermit, err error) {
	// A response admitted before a concurrent transition must not close or
	// reset a newer open generation. This is the key race in naive breakers.
	if permit.epoch != state.epoch.Load() {
		return
	}
	if err == nil {
		if permit.mode == breakerHalfOpen {
			state.failures.Store(0)
			state.probes.Store(0)
			state.mode.Store(int32(breakerClosed))
		} else if breakerMode(state.mode.Load()) == breakerClosed {
			state.failures.Store(0)
		}
		return
	}
	classified := job.Classify(err)
	if !b.trips(classified) {
		return
	}
	if permit.mode == breakerHalfOpen || state.failures.Add(1) >= int32(b.config.ConsecutiveFailures) {
		state.openUntilNS.Store(time.Now().Add(b.config.OpenFor).UnixNano())
		state.probes.Store(0)
		state.epoch.Add(1)
		state.mode.Store(int32(breakerOpen))
	}
}

func (b *CircuitBreaker) trips(classified *job.ClassifiedError) (allowed bool) {
	if b.config.TripOn != nil {
		defer func() { _ = recover() }()
		return b.config.TripOn(classified)
	}
	switch classified.Class {
	case job.Transient, job.DependencyUnavailable, job.Timeout, job.Panic:
		return true
	default:
		return false
	}
}

type BulkheadConfig struct {
	Limit   int
	MaxKeys int
}

type bulkheadState struct{ inFlight atomic.Int32 }

// Bulkhead is a local, non-waiting concurrency guard. Rejection is classified
// as RATE_LIMITED so the durable retry policy decides when it is safe to try
// again. It is complementary to, not a replacement for, SQL global limits.
type Bulkhead struct {
	config BulkheadConfig
	states sync.Map // map[string]*bulkheadState
	keys   atomic.Int64
}

func NewBulkhead(config BulkheadConfig) *Bulkhead {
	if config.Limit < 1 {
		config.Limit = 1
	}
	if config.MaxKeys < 1 {
		config.MaxKeys = 10_000
	}
	return &Bulkhead{config: config}
}

func (b *Bulkhead) Middleware() Middleware {
	return func(next job.Handler) job.Handler {
		return func(execution *job.ExecutionContext) error {
			state := b.stateFor(key(execution))
			if state == nil || !tryAcquire(&state.inFlight, int32(b.config.Limit)) {
				if state == nil {
					return next(execution) // fail open when key cardinality is bounded.
				}
				return &job.ClassifiedError{Class: job.RateLimited, Code: "BULKHEAD_FULL", Err: ErrBulkheadFull}
			}
			defer state.inFlight.Add(-1)
			return next(execution)
		}
	}
}

func (b *Bulkhead) stateFor(name string) *bulkheadState {
	if name == "" || len(name) > 512 {
		return nil
	}
	if existing, ok := b.states.Load(name); ok {
		return existing.(*bulkheadState)
	}
	if b.keys.Load() >= int64(b.config.MaxKeys) {
		return nil
	}
	candidate := &bulkheadState{}
	actual, loaded := b.states.LoadOrStore(name, candidate)
	if loaded {
		return actual.(*bulkheadState)
	}
	if b.keys.Add(1) > int64(b.config.MaxKeys) {
		b.states.Delete(name)
		b.keys.Add(-1)
		return nil
	}
	return candidate
}

func tryAcquire(value *atomic.Int32, limit int32) bool {
	for {
		current := value.Load()
		if current >= limit {
			return false
		}
		if value.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func key(execution *job.ExecutionContext) string {
	if execution == nil || execution.Execution == nil {
		return ""
	}
	return execution.Execution.FunctionKey + "@" + execution.Execution.FunctionVersion
}
