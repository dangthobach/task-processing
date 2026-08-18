package worker

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

type Intervals struct {
	Claim, Outbox, AuditOutbox, RetryPromotion time.Duration
	LeaseRecovery, BatchRecovery, Heartbeat    time.Duration
}

type Config struct {
	Concurrency  int
	Lease        time.Duration
	DrainTimeout time.Duration
	Intervals    Intervals
}

func DefaultConfig() Config {
	return Config{Concurrency: 10, Lease: time.Minute, DrainTimeout: 30 * time.Second, Intervals: Intervals{
		Claim: 250 * time.Millisecond, Outbox: 500 * time.Millisecond, AuditOutbox: time.Second,
		RetryPromotion: time.Second, LeaseRecovery: 5 * time.Second, BatchRecovery: 5 * time.Second, Heartbeat: 10 * time.Second,
	}}
}

// LoadConfig validates all worker timing inputs at process start. Invalid
// configuration is intentionally fatal to composition roots, never to the
// worker loop itself.
func LoadConfig(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := DefaultConfig()
	var err error
	if cfg.Concurrency, err = positiveInt(getenv, "WORKER_CONCURRENCY", cfg.Concurrency, 1); err != nil {
		return Config{}, err
	}
	if cfg.Lease, err = seconds(getenv, "WORKER_LEASE_SECONDS", cfg.Lease, 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.DrainTimeout, err = seconds(getenv, "WORKER_DRAIN_TIMEOUT_SECONDS", cfg.DrainTimeout, time.Second); err != nil {
		return Config{}, err
	}
	if cfg.Intervals.Claim, err = millis(getenv, "WORKER_CLAIM_INTERVAL_MS", cfg.Intervals.Claim, 50*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.Intervals.Outbox, err = millis(getenv, "WORKER_OUTBOX_INTERVAL_MS", cfg.Intervals.Outbox, 50*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.Intervals.AuditOutbox, err = millis(getenv, "WORKER_AUDIT_INTERVAL_MS", cfg.Intervals.AuditOutbox, 500*time.Millisecond); err != nil {
		return Config{}, err
	}
	if cfg.Intervals.RetryPromotion, err = millis(getenv, "WORKER_RETRY_INTERVAL_MS", cfg.Intervals.RetryPromotion, time.Second); err != nil {
		return Config{}, err
	}
	if cfg.Intervals.LeaseRecovery, err = millis(getenv, "WORKER_LEASE_RECOVERY_INTERVAL_MS", cfg.Intervals.LeaseRecovery, 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.Intervals.BatchRecovery, err = millis(getenv, "WORKER_BATCH_RECOVERY_INTERVAL_MS", cfg.Intervals.BatchRecovery, 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.Intervals.Heartbeat, err = millis(getenv, "WORKER_HEARTBEAT_INTERVAL_MS", cfg.Intervals.Heartbeat, 10*time.Second); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func positiveInt(getenv func(string) string, name string, fallback, minimum int) (int, error) {
	raw := getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum {
		return 0, fmt.Errorf("%s must be an integer >= %d", name, minimum)
	}
	return value, nil
}
func seconds(getenv func(string) string, name string, fallback, minimum time.Duration) (time.Duration, error) {
	value, err := positiveInt(getenv, name, int(fallback/time.Second), int(minimum/time.Second))
	return time.Duration(value) * time.Second, err
}
func millis(getenv func(string) string, name string, fallback, minimum time.Duration) (time.Duration, error) {
	value, err := positiveInt(getenv, name, int(fallback/time.Millisecond), int(minimum/time.Millisecond))
	return time.Duration(value) * time.Millisecond, err
}

// slots atomically reserves available execution capacity. A reservation is
// held before database claim, eliminating the check-then-act race.
type slots struct {
	mu          sync.Mutex
	limit, held int
}

func newSlots(limit int) *slots { return &slots{limit: limit} }
func (s *slots) ReserveAvailable() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.limit - s.held
	s.held += n
	return n
}
func (s *slots) ReleaseUnused(n int) { s.Release(n) }
func (s *slots) Release(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.held -= n
	if s.held < 0 {
		panic("worker slots released more than reserved")
	}
}
