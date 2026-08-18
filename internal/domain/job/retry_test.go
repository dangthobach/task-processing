package job

import (
	"testing"
	"time"
)

func TestNextRetryExponentialBoundedWithJitter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := RetryPolicy{Strategy: "EXPONENTIAL", InitialDelayMS: 1000, Multiplier: 2, MaxDelayMS: 3000, JitterPct: 20}
	got := NextRetry(now, 4, p, func() float64 { return 1 })
	if got.Sub(now) != 3600*time.Millisecond {
		t.Fatalf("delay = %s", got.Sub(now))
	}
}
func TestRetryable(t *testing.T) {
	p := RetryPolicy{RetryTimeout: true}
	if !Retryable(Timeout, p) || Retryable(Permanent, p) {
		t.Fatal("unexpected retry classification")
	}
}
