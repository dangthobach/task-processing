package worker

import "testing"

func TestLoadConfigRejectsUnsafeLease(t *testing.T) {
	_, err := LoadConfig(func(name string) string {
		if name == "WORKER_LEASE_SECONDS" {
			return "4"
		}
		return ""
	})
	if err == nil {
		t.Fatal("expected lease validation error")
	}
}
func TestSlotsReserveAtomically(t *testing.T) {
	s := newSlots(3)
	if got := s.ReserveAvailable(); got != 3 {
		t.Fatalf("reserve=%d", got)
	}
	if got := s.ReserveAvailable(); got != 0 {
		t.Fatalf("reserve=%d", got)
	}
	s.ReleaseUnused(1)
	if got := s.ReserveAvailable(); got != 1 {
		t.Fatalf("reserve=%d", got)
	}
}
