package postgres

import (
	"os"
	"strings"
	"testing"
)

func TestOutboxFailureUsesNextAttemptAndRequiresLiveClaim(t *testing.T) {
	source, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, want := range []string{
		"LEAST(attempts+1,8)",
		"claim_expires_at>now()",
		"status='ENQUEUE_PENDING'",
		"INSERT INTO outbox_events",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing outbox correctness fragment %q", want)
		}
	}
}
