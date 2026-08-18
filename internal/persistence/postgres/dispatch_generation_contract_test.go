package postgres

import (
	"os"
	"strings"
	"testing"
)

// These source-level contract checks protect the cross-backend invariant until
// Redis Streams and JetStream adapters exercise it in integration tests.
func TestDispatchGenerationIsFencedAtEveryTransition(t *testing.T) {
	source, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, want := range []string{
		"current_dispatch_id=$2 AND status='ENQUEUE_PENDING'",
		"current_dispatch_id=gen_random_uuid()",
		"o.dispatch_id=r.current_dispatch_id",
		"TakeDispatchOwnership",
		"r.current_dispatch_id=$2 AND r.status='QUEUED'",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing fenced dispatch transition %q", want)
		}
	}
}
