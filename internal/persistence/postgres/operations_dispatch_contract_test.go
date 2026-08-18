package postgres

import (
	"os"
	"strings"
	"testing"
)

func TestManualOperationsUseDispatchGenerationAndDurableOutbox(t *testing.T) {
	source, err := os.ReadFile("operations.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, want := range []string{
		"status='CANCELLED',current_dispatch_id=gen_random_uuid()",
		"status='ENQUEUE_PENDING',current_dispatch_id=gen_random_uuid()",
		"INSERT INTO outbox_events(project_id,event_type,aggregate_id,dispatch_id,payload)",
		"tx.Commit(ctx)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("manual lifecycle operation is missing %q", want)
		}
	}
}
