package postgres

import (
	"os"
	"strings"
	"testing"
)

func TestIdempotencyUsesExplicitConflictTargetsAndFreshLookup(t *testing.T) {
	source, err := os.ReadFile("store.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, want := range []string{
		"ON CONFLICT (project_id,idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING",
		"ON CONFLICT (schedule_id,scheduled_for) WHERE schedule_id IS NOT NULL DO NOTHING",
		"func readExistingRun",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing idempotency protocol fragment %q", want)
		}
	}
	if strings.Contains(text, "WITH input AS (") {
		t.Fatal("single-statement idempotency CTE must not return")
	}
}
