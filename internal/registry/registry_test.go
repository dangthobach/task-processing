package registry

import (
	"testing"

	"github.com/example/task-processing/internal/domain/job"
)

func TestVersionedHandlerPrefersExactThenWildcard(t *testing.T) {
	registry := New()
	wildcard := func(*job.ExecutionContext) error { return nil }
	exact := func(*job.ExecutionContext) error { return nil }
	if err := registry.Register("payments.capture", wildcard); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterVersion("payments.capture", "v2", exact); err != nil {
		t.Fatal(err)
	}
	handler, err := registry.GetVersion("payments.capture", "v2")
	if err != nil || handler == nil {
		t.Fatalf("handler=%v err=%v", handler, err)
	}
	if _, err = registry.GetVersion("payments.capture", "v1"); err != nil {
		t.Fatalf("wildcard err=%v", err)
	}
}

func TestRegistryRejectsDuplicateAndPublishesCapabilities(t *testing.T) {
	registry := New()
	if err := registry.RegisterVersion("payments.capture", "v1", func(*job.ExecutionContext) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterVersion("payments.capture", "v1", func(*job.ExecutionContext) error { return nil }); err == nil {
		t.Fatal("expected duplicate error")
	}
	if err := registry.RegisterBatchVersion("payments.capture", "v1", func(*job.BatchExecutionContext) ([]job.BatchItemResult, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	capabilities := registry.Capabilities()
	if len(capabilities) != 2 || capabilities[0].ExecutionMode != "BATCH" || capabilities[1].ExecutionMode != "SINGLE" {
		t.Fatalf("capabilities=%+v", capabilities)
	}
}
