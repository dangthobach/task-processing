package migration

import (
	"os"
	"strings"
	"testing"
)

func TestBaselineDetectionIsScopedToCurrentSchema(t *testing.T) {
	for _, filename := range []string{"migration.go", "baseline.go"} {
		source, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(source), "n.nspname=current_schema()") {
			t.Fatalf("%s must scope baseline inspection to current_schema", filename)
		}
	}
}
