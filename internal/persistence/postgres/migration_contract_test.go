package postgres

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Guards the subtle collision between semantic versions (handler/build) and
// optimistic-lock versions. PostgreSQL integration tests additionally execute
// these scripts when TEST_DATABASE_URL is available.
func TestGovernanceMigrationUsesRowVersion(t *testing.T) {
	path := filepath.Join("..", "..", "..", "migrations", "006_api_governance.sql")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sql := string(b)
	if !strings.Contains(sql, "row_version bigint") || !strings.Contains(sql, "NEW.row_version := OLD.row_version + 1") {
		t.Fatal("governance migration must use row_version")
	}
	if strings.Contains(sql, "ADD COLUMN IF NOT EXISTS version bigint") {
		t.Fatal("row version must never reuse semantic version column")
	}
}
