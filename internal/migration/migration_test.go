package migration

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"testing/fstest"
)

func TestLoadOrdersAndChecksumsMigrations(t *testing.T) {
	fsys := fstest.MapFS{
		"010_later.sql": {Data: []byte("SELECT 10;")},
		"002_first.sql": {Data: []byte("SELECT 2;")},
		"README.md":     {Data: []byte("ignored")},
	}
	got, err := Load(fsys)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Version != 2 || got[1].Version != 10 {
		t.Fatalf("unexpected migration order: %#v", got)
	}
	digest := sha256.Sum256([]byte("SELECT 2;"))
	if got[0].Checksum != hex.EncodeToString(digest[:]) {
		t.Fatalf("checksum = %q", got[0].Checksum)
	}
}

func TestLoadRejectsDuplicateVersions(t *testing.T) {
	_, err := Load(fstest.MapFS{
		"001_first.sql":  {Data: []byte("SELECT 1;")},
		"001_second.sql": {Data: []byte("SELECT 2;")},
	})
	if err == nil {
		t.Fatal("expected duplicate version error")
	}
}
