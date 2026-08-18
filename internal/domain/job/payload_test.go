package job

import (
	"encoding/json"
	"testing"
)

func TestNormalizePayloadDoesNotPanicForInvalidJSON(t *testing.T) {
	if _, err := NormalizePayload(json.RawMessage(`{`)); err == nil {
		t.Fatal("expected invalid payload error")
	}
	got, err := NormalizePayload(nil)
	if err != nil || string(got) != "{}" {
		t.Fatalf("normalized payload = %q, %v", got, err)
	}
}
