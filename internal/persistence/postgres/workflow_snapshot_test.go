package postgres

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestDecodeWorkflowDefinitionUsesCapturedValues(t *testing.T) {
	id, queue := uuid.New(), uuid.New()
	snapshot := json.RawMessage(`{"definition_id":"` + id.String() + `","queue_id":"` + queue.String() + `","function_key":"payment.capture","function_version":"v1","priority":3,"policy":{"timeout_ms":1000,"retry":{"max_attempts":3,"strategy":"FIXED"}}}`)
	got, err := decodeWorkflowDefinition(snapshot)
	if err != nil { t.Fatal(err) }
	if got.ID != id || got.QueueID != queue || got.FunctionKey != "payment.capture" || got.Policy.TimeoutMS != 1000 || got.Policy.Retry.MaxAttempts != 3 { t.Fatalf("snapshot decoded incorrectly: %+v", got) }
}
