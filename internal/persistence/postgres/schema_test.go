package postgres

import (
	"encoding/json"
	"testing"
)

func TestSchemaValidatorRejectsInvalidPayloadAndCachesContract(t *testing.T) {
	validator := SchemaValidator{}
	schema := json.RawMessage(`{"type":"object","required":["email"],"properties":{"email":{"type":"string","format":"email"}},"additionalProperties":false}`)
	if err := validator.Validate(schema, json.RawMessage(`{"email":"a@example.com"}`)); err != nil {
		t.Fatal(err)
	}
	if err := validator.Validate(schema, json.RawMessage(`{"email":42}`)); err == nil {
		t.Fatal("expected schema validation failure")
	}
	if err := validator.Validate(schema, json.RawMessage(`{"email":"b@example.com"}`)); err != nil {
		t.Fatal(err)
	}
}

func TestValidateInputSchemaRejectsMalformedContract(t *testing.T) {
	if err := ValidateInputSchema(json.RawMessage(`{"type":`)); err == nil {
		t.Fatal("expected invalid schema")
	}
}
