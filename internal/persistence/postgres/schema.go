package postgres

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// SchemaValidator caches compiled schemas by content hash. Function versions
// are immutable, so a changed contract gets a different cache key.
type SchemaValidator struct{ compiled sync.Map }

func (v *SchemaValidator) Validate(schema, payload json.RawMessage) error {
	if len(schema) == 0 || string(schema) == "{}" {
		return nil
	}
	if !json.Valid(schema) {
		return fmt.Errorf("stored input_schema is invalid JSON")
	}
	if !json.Valid(payload) {
		return fmt.Errorf("payload must be valid JSON")
	}
	hash := sha256.Sum256(schema)
	key := hex.EncodeToString(hash[:])
	var compiled *jsonschema.Schema
	if cached, ok := v.compiled.Load(key); ok {
		compiled = cached.(*jsonschema.Schema)
	} else {
		compiler := jsonschema.NewCompiler()
		if err := compiler.AddResource("schema.json", strings.NewReader(string(schema))); err != nil {
			return fmt.Errorf("invalid input_schema: %w", err)
		}
		value, err := compiler.Compile("schema.json")
		if err != nil {
			return fmt.Errorf("invalid input_schema: %w", err)
		}
		actual, _ := v.compiled.LoadOrStore(key, value)
		compiled = actual.(*jsonschema.Schema)
	}
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return fmt.Errorf("payload must be valid JSON: %w", err)
	}
	if err := compiled.Validate(value); err != nil {
		return fmt.Errorf("payload does not satisfy function input_schema: %w", err)
	}
	return nil
}

func ValidateInputSchema(schema json.RawMessage) error {
	if len(schema) == 0 {
		return nil
	}
	// Compile now so an invalid contract cannot enter the control plane.
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("schema.json", strings.NewReader(string(schema))); err != nil {
		return fmt.Errorf("invalid input_schema: %w", err)
	}
	if _, err := compiler.Compile("schema.json"); err != nil {
		return fmt.Errorf("invalid input_schema: %w", err)
	}
	return nil
}
