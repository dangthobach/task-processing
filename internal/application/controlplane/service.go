// Package controlplane contains validation rules for mutable control-plane
// aggregates. HTTP handlers own transport concerns only; this package keeps
// business invariants reusable by future transports.
package controlplane

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

type Patch map[string]json.RawMessage

var allowed = map[string]map[string]struct{}{
	"queue":               {"name": {}, "backend_id": {}, "max_concurrency": {}, "default_priority": {}},
	"retry_policy":        {"name": {}, "max_attempts": {}, "strategy": {}, "initial_delay_ms": {}, "multiplier": {}, "max_delay_ms": {}, "jitter_pct": {}, "retry_timeout": {}, "retry_rate_limited": {}, "retry_dependency_error": {}, "retry_validation_error": {}},
	"rate_limit_policy":   {"name": {}, "capacity": {}, "refill_tokens": {}, "refill_period_ms": {}, "status": {}},
	"function_definition": {"input_schema": {}, "status": {}},
	"job_definition":      {"name": {}, "retry_policy_id": {}, "default_priority": {}, "timeout_ms": {}, "execution_mode": {}, "batch_size": {}, "batch_max_wait_ms": {}, "status": {}},
	"schedule":            {"cron_expression": {}, "timezone": {}, "with_seconds": {}, "run_at": {}, "status": {}},
}

func ValidatePatch(resource string, patch Patch) error {
	fields, ok := allowed[resource]
	if !ok {
		return fmt.Errorf("unsupported control-plane resource")
	}
	if len(patch) == 0 {
		return fmt.Errorf("at least one mutable field is required")
	}
	for name, raw := range patch {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("%s cannot be changed", name)
		}
		if len(raw) == 0 || !json.Valid(raw) {
			return fmt.Errorf("%s must be valid JSON", name)
		}
	}
	switch resource {
	case "queue":
		if err := nonBlank(patch, "name"); err != nil {
			return err
		}
		if err := positiveInt(patch, "max_concurrency", 1, 100000); err != nil {
			return err
		}
		return positiveInt(patch, "default_priority", 1, 5)
	case "retry_policy":
		if err := nonBlank(patch, "name"); err != nil {
			return err
		}
		if err := positiveInt(patch, "max_attempts", 1, 100000); err != nil {
			return err
		}
		if err := enum(patch, "strategy", "FIXED", "EXPONENTIAL"); err != nil {
			return err
		}
		if err := positiveInt(patch, "initial_delay_ms", 0, 604800000); err != nil {
			return err
		}
		if err := positiveInt(patch, "max_delay_ms", 0, 604800000); err != nil {
			return err
		}
		if err := decimal(patch, "multiplier", 1, 1000); err != nil {
			return err
		}
		if err := decimal(patch, "jitter_pct", 0, 100); err != nil {
			return err
		}
		return booleans(patch, "retry_timeout", "retry_rate_limited", "retry_dependency_error", "retry_validation_error")
	case "rate_limit_policy":
		if err := nonBlank(patch, "name"); err != nil {
			return err
		}
		if err := positiveInt(patch, "capacity", 1, 1000000000); err != nil {
			return err
		}
		if err := positiveInt(patch, "refill_tokens", 1, 1000000000); err != nil {
			return err
		}
		if err := positiveInt(patch, "refill_period_ms", 1, 86400000); err != nil {
			return err
		}
		return enum(patch, "status", "ACTIVE", "DISABLED")
	case "function_definition":
		if raw, ok := patch["input_schema"]; ok {
			trimmed := bytes.TrimSpace(raw)
			if bytes.Equal(trimmed, []byte("null")) || !json.Valid(trimmed) || len(trimmed) == 0 || trimmed[0] != '{' {
				return fmt.Errorf("input_schema must be a JSON object")
			}
		}
		return enum(patch, "status", "ACTIVE", "DISABLED")
	case "job_definition":
		if err := nonBlank(patch, "name"); err != nil {
			return err
		}
		if err := nullableUUID(patch, "retry_policy_id"); err != nil {
			return err
		}
		if err := positiveInt(patch, "default_priority", 1, 5); err != nil {
			return err
		}
		if err := positiveInt(patch, "timeout_ms", 1, 86400000); err != nil {
			return err
		}
		if err := enum(patch, "execution_mode", "SINGLE", "BATCH"); err != nil {
			return err
		}
		if err := positiveInt(patch, "batch_size", 2, 1000); err != nil {
			return err
		}
		if err := positiveInt(patch, "batch_max_wait_ms", 0, 3600000); err != nil {
			return err
		}
		return enum(patch, "status", "ACTIVE", "DISABLED")
	case "schedule":
		if err := nonBlank(patch, "cron_expression"); err != nil {
			return err
		}
		if err := timezone(patch); err != nil {
			return err
		}
		if err := booleans(patch, "with_seconds"); err != nil {
			return err
		}
		if err := enum(patch, "status", "ACTIVE", "PAUSED", "DISABLED"); err != nil {
			return err
		}
		if raw, ok := patch["run_at"]; ok {
			var value string
			if err := json.Unmarshal(raw, &value); err != nil {
				return fmt.Errorf("run_at must be RFC3339")
			}
			if _, err := time.Parse(time.RFC3339, value); err != nil {
				return fmt.Errorf("run_at must be RFC3339")
			}
		}
	}
	return nil
}

func ValidateScheduleExpression(expression, timezoneName string, withSeconds bool) error {
	if strings.TrimSpace(expression) == "" {
		return fmt.Errorf("cron_expression is required")
	}
	if _, err := time.LoadLocation(timezoneName); err != nil {
		return fmt.Errorf("timezone is invalid")
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	if withSeconds {
		parser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	}
	if _, err := parser.Parse(expression); err != nil {
		return fmt.Errorf("cron_expression is invalid")
	}
	return nil
}

func ValidateRetryPolicyValues(strategy string, initialDelay, maxDelay int64, multiplier float64) error {
	if initialDelay > maxDelay {
		return fmt.Errorf("initial_delay_ms cannot exceed max_delay_ms")
	}
	if strategy == "EXPONENTIAL" && multiplier <= 1 {
		return fmt.Errorf("multiplier must be greater than 1 for EXPONENTIAL strategy")
	}
	return nil
}

func nonBlank(p Patch, field string) error {
	raw, ok := p[field]
	if !ok {
		return nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || strings.TrimSpace(value) == "" || len(value) > 255 {
		return fmt.Errorf("%s must be a non-blank string up to 255 characters", field)
	}
	return nil
}
func positiveInt(p Patch, field string, min, max int64) error {
	raw, ok := p[field]
	if !ok {
		return nil
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil || value < min || value > max {
		return fmt.Errorf("%s must be between %d and %d", field, min, max)
	}
	return nil
}
func decimal(p Patch, field string, min, max float64) error {
	raw, ok := p[field]
	if !ok {
		return nil
	}
	var value float64
	if err := json.Unmarshal(raw, &value); err != nil || value < min || value > max {
		return fmt.Errorf("%s must be between %g and %g", field, min, max)
	}
	return nil
}
func enum(p Patch, field string, values ...string) error {
	raw, ok := p[field]
	if !ok {
		return nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("%s is invalid", field)
	}
	for _, allowed := range values {
		if value == allowed {
			return nil
		}
	}
	return fmt.Errorf("%s is invalid", field)
}
func booleans(p Patch, fields ...string) error {
	for _, field := range fields {
		raw, ok := p[field]
		if !ok {
			continue
		}
		var value bool
		if err := json.Unmarshal(raw, &value); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("%s must be boolean", field)
		}
	}
	return nil
}
func nullableUUID(p Patch, field string) error {
	raw, ok := p[field]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("%s must be a UUID or null", field)
	}
	if _, err := uuid.Parse(value); err != nil {
		return fmt.Errorf("%s must be a UUID or null", field)
	}
	return nil
}
func timezone(p Patch) error {
	raw, ok := p["timezone"]
	if !ok {
		return nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("timezone is invalid")
	}
	if _, err := time.LoadLocation(value); err != nil {
		return fmt.Errorf("timezone is invalid")
	}
	return nil
}
