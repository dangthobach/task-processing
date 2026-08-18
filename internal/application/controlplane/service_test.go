package controlplane

import (
	"encoding/json"
	"testing"
)

func TestValidatePatchRejectsImmutableAndInvalidValues(t *testing.T) {
	cases := []struct {
		name     string
		resource string
		patch    Patch
	}{
		{"immutable project", "queue", Patch{"project_id": json.RawMessage("\"00000000-0000-0000-0000-000000000000\"")}},
		{"zero concurrency", "queue", Patch{"max_concurrency": json.RawMessage("0")}},
		{"invalid retry enum", "retry_policy", Patch{"strategy": json.RawMessage("\"LINEAR\"")}},
		{"bad retry reference", "job_definition", Patch{"retry_policy_id": json.RawMessage("\"not-a-uuid\"")}},
		{"invalid batch size", "job_definition", Patch{"batch_size": json.RawMessage("1")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidatePatch(tc.resource, tc.patch); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestRetryPolicyCrossFieldValidation(t *testing.T) {
	if err := ValidateRetryPolicyValues("EXPONENTIAL", 100, 10, 2); err == nil {
		t.Fatal("initial delay above maximum must fail")
	}
	if err := ValidateRetryPolicyValues("EXPONENTIAL", 10, 100, 1); err == nil {
		t.Fatal("non-growing exponential policy must fail")
	}
	if err := ValidateRetryPolicyValues("FIXED", 10, 10, 1); err != nil {
		t.Fatalf("fixed policy should be valid: %v", err)
	}
}

func TestScheduleValidation(t *testing.T) {
	if err := ValidateScheduleExpression("not a cron", "UTC", false); err == nil {
		t.Fatal("invalid cron must fail")
	}
	if err := ValidateScheduleExpression("*/5 * * * *", "Mars/Olympus", false); err == nil {
		t.Fatal("invalid timezone must fail")
	}
	if err := ValidateScheduleExpression("*/5 * * * *", "UTC", false); err != nil {
		t.Fatalf("valid cron should pass: %v", err)
	}
}
