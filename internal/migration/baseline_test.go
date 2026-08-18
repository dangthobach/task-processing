package migration

import "testing"

func TestBaselineManifestCoversLatestKnownSchema(t *testing.T) {
	seenRateLimit := false
	seenWorkflow := false
	for _, requirement := range baselineRequirements {
		if requirement.introduced == 12 && requirement.table == "rate_limit_policies" {
			seenRateLimit = true
		}
		if requirement.introduced == 10 && requirement.table == "workflow_definitions" {
			seenWorkflow = true
		}
	}
	if !seenRateLimit || !seenWorkflow {
		t.Fatal("baseline manifest must guard late migration schema")
	}
}
