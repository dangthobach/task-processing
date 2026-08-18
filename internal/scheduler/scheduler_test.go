package scheduler

import (
	"testing"
	"time"

	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
)

func TestFingerprintIncludesCapturedRoutingFields(t *testing.T) {
	runAt := time.Now().UTC()
	base := postgres.SchedulePlanState{ID: uuid.New(), ProjectID: uuid.New(), DefinitionID: uuid.New(), Type: "CRON", Cron: "* * * * *", Timezone: "UTC", RunAt: &runAt, MisfirePolicy: "SKIP"}
	changedDefinition := base
	changedDefinition.DefinitionID = uuid.New()
	changedProject := base
	changedProject.ProjectID = uuid.New()
	changedTimezone := base
	changedTimezone.Timezone = "Asia/Ho_Chi_Minh"
	for _, changed := range []postgres.SchedulePlanState{changedDefinition, changedProject, changedTimezone} {
		if fingerprint(base) == fingerprint(changed) {
			t.Fatalf("fingerprint did not change for %+v", changed)
		}
	}
}
