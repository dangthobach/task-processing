package scheduling

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSkipAdvancesCursorWithoutReplaying(t *testing.T) {
	created := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	spec := ScheduleSpec{ID: uuid.New(), Type: "CRON", Cron: "* * * * *", Timezone: "UTC", MisfirePolicy: "SKIP", CreatedAt: created}
	decision, err := Evaluate(spec, nil, created.Add(3*time.Minute+30*time.Second))
	if err != nil || !decision.EvaluatedThrough.Equal(created.Add(3*time.Minute)) || decision.Occurrence != nil {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	next, err := Evaluate(spec, &decision.EvaluatedThrough, created.Add(3*time.Minute+45*time.Second))
	if err != nil || !next.EvaluatedThrough.Equal(decision.EvaluatedThrough) {
		t.Fatalf("next=%+v err=%v", next, err)
	}
}

func TestFireOnceUsesLatestMissedOccurrence(t *testing.T) {
	created := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	spec := ScheduleSpec{ID: uuid.New(), DefinitionID: uuid.New(), ProjectID: uuid.New(), Type: "CRON", Cron: "* * * * *", Timezone: "UTC", MisfirePolicy: "FIRE_ONCE", CreatedAt: created}
	decision, err := Evaluate(spec, nil, created.Add(3*time.Minute+30*time.Second))
	if err != nil || decision.Occurrence == nil || !decision.Occurrence.ScheduledFor.Equal(created.Add(3*time.Minute)) {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}

func TestRejectsCompetingTimezonePrefix(t *testing.T) {
	_, err := (ScheduleSpec{Cron: "CRON_TZ=UTC * * * * *", Timezone: "UTC"}).CronExpression()
	if err == nil {
		t.Fatal("expected timezone prefix rejection")
	}
}
