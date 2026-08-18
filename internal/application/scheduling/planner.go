// Package scheduling owns deterministic schedule-occurrence semantics. Runtime
// schedulers only wake this planner; they never provide canonical timestamps.
package scheduling

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

const maxDueOccurrences = 10000

type ScheduleSpec struct {
	ID, DefinitionID, ProjectID uuid.UUID
	Type                        string
	Cron                        string
	WithSeconds                 bool
	Timezone                    string
	RunAt                       *time.Time
	MisfirePolicy               string
	CreatedAt                   time.Time
}

type Occurrence struct {
	ScheduleID, DefinitionID, ProjectID uuid.UUID
	ScheduledFor                        time.Time
}

type Decision struct {
	EvaluatedThrough time.Time
	Occurrence       *Occurrence
}

// CronExpression keeps timezone as a single source of truth. Persisted cron
// text is intentionally forbidden from carrying a competing TZ prefix.
func (s ScheduleSpec) CronExpression() (string, error) {
	cronText := strings.TrimSpace(s.Cron)
	upper := strings.ToUpper(cronText)
	if strings.HasPrefix(upper, "TZ=") || strings.HasPrefix(upper, "CRON_TZ=") {
		return "", fmt.Errorf("cron expression must not include a timezone prefix")
	}
	if cronText == "" {
		return "", fmt.Errorf("cron expression is required")
	}
	tz := s.Timezone
	if tz == "" {
		tz = "UTC"
	}
	if _, err := time.LoadLocation(tz); err != nil {
		return "", fmt.Errorf("invalid timezone %q: %w", tz, err)
	}
	return "CRON_TZ=" + tz + " " + cronText, nil
}

// Evaluate advances only through schedule boundaries that are due. For SKIP,
// all due boundaries are recorded as evaluated without creating a run. For
// FIRE_ONCE, exactly one run represents the latest missed boundary.
func Evaluate(spec ScheduleSpec, evaluatedThrough *time.Time, now time.Time) (Decision, error) {
	now = now.UTC()
	cursor := spec.CreatedAt.UTC()
	if evaluatedThrough != nil {
		cursor = evaluatedThrough.UTC()
	}
	if cursor.After(now) {
		return Decision{EvaluatedThrough: cursor}, nil
	}
	switch spec.Type {
	case "ONE_TIME":
		if spec.RunAt == nil {
			return Decision{}, fmt.Errorf("one-time schedule requires run_at")
		}
		runAt := spec.RunAt.UTC()
		if !runAt.After(cursor) || runAt.After(now) {
			return Decision{EvaluatedThrough: cursor}, nil
		}
		return fireDecision(spec, runAt)
	case "CRON":
		expr, err := spec.CronExpression()
		if err != nil {
			return Decision{}, err
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		if spec.WithSeconds {
			parser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		}
		schedule, err := parser.Parse(expr)
		if err != nil {
			return Decision{}, fmt.Errorf("parse cron: %w", err)
		}
		latest := time.Time{}
		for occurrence, count := schedule.Next(cursor), 0; !occurrence.After(now); occurrence, count = schedule.Next(occurrence), count+1 {
			if count >= maxDueOccurrences {
				return Decision{}, fmt.Errorf("too many due schedule occurrences")
			}
			latest = occurrence.UTC()
		}
		if latest.IsZero() {
			return Decision{EvaluatedThrough: cursor}, nil
		}
		return fireDecision(spec, latest)
	default:
		return Decision{}, fmt.Errorf("unsupported schedule type %q", spec.Type)
	}
}

func fireDecision(spec ScheduleSpec, latest time.Time) (Decision, error) {
	decision := Decision{EvaluatedThrough: latest}
	switch spec.MisfirePolicy {
	case "", "SKIP":
		return decision, nil
	case "FIRE_ONCE":
		decision.Occurrence = &Occurrence{ScheduleID: spec.ID, DefinitionID: spec.DefinitionID, ProjectID: spec.ProjectID, ScheduledFor: latest}
		return decision, nil
	default:
		return Decision{}, fmt.Errorf("unsupported misfire policy %q", spec.MisfirePolicy)
	}
}
