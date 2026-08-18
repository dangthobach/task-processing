package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type SchedulePlanState struct {
	ID, DefinitionID, ProjectID uuid.UUID
	Type                        string
	Cron                        string
	WithSeconds                 bool
	Timezone, MisfirePolicy     string
	RunAt, Cursor               *time.Time
	CreatedAt                   time.Time
}

func (s *Store) LoadSchedulePlan(ctx context.Context, id uuid.UUID) (SchedulePlanState, error) {
	var state SchedulePlanState
	err := s.Pool.QueryRow(ctx, `SELECT s.id,s.job_definition_id,jd.project_id,s.schedule_type,COALESCE(s.cron_expression,''),s.with_seconds,s.timezone,s.misfire_policy,s.run_at,c.evaluated_through,s.created_at
		FROM schedules s JOIN job_definitions jd ON jd.id=s.job_definition_id
		LEFT JOIN schedule_cursors c ON c.schedule_id=s.id
		WHERE s.id=$1 AND s.status='ACTIVE' AND s.deleted_at IS NULL AND jd.deleted_at IS NULL`, id).Scan(
		&state.ID, &state.DefinitionID, &state.ProjectID, &state.Type, &state.Cron, &state.WithSeconds, &state.Timezone, &state.MisfirePolicy, &state.RunAt, &state.Cursor, &state.CreatedAt)
	return state, err
}

func (s *Store) ListActiveSchedulePlans(ctx context.Context) ([]SchedulePlanState, error) {
	rows, err := s.Pool.Query(ctx, `SELECT s.id,s.job_definition_id,jd.project_id,s.schedule_type,COALESCE(s.cron_expression,''),s.with_seconds,s.timezone,s.misfire_policy,s.run_at,c.evaluated_through,s.created_at
		FROM schedules s JOIN job_definitions jd ON jd.id=s.job_definition_id
		LEFT JOIN schedule_cursors c ON c.schedule_id=s.id
		WHERE s.status='ACTIVE' AND s.deleted_at IS NULL AND jd.deleted_at IS NULL ORDER BY s.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []SchedulePlanState{}
	for rows.Next() {
		var state SchedulePlanState
		if err = rows.Scan(&state.ID, &state.DefinitionID, &state.ProjectID, &state.Type, &state.Cron, &state.WithSeconds, &state.Timezone, &state.MisfirePolicy, &state.RunAt, &state.Cursor, &state.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, state)
	}
	return items, rows.Err()
}

// AdvanceScheduleCursor is monotonic. Re-evaluating an older callback cannot
// move the durable decision boundary backwards.
func (s *Store) AdvanceScheduleCursor(ctx context.Context, id uuid.UUID, through time.Time) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO schedule_cursors(schedule_id,evaluated_through,updated_at)
		VALUES($1,$2,now()) ON CONFLICT(schedule_id) DO UPDATE
		SET evaluated_through=EXCLUDED.evaluated_through,updated_at=now()
		WHERE schedule_cursors.evaluated_through < EXCLUDED.evaluated_through`, id, through.UTC())
	return err
}

func (s *Store) CompleteOneTimeSchedule(ctx context.Context, id uuid.UUID) error {
	_, err := s.Pool.Exec(ctx, "UPDATE schedules SET status='DISABLED',updated_at=now() WHERE id=$1 AND schedule_type='ONE_TIME' AND status='ACTIVE'", id)
	return err
}
