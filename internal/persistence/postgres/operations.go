package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type CreateQueue struct {
	ProjectID                       uuid.UUID
	Name                            string
	MaxConcurrency, DefaultPriority int
}

func (s *Store) CreateQueue(ctx context.Context, in CreateQueue) (uuid.UUID, error) {
	var id uuid.UUID
	err := s.Pool.QueryRow(ctx, "INSERT INTO queues(project_id,name,max_concurrency,default_priority) VALUES($1,$2,$3,$4) RETURNING id", in.ProjectID, in.Name, in.MaxConcurrency, in.DefaultPriority).Scan(&id)
	return id, err
}
func (s *Store) TransitionQueue(ctx context.Context, project, id uuid.UUID, to string, from []string, version int64) (bool, error) {
	tag, err := s.Pool.Exec(ctx, "UPDATE queues SET status=$1 WHERE id=$2 AND project_id=$3 AND status=ANY($4) AND row_version=$5", to, id, project, from, version)
	return tag.RowsAffected() == 1, err
}

type CreateSchedule struct {
	ProjectID, DefinitionID uuid.UUID
	Cron, Timezone          string
	WithSeconds             bool
}

func (s *Store) CreateSchedule(ctx context.Context, in CreateSchedule) (uuid.UUID, error) {
	var id uuid.UUID
	err := s.Pool.QueryRow(ctx, `INSERT INTO schedules(job_definition_id,schedule_type,cron_expression,timezone,with_seconds) SELECT id,'CRON',$1,$2,$3 FROM job_definitions WHERE id=$4 AND project_id=$5 AND deleted_at IS NULL RETURNING id`, in.Cron, in.Timezone, in.WithSeconds, in.DefinitionID, in.ProjectID).Scan(&id)
	return id, err
}
func (s *Store) TransitionSchedule(ctx context.Context, project, id uuid.UUID, to string, version int64) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE schedules s SET status=$1 WHERE s.id=$2 AND s.row_version=$4 AND s.deleted_at IS NULL AND EXISTS(SELECT 1 FROM job_definitions jd WHERE jd.id=s.job_definition_id AND jd.project_id=$3 AND jd.deleted_at IS NULL)`, to, id, project, version)
	return tag.RowsAffected() == 1, err
}
func (s *Store) CancelRun(ctx context.Context, project, id uuid.UUID, version int64) (bool, error) {
	tag, err := s.Pool.Exec(ctx, "UPDATE job_runs SET status='CANCELLED',finished_at=now(),updated_at=now() WHERE id=$1 AND project_id=$2 AND row_version=$3 AND status IN ('CREATED','ENQUEUE_PENDING','QUEUED','RETRY_WAIT')", id, project, version)
	return tag.RowsAffected() == 1, err
}
func (s *Store) RetryRun(ctx context.Context, project, id uuid.UUID, version int64) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `UPDATE job_runs SET status='QUEUED',available_at=now(),finished_at=NULL,updated_at=now() WHERE id=$1 AND project_id=$2 AND row_version=$3 AND status IN ('DEAD_LETTER','CANCELLED')`, id, project, version)
	if err == nil && tag.RowsAffected() == 1 {
		_, err = s.Pool.Exec(ctx, "UPDATE dlq_entries SET replayed_at=now() WHERE job_run_id=$1", id)
	}
	return tag.RowsAffected() == 1, err
}
func (s *Store) ReplayDLQ(ctx context.Context, project, id uuid.UUID, version int64) (uuid.UUID, bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, false, err
	}
	defer tx.Rollback(ctx)
	var run uuid.UUID
	err = tx.QueryRow(ctx, "SELECT job_run_id FROM dlq_entries WHERE id=$1 AND row_version=$2 AND replayed_at IS NULL FOR UPDATE", id, version).Scan(&run)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, err
	}
	tag, err := tx.Exec(ctx, "UPDATE job_runs SET status='QUEUED',available_at=now(),finished_at=NULL,updated_at=now() WHERE id=$1 AND project_id=$2 AND status='DEAD_LETTER'", run, project)
	if err != nil {
		return uuid.Nil, false, err
	}
	if tag.RowsAffected() != 1 {
		return uuid.Nil, false, nil
	}
	if _, err = tx.Exec(ctx, "UPDATE dlq_entries SET replayed_at=now() WHERE id=$1 AND row_version=$2", id, version); err != nil {
		return uuid.Nil, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return uuid.Nil, false, err
	}
	return run, true, nil
}
func (s *Store) Heartbeat(ctx context.Context, id uuid.UUID) error {
	_, err := s.Pool.Exec(ctx, "UPDATE workers SET heartbeat_at=now(),status='ONLINE' WHERE id=$1", id)
	return err
}
func (s *Store) SetWorkerStatus(ctx context.Context, id uuid.UUID, status string) error {
	_, err := s.Pool.Exec(ctx, "UPDATE workers SET status=$2 WHERE id=$1", id, status)
	return err
}
