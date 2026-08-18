package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/example/task-processing/internal/domain/job"
	"github.com/example/task-processing/internal/security/kms"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type Store struct {
	Pool             *pgxpool.Pool
	PayloadProtector kms.Protector
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	protector, err := kms.FromEnv()
	if err != nil {
		p.Close()
		return nil, err
	}
	return &Store{Pool: p, PayloadProtector: protector}, nil
}
func (s *Store) Close() { s.Pool.Close() }

type Definition struct {
	ID, ProjectID, QueueID       uuid.UUID
	FunctionKey, FunctionVersion string
	Priority                     job.Priority
	Policy                       job.PolicySnapshot
}

func (s *Store) Definition(ctx context.Context, id, projectID uuid.UUID) (Definition, error) {
	var d Definition
	var pol []byte
	err := s.Pool.QueryRow(ctx, `SELECT jd.id,jd.project_id,jd.queue_id,fd.function_key,fd.version,jd.default_priority,jsonb_build_object('retry',jsonb_build_object('max_attempts',COALESCE(rp.max_attempts,1),'strategy',COALESCE(rp.strategy,'FIXED'),'initial_delay_ms',COALESCE(rp.initial_delay_ms,0),'multiplier',COALESCE(rp.multiplier,1),'max_delay_ms',COALESCE(rp.max_delay_ms,0),'jitter_pct',COALESCE(rp.jitter_pct,0),'retry_timeout',COALESCE(rp.retry_timeout,false),'retry_rate_limited',COALESCE(rp.retry_rate_limited,false),'retry_dependency_error',COALESCE(rp.retry_dependency_error,false),'retry_validation_error',COALESCE(rp.retry_validation_error,false)),'timeout_ms',jd.timeout_ms) FROM job_definitions jd JOIN function_definitions fd ON fd.id=jd.function_id LEFT JOIN retry_policies rp ON rp.id=jd.retry_policy_id WHERE jd.id=$1 AND jd.project_id=$2 AND jd.status='ACTIVE' AND jd.deleted_at IS NULL AND fd.deleted_at IS NULL AND (rp.id IS NULL OR rp.deleted_at IS NULL)`, id, projectID).Scan(&d.ID, &d.ProjectID, &d.QueueID, &d.FunctionKey, &d.FunctionVersion, &d.Priority, &pol)
	if err != nil {
		return d, err
	}
	if err = json.Unmarshal(pol, &d.Policy); err != nil {
		return d, err
	}
	return d, nil
}

type Submit struct {
	ProjectID, DefinitionID uuid.UUID
	Payload                 json.RawMessage
	IdempotencyKey          string
	Priority                *job.Priority
	ScheduleID              *uuid.UUID
	ScheduledFor            *time.Time
}

func (s *Store) Submit(ctx context.Context, input Submit) (job.Run, error) {
	d, err := s.Definition(ctx, input.DefinitionID, input.ProjectID)
	if err != nil {
		return job.Run{}, err
	}
	qstatus := ""
	if err = s.Pool.QueryRow(ctx, "SELECT status FROM queues WHERE id=$1 AND deleted_at IS NULL", d.QueueID).Scan(&qstatus); err != nil {
		return job.Run{}, err
	}
	if qstatus == "DISABLED" || qstatus == "DRAINING" {
		return job.Run{}, fmt.Errorf("queue is not accepting work: %s", qstatus)
	}
	priority := d.Priority
	if input.Priority != nil {
		priority = *input.Priority
	}
	payload := job.MustPayload(input.Payload)
	storedPayload, ciphertext, keyRef, err := s.sealPayload(ctx, input.ProjectID, d.ID, payload)
	if err != nil {
		return job.Run{}, err
	}
	policy, _ := json.Marshal(d.Policy)
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return job.Run{}, err
	}
	defer tx.Rollback(ctx)
	id := uuid.New()
	_, err = tx.Exec(ctx, `INSERT INTO job_runs(id,project_id,job_definition_id,queue_id,schedule_id,idempotency_key,status,priority,scheduled_for,payload,payload_ciphertext,encryption_key_ref,function_version,policy_snapshot) VALUES($1,$2,$3,$4,$5,NULLIF($6,''),'ENQUEUE_PENDING',$7,$8,$9,$10,NULLIF($11,''),$12,$13)`, id, input.ProjectID, d.ID, d.QueueID, input.ScheduleID, input.IdempotencyKey, priority, input.ScheduledFor, storedPayload, ciphertext, keyRef, d.FunctionVersion, policy)
	if err != nil {
		if isUnique(err) {
			var existing job.Run
			var e error
			if input.IdempotencyKey != "" {
				e = tx.QueryRow(ctx, "SELECT id,status FROM job_runs WHERE project_id=$1 AND idempotency_key=$2", input.ProjectID, input.IdempotencyKey).Scan(&existing.ID, &existing.Status)
			} else if input.ScheduleID != nil && input.ScheduledFor != nil {
				e = tx.QueryRow(ctx, "SELECT id,status FROM job_runs WHERE schedule_id=$1 AND scheduled_for=$2", input.ScheduleID, input.ScheduledFor).Scan(&existing.ID, &existing.Status)
			}
			if e == nil {
				return existing, tx.Commit(ctx)
			}
		}
		return job.Run{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events(project_id,event_type,aggregate_id,payload) VALUES($1,'job.enqueue',$2,jsonb_build_object('run_id',$2))`, input.ProjectID, id)
	if err != nil {
		return job.Run{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return job.Run{}, err
	}
	return job.Run{ID: id, ProjectID: input.ProjectID, DefinitionID: d.ID, QueueID: d.QueueID, Status: job.EnqueuePending, Priority: priority, Payload: payload, Policy: d.Policy, FunctionKey: d.FunctionKey, FunctionVersion: d.FunctionVersion, IdempotencyKey: input.IdempotencyKey}, nil
}

// SubmitBulk is atomic at the control-plane boundary: either every run/outbox intent exists or none do.
func (s *Store) SubmitBulk(ctx context.Context, inputs []Submit) ([]job.Run, error) {
	if len(inputs) == 0 || len(inputs) > 1000 {
		return nil, fmt.Errorf("bulk size must be between 1 and 1000")
	}
	first := inputs[0]
	d, err := s.Definition(ctx, first.DefinitionID, first.ProjectID)
	if err != nil {
		return nil, err
	}
	var queueStatus string
	if qerr := s.Pool.QueryRow(ctx, "SELECT status FROM queues WHERE id=$1 AND deleted_at IS NULL", d.QueueID).Scan(&queueStatus); qerr != nil {
		return nil, qerr
	}
	if queueStatus == "DISABLED" || queueStatus == "DRAINING" {
		return nil, fmt.Errorf("queue is not accepting work: %s", queueStatus)
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	runs := make([]job.Run, 0, len(inputs))
	policy, _ := json.Marshal(d.Policy)
	for _, in := range inputs {
		if in.ProjectID != first.ProjectID || in.DefinitionID != first.DefinitionID {
			return nil, fmt.Errorf("bulk must target one project and job definition")
		}
		priority := d.Priority
		if in.Priority != nil {
			priority = *in.Priority
		}
		id := uuid.New()
		payload := job.MustPayload(in.Payload)
		storedPayload, ciphertext, keyRef, sealErr := s.sealPayload(ctx, in.ProjectID, d.ID, payload)
		if sealErr != nil {
			return nil, sealErr
		}
		_, err = tx.Exec(ctx, `INSERT INTO job_runs(id,project_id,job_definition_id,queue_id,idempotency_key,status,priority,payload,payload_ciphertext,encryption_key_ref,function_version,policy_snapshot) VALUES($1,$2,$3,$4,NULLIF($5,''),'ENQUEUE_PENDING',$6,$7,$8,$9,NULLIF($10,''),$11,$12)`, id, in.ProjectID, d.ID, d.QueueID, in.IdempotencyKey, priority, storedPayload, ciphertext, keyRef, d.FunctionVersion, policy)
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(project_id,event_type,aggregate_id,payload) VALUES($1,'job.enqueue',$2,jsonb_build_object('run_id',$2))`, in.ProjectID, id); err != nil {
			return nil, err
		}
		runs = append(runs, job.Run{ID: id, ProjectID: in.ProjectID, DefinitionID: d.ID, QueueID: d.QueueID, Status: job.EnqueuePending, Priority: priority, Payload: payload, Policy: d.Policy, FunctionKey: d.FunctionKey, FunctionVersion: d.FunctionVersion, IdempotencyKey: in.IdempotencyKey})
	}
	return runs, tx.Commit(ctx)
}
func isUnique(err error) bool { var e *pgconn.PgError; return errors.As(err, &e) && e.Code == "23505" }
