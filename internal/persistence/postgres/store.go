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
	// Snapshot is used by immutable workflow runtime nodes. It carries the
	// execution values captured at workflow start instead of re-reading a
	// mutable retry policy or job definition at downstream dispatch time.
	Snapshot *Definition
}

func (s *Store) Submit(ctx context.Context, input Submit) (job.Run, error) {
	d := Definition{}
	var err error
	if input.Snapshot != nil {
		d = *input.Snapshot
		if d.ID != input.DefinitionID || d.ProjectID != input.ProjectID {
			return job.Run{}, errors.New("execution snapshot does not match submit target")
		}
	} else if d, err = s.Definition(ctx, input.DefinitionID, input.ProjectID); err != nil {
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
	payload, err := job.NormalizePayload(input.Payload)
	if err != nil {
		return job.Run{}, err
	}
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
	run, created, err := insertIdempotentRun(ctx, tx, input, d, priority, storedPayload, ciphertext, keyRef, policy)
	if err != nil {
		return job.Run{}, err
	}
	if created {
		if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(project_id,event_type,aggregate_id,dispatch_id,payload) VALUES($1,'job.enqueue',$2,$3,jsonb_build_object('run_id',$2,'dispatch_id',$3))`, input.ProjectID, run.ID, run.DispatchID); err != nil {
			return job.Run{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return job.Run{}, err
	}
	if !created {
		return run, nil
	}
	return job.Run{ID: run.ID, ProjectID: input.ProjectID, DefinitionID: d.ID, QueueID: d.QueueID, Status: job.EnqueuePending, Priority: priority, Payload: payload, Policy: d.Policy, FunctionKey: d.FunctionKey, FunctionVersion: d.FunctionVersion, IdempotencyKey: input.IdempotencyKey, DispatchID: run.DispatchID}, nil
}

const insertRunSQL = `INSERT INTO job_runs(id,project_id,job_definition_id,queue_id,schedule_id,idempotency_key,status,priority,scheduled_for,payload,payload_ciphertext,encryption_key_ref,function_version,policy_snapshot,current_dispatch_id)
VALUES($1,$2,$3,$4,$5,NULLIF($6,''),'ENQUEUE_PENDING',$7,$8,$9,$10,NULLIF($11,''),$12,$13,$14)`

// insertIdempotentRun deliberately uses separate INSERT and SELECT commands.
// At READ COMMITTED PostgreSQL assigns a fresh snapshot to the second command,
// which makes a row committed by the conflicting transaction visible. Keeping
// the conflict targets separate avoids accidentally swallowing an unrelated
// future unique constraint.
func insertIdempotentRun(ctx context.Context, tx pgx.Tx, input Submit, definition Definition, priority job.Priority, payload json.RawMessage, ciphertext []byte, keyRef string, policy []byte) (job.Run, bool, error) {
	if input.ScheduleID != nil || input.ScheduledFor != nil {
		if input.ScheduleID == nil || input.ScheduledFor == nil {
			return job.Run{}, false, errors.New("schedule_id and scheduled_for must be supplied together")
		}
		if input.IdempotencyKey != "" {
			return job.Run{}, false, errors.New("scheduled occurrence cannot also carry an idempotency key")
		}
		return insertOccurrence(ctx, tx, input, definition, priority, payload, ciphertext, keyRef, policy)
	}
	if input.IdempotencyKey != "" {
		return insertByIdempotencyKey(ctx, tx, input, definition, priority, payload, ciphertext, keyRef, policy)
	}
	var run job.Run
	run.DispatchID = uuid.New()
	err := tx.QueryRow(ctx, insertRunSQL+" RETURNING id,status,current_dispatch_id", uuid.New(), input.ProjectID, definition.ID, definition.QueueID, nil, "", priority, nil, payload, ciphertext, keyRef, definition.FunctionVersion, policy, run.DispatchID).Scan(&run.ID, &run.Status, &run.DispatchID)
	return run, err == nil, err
}

func insertByIdempotencyKey(ctx context.Context, tx pgx.Tx, input Submit, definition Definition, priority job.Priority, payload json.RawMessage, ciphertext []byte, keyRef string, policy []byte) (job.Run, bool, error) {
	var run job.Run
	run.DispatchID = uuid.New()
	err := tx.QueryRow(ctx, insertRunSQL+" ON CONFLICT (project_id,idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING RETURNING id,status,current_dispatch_id", uuid.New(), input.ProjectID, definition.ID, definition.QueueID, nil, input.IdempotencyKey, priority, nil, payload, ciphertext, keyRef, definition.FunctionVersion, policy, run.DispatchID).Scan(&run.ID, &run.Status, &run.DispatchID)
	if err == nil {
		return run, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Run{}, false, err
	}
	return readExistingRun(ctx, tx, "SELECT id,status,current_dispatch_id FROM job_runs WHERE project_id=$1 AND idempotency_key=$2", input.ProjectID, input.IdempotencyKey)
}

func insertOccurrence(ctx context.Context, tx pgx.Tx, input Submit, definition Definition, priority job.Priority, payload json.RawMessage, ciphertext []byte, keyRef string, policy []byte) (job.Run, bool, error) {
	var run job.Run
	run.DispatchID = uuid.New()
	err := tx.QueryRow(ctx, insertRunSQL+" ON CONFLICT (schedule_id,scheduled_for) WHERE schedule_id IS NOT NULL DO NOTHING RETURNING id,status,current_dispatch_id", uuid.New(), input.ProjectID, definition.ID, definition.QueueID, input.ScheduleID, "", priority, input.ScheduledFor, payload, ciphertext, keyRef, definition.FunctionVersion, policy, run.DispatchID).Scan(&run.ID, &run.Status, &run.DispatchID)
	if err == nil {
		return run, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Run{}, false, err
	}
	return readExistingRun(ctx, tx, "SELECT id,status,current_dispatch_id FROM job_runs WHERE schedule_id=$1 AND scheduled_for=$2", input.ScheduleID, input.ScheduledFor)
}

func readExistingRun(ctx context.Context, tx pgx.Tx, query string, args ...any) (job.Run, bool, error) {
	// The bounded retry is defensive for a concurrent transaction that becomes
	// visible immediately after ON CONFLICT resolves. Every query is a fresh
	// READ COMMITTED statement snapshot; no transaction is left aborted.
	for attempt := 0; attempt < 3; attempt++ {
		var run job.Run
		err := tx.QueryRow(ctx, query, args...).Scan(&run.ID, &run.Status, &run.DispatchID)
		if err == nil {
			return run, false, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return job.Run{}, false, err
		}
		if attempt < 2 {
			timer := time.NewTimer(time.Duration(attempt+1) * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return job.Run{}, false, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return job.Run{}, false, errors.New("idempotency conflict row was not visible after bounded retry")
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
		payload, normalizeErr := job.NormalizePayload(in.Payload)
		if normalizeErr != nil {
			return nil, normalizeErr
		}
		storedPayload, ciphertext, keyRef, sealErr := s.sealPayload(ctx, in.ProjectID, d.ID, payload)
		if sealErr != nil {
			return nil, sealErr
		}
		dispatchID := uuid.New()
		_, err = tx.Exec(ctx, `INSERT INTO job_runs(id,project_id,job_definition_id,queue_id,idempotency_key,status,priority,payload,payload_ciphertext,encryption_key_ref,function_version,policy_snapshot,current_dispatch_id) VALUES($1,$2,$3,$4,NULLIF($5,''),'ENQUEUE_PENDING',$6,$7,$8,$9,NULLIF($10,''),$11,$12,$13)`, id, in.ProjectID, d.ID, d.QueueID, in.IdempotencyKey, priority, storedPayload, ciphertext, keyRef, d.FunctionVersion, policy, dispatchID)
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO outbox_events(project_id,event_type,aggregate_id,dispatch_id,payload) VALUES($1,'job.enqueue',$2,$3,jsonb_build_object('run_id',$2,'dispatch_id',$3))`, in.ProjectID, id, dispatchID); err != nil {
			return nil, err
		}
		runs = append(runs, job.Run{ID: id, ProjectID: in.ProjectID, DefinitionID: d.ID, QueueID: d.QueueID, Status: job.EnqueuePending, Priority: priority, Payload: payload, Policy: d.Policy, FunctionKey: d.FunctionKey, FunctionVersion: d.FunctionVersion, IdempotencyKey: in.IdempotencyKey, DispatchID: dispatchID})
	}
	return runs, tx.Commit(ctx)
}
