package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/example/task-processing/internal/domain/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/trace"
	"time"
)

var ErrConcurrencyLimited = errors.New("execution concurrency limit reached")
var ErrRateLimited = errors.New("rate limit reached")
var ErrDispatchLost = errors.New("dispatch is stale or no longer claimable")

type QueueDepthStats struct {
	Queued  int64
	Running int64
	Retry   int64
	Oldest  *time.Time
}
type QueueAge struct {
	Name       string
	AgeSeconds float64
}

func (s *Store) QueueAges(ctx context.Context) ([]QueueAge, error) {
	rows, err := s.Pool.Query(ctx, "SELECT q.name,COALESCE(EXTRACT(EPOCH FROM now()-min(r.available_at) FILTER (WHERE r.status='QUEUED')),0) FROM queues q LEFT JOIN job_runs r ON r.queue_id=q.id WHERE q.deleted_at IS NULL GROUP BY q.id,q.name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]QueueAge, 0)
	for rows.Next() {
		var item QueueAge
		if err = rows.Scan(&item.Name, &item.AgeSeconds); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

type ClaimedOutboxMessage struct {
	ID          uuid.UUID
	Token       uuid.UUID
	DispatchID  uuid.UUID
	BackendType string
	RunID       uuid.UUID
	ProjectID   uuid.UUID
	QueueID     uuid.UUID
	Priority    int16
	AvailableAt time.Time
}

// ClaimOutbox obtains short-lived, fenced publication ownership. Publishing is
// intentionally performed after this transaction commits, so no network call
// happens while a PostgreSQL transaction is open.
func (s *Store) ClaimOutbox(ctx context.Context, owner string, limit int, lease time.Duration) ([]ClaimedOutboxMessage, error) {
	if owner == "" {
		return nil, errors.New("outbox claim owner is required")
	}
	if limit < 1 || limit > 1000 {
		limit = 100
	}
	if lease <= 0 {
		lease = time.Minute
	}
	// A newer retry or lease-recovery dispatch supersedes an older publication
	// intent. It is safe to acknowledge the old intent because ownership will
	// only ever be granted for the current dispatch ID.
	if _, err := s.Pool.Exec(ctx, `UPDATE outbox_events o SET published_at=now(),last_error='superseded dispatch',claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL
		FROM job_runs r WHERE o.event_type='job.enqueue' AND o.aggregate_id=r.id AND o.published_at IS NULL AND o.dispatch_id<>r.current_dispatch_id`); err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `WITH picked AS (
		SELECT id FROM outbox_events
		WHERE event_type='job.enqueue' AND published_at IS NULL AND available_at<=now()
		  AND (claim_expires_at IS NULL OR claim_expires_at<=now())
		ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT $3
	) UPDATE outbox_events o
	SET claimed_by=$1,claim_token=gen_random_uuid(),claim_expires_at=now()+($2 * interval '1 millisecond')
	FROM picked p, job_runs r JOIN queues q ON q.id=r.queue_id LEFT JOIN queue_backends b ON b.id=q.backend_id
	WHERE o.id=p.id AND r.id=o.aggregate_id AND o.dispatch_id=r.current_dispatch_id
	RETURNING o.id,o.claim_token,o.dispatch_id,COALESCE(b.backend_type,'POSTGRES'),r.id,r.project_id,r.queue_id,r.priority,r.available_at`, owner, lease.Milliseconds(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ClaimedOutboxMessage{}
	for rows.Next() {
		var item ClaimedOutboxMessage
		if err = rows.Scan(&item.ID, &item.Token, &item.DispatchID, &item.BackendType, &item.RunID, &item.ProjectID, &item.QueueID, &item.Priority, &item.AvailableAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
func (s *Store) MarkOutboxPublished(ctx context.Context, id, token uuid.UUID) error {
	tag, err := s.Pool.Exec(ctx, "UPDATE outbox_events SET published_at=now(),attempts=attempts+1,last_error=NULL,claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL WHERE id=$1 AND claim_token=$2 AND claim_expires_at>now() AND published_at IS NULL", id, token)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("outbox claim lost before publication acknowledgement")
	}
	return nil
}
func (s *Store) RecordOutboxFailure(ctx context.Context, id, token uuid.UUID, cause error) error {
	tag, err := s.Pool.Exec(ctx, "UPDATE outbox_events SET attempts=attempts+1,last_error=$3,available_at=now()+(LEAST(attempts+1,8) * interval '1 second'),claimed_by=NULL,claim_token=NULL,claim_expires_at=NULL WHERE id=$1 AND claim_token=$2 AND claim_expires_at>now() AND published_at IS NULL", id, token, truncate(cause.Error(), 1000))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("outbox claim lost before failure acknowledgement")
	}
	return nil
}

// DispatchRef identifies one immutable attempt to dispatch a job run. A stale
// message must never transition a newer generation into a claimable state.
type DispatchRef struct {
	RunID      uuid.UUID
	DispatchID uuid.UUID
}

// EnqueueDispatches is the PostgreSQL implementation of QueueBackend.EnqueueBatch.
// It is transactional: either every accepted current generation becomes
// claimable or none do; replaying an already-queued generation is idempotent.
func (s *Store) EnqueueDispatches(ctx context.Context, dispatches []DispatchRef) error {
	if len(dispatches) == 0 {
		return nil
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, dispatch := range dispatches {
		if dispatch.RunID == uuid.Nil || dispatch.DispatchID == uuid.Nil {
			return errors.New("dispatch reference requires run and dispatch IDs")
		}
		tag, err := tx.Exec(ctx, "UPDATE job_runs SET status='QUEUED',updated_at=now() WHERE id=$1 AND current_dispatch_id=$2 AND status='ENQUEUE_PENDING'", dispatch.RunID, dispatch.DispatchID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			var status string
			var current uuid.UUID
			if err = tx.QueryRow(ctx, "SELECT status,current_dispatch_id FROM job_runs WHERE id=$1", dispatch.RunID).Scan(&status, &current); err != nil {
				return err
			}
			if status != "QUEUED" || current != dispatch.DispatchID {
				return fmt.Errorf("dispatch %s for run %s is stale or no longer enqueue-pending", dispatch.DispatchID, dispatch.RunID)
			}
		}
	}
	return tx.Commit(ctx)
}

// DispatchOwnership is the fenced result of accepting a backend delivery. A
// Redis Streams or JetStream adapter must obtain this ownership before it
// starts an attempt; acknowledgement of the transport message alone is never
// proof that it owns the job.
type DispatchOwnership struct {
	RunID          uuid.UUID
	DispatchID     uuid.UUID
	LeaseToken     uuid.UUID
	LeaseExpiresAt time.Time
}

// TakeDispatchOwnership performs the compare-and-set that makes an external
// backend delivery authoritative. It deliberately accepts only QUEUED work
// whose generation is still current, so delayed or redelivered messages from
// a previous retry cannot execute the newer generation.
func (s *Store) TakeDispatchOwnership(ctx context.Context, runID, dispatchID uuid.UUID, owner string, lease time.Duration) (DispatchOwnership, error) {
	if runID == uuid.Nil || dispatchID == uuid.Nil || owner == "" {
		return DispatchOwnership{}, errors.New("run ID, dispatch ID and owner are required")
	}
	if lease <= 0 {
		lease = time.Minute
	}
	var claim DispatchOwnership
	err := s.Pool.QueryRow(ctx, `UPDATE job_runs r SET status='RESERVED',lease_owner=$3,lease_token=gen_random_uuid(),lease_expires_at=now()+($4 * interval '1 millisecond'),reserved_at=now(),updated_at=now()
		FROM queues q, job_definitions jd
		WHERE r.id=$1 AND r.current_dispatch_id=$2 AND r.status='QUEUED' AND r.available_at<=now()
		  AND q.id=r.queue_id AND jd.id=r.job_definition_id AND q.status='ACTIVE' AND q.deleted_at IS NULL AND jd.deleted_at IS NULL AND jd.execution_mode='SINGLE'
		RETURNING r.id,r.current_dispatch_id,r.lease_token,r.lease_expires_at`, runID, dispatchID, owner, lease.Milliseconds()).Scan(&claim.RunID, &claim.DispatchID, &claim.LeaseToken, &claim.LeaseExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return DispatchOwnership{}, ErrDispatchLost
	}
	return claim, err
}

func (s *Store) QueueDepth(ctx context.Context, queue uuid.UUID) (QueueDepthStats, error) {
	var stats QueueDepthStats
	err := s.Pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='QUEUED'), count(*) FILTER (WHERE status='RUNNING'), count(*) FILTER (WHERE status='RETRY_WAIT'), min(available_at) FILTER (WHERE status='QUEUED') FROM job_runs WHERE queue_id=$1`, queue).Scan(&stats.Queued, &stats.Running, &stats.Retry, &stats.Oldest)
	return stats, err
}

// DispatchOutbox publishes the PostgreSQL queue adapter atomically: outbox rows are the durable publication intent.
func (s *Store) DispatchOutbox(ctx context.Context, limit int) (int, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id,aggregate_id,dispatch_id FROM outbox_events WHERE event_type='job.enqueue' AND published_at IS NULL AND available_at<=now() ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	var dispatches []DispatchRef
	for rows.Next() {
		var id uuid.UUID
		var dispatch DispatchRef
		if err = rows.Scan(&id, &dispatch.RunID, &dispatch.DispatchID); err != nil {
			return 0, err
		}
		ids = append(ids, id)
		dispatches = append(dispatches, dispatch)
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	for _, dispatch := range dispatches {
		if _, err = tx.Exec(ctx, "UPDATE job_runs SET status='QUEUED',updated_at=now() WHERE id=$1 AND current_dispatch_id=$2 AND status='ENQUEUE_PENDING'", dispatch.RunID, dispatch.DispatchID); err != nil {
			return 0, err
		}
	}
	for _, id := range ids {
		if _, err = tx.Exec(ctx, "UPDATE outbox_events SET published_at=now(),attempts=attempts+1 WHERE id=$1", id); err != nil {
			return 0, err
		}
	}
	return len(ids), tx.Commit(ctx)
}
func (s *Store) PromoteRetries(ctx context.Context) (int64, error) {
	// A retry is a new durable dispatch intent. The status transition and its
	// outbox event are one statement, preventing a crash from leaving a QUEUED
	// run permanently invisible to an external backend.
	tag, err := s.Pool.Exec(ctx, `WITH promoted AS (
		UPDATE job_runs SET status='ENQUEUE_PENDING',current_dispatch_id=gen_random_uuid(),updated_at=now()
		WHERE status='RETRY_WAIT' AND available_at<=now()
		RETURNING id,project_id,current_dispatch_id
	) INSERT INTO outbox_events(project_id,event_type,aggregate_id,dispatch_id,payload)
	SELECT project_id,'job.enqueue',id,current_dispatch_id,jsonb_build_object('run_id',id,'dispatch_id',current_dispatch_id) FROM promoted`)
	return tag.RowsAffected(), err
}
func (s *Store) RecoverExpiredLeases(ctx context.Context) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `WITH recovered AS (
		UPDATE job_runs SET status='ENQUEUE_PENDING',current_dispatch_id=gen_random_uuid(),lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now()
		WHERE status IN ('RESERVED','RUNNING') AND lease_expires_at<now()
		RETURNING id,project_id,current_dispatch_id
	) INSERT INTO outbox_events(project_id,event_type,aggregate_id,dispatch_id,payload)
	SELECT project_id,'job.enqueue',id,current_dispatch_id,jsonb_build_object('run_id',id,'dispatch_id',current_dispatch_id) FROM recovered`)
	return tag.RowsAffected(), err
}

var ErrLeaseLost = errors.New("job lease lost")

func (s *Store) ExtendLease(ctx context.Context, runID uuid.UUID, owner string, token uuid.UUID, lease time.Duration) (time.Time, error) {
	var expiresAt time.Time
	err := s.Pool.QueryRow(ctx, "UPDATE job_runs SET lease_expires_at=now()+($4 * interval '1 millisecond'),updated_at=now() WHERE id=$1 AND status='RUNNING' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at>now() RETURNING lease_expires_at", runID, owner, token, lease.Milliseconds()).Scan(&expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrLeaseLost
	}
	if err != nil {
		return time.Time{}, err
	}
	return expiresAt, nil
}

func (s *Store) Claim(ctx context.Context, workerID uuid.UUID, owner string, limit int, lease time.Duration) ([]job.Run, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `WITH picked AS (SELECT r.id FROM job_runs r JOIN queues q ON q.id=r.queue_id JOIN job_definitions jd ON jd.id=r.job_definition_id WHERE r.status='QUEUED' AND r.available_at<=now() AND q.status='ACTIVE' AND q.deleted_at IS NULL AND jd.deleted_at IS NULL AND jd.execution_mode='SINGLE' ORDER BY r.priority DESC,r.available_at ASC,r.id ASC FOR UPDATE SKIP LOCKED LIMIT $1) UPDATE job_runs r SET status='RESERVED',lease_owner=$2,lease_token=gen_random_uuid(),lease_expires_at=now()+($3 * interval '1 millisecond'),reserved_at=now(),updated_at=now() FROM picked WHERE r.id=picked.id RETURNING r.id,r.project_id,r.job_definition_id,r.queue_id,r.idempotency_key,r.priority,r.payload,r.function_version,r.policy_snapshot,r.lease_token,r.current_dispatch_id`, limit, owner, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []job.Run
	for rows.Next() {
		var r job.Run
		var policy []byte
		if err = rows.Scan(&r.ID, &r.ProjectID, &r.DefinitionID, &r.QueueID, &r.IdempotencyKey, &r.Priority, &r.Payload, &r.FunctionVersion, &policy, &r.LeaseToken, &r.DispatchID); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(policy, &r.Policy); err != nil {
			return nil, err
		}
		if err = tx.QueryRow(ctx, `SELECT fd.function_key FROM job_definitions jd JOIN function_definitions fd ON fd.id=jd.function_id WHERE jd.id=$1 AND jd.deleted_at IS NULL AND fd.deleted_at IS NULL`, r.DefinitionID).Scan(&r.FunctionKey); err != nil {
			return nil, err
		}
		r.Status = job.Reserved
		result = append(result, r)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return result, tx.Commit(ctx)
}
func (s *Store) StartAttempt(ctx context.Context, runID, workerID uuid.UUID, owner string, token uuid.UUID, lease time.Duration) (job.Execution, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return job.Execution{}, err
	}
	defer tx.Rollback(ctx)
	if err = setSystemAuditContext(ctx, tx); err != nil {
		return job.Execution{}, err
	}
	var r job.Run
	var tenantID uuid.UUID
	var functionID uuid.UUID
	var queueLimit, functionLimit, projectLimit, tenantLimit int
	if err = tx.QueryRow(ctx, `SELECT t.id,r.project_id,r.queue_id,r.job_definition_id,fd.id,q.max_concurrency,fd.max_concurrency,p.max_concurrency,t.max_concurrency FROM job_runs r JOIN projects p ON p.id=r.project_id JOIN tenants t ON t.id=p.tenant_id JOIN queues q ON q.id=r.queue_id JOIN job_definitions jd ON jd.id=r.job_definition_id JOIN function_definitions fd ON fd.id=jd.function_id WHERE r.id=$1 AND r.status='RESERVED' AND r.lease_owner=$2 AND r.lease_token=$3 AND r.lease_expires_at>now() AND t.deleted_at IS NULL AND p.deleted_at IS NULL AND q.deleted_at IS NULL AND jd.deleted_at IS NULL AND fd.deleted_at IS NULL FOR UPDATE OF r`, runID, owner, token).Scan(&tenantID, &r.ProjectID, &r.QueueID, &r.DefinitionID, &functionID, &queueLimit, &functionLimit, &projectLimit, &tenantLimit); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return job.Execution{}, ErrLeaseLost
		}
		return job.Execution{}, err
	}
	if allowed, rateErr := s.takeRateLimits(ctx, tx, r.ProjectID, r.QueueID, r.DefinitionID); rateErr != nil {
		return job.Execution{}, rateErr
	} else if !allowed {
		return job.Execution{}, ErrRateLimited
	}
	for _, scope := range []string{"tenant:" + tenantID.String(), "project:" + r.ProjectID.String(), "queue:" + r.QueueID.String(), "function:" + functionID.String()} {
		if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", scope); err != nil {
			return job.Execution{}, err
		}
	}
	checks := []struct {
		query string
		id    uuid.UUID
		limit int
	}{
		{"SELECT count(*) FROM job_runs WHERE status='RUNNING' AND lease_expires_at>now() AND project_id=$1", r.ProjectID, projectLimit},
		{"SELECT count(*) FROM job_runs WHERE status='RUNNING' AND lease_expires_at>now() AND queue_id=$1", r.QueueID, queueLimit},
		{"SELECT count(*) FROM job_runs r JOIN job_definitions jd ON jd.id=r.job_definition_id WHERE r.status='RUNNING' AND r.lease_expires_at>now() AND jd.function_id=$1", functionID, functionLimit},
	}
	for _, check := range checks {
		var active int
		if err = tx.QueryRow(ctx, check.query, check.id).Scan(&active); err != nil {
			return job.Execution{}, err
		}
		if active >= check.limit {
			return job.Execution{}, ErrConcurrencyLimited
		}
	}
	var tenantActive int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM job_runs r JOIN projects p ON p.id=r.project_id WHERE r.status='RUNNING' AND r.lease_expires_at>now() AND p.tenant_id=$1`, tenantID).Scan(&tenantActive); err != nil {
		return job.Execution{}, err
	}
	if tenantActive >= tenantLimit {
		return job.Execution{}, ErrConcurrencyLimited
	}
	var policy, ciphertext []byte
	var keyRef string
	var leaseExpiresAt time.Time
	err = tx.QueryRow(ctx, `UPDATE job_runs r SET status='RUNNING',started_at=COALESCE(started_at,now()),lease_expires_at=now()+($4 * interval '1 millisecond'),updated_at=now() FROM job_definitions jd JOIN function_definitions fd ON fd.id=jd.function_id WHERE r.id=$1 AND r.job_definition_id=jd.id AND r.status='RESERVED' AND r.lease_owner=$2 AND r.lease_token=$3 AND r.lease_expires_at>now() AND jd.deleted_at IS NULL AND fd.deleted_at IS NULL RETURNING r.id,r.function_version,fd.function_key,r.idempotency_key,r.payload,r.payload_ciphertext,COALESCE(r.encryption_key_ref,''),r.policy_snapshot,r.lease_expires_at`, runID, owner, token, lease.Milliseconds()).Scan(&r.ID, &r.FunctionVersion, &r.FunctionKey, &r.IdempotencyKey, &r.Payload, &ciphertext, &keyRef, &policy, &leaseExpiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return job.Execution{}, ErrLeaseLost
		}
		return job.Execution{}, err
	}
	if err = json.Unmarshal(policy, &r.Policy); err != nil {
		return job.Execution{}, err
	}
	if r.Payload, err = s.openPayload(ctx, r.ProjectID, r.DefinitionID, r.Payload, ciphertext, keyRef); err != nil {
		return job.Execution{}, err
	}
	var n int
	if err = tx.QueryRow(ctx, "SELECT COALESCE(MAX(attempt_number),0)+1 FROM job_attempts WHERE job_run_id=$1", runID).Scan(&n); err != nil {
		return job.Execution{}, err
	}
	attemptID := uuid.New()
	traceID := ""
	if span := trace.SpanContextFromContext(ctx); span.IsValid() {
		traceID = span.TraceID().String()
	}
	_, err = tx.Exec(ctx, "INSERT INTO job_attempts(id,job_run_id,worker_id,attempt_number,status,trace_id) VALUES($1,$2,$3,$4,'RUNNING',NULLIF($5,''))", attemptID, runID, workerID, n, traceID)
	if err != nil {
		return job.Execution{}, err
	}
	_, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,'job.started','job_run',$2,jsonb_build_object('attempt_id',$3,'attempt_number',$4))", r.ProjectID, runID, attemptID, n)
	if err != nil {
		return job.Execution{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return job.Execution{}, err
	}
	return job.Execution{RunID: runID, AttemptID: attemptID, TenantID: tenantID, ProjectID: r.ProjectID, QueueID: r.QueueID, DefinitionID: r.DefinitionID, FunctionKey: r.FunctionKey, FunctionVersion: r.FunctionVersion, IdempotencyKey: r.IdempotencyKey, Payload: r.Payload, Policy: r.Policy, Attempt: n, LeaseToken: token, LeaseExpiresAt: leaseExpiresAt}, nil
}

func (s *Store) takeRateLimits(ctx context.Context, tx pgx.Tx, project, queue, definition uuid.UUID) (bool, error) {
	rows, err := tx.Query(ctx, "SELECT p.id,p.capacity,p.refill_tokens,p.refill_period_ms,b.tokens,b.last_refilled_at FROM rate_limit_policies p JOIN rate_limit_buckets b ON b.policy_id=p.id WHERE p.project_id=$1 AND p.status='ACTIVE' AND p.deleted_at IS NULL AND (p.scope='PROJECT' OR (p.scope='QUEUE' AND p.target_id=$2) OR (p.scope='FUNCTION' AND p.target_id=(SELECT function_id FROM job_definitions WHERE id=$3))) ORDER BY p.id FOR UPDATE OF p,b", project, queue, definition)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	type bucket struct {
		id               uuid.UUID
		capacity, refill int
		period           int64
		tokens           float64
		at               time.Time
	}
	items := []bucket{}
	for rows.Next() {
		var item bucket
		if err = rows.Scan(&item.id, &item.capacity, &item.refill, &item.period, &item.tokens, &item.at); err != nil {
			return false, err
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return false, err
	}
	now := time.Now()
	for _, item := range items {
		elapsed := now.Sub(item.at).Milliseconds()
		if elapsed > 0 {
			item.tokens = minFloat(float64(item.capacity), item.tokens+float64(elapsed/item.period*int64(item.refill)))
			if _, err = tx.Exec(ctx, "UPDATE rate_limit_buckets SET tokens=$2,last_refilled_at=$3,updated_at=now() WHERE policy_id=$1", item.id, item.tokens, now); err != nil {
				return false, err
			}
		}
		if item.tokens < 1 {
			return false, nil
		}
	}
	for _, item := range items {
		if _, err = tx.Exec(ctx, "UPDATE rate_limit_buckets SET tokens=tokens-1,updated_at=now() WHERE policy_id=$1 AND tokens>=1", item.id); err != nil {
			return false, err
		}
	}
	return true, nil
}
func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
func (s *Store) ReleaseReservation(ctx context.Context, runID uuid.UUID, owner string, token uuid.UUID) error {
	_, err := s.Pool.Exec(ctx, "UPDATE job_runs SET status='QUEUED',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND status='RESERVED' AND lease_owner=$2 AND lease_token=$3", runID, owner, token)
	return err
}
func (s *Store) CompleteSuccess(ctx context.Context, ex job.Execution, owner string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = setSystemAuditContext(ctx, tx); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, "UPDATE job_runs SET status='SUCCEEDED',finished_at=now(),lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND status='RUNNING' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at>now()", ex.RunID, owner, ex.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	_, err = tx.Exec(ctx, "UPDATE job_attempts SET status='SUCCEEDED',finished_at=now() WHERE id=$1", ex.AttemptID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,'job.succeeded','job_run',$2,jsonb_build_object('attempt_id',$3))", ex.ProjectID, ex.RunID, ex.AttemptID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) CompleteFailure(ctx context.Context, ex job.Execution, owner string, class *job.ClassifiedError, policy job.RetryPolicy, now time.Time) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = setSystemAuditContext(ctx, tx); err != nil {
		return err
	}
	retry := ex.Attempt < policy.MaxAttempts && job.Retryable(class.Class, policy)
	status := "DEAD_LETTER"
	available := now
	if retry {
		status = "RETRY_WAIT"
		available = job.NextRetry(now, ex.Attempt, policy, nil)
	}
	tag, err := tx.Exec(ctx, "UPDATE job_runs SET status=$4,available_at=$5,finished_at=CASE WHEN $4='DEAD_LETTER' THEN now() ELSE NULL END,lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND status='RUNNING' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at>now()", ex.RunID, owner, ex.LeaseToken, status, available)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	_, err = tx.Exec(ctx, "UPDATE job_attempts SET status='FAILED',error_class=$2,error_code=$3,error_message=$4,finished_at=now() WHERE id=$1", ex.AttemptID, class.Class, class.Code, truncate(class.Error(), 1000))
	if err != nil {
		return err
	}
	if !retry {
		_, err = tx.Exec(ctx, "INSERT INTO dlq_entries(job_run_id,reason) VALUES($1,$2) ON CONFLICT(job_run_id) DO NOTHING", ex.RunID, class.Class)
		if err != nil {
			return err
		}
	}
	eventType := "job.retry_scheduled"
	if !retry {
		eventType = "job.dlq"
	}
	_, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,$2,'job_run',$3,jsonb_build_object('attempt_id',$4,'error_class',$5,'available_at',$6))", ex.ProjectID, eventType, ex.RunID, ex.AttemptID, class.Class, available)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) ReportProgress(ctx context.Context, ex *job.Execution, owner string, percent int, message string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE job_attempts a SET progress_pct=$3,progress_message=$4,updated_at=now() FROM job_runs r WHERE a.id=$1 AND a.job_run_id=r.id AND r.id=$2 AND r.status='RUNNING' AND r.lease_owner=$5 AND r.lease_token=$6 AND r.lease_expires_at>now()`, ex.AttemptID, ex.RunID, percent, truncate(message, 500), owner, ex.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	_, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,'job.progress','job_run',$2,jsonb_build_object('attempt_id',$3,'percent',$4,'message',$5))", ex.ProjectID, ex.RunID, ex.AttemptID, percent, truncate(message, 500))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
