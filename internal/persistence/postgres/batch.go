package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/example/task-processing/internal/domain/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/trace"
	"strings"
	"time"
)

// ClaimBatches groups only compatible BATCH-mode runs. A database transaction and
// SKIP LOCKED make a run belong to at most one batch even with many workers.
func (s *Store) ClaimBatches(ctx context.Context, worker uuid.UUID, owner string, limit int, lease time.Duration) ([]uuid.UUID, error) {
	var batches []uuid.UUID
	for len(batches) < limit {
		tx, err := s.Pool.Begin(ctx)
		if err != nil {
			return batches, err
		}
		var project, queue, definition uuid.UUID
		var size int
		err = tx.QueryRow(ctx, `SELECT r.project_id,r.queue_id,r.job_definition_id,jd.batch_size FROM job_runs r JOIN job_definitions jd ON jd.id=r.job_definition_id JOIN queues q ON q.id=r.queue_id LEFT JOIN queue_backends qb ON qb.id=q.backend_id WHERE r.status='QUEUED' AND r.available_at<=now() AND jd.execution_mode='BATCH' AND q.status='ACTIVE' AND q.deleted_at IS NULL AND jd.deleted_at IS NULL AND (q.backend_id IS NULL OR (qb.backend_type='POSTGRES' AND qb.status='ACTIVE' AND qb.deleted_at IS NULL)) AND (r.created_at<=now()-(jd.batch_max_wait_ms * interval '1 millisecond') OR (SELECT count(*) FROM job_runs candidate WHERE candidate.status='QUEUED' AND candidate.available_at<=now() AND candidate.project_id=r.project_id AND candidate.queue_id=r.queue_id AND candidate.job_definition_id=r.job_definition_id)>=jd.batch_size) ORDER BY r.priority DESC,r.available_at,r.id FOR UPDATE OF r SKIP LOCKED LIMIT 1`).Scan(&project, &queue, &definition, &size)
		if err == pgx.ErrNoRows {
			_ = tx.Rollback(ctx)
			break
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return batches, err
		}
		rows, err := tx.Query(ctx, `SELECT id FROM job_runs WHERE status='QUEUED' AND available_at<=now() AND project_id=$1 AND queue_id=$2 AND job_definition_id=$3 ORDER BY priority DESC,available_at,id FOR UPDATE SKIP LOCKED LIMIT $4`, project, queue, definition, size)
		if err != nil {
			_ = tx.Rollback(ctx)
			return batches, err
		}
		var runs []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				_ = tx.Rollback(ctx)
				return batches, err
			}
			runs = append(runs, id)
		}
		rows.Close()
		if len(runs) == 0 {
			_ = tx.Rollback(ctx)
			continue
		}
		batch := uuid.New()
		leaseToken := uuid.New()
		_, err = tx.Exec(ctx, `INSERT INTO job_batches(id,project_id,queue_id,job_definition_id,worker_id,lease_owner,lease_token,lease_expires_at,status,total_items) VALUES($1,$2,$3,$4,$5,$6,$7,now()+($8::bigint * interval '1 millisecond'),'RESERVED',$9)`, batch, project, queue, definition, worker, owner, leaseToken, lease.Milliseconds(), len(runs))
		if err != nil {
			_ = tx.Rollback(ctx)
			return batches, err
		}
		for ordinal, run := range runs {
			tag, updateErr := tx.Exec(ctx, "UPDATE job_runs SET status='RESERVED',lease_owner=$2,lease_token=$3,lease_expires_at=now()+($4::bigint * interval '1 millisecond'),reserved_at=now(),updated_at=now() WHERE id=$1 AND status='QUEUED'", run, owner, leaseToken, lease.Milliseconds())
			if updateErr != nil {
				err = updateErr
				_ = tx.Rollback(ctx)
				return batches, err
			}
			if tag.RowsAffected() != 1 {
				_ = tx.Rollback(ctx)
				return batches, ErrLeaseLost
			}
			if _, err = tx.Exec(ctx, "INSERT INTO job_batch_items(batch_id,job_run_id,ordinal) VALUES($1,$2,$3)", batch, run, ordinal); err != nil {
				_ = tx.Rollback(ctx)
				return batches, err
			}
		}
		if _, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,'batch.reserved','job_batch',$2,jsonb_build_object('total_items',$3::integer))", project, batch, len(runs)); err != nil {
			_ = tx.Rollback(ctx)
			return batches, err
		}
		if err = tx.Commit(ctx); err != nil {
			return batches, err
		}
		batches = append(batches, batch)
	}
	return batches, nil
}

func (s *Store) StartBatch(ctx context.Context, batchID, worker uuid.UUID, owner string, lease time.Duration) (job.BatchExecution, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return job.BatchExecution{}, err
	}
	defer tx.Rollback(ctx)
	if err = setSystemAuditContext(ctx, tx); err != nil {
		return job.BatchExecution{}, err
	}
	var b job.BatchExecution
	var policy []byte
	err = tx.QueryRow(ctx, `SELECT b.project_id,b.queue_id,b.job_definition_id,fd.function_key,r.function_version,r.policy_snapshot,b.lease_token,b.lease_expires_at FROM job_batches b JOIN job_definitions jd ON jd.id=b.job_definition_id JOIN function_definitions fd ON fd.id=jd.function_id JOIN job_batch_items bi ON bi.batch_id=b.id JOIN job_runs r ON r.id=bi.job_run_id WHERE b.id=$1 AND b.status='RESERVED' AND b.lease_owner=$2 AND b.lease_token IS NOT NULL AND b.lease_expires_at>now() ORDER BY bi.ordinal LIMIT 1 FOR UPDATE OF b`, batchID, owner).Scan(&b.ProjectID, &b.QueueID, &b.DefinitionID, &b.FunctionKey, &b.FunctionVersion, &policy, &b.LeaseToken, &b.LeaseExpiresAt)
	if err != nil {
		return b, err
	}
	if err = json.Unmarshal(policy, &b.Policy); err != nil {
		return b, err
	}
	var n int
	if err = tx.QueryRow(ctx, "SELECT COALESCE(MAX(attempt_number),0)+1 FROM job_batch_attempts WHERE batch_id=$1", batchID).Scan(&n); err != nil {
		return b, err
	}
	attempt := uuid.New()
	var expiry time.Time
	err = tx.QueryRow(ctx, "UPDATE job_batches SET status='RUNNING',started_at=COALESCE(started_at,now()),lease_expires_at=now()+($4::bigint * interval '1 millisecond') WHERE id=$1 AND status='RESERVED' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at>now() RETURNING lease_expires_at", batchID, owner, b.LeaseToken, lease.Milliseconds()).Scan(&expiry)
	if err != nil {
		return b, err
	}
	b.LeaseExpiresAt = expiry
	traceID := ""
	if span := trace.SpanContextFromContext(ctx); span.IsValid() {
		traceID = span.TraceID().String()
	}
	_, err = tx.Exec(ctx, "INSERT INTO job_batch_attempts(id,batch_id,worker_id,attempt_number,status,total_items,trace_id) SELECT $1,$2,$3,$4,'RUNNING',total_items,NULLIF($5,'') FROM job_batches WHERE id=$2", attempt, batchID, worker, n, traceID)
	if err != nil {
		return b, err
	}
	rows, err := tx.Query(ctx, `SELECT bi.id,bi.ordinal,r.id,r.project_id,r.queue_id,r.job_definition_id,COALESCE(r.idempotency_key,''),r.payload,r.payload_ciphertext,COALESCE(r.encryption_key_ref,''),r.function_version,r.policy_snapshot FROM job_batch_items bi JOIN job_runs r ON r.id=bi.job_run_id WHERE bi.batch_id=$1 ORDER BY bi.ordinal`, batchID)
	if err != nil {
		return b, err
	}
	for rows.Next() {
		var item job.BatchItem
		var p, ciphertext []byte
		var keyRef string
		if err = rows.Scan(&item.ItemID, &item.Ordinal, &item.Run.RunID, &item.Run.ProjectID, &item.Run.QueueID, &item.Run.DefinitionID, &item.Run.IdempotencyKey, &item.Run.Payload, &ciphertext, &keyRef, &item.Run.FunctionVersion, &p); err != nil {
			return b, err
		}
		if err = json.Unmarshal(p, &item.Run.Policy); err != nil {
			return b, err
		}
		if item.Run.Payload, err = s.openPayload(ctx, item.Run.ProjectID, item.Run.DefinitionID, item.Run.Payload, ciphertext, keyRef); err != nil {
			return b, err
		}
		item.Run.FunctionKey = b.FunctionKey
		b.Items = append(b.Items, item)
	}
	if err = rows.Err(); err != nil {
		return b, err
	}
	rows.Close()
	for i := range b.Items {
		item := &b.Items[i]
		var itemAttempt int
		if err = tx.QueryRow(ctx, "SELECT COALESCE(MAX(attempt_number),0)+1 FROM job_attempts WHERE job_run_id=$1", item.Run.RunID).Scan(&itemAttempt); err != nil {
			return b, err
		}
		item.Run.AttemptID = uuid.New()
		item.Run.Attempt = itemAttempt
		var tag pgconn.CommandTag
		tag, err = tx.Exec(ctx, "UPDATE job_runs SET status='RUNNING',started_at=COALESCE(started_at,now()),lease_expires_at=now()+($4::bigint * interval '1 millisecond') WHERE id=$1 AND status='RESERVED' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at>now()", item.Run.RunID, owner, b.LeaseToken, lease.Milliseconds())
		if err != nil {
			return b, err
		}
		if tag.RowsAffected() != 1 {
			return b, ErrLeaseLost
		}
		_, err = tx.Exec(ctx, "INSERT INTO job_attempts(id,job_run_id,worker_id,batch_attempt_id,attempt_number,status) VALUES($1,$2,$3,$4,$5,'RUNNING')", item.Run.AttemptID, item.Run.RunID, worker, attempt, itemAttempt)
		if err != nil {
			return b, err
		}
	}
	b.BatchID = batchID
	b.AttemptID = attempt
	b.Attempt = n
	_, err = tx.Exec(ctx, "INSERT INTO job_batch_logs(project_id,batch_id,batch_attempt_id,level,message) VALUES($1,$2,$3,'INFO','batch handler started')", b.ProjectID, batchID, attempt)
	if err != nil {
		return b, err
	}
	_, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,'batch.started','job_batch',$2,jsonb_build_object('attempt_id',$3::uuid,'total_items',$4::integer))", b.ProjectID, batchID, attempt, len(b.Items))
	if err != nil {
		return b, err
	}
	if err = tx.Commit(ctx); err != nil {
		return b, err
	}
	return b, nil
}

func (s *Store) CompleteBatch(ctx context.Context, b *job.BatchExecution, owner string, results []job.BatchItemResult, handlerErr error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = setSystemAuditContext(ctx, tx); err != nil {
		return err
	}
	// Lock and verify aggregate ownership once before any item mutation. Every
	// write below repeats the fence as defence in depth; this prevents a worker
	// that wakes after recovery from committing a partial batch result.
	var held bool
	if err = tx.QueryRow(ctx, "SELECT true FROM job_batches WHERE id=$1 AND status='RUNNING' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at>now() FOR UPDATE", b.BatchID, owner, b.LeaseToken).Scan(&held); err != nil {
		if err == pgx.ErrNoRows {
			return ErrLeaseLost
		}
		return err
	}
	byID := map[uuid.UUID]job.BatchItemResult{}
	for _, r := range results {
		if _, exists := byID[r.ItemID]; exists {
			handlerErr = &job.ClassifiedError{Class: job.Permanent, Code: "DUPLICATE_BATCH_RESULT", Err: fmt.Errorf("duplicate result")}
			break
		}
		byID[r.ItemID] = r
	}
	if handlerErr == nil && len(byID) != len(b.Items) {
		handlerErr = &job.ClassifiedError{Class: job.Permanent, Code: "INCOMPLETE_BATCH_RESULT", Err: fmt.Errorf("handler returned %d results for %d items", len(byID), len(b.Items))}
	}
	successes, failures, retries := 0, 0, 0
	for _, item := range b.Items {
		result := byID[item.ItemID]
		itemErr := handlerErr
		if handlerErr == nil && !result.Success {
			itemErr = result.Error
			if itemErr == nil {
				itemErr = &job.ClassifiedError{Class: job.Permanent, Code: "ITEM_FAILED", Err: fmt.Errorf("item failed without error")}
			}
		}
		if itemErr == nil {
			successes++
			tag, updateErr := tx.Exec(ctx, "UPDATE job_runs SET status='SUCCEEDED',finished_at=now(),lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND status='RUNNING' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at>now()", item.Run.RunID, owner, b.LeaseToken)
			if updateErr != nil {
				err = updateErr
				return err
			}
			if tag.RowsAffected() != 1 {
				return ErrLeaseLost
			}
			if _, err = tx.Exec(ctx, "UPDATE job_attempts SET status='SUCCEEDED',progress_pct=100,finished_at=now(),updated_at=now() WHERE id=$1", item.Run.AttemptID); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, "UPDATE job_batch_items SET status='SUCCEEDED',completed_at=now() WHERE id=$1", item.ItemID); err != nil {
				return err
			}
			continue
		}
		failures++
		classified := job.Classify(itemErr)
		retry := item.Run.Attempt < item.Run.Policy.Retry.MaxAttempts && job.Retryable(classified.Class, item.Run.Policy.Retry)
		status := "DEAD_LETTER"
		available := time.Now()
		itemStatus := "DEAD_LETTER"
		if retry {
			status = "RETRY_WAIT"
			itemStatus = "RETRY_SCHEDULED"
			available = job.NextRetry(available, item.Run.Attempt, item.Run.Policy.Retry, nil)
			retries++
		}
		tag, updateErr := tx.Exec(ctx, "UPDATE job_runs SET status=$4,available_at=$5,finished_at=CASE WHEN $4='DEAD_LETTER' THEN now() ELSE NULL END,lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 AND status='RUNNING' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at>now()", item.Run.RunID, owner, b.LeaseToken, status, available)
		if updateErr != nil {
			err = updateErr
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrLeaseLost
		}
		if _, err = tx.Exec(ctx, "UPDATE job_attempts SET status='FAILED',error_class=$2,error_code=$3,error_message=$4,finished_at=now(),updated_at=now() WHERE id=$1", item.Run.AttemptID, classified.Class, classified.Code, truncate(classified.Error(), 1000)); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "UPDATE job_batch_items SET status=$2,error_class=$3,error_code=$4,error_message=$5,completed_at=now() WHERE id=$1", item.ItemID, itemStatus, classified.Class, classified.Code, truncate(classified.Error(), 1000)); err != nil {
			return err
		}
		if !retry {
			if _, err = tx.Exec(ctx, "INSERT INTO dlq_entries(job_run_id,reason) VALUES($1,$2) ON CONFLICT(job_run_id) DO NOTHING", item.Run.RunID, classified.Class); err != nil {
				return err
			}
		}
	}
	batchStatus := "SUCCEEDED"
	if failures > 0 && successes > 0 {
		batchStatus = "PARTIAL_FAILED"
	}
	if failures > 0 && successes == 0 {
		batchStatus = "FAILED"
	}
	tag, updateErr := tx.Exec(ctx, "UPDATE job_batches SET status=$3,processed_items=$4,succeeded_items=$5,failed_items=$6,retry_scheduled_items=$7,finished_at=now(),lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL WHERE id=$1 AND status='RUNNING' AND lease_owner=$2 AND lease_token=$8 AND lease_expires_at>now()", b.BatchID, owner, batchStatus, len(b.Items), successes, failures, retries, b.LeaseToken)
	if updateErr != nil {
		return updateErr
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	tag, err = tx.Exec(ctx, "UPDATE job_batch_attempts SET status=$3,processed_items=$4,succeeded_items=$5,failed_items=$6,progress_pct=100,finished_at=now() WHERE id=$1 AND batch_id=$2 AND status='RUNNING'", b.AttemptID, b.BatchID, batchStatus, len(b.Items), successes, failures)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	level := "INFO"
	if failures > 0 {
		level = "ERROR"
	}
	_, err = tx.Exec(ctx, "INSERT INTO job_batch_logs(project_id,batch_id,batch_attempt_id,level,message,fields) VALUES($1,$2,$3,$4,'batch handler completed',jsonb_build_object('succeeded',$5::integer,'failed',$6::integer,'retry_scheduled',$7::integer))", b.ProjectID, b.BatchID, b.AttemptID, level, successes, failures, retries)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,$2,'job_batch',$3,jsonb_build_object('succeeded',$4::integer,'failed',$5::integer,'retry_scheduled',$6::integer))", b.ProjectID, "batch."+strings.ToLower(batchStatus), b.BatchID, successes, failures, retries)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) ReportBatchProgress(ctx context.Context, b *job.BatchExecution, processed int, message string) error {
	if b == nil || len(b.Items) == 0 || processed < 0 || processed > len(b.Items) {
		return fmt.Errorf("invalid batch progress")
	}
	pct := processed * 100 / len(b.Items)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, "UPDATE job_batch_attempts a SET processed_items=$3,progress_pct=$4,progress_message=$5 WHERE a.id=$1 AND a.batch_id=$2 AND a.status='RUNNING' AND EXISTS(SELECT 1 FROM job_batches b WHERE b.id=$2 AND b.status='RUNNING' AND b.lease_token=$6 AND b.lease_expires_at>now())", b.AttemptID, b.BatchID, processed, pct, truncate(message, 500), b.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	tag, err = tx.Exec(ctx, "UPDATE job_batches SET processed_items=$2 WHERE id=$1 AND status='RUNNING' AND lease_token=$3 AND lease_expires_at>now()", b.BatchID, processed, b.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	_, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,'batch.progress','job_batch',$2,jsonb_build_object('attempt_id',$3::uuid,'processed',$4::integer,'total',$5::integer,'percent',$6::integer,'message',$7::text))", b.ProjectID, b.BatchID, b.AttemptID, processed, len(b.Items), pct, truncate(message, 500))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) WriteBatchLog(ctx context.Context, b *job.BatchExecution, level, message string, fields map[string]any) error {
	if b == nil {
		return fmt.Errorf("batch execution is required")
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	tag, err := s.Pool.Exec(ctx, "INSERT INTO job_batch_logs(project_id,batch_id,batch_attempt_id,level,message,fields) SELECT $1,$2,$3,$4,$5,$6 WHERE EXISTS(SELECT 1 FROM job_batches WHERE id=$2 AND status='RUNNING' AND lease_token=$7 AND lease_expires_at>now())", b.ProjectID, b.BatchID, b.AttemptID, level, truncate(message, 2000), raw, b.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) ExtendBatchLease(ctx context.Context, batch uuid.UUID, owner string, token uuid.UUID, lease time.Duration) (time.Time, error) {
	var expiry time.Time
	err := s.Pool.QueryRow(ctx, "UPDATE job_batches SET lease_expires_at=now()+($4::bigint * interval '1 millisecond') WHERE id=$1 AND status='RUNNING' AND lease_owner=$2 AND lease_token=$3 AND lease_expires_at>now() RETURNING lease_expires_at", batch, owner, token, lease.Milliseconds()).Scan(&expiry)
	if err != nil {
		if err == pgx.ErrNoRows {
			return time.Time{}, ErrLeaseLost
		}
		return time.Time{}, err
	}
	return expiry, nil
}
func (s *Store) RecoverExpiredBatches(ctx context.Context) (int, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, "SELECT id,project_id FROM job_batches WHERE status IN ('RESERVED','RUNNING') AND lease_expires_at<now() FOR UPDATE SKIP LOCKED")
	if err != nil {
		return 0, err
	}
	type expired struct{ id, project uuid.UUID }
	var found []expired
	for rows.Next() {
		var e expired
		if err = rows.Scan(&e.id, &e.project); err != nil {
			rows.Close()
			return 0, err
		}
		found = append(found, e)
	}
	rows.Close()
	for _, e := range found {
		if _, err = tx.Exec(ctx, "UPDATE job_runs SET status='QUEUED',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() WHERE id IN(SELECT job_run_id FROM job_batch_items WHERE batch_id=$1) AND status IN ('RESERVED','RUNNING')", e.id); err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, "UPDATE job_batch_items SET status='FAILED',error_class='TRANSIENT',error_code='LEASE_EXPIRED',error_message='batch worker lease expired',completed_at=now() WHERE batch_id=$1 AND status='PENDING'", e.id); err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, "UPDATE job_attempts SET status='FAILED',error_class='TRANSIENT',error_code='LEASE_EXPIRED',error_message='batch worker lease expired',finished_at=now(),updated_at=now() WHERE batch_attempt_id IN(SELECT id FROM job_batch_attempts WHERE batch_id=$1 AND status='RUNNING')", e.id); err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, "UPDATE job_batch_attempts SET status='FAILED',finished_at=now(),progress_message='worker lease expired' WHERE batch_id=$1 AND status='RUNNING'", e.id); err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, "UPDATE job_batches SET status='FAILED',finished_at=now(),lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL WHERE id=$1", e.id); err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO job_batch_logs(project_id,batch_id,level,message) VALUES($1,$2,'ERROR','batch lease expired; items requeued')", e.project, e.id); err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,'batch.lease_expired','job_batch',$2,'{}'::jsonb)", e.project, e.id); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(found), nil
}
