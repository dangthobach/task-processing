package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/example/task-processing/internal/domain/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrWorkflowRetryOptimisticLock = errors.New("workflow retry optimistic lock lost")
var ErrWorkflowRunNotRetryable = errors.New("workflow run is not retryable")
var ErrWorkflowRunExecuting = errors.New("workflow run has an executing node")

type WorkflowRetryAudit func(context.Context, pgx.Tx, uuid.UUID, json.RawMessage) error

type WorkflowRetryResult struct {
	RunID   uuid.UUID
	Status  string
	Version int64
	Created bool
}

// StartWorkflow creates its immutable runtime graph, then dispatches every
// root. Dispatch is idempotent through the workflow/node idempotency key.
func (s *Store) StartWorkflow(ctx context.Context, project, workflowID uuid.UUID, input json.RawMessage) (uuid.UUID, error) {
	if len(input) == 0 {
		input = json.RawMessage("{}")
	}
	if !json.Valid(input) {
		return uuid.Nil, fmt.Errorf("workflow input must be JSON")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback(ctx)
	run, err := startWorkflowTx(ctx, tx, project, workflowID, input, nil, nil)
	if err != nil {
		return uuid.Nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return run, nil
}

func startWorkflowTx(ctx context.Context, tx pgx.Tx, project, workflowID uuid.UUID, input json.RawMessage, retryOf *uuid.UUID, failureOverride *string) (uuid.UUID, error) {
	var failurePolicy string
	if err := tx.QueryRow(ctx, "SELECT failure_policy FROM workflow_definitions WHERE id=$1 AND project_id=$2 AND status='ACTIVE' AND deleted_at IS NULL FOR UPDATE", workflowID, project).Scan(&failurePolicy); err != nil {
		return uuid.Nil, fmt.Errorf("workflow not found or inactive")
	}
	if failureOverride != nil {
		failurePolicy = *failureOverride
	}
	run := uuid.New()
	if _, err := tx.Exec(ctx, "INSERT INTO workflow_runs(id,project_id,workflow_id,retry_of_run_id,status,input,failure_policy_snapshot) VALUES($1,$2,$3,$4,'RUNNING',$5,$6)", run, project, workflowID, retryOf, input, failurePolicy); err != nil {
		return uuid.Nil, err
	}
	if retryOf != nil {
		if err := copyWorkflowSnapshotTx(ctx, tx, *retryOf, run); err != nil {
			return uuid.Nil, err
		}
	} else if _, err := tx.Exec(ctx, `INSERT INTO workflow_node_runs(workflow_run_id,node_id,node_key,job_definition_id,execution_snapshot,status)
		SELECT $1,n.id,n.node_key,n.job_definition_id,jsonb_build_object(
			'definition_id',jd.id,'queue_id',jd.queue_id,'function_key',fd.function_key,'function_version',fd.version,'input_schema',fd.input_schema,'priority',jd.default_priority,
			'policy',jsonb_build_object('retry',jsonb_build_object('max_attempts',COALESCE(rp.max_attempts,1),'strategy',COALESCE(rp.strategy,'FIXED'),'initial_delay_ms',COALESCE(rp.initial_delay_ms,0),'multiplier',COALESCE(rp.multiplier,1),'max_delay_ms',COALESCE(rp.max_delay_ms,0),'jitter_pct',COALESCE(rp.jitter_pct,0),'retry_timeout',COALESCE(rp.retry_timeout,false),'retry_rate_limited',COALESCE(rp.retry_rate_limited,false),'retry_dependency_error',COALESCE(rp.retry_dependency_error,false),'retry_validation_error',COALESCE(rp.retry_validation_error,false)),'timeout_ms',jd.timeout_ms)
		),'PENDING'
		FROM workflow_nodes n JOIN job_definitions jd ON jd.id=n.job_definition_id JOIN function_definitions fd ON fd.id=jd.function_id LEFT JOIN retry_policies rp ON rp.id=jd.retry_policy_id
		WHERE n.workflow_id=$2 AND jd.deleted_at IS NULL AND fd.deleted_at IS NULL AND (rp.id IS NULL OR rp.deleted_at IS NULL)`, run, workflowID); err != nil {
		return uuid.Nil, err
	}
	if retryOf == nil {
		if _, err := tx.Exec(ctx, `INSERT INTO workflow_run_edges(workflow_run_id,from_node_run_id,to_node_run_id,condition_type)
		SELECT $1,from_run.id,to_run.id,e.condition_type FROM workflow_edges e
		JOIN workflow_node_runs from_run ON from_run.workflow_run_id=$1 AND from_run.node_id=e.from_node_id
		JOIN workflow_node_runs to_run ON to_run.workflow_run_id=$1 AND to_run.node_id=e.to_node_id
		WHERE e.workflow_id=$2`, run, workflowID); err != nil {
			return uuid.Nil, err
		}
	}
	if err := enqueueWorkflowDispatchTx(ctx, tx, run); err != nil {
		return uuid.Nil, err
	}
	return run, nil
}

func copyWorkflowSnapshotTx(ctx context.Context, tx pgx.Tx, sourceRun, targetRun uuid.UUID) error {
	// Do not read mutable workflow definitions here. The source run contains
	// the captured function, queue, retry-policy and graph values it originally
	// executed with, which is the only safe retry contract.
	if _, err := tx.Exec(ctx, `INSERT INTO workflow_node_runs(workflow_run_id,node_id,node_key,job_definition_id,execution_snapshot,status)
		SELECT $2,node_id,node_key,job_definition_id,execution_snapshot,'PENDING'
		FROM workflow_node_runs WHERE workflow_run_id=$1`, sourceRun, targetRun); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO workflow_run_edges(workflow_run_id,from_node_run_id,to_node_run_id,condition_type)
		SELECT $2,new_from.id,new_to.id,old_edge.condition_type
		FROM workflow_run_edges old_edge
		JOIN workflow_node_runs old_from ON old_from.id=old_edge.from_node_run_id
		JOIN workflow_node_runs old_to ON old_to.id=old_edge.to_node_run_id
		JOIN workflow_node_runs new_from ON new_from.workflow_run_id=$2 AND new_from.node_id=old_from.node_id
		JOIN workflow_node_runs new_to ON new_to.workflow_run_id=$2 AND new_to.node_id=old_to.node_id
		WHERE old_edge.workflow_run_id=$1`, sourceRun, targetRun)
	return err
}

// RetryWorkflow creates at most one child execution for a failed, cancelled or
// manually-paused source. The source row is locked first, making concurrent
// retries idempotently return the same child. It refuses to replay while a
// parallel source node is still running.
func (s *Store) RetryWorkflow(ctx context.Context, project, source uuid.UUID, version int64, audit WorkflowRetryAudit) (WorkflowRetryResult, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return WorkflowRetryResult{}, err
	}
	defer tx.Rollback(ctx)
	var workflow uuid.UUID
	var input json.RawMessage
	var current int64
	var status string
	var failurePolicy string
	err = tx.QueryRow(ctx, "SELECT workflow_id,input,row_version,status,failure_policy_snapshot FROM workflow_runs WHERE id=$1 AND project_id=$2 FOR UPDATE", source, project).Scan(&workflow, &input, &current, &status, &failurePolicy)
	if err != nil {
		return WorkflowRetryResult{}, err
	}
	if current != version {
		return WorkflowRetryResult{}, ErrWorkflowRetryOptimisticLock
	}
	if status != "FAILED" && status != "CANCELLED" && status != "AWAITING_INTERVENTION" {
		return WorkflowRetryResult{}, ErrWorkflowRunNotRetryable
	}
	var active bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workflow_node_runs WHERE workflow_run_id=$1 AND status='RUNNING')", source).Scan(&active); err != nil {
		return WorkflowRetryResult{}, err
	}
	if active {
		return WorkflowRetryResult{}, ErrWorkflowRunExecuting
	}
	var existing WorkflowRetryResult
	err = tx.QueryRow(ctx, "SELECT id,status,row_version FROM workflow_runs WHERE retry_of_run_id=$1", source).Scan(&existing.RunID, &existing.Status, &existing.Version)
	if err == nil {
		err = tx.Commit(ctx)
		return existing, err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return WorkflowRetryResult{}, err
	}
	child, err := startWorkflowTx(ctx, tx, project, workflow, input, &source, &failurePolicy)
	if err != nil {
		return WorkflowRetryResult{}, err
	}
	result := WorkflowRetryResult{RunID: child, Status: "RUNNING", Created: true}
	if audit != nil {
		var after json.RawMessage
		if err = tx.QueryRow(ctx, "SELECT row_version,((to_jsonb(workflow_runs) - 'row_version') || jsonb_build_object('version', row_version)) FROM workflow_runs WHERE id=$1", child).Scan(&result.Version, &after); err != nil {
			return WorkflowRetryResult{}, err
		}
		if err = audit(ctx, tx, child, after); err != nil {
			return WorkflowRetryResult{}, err
		}
	} else if err = tx.QueryRow(ctx, "SELECT row_version FROM workflow_runs WHERE id=$1", child).Scan(&result.Version); err != nil {
		return WorkflowRetryResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return WorkflowRetryResult{}, err
	}
	return result, nil
}

func enqueueWorkflowDispatchTx(ctx context.Context, tx pgx.Tx, workflowRun uuid.UUID) error {
	_, err := tx.Exec(ctx, `INSERT INTO workflow_dispatch_outbox(workflow_run_id,available_at,lease_owner,lease_token,lease_expires_at,last_error,updated_at)
		VALUES($1,now(),NULL,NULL,NULL,NULL,now())
		ON CONFLICT(workflow_run_id) DO UPDATE SET available_at=LEAST(workflow_dispatch_outbox.available_at,EXCLUDED.available_at),lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,last_error=NULL,updated_at=now()`, workflowRun)
	return err
}

func (s *Store) DispatchReadyWorkflowNodes(ctx context.Context, run uuid.UUID) error {
	// Serialize dispatch and cancellation per workflow. Submit itself has a
	// separate, durable transaction; holding this short row lock prevents a
	// cancellation from racing between submit and node state publication.
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var locked uuid.UUID
	err = tx.QueryRow(ctx, "SELECT id FROM workflow_runs WHERE id=$1 AND status='RUNNING' FOR UPDATE", run).Scan(&locked)
	if err == pgx.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	var nodeLimit int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM workflow_node_runs WHERE workflow_run_id=$1", run).Scan(&nodeLimit); err != nil {
		return err
	}
	for dispatched := 0; dispatched < nodeLimit; dispatched++ {
		// A node whose parents have all terminated but whose edge conditions do
		// not all match is terminally skipped. This retains the existing DAG
		// join semantics (every incoming edge is required) while allowing a
		// failure branch to be expressed without a gateway primitive.
		if _, err = tx.Exec(ctx, `UPDATE workflow_node_runs nr SET status='SKIPPED',finished_at=now()
			WHERE nr.workflow_run_id=$1 AND nr.status='PENDING'
			AND EXISTS(SELECT 1 FROM workflow_run_edges e WHERE e.workflow_run_id=nr.workflow_run_id AND e.to_node_run_id=nr.id)
			AND NOT EXISTS(SELECT 1 FROM workflow_run_edges e JOIN workflow_node_runs parent ON parent.id=e.from_node_run_id
				WHERE e.workflow_run_id=nr.workflow_run_id AND e.to_node_run_id=nr.id AND parent.status IN ('PENDING','BLOCKED','RUNNING'))
			AND EXISTS(SELECT 1 FROM workflow_run_edges e JOIN workflow_node_runs parent ON parent.id=e.from_node_run_id
				WHERE e.workflow_run_id=nr.workflow_run_id AND e.to_node_run_id=nr.id AND NOT (
					(e.condition_type='ON_SUCCESS' AND parent.status='SUCCEEDED') OR
					(e.condition_type='ON_FAILURE' AND parent.status='FAILED') OR
					(e.condition_type='ALWAYS' AND parent.status IN ('SUCCEEDED','FAILED','CANCELLED','SKIPPED'))
				))`, run); err != nil {
			return err
		}
		var nodeRun, definition, project uuid.UUID
		var input, snapshot json.RawMessage
		err := tx.QueryRow(ctx, `SELECT nr.id,nr.job_definition_id,wr.project_id,wr.input,nr.execution_snapshot
			FROM workflow_node_runs nr JOIN workflow_runs wr ON wr.id=nr.workflow_run_id
			WHERE nr.workflow_run_id=$1 AND nr.status='PENDING' AND NOT EXISTS(
				SELECT 1 FROM workflow_run_edges e JOIN workflow_node_runs parent ON parent.id=e.from_node_run_id
				WHERE e.workflow_run_id=nr.workflow_run_id AND e.to_node_run_id=nr.id AND NOT (
					(e.condition_type='ON_SUCCESS' AND parent.status='SUCCEEDED') OR
					(e.condition_type='ON_FAILURE' AND parent.status='FAILED') OR
					(e.condition_type='ALWAYS' AND parent.status IN ('SUCCEEDED','FAILED','CANCELLED','SKIPPED'))
				)
			) ORDER BY nr.created_at,nr.id LIMIT 1 FOR UPDATE OF nr`, run).Scan(&nodeRun, &definition, &project, &input, &snapshot)
		if err != nil {
			if err == pgx.ErrNoRows {
				if _, err = completeWorkflowIfTerminalTx(ctx, tx, run); err != nil {
					return err
				}
				return tx.Commit(ctx)
			}
			return err
		}
		captured, err := decodeWorkflowDefinition(snapshot)
		if err != nil {
			return err
		}
		captured.ProjectID = project
		if captured.ID != definition {
			return fmt.Errorf("workflow execution snapshot definition mismatch")
		}
		key := "workflow:" + run.String() + ":" + nodeRun.String()
		submitted, err := s.Submit(ctx, Submit{ProjectID: project, DefinitionID: definition, Payload: input, IdempotencyKey: key, Snapshot: &captured})
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, "UPDATE workflow_node_runs SET status='RUNNING',job_run_id=$2 WHERE id=$1 AND status='PENDING'", nodeRun, submitted.ID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			continue
		}
	}
	var pending bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workflow_node_runs WHERE workflow_run_id=$1 AND status='PENDING')", run).Scan(&pending); err != nil {
		return err
	}
	if !pending {
		return tx.Commit(ctx)
	}
	return fmt.Errorf("workflow dispatch exceeded immutable node bound")
}

func decodeWorkflowDefinition(snapshot json.RawMessage) (Definition, error) {
	var raw struct {
		DefinitionID uuid.UUID          `json:"definition_id"`
		QueueID      uuid.UUID          `json:"queue_id"`
		FunctionKey  string             `json:"function_key"`
		Version      string             `json:"function_version"`
		InputSchema  json.RawMessage    `json:"input_schema"`
		Priority     job.Priority       `json:"priority"`
		Policy       job.PolicySnapshot `json:"policy"`
	}
	if err := json.Unmarshal(snapshot, &raw); err != nil {
		return Definition{}, err
	}
	if raw.DefinitionID == uuid.Nil || raw.QueueID == uuid.Nil || raw.FunctionKey == "" || raw.Version == "" || raw.Priority < job.Bulk || raw.Priority > job.Critical {
		return Definition{}, fmt.Errorf("workflow execution snapshot is invalid")
	}
	return Definition{ID: raw.DefinitionID, QueueID: raw.QueueID, FunctionKey: raw.FunctionKey, FunctionVersion: raw.Version, InputSchema: raw.InputSchema, Priority: raw.Priority, Policy: raw.Policy}, nil
}

func advanceWorkflowForJobTx(ctx context.Context, tx pgx.Tx, runID uuid.UUID, succeeded bool) (uuid.UUID, error) {
	var workflowRun uuid.UUID
	err := tx.QueryRow(ctx, "SELECT workflow_run_id FROM workflow_node_runs WHERE job_run_id=$1", runID).Scan(&workflowRun)
	if err != nil {
		if err == pgx.ErrNoRows {
			return uuid.Nil, nil
		}
		return uuid.Nil, err
	}
	// Every transition takes the run fence before the node fence. Dispatch,
	// cancel and retry use the same order, preventing cyclic lock waits under
	// parallel completions.
	var project uuid.UUID
	var workflowStatus, failurePolicy string
	if err = tx.QueryRow(ctx, "SELECT project_id,status,failure_policy_snapshot FROM workflow_runs WHERE id=$1 FOR UPDATE", workflowRun).Scan(&project, &workflowStatus, &failurePolicy); err != nil {
		return uuid.Nil, err
	}
	var nodeRun uuid.UUID
	err = tx.QueryRow(ctx, "SELECT id FROM workflow_node_runs WHERE job_run_id=$1 AND status='RUNNING' FOR UPDATE", runID).Scan(&nodeRun)
	if err != nil {
		if err == pgx.ErrNoRows {
			return uuid.Nil, nil
		}
		return uuid.Nil, err
	}
	if succeeded {
		if _, err = tx.Exec(ctx, "UPDATE workflow_node_runs SET status='SUCCEEDED',finished_at=now() WHERE id=$1", nodeRun); err != nil {
			return uuid.Nil, err
		}
	} else {
		if _, err = tx.Exec(ctx, "UPDATE workflow_node_runs SET status='FAILED',finished_at=now() WHERE id=$1", nodeRun); err != nil {
			return uuid.Nil, err
		}
	}

	if workflowStatus != "RUNNING" {
		return workflowRun, nil
	}
	if !succeeded {
		switch failurePolicy {
		case "FAIL_FAST":
			if _, err = tx.Exec(ctx, "UPDATE workflow_node_runs SET status='CANCELLED',finished_at=now() WHERE workflow_run_id=$1 AND status IN ('PENDING','BLOCKED')", workflowRun); err != nil {
				return uuid.Nil, err
			}
			if _, err = tx.Exec(ctx, "DELETE FROM workflow_dispatch_outbox WHERE workflow_run_id=$1", workflowRun); err != nil {
				return uuid.Nil, err
			}
			if _, err = tx.Exec(ctx, "UPDATE workflow_runs SET status='FAILED',finished_at=now() WHERE id=$1 AND status='RUNNING'", workflowRun); err != nil {
				return uuid.Nil, err
			}
			_, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,'workflow.failed','workflow_run',$2,jsonb_build_object('policy',$3::text))", project, workflowRun, failurePolicy)
			return workflowRun, err
		case "MANUAL_INTERVENTION":
			if _, err = tx.Exec(ctx, "UPDATE workflow_node_runs SET status='BLOCKED' WHERE workflow_run_id=$1 AND status='PENDING'", workflowRun); err != nil {
				return uuid.Nil, err
			}
			if _, err = tx.Exec(ctx, "DELETE FROM workflow_dispatch_outbox WHERE workflow_run_id=$1", workflowRun); err != nil {
				return uuid.Nil, err
			}
			if _, err = tx.Exec(ctx, "UPDATE workflow_runs SET status='AWAITING_INTERVENTION' WHERE id=$1 AND status='RUNNING'", workflowRun); err != nil {
				return uuid.Nil, err
			}
			_, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,'workflow.awaiting_intervention','workflow_run',$2,jsonb_build_object('policy',$3::text))", project, workflowRun, failurePolicy)
			return workflowRun, err
		case "CONTINUE":
			// continue below: successful failure-branch edges may now be ready.
		default:
			return uuid.Nil, fmt.Errorf("workflow failure policy snapshot is invalid")
		}
	}
	completed, err := completeWorkflowIfTerminalTx(ctx, tx, workflowRun)
	if err != nil {
		return uuid.Nil, err
	}
	if !completed {
		if err = enqueueWorkflowDispatchTx(ctx, tx, workflowRun); err != nil {
			return uuid.Nil, err
		}
	}
	return workflowRun, nil
}

// completeWorkflowIfTerminalTx derives the aggregate terminal state from its
// immutable node runs. Skipped nodes are successful routing outcomes; any
// failed node makes a CONTINUE workflow terminally FAILED.
func completeWorkflowIfTerminalTx(ctx context.Context, tx pgx.Tx, workflowRun uuid.UUID) (bool, error) {
	var incomplete bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workflow_node_runs WHERE workflow_run_id=$1 AND status IN ('PENDING','BLOCKED','RUNNING'))", workflowRun).Scan(&incomplete); err != nil {
		return false, err
	}
	if incomplete {
		return false, nil
	}
	var failed bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workflow_node_runs WHERE workflow_run_id=$1 AND status='FAILED')", workflowRun).Scan(&failed); err != nil {
		return false, err
	}
	status := "SUCCEEDED"
	if failed {
		status = "FAILED"
	}
	tag, err := tx.Exec(ctx, "UPDATE workflow_runs SET status=$2,finished_at=now() WHERE id=$1 AND status='RUNNING'", workflowRun, status)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// AdvanceWorkflowForJob is retained for operational callers. Normal terminal
// completion calls the transactional helper from CompleteSuccess/Failure.
func (s *Store) AdvanceWorkflowForJob(ctx context.Context, runID uuid.UUID, succeeded bool) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = advanceWorkflowForJobTx(ctx, tx, runID, succeeded)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ReconcileWorkflowTransitions repairs records left by older releases or a
// process crash, then drains durable dispatch signals. Submission is safe to
// repeat because every node has a stable idempotency key.
func (s *Store) ReconcileWorkflowTransitions(ctx context.Context, owner string, limit int, lease time.Duration) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	if lease <= 0 {
		lease = 30 * time.Second
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT nr.job_run_id, jr.status='SUCCEEDED'
		FROM workflow_node_runs nr JOIN job_runs jr ON jr.id=nr.job_run_id
		WHERE nr.status='RUNNING' AND jr.status IN ('SUCCEEDED','DEAD_LETTER','CANCELLED')
		ORDER BY nr.created_at LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var id uuid.UUID
		var succeeded bool
		if err = rows.Scan(&id, &succeeded); err != nil {
			rows.Close()
			return 0, err
		}
		if _, err = advanceWorkflowForJobTx(ctx, tx, id, succeeded); err != nil {
			rows.Close()
			return 0, err
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}

	rows, err = s.Pool.Query(ctx, `UPDATE workflow_dispatch_outbox SET lease_owner=$1,lease_token=gen_random_uuid(),lease_expires_at=now()+$3::interval,attempts=attempts+1,updated_at=now()
		WHERE workflow_run_id IN (SELECT workflow_run_id FROM workflow_dispatch_outbox WHERE available_at<=now() AND (lease_expires_at IS NULL OR lease_expires_at<now()) ORDER BY available_at,created_at LIMIT $2 FOR UPDATE SKIP LOCKED)
		RETURNING workflow_run_id,lease_token`, owner, limit, lease.String())
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var run, token uuid.UUID
		if err = rows.Scan(&run, &token); err != nil {
			return count, err
		}
		if err = s.DispatchReadyWorkflowNodes(ctx, run); err != nil {
			_, _ = s.Pool.Exec(ctx, `UPDATE workflow_dispatch_outbox SET available_at=now()+LEAST((attempts * 2),60) * interval '1 second',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,last_error=$3,updated_at=now() WHERE workflow_run_id=$1 AND lease_owner=$2`, run, owner, truncate(err.Error(), 1000))
			continue
		}
		if _, err = s.Pool.Exec(ctx, "DELETE FROM workflow_dispatch_outbox WHERE workflow_run_id=$1 AND lease_owner=$2 AND lease_token=$3", run, owner, token); err != nil {
			return count, err
		}
		count++
	}
	return count, rows.Err()
}
