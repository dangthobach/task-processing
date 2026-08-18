package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/example/task-processing/internal/domain/job"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

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
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workflow_definitions WHERE id=$1 AND project_id=$2 AND status='ACTIVE' AND deleted_at IS NULL FOR UPDATE)", workflowID, project).Scan(&exists); err != nil || !exists {
		return uuid.Nil, fmt.Errorf("workflow not found or inactive")
	}
	run := uuid.New()
	if _, err = tx.Exec(ctx, "INSERT INTO workflow_runs(id,project_id,workflow_id,status,input) VALUES($1,$2,$3,'RUNNING',$4)", run, project, workflowID, input); err != nil {
		return uuid.Nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO workflow_node_runs(workflow_run_id,node_id,node_key,job_definition_id,execution_snapshot,status)
		SELECT $1,n.id,n.node_key,n.job_definition_id,jsonb_build_object(
			'definition_id',jd.id,'queue_id',jd.queue_id,'function_key',fd.function_key,'function_version',fd.version,'priority',jd.default_priority,
			'policy',jsonb_build_object('retry',jsonb_build_object('max_attempts',COALESCE(rp.max_attempts,1),'strategy',COALESCE(rp.strategy,'FIXED'),'initial_delay_ms',COALESCE(rp.initial_delay_ms,0),'multiplier',COALESCE(rp.multiplier,1),'max_delay_ms',COALESCE(rp.max_delay_ms,0),'jitter_pct',COALESCE(rp.jitter_pct,0),'retry_timeout',COALESCE(rp.retry_timeout,false),'retry_rate_limited',COALESCE(rp.retry_rate_limited,false),'retry_dependency_error',COALESCE(rp.retry_dependency_error,false),'retry_validation_error',COALESCE(rp.retry_validation_error,false)),'timeout_ms',jd.timeout_ms)
		),'PENDING'
		FROM workflow_nodes n JOIN job_definitions jd ON jd.id=n.job_definition_id JOIN function_definitions fd ON fd.id=jd.function_id LEFT JOIN retry_policies rp ON rp.id=jd.retry_policy_id
		WHERE n.workflow_id=$2 AND jd.deleted_at IS NULL AND fd.deleted_at IS NULL AND (rp.id IS NULL OR rp.deleted_at IS NULL)`, run, workflowID); err != nil {
		return uuid.Nil, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO workflow_run_edges(workflow_run_id,from_node_run_id,to_node_run_id)
		SELECT $1,from_run.id,to_run.id FROM workflow_edges e
		JOIN workflow_node_runs from_run ON from_run.workflow_run_id=$1 AND from_run.node_id=e.from_node_id
		JOIN workflow_node_runs to_run ON to_run.workflow_run_id=$1 AND to_run.node_id=e.to_node_id
		WHERE e.workflow_id=$2`, run, workflowID); err != nil {
		return uuid.Nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return run, s.DispatchReadyWorkflowNodes(ctx, run)
}

func (s *Store) DispatchReadyWorkflowNodes(ctx context.Context, run uuid.UUID) error {
	for {
		var nodeRun, definition, project uuid.UUID
		var input, snapshot json.RawMessage
		err := s.Pool.QueryRow(ctx, "SELECT nr.id,nr.job_definition_id,wr.project_id,wr.input,nr.execution_snapshot FROM workflow_node_runs nr JOIN workflow_runs wr ON wr.id=nr.workflow_run_id WHERE nr.workflow_run_id=$1 AND nr.status='PENDING' AND NOT EXISTS(SELECT 1 FROM workflow_run_edges e JOIN workflow_node_runs parent ON parent.id=e.from_node_run_id WHERE e.workflow_run_id=nr.workflow_run_id AND e.to_node_run_id=nr.id AND parent.status<>'SUCCEEDED') ORDER BY nr.created_at,nr.id LIMIT 1", run).Scan(&nodeRun, &definition, &project, &input, &snapshot)
		if err != nil {
			if err == pgx.ErrNoRows {
				return nil
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
		tag, err := s.Pool.Exec(ctx, "UPDATE workflow_node_runs SET status='RUNNING',job_run_id=$2 WHERE id=$1 AND status='PENDING'", nodeRun, submitted.ID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			continue
		}
	}
}

func decodeWorkflowDefinition(snapshot json.RawMessage) (Definition, error) {
	var raw struct {
		DefinitionID uuid.UUID          `json:"definition_id"`
		QueueID      uuid.UUID          `json:"queue_id"`
		FunctionKey  string             `json:"function_key"`
		Version      string             `json:"function_version"`
		Priority     job.Priority       `json:"priority"`
		Policy       job.PolicySnapshot `json:"policy"`
	}
	if err := json.Unmarshal(snapshot, &raw); err != nil {
		return Definition{}, err
	}
	if raw.DefinitionID == uuid.Nil || raw.QueueID == uuid.Nil || raw.FunctionKey == "" || raw.Version == "" || raw.Priority < job.Bulk || raw.Priority > job.Critical {
		return Definition{}, fmt.Errorf("workflow execution snapshot is invalid")
	}
	return Definition{ID: raw.DefinitionID, QueueID: raw.QueueID, FunctionKey: raw.FunctionKey, FunctionVersion: raw.Version, Priority: raw.Priority, Policy: raw.Policy}, nil
}

func (s *Store) AdvanceWorkflowForJob(ctx context.Context, runID uuid.UUID, succeeded bool) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var nodeRun, workflowRun uuid.UUID
	err = tx.QueryRow(ctx, "SELECT id,workflow_run_id FROM workflow_node_runs WHERE job_run_id=$1 AND status='RUNNING' FOR UPDATE", runID).Scan(&nodeRun, &workflowRun)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return err
	}
	if succeeded {
		if _, err = tx.Exec(ctx, "UPDATE workflow_node_runs SET status='SUCCEEDED',finished_at=now() WHERE id=$1", nodeRun); err != nil {
			return err
		}
		var remaining int
		if err = tx.QueryRow(ctx, "SELECT count(*) FROM workflow_node_runs WHERE workflow_run_id=$1 AND status<>'SUCCEEDED'", workflowRun).Scan(&remaining); err != nil {
			return err
		}
		if remaining == 0 {
			_, err = tx.Exec(ctx, "UPDATE workflow_runs SET status='SUCCEEDED',finished_at=now() WHERE id=$1", workflowRun)
		}
	} else {
		if _, err = tx.Exec(ctx, "UPDATE workflow_node_runs SET status='FAILED',finished_at=now() WHERE id=$1", nodeRun); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "UPDATE workflow_node_runs SET status='CANCELLED',finished_at=now() WHERE workflow_run_id=$1 AND status='PENDING'", workflowRun)
		if err == nil {
			_, err = tx.Exec(ctx, "UPDATE workflow_runs SET status='FAILED',finished_at=now() WHERE id=$1 AND status='RUNNING'", workflowRun)
		}
	}
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if succeeded {
		return s.DispatchReadyWorkflowNodes(ctx, workflowRun)
	}
	return nil
}
