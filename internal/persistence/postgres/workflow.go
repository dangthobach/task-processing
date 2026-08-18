package postgres

import (
	"context"
	"encoding/json"
	"fmt"

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
	if _, err = tx.Exec(ctx, "INSERT INTO workflow_node_runs(workflow_run_id,node_id,status) SELECT $1,id,'PENDING' FROM workflow_nodes WHERE workflow_id=$2", run, workflowID); err != nil {
		return uuid.Nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return run, s.DispatchReadyWorkflowNodes(ctx, run)
}

func (s *Store) DispatchReadyWorkflowNodes(ctx context.Context, run uuid.UUID) error {
	for {
		var nodeRun, node, definition, project uuid.UUID
		var input json.RawMessage
		err := s.Pool.QueryRow(ctx, "SELECT nr.id,nr.node_id,n.job_definition_id,wr.project_id,wr.input FROM workflow_node_runs nr JOIN workflow_runs wr ON wr.id=nr.workflow_run_id JOIN workflow_nodes n ON n.id=nr.node_id WHERE nr.workflow_run_id=$1 AND nr.status='PENDING' AND NOT EXISTS(SELECT 1 FROM workflow_edges e JOIN workflow_node_runs parent ON parent.workflow_run_id=nr.workflow_run_id AND parent.node_id=e.from_node_id WHERE e.workflow_id=wr.workflow_id AND e.to_node_id=nr.node_id AND parent.status<>'SUCCEEDED') ORDER BY nr.created_at,nr.id LIMIT 1", run).Scan(&nodeRun, &node, &definition, &project, &input)
		if err != nil {
			if err == pgx.ErrNoRows {
				return nil
			}
			return err
		}
		key := "workflow:" + run.String() + ":" + node.String()
		submitted, err := s.Submit(ctx, Submit{ProjectID: project, DefinitionID: definition, Payload: input, IdempotencyKey: key})
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
