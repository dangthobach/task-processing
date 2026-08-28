package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	workflowgraph "github.com/example/task-processing/internal/application/workflow"
	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrWorkflowRunning = errors.New("workflow has active runs")
var ErrWorkflowRunActive = errors.New("workflow run has an executing node")
var ErrWorkflowRunTerminal = errors.New("workflow run is already terminal")
var ErrWorkflowRunNotRetryable = errors.New("workflow run is not retryable")

type WorkflowNodeInput struct {
	Key             string
	JobDefinitionID uuid.UUID
}
type WorkflowEdgeInput struct {
	From, To      string
	ConditionType string
}
type WorkflowInput struct {
	ProjectID     uuid.UUID
	Name          string
	FailurePolicy string
	Nodes         []WorkflowNodeInput
	Edges         []WorkflowEdgeInput
}
type WorkflowService struct{ Store *postgres.Store }

type WorkflowRunResource struct {
	ID      uuid.UUID
	Version int64
	Status  string
}

type WorkflowRetryResource struct {
	ID      uuid.UUID
	Status  string
	Version int64
	Created bool
}

// RetryRun creates one child generation from the source's immutable graph and
// execution snapshot. It never rewrites failed node history.
func (s WorkflowService) RetryRun(ctx context.Context, project, source uuid.UUID, version int64, audit ResourceMutationAudit) (WorkflowRetryResource, error) {
	if s.Store == nil {
		return WorkflowRetryResource{}, fmt.Errorf("workflow store is required")
	}
	if project == uuid.Nil || source == uuid.Nil || version <= 0 {
		return WorkflowRetryResource{}, fmt.Errorf("project_id, workflow_run_id, and version are required")
	}
	result, err := s.Store.RetryWorkflow(ctx, project, source, version, func(ctx context.Context, tx pgx.Tx, child uuid.UUID, after json.RawMessage) error {
		if audit == nil {
			return nil
		}
		return audit(ctx, tx, child, nil, after)
	})
	if errors.Is(err, postgres.ErrWorkflowRetryOptimisticLock) {
		return WorkflowRetryResource{}, ErrOptimisticLock
	}
	if errors.Is(err, postgres.ErrWorkflowRunExecuting) {
		return WorkflowRetryResource{}, ErrWorkflowRunActive
	}
	if errors.Is(err, postgres.ErrWorkflowRunNotRetryable) {
		return WorkflowRetryResource{}, ErrWorkflowRunNotRetryable
	}
	if err != nil {
		return WorkflowRetryResource{}, err
	}
	return WorkflowRetryResource{ID: result.RunID, Status: result.Status, Version: result.Version, Created: result.Created}, nil
}

// CancelRun stops a workflow only at a safe execution boundary. A handler
// which has already started is never abandoned: callers receive
// ErrWorkflowRunActive and can retry after it reaches a terminal state.
// Pending work and its dispatch signal are cancelled in the same transaction.
func (s WorkflowService) CancelRun(ctx context.Context, project, id uuid.UUID, version int64, audit ResourceMutationAudit) (WorkflowRunResource, error) {
	if s.Store == nil {
		return WorkflowRunResource{}, fmt.Errorf("workflow store is required")
	}
	if project == uuid.Nil || id == uuid.Nil || version <= 0 {
		return WorkflowRunResource{}, fmt.Errorf("project_id, workflow_run_id, and version are required")
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return WorkflowRunResource{}, err
	}
	defer tx.Rollback(ctx)

	var currentVersion int64
	var status string
	var before json.RawMessage
	err = tx.QueryRow(ctx, `SELECT row_version,status,
		((to_jsonb(workflow_runs) - 'row_version') || jsonb_build_object('version', row_version))
		FROM workflow_runs WHERE id=$1 AND project_id=$2 FOR UPDATE`, id, project).Scan(&currentVersion, &status, &before)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkflowRunResource{}, pgx.ErrNoRows
	}
	if err != nil {
		return WorkflowRunResource{}, err
	}
	if currentVersion != version {
		return WorkflowRunResource{}, ErrOptimisticLock
	}
	if status != "PENDING" && status != "RUNNING" {
		return WorkflowRunResource{}, ErrWorkflowRunTerminal
	}
	var executing bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workflow_node_runs WHERE workflow_run_id=$1 AND status='RUNNING')", id).Scan(&executing); err != nil {
		return WorkflowRunResource{}, err
	}
	if executing {
		return WorkflowRunResource{}, ErrWorkflowRunActive
	}
	// Invalidate existing dispatch generations before the workflow signal is
	// removed. A broker message published just before this command can then no
	// longer reserve one of these jobs.
	if _, err = tx.Exec(ctx, `UPDATE job_runs jr
		SET status='CANCELLED', current_dispatch_id=gen_random_uuid(), finished_at=now(), updated_at=now()
		FROM workflow_node_runs nr
		WHERE nr.workflow_run_id=$1 AND nr.job_run_id=jr.id
		  AND nr.status IN ('PENDING','BLOCKED')
		  AND jr.status IN ('CREATED','ENQUEUE_PENDING','QUEUED','RETRY_WAIT')`, id); err != nil {
		return WorkflowRunResource{}, err
	}
	if _, err = tx.Exec(ctx, "UPDATE workflow_node_runs SET status='CANCELLED',finished_at=now() WHERE workflow_run_id=$1 AND status IN ('PENDING','BLOCKED')", id); err != nil {
		return WorkflowRunResource{}, err
	}
	if _, err = tx.Exec(ctx, "DELETE FROM workflow_dispatch_outbox WHERE workflow_run_id=$1", id); err != nil {
		return WorkflowRunResource{}, err
	}
	var after json.RawMessage
	out := WorkflowRunResource{ID: id, Status: "CANCELLED"}
	err = tx.QueryRow(ctx, `UPDATE workflow_runs SET status='CANCELLED',finished_at=now()
		WHERE id=$1 AND project_id=$2 AND row_version=$3 AND status IN ('PENDING','RUNNING')
		RETURNING row_version, ((to_jsonb(workflow_runs) - 'row_version') || jsonb_build_object('version', row_version))`, id, project, version).Scan(&out.Version, &after)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkflowRunResource{}, ErrOptimisticLock
	}
	if err != nil {
		return WorkflowRunResource{}, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, before, after); err != nil {
			return WorkflowRunResource{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return WorkflowRunResource{}, err
	}
	return out, nil
}

func (s WorkflowService) Create(ctx context.Context, in WorkflowInput, audit AuditInTransaction) (VersionedResource, error) {
	if s.Store == nil {
		return VersionedResource{}, fmt.Errorf("workflow store is required")
	}
	in.Name = strings.TrimSpace(in.Name)
	in.FailurePolicy = strings.ToUpper(strings.TrimSpace(in.FailurePolicy))
	if in.FailurePolicy == "" {
		in.FailurePolicy = "FAIL_FAST"
	}
	if in.ProjectID == uuid.Nil || in.Name == "" || len(in.Name) > 255 {
		return VersionedResource{}, fmt.Errorf("project_id and workflow name are required")
	}
	if in.FailurePolicy != "FAIL_FAST" && in.FailurePolicy != "CONTINUE" && in.FailurePolicy != "MANUAL_INTERVENTION" {
		return VersionedResource{}, fmt.Errorf("workflow failure_policy is invalid")
	}
	nodes := make([]workflowgraph.Node, 0, len(in.Nodes))
	for _, node := range in.Nodes {
		nodes = append(nodes, workflowgraph.Node{ID: node.Key, Key: node.Key})
	}
	edges := make([]workflowgraph.Edge, 0, len(in.Edges))
	for i := range in.Edges {
		edge := &in.Edges[i]
		edge.ConditionType = strings.ToUpper(strings.TrimSpace(edge.ConditionType))
		if edge.ConditionType == "" {
			edge.ConditionType = "ON_SUCCESS"
		}
		if edge.ConditionType != "ON_SUCCESS" && edge.ConditionType != "ON_FAILURE" && edge.ConditionType != "ALWAYS" {
			return VersionedResource{}, fmt.Errorf("workflow edge condition_type is invalid")
		}
		edges = append(edges, workflowgraph.Edge{From: edge.From, To: edge.To})
	}
	if _, err := workflowgraph.Validate(nodes, edges); err != nil {
		return VersionedResource{}, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return VersionedResource{}, err
	}
	defer tx.Rollback(ctx)
	for _, node := range in.Nodes {
		if node.JobDefinitionID == uuid.Nil {
			return VersionedResource{}, ErrDependencyNotFound
		}
		var active bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM job_definitions WHERE id=$1 AND project_id=$2 AND status='ACTIVE' AND deleted_at IS NULL FOR UPDATE)", node.JobDefinitionID, in.ProjectID).Scan(&active); err != nil {
			return VersionedResource{}, err
		}
		if !active {
			return VersionedResource{}, ErrDependencyNotFound
		}
	}
	out := VersionedResource{Status: "ACTIVE"}
	if err = tx.QueryRow(ctx, "INSERT INTO workflow_definitions(project_id,name,failure_policy) VALUES($1,$2,$3) RETURNING id,row_version", in.ProjectID, in.Name, in.FailurePolicy).Scan(&out.ID, &out.Version); err != nil {
		return VersionedResource{}, err
	}
	ids := map[string]uuid.UUID{}
	for _, node := range in.Nodes {
		var id uuid.UUID
		if err = tx.QueryRow(ctx, "INSERT INTO workflow_nodes(workflow_id,node_key,job_definition_id) VALUES($1,$2,$3) RETURNING id", out.ID, node.Key, node.JobDefinitionID).Scan(&id); err != nil {
			return VersionedResource{}, err
		}
		ids[node.Key] = id
	}
	for _, edge := range in.Edges {
		if _, err = tx.Exec(ctx, "INSERT INTO workflow_edges(workflow_id,from_node_id,to_node_id,condition_type) VALUES($1,$2,$3,$4)", out.ID, ids[edge.From], ids[edge.To], edge.ConditionType); err != nil {
			return VersionedResource{}, err
		}
	}
	if audit != nil {
		if err = audit(ctx, tx, out.ID); err != nil {
			return VersionedResource{}, err
		}
	}
	return out, tx.Commit(ctx)
}
func (s WorkflowService) Start(ctx context.Context, project, id uuid.UUID, input json.RawMessage) (uuid.UUID, error) {
	if s.Store == nil {
		return uuid.Nil, fmt.Errorf("workflow store is required")
	}
	if project == uuid.Nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("project_id and workflow_id are required")
	}
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	if !json.Valid(input) {
		return uuid.Nil, fmt.Errorf("workflow input must be JSON")
	}
	return s.Store.StartWorkflow(ctx, project, id, input)
}
func (s WorkflowService) SoftDelete(ctx context.Context, project, id uuid.UUID, version int64, actor string, audit AuditInTransaction) error {
	if s.Store == nil {
		return fmt.Errorf("workflow store is required")
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var running bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM workflow_runs WHERE workflow_id=$1 AND status IN ('PENDING','RUNNING'))", id).Scan(&running); err != nil {
		return err
	}
	if running {
		return ErrWorkflowRunning
	}
	tag, err := tx.Exec(ctx, "UPDATE workflow_definitions SET deleted_at=now(),deleted_by=$3 WHERE id=$1 AND project_id=$2 AND row_version=$4 AND deleted_at IS NULL", id, project, actor, version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return pgx.ErrNoRows
	}
	if audit != nil {
		if err = audit(ctx, tx, id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (s WorkflowService) Restore(ctx context.Context, project, id uuid.UUID, version int64, audit AuditInTransaction) error {
	if s.Store == nil {
		return fmt.Errorf("workflow store is required")
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, "UPDATE workflow_definitions SET deleted_at=NULL,deleted_by=NULL WHERE id=$1 AND project_id=$2 AND row_version=$3 AND deleted_at IS NOT NULL", id, project, version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return pgx.ErrNoRows
	}
	if audit != nil {
		if err = audit(ctx, tx, id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
