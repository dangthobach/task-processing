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

type WorkflowNodeInput struct {
	Key             string
	JobDefinitionID uuid.UUID
}
type WorkflowEdgeInput struct{ From, To string }
type WorkflowInput struct {
	ProjectID uuid.UUID
	Name      string
	Nodes     []WorkflowNodeInput
	Edges     []WorkflowEdgeInput
}
type WorkflowService struct{ Store *postgres.Store }

func (s WorkflowService) Create(ctx context.Context, in WorkflowInput, audit AuditInTransaction) (VersionedResource, error) {
	if s.Store == nil {
		return VersionedResource{}, fmt.Errorf("workflow store is required")
	}
	in.Name = strings.TrimSpace(in.Name)
	if in.ProjectID == uuid.Nil || in.Name == "" || len(in.Name) > 255 {
		return VersionedResource{}, fmt.Errorf("project_id and workflow name are required")
	}
	nodes := make([]workflowgraph.Node, 0, len(in.Nodes))
	for _, node := range in.Nodes {
		nodes = append(nodes, workflowgraph.Node{ID: node.Key, Key: node.Key})
	}
	edges := make([]workflowgraph.Edge, 0, len(in.Edges))
	for _, edge := range in.Edges {
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
	if err = tx.QueryRow(ctx, "INSERT INTO workflow_definitions(project_id,name) VALUES($1,$2) RETURNING id,row_version", in.ProjectID, in.Name).Scan(&out.ID, &out.Version); err != nil {
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
		if _, err = tx.Exec(ctx, "INSERT INTO workflow_edges(workflow_id,from_node_id,to_node_id) VALUES($1,$2,$3)", out.ID, ids[edge.From], ids[edge.To]); err != nil {
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
