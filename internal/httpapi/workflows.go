package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"errors"

	"github.com/example/task-processing/internal/application/controlplane"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func (a *API) createWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
		return
	}
	var req struct {
		ProjectID uuid.UUID `json:"project_id"`
		Name      string    `json:"name"`
		Nodes     []struct {
			Key             string    `json:"key"`
			JobDefinitionID uuid.UUID `json:"job_definition_id"`
		} `json:"nodes"`
		Edges []struct {
			From string `json:"from"`
			To   string `json:"to"`
		} `json:"edges"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !a.projectOK(r.Context(), principal(r), req.ProjectID) {
		problem(w, r, 404, "PROJECT_NOT_FOUND", "Project not found", false)
		return
	}
	nodes := make([]controlplane.WorkflowNodeInput, 0, len(req.Nodes))
	for _, node := range req.Nodes {
		nodes = append(nodes, controlplane.WorkflowNodeInput{Key: node.Key, JobDefinitionID: node.JobDefinitionID})
	}
	edges := make([]controlplane.WorkflowEdgeInput, 0, len(req.Edges))
	for _, edge := range req.Edges {
		edges = append(edges, controlplane.WorkflowEdgeInput{From: edge.From, To: edge.To})
	}
	out, err := (controlplane.WorkflowService{Store: a.Store}).Create(r.Context(), controlplane.WorkflowInput{ProjectID: req.ProjectID, Name: req.Name, Nodes: nodes, Edges: edges}, func(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
		return a.auditChangeTx(ctx, tx, principal(r), req.ProjectID, "workflow.create", "workflow_definition", id, nil, req)
	})
	if errors.Is(err, controlplane.ErrDependencyNotFound) {
		problem(w, r, 404, "JOB_DEFINITION_NOT_FOUND", "Workflow step has no active job definition", false)
		return
	}
	if err != nil {
		var databaseError *pgconn.PgError
		if errors.As(err, &databaseError) {
			handleErr(w, r, err)
			return
		}
		problem(w, r, 400, "INVALID_WORKFLOW_DAG", err.Error(), false)
		return
	}
	a.emit(r.Context(), req.ProjectID, "workflow.changed", "workflow_definition", out.ID, map[string]any{"id": out.ID, "version": out.Version})
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprint(out.Version)))
	writeJSON(w, 201, map[string]any{"id": out.ID, "version": out.Version})
}
func (a *API) listWorkflows(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), "SELECT id,name,status,row_version,created_at FROM workflow_definitions WHERE project_id=$1 AND deleted_at IS NULL ORDER BY created_at DESC", project)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id uuid.UUID
		var name, status string
		var version int64
		var created any
		if err = rows.Scan(&id, &name, &status, &version, &created); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": id, "name": name, "status": status, "version": version, "created_at": created})
	}
	writeJSON(w, 200, out)
}
func (a *API) startWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer", "operator") {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, ok := workflowID(w, r)
	if !ok {
		return
	}
	var req struct {
		Input json.RawMessage `json:"input"`
	}
	if !decode(w, r, &req) {
		return
	}
	run, err := (controlplane.WorkflowService{Store: a.Store}).Start(r.Context(), project, id, req.Input)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, 201, map[string]any{"id": run, "status": "RUNNING"})
}
func (a *API) deleteWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, ok := workflowID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	err := (controlplane.WorkflowService{Store: a.Store}).SoftDelete(r.Context(), project, id, version, principal(r).Actor, func(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
		return a.auditChangeTx(ctx, tx, principal(r), project, "workflow.soft_delete", "workflow_definition", id, nil, map[string]any{"deleted": true})
	})
	if errors.Is(err, controlplane.ErrWorkflowRunning) {
		problem(w, r, 409, "WORKFLOW_RUNNING", "Workflow has active runs", false)
		return
	}
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 412, "PRECONDITION_FAILED", "Workflow changed or cannot be deleted", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "version": version + 1, "deleted": true})
}
func (a *API) restoreWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, ok := workflowID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	err := (controlplane.WorkflowService{Store: a.Store}).Restore(r.Context(), project, id, version, func(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
		return a.auditChangeTx(ctx, tx, principal(r), project, "workflow.restore", "workflow_definition", id, nil, map[string]any{"restored": true})
	})
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 412, "PRECONDITION_FAILED", "Workflow changed or cannot be restored", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "version": version + 1, "restored": true})
}
func workflowID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid workflow ID", false)
		return uuid.Nil, false
	}
	return id, true
}
