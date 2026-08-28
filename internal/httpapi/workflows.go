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
		ProjectID     uuid.UUID `json:"project_id"`
		Name          string    `json:"name"`
		FailurePolicy string    `json:"failure_policy"`
		Nodes         []struct {
			Key             string    `json:"key"`
			JobDefinitionID uuid.UUID `json:"job_definition_id"`
		} `json:"nodes"`
		Edges []struct {
			From          string `json:"from"`
			To            string `json:"to"`
			ConditionType string `json:"condition_type"`
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
		edges = append(edges, controlplane.WorkflowEdgeInput{From: edge.From, To: edge.To, ConditionType: edge.ConditionType})
	}
	out, err := (controlplane.WorkflowService{Store: a.Store}).Create(r.Context(), controlplane.WorkflowInput{ProjectID: req.ProjectID, Name: req.Name, FailurePolicy: req.FailurePolicy, Nodes: nodes, Edges: edges}, func(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
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
func (a *API) listWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, ok := workflowID(w, r)
	if !ok {
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), "SELECT id,status,row_version,retry_of_run_id,failure_policy_snapshot,created_at,finished_at FROM workflow_runs WHERE project_id=$1 AND workflow_id=$2 ORDER BY created_at DESC LIMIT 200", project, id)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var run uuid.UUID
		var status string
		var version int64
		var retryOf *uuid.UUID
		var failurePolicy string
		var created any
		var finished any
		if err = rows.Scan(&run, &status, &version, &retryOf, &failurePolicy, &created, &finished); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": run, "status": status, "version": version, "retry_of_run_id": retryOf, "failure_policy": failurePolicy, "created_at": created, "finished_at": finished})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}
func (a *API) getWorkflowRun(w http.ResponseWriter, r *http.Request) {
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid workflow run ID", false)
		return
	}
	var workflow uuid.UUID
	var status string
	var version int64
	var retryOf *uuid.UUID
	var failurePolicy string
	var created, finished any
	err = a.Store.Pool.QueryRow(r.Context(), "SELECT workflow_id,status,row_version,retry_of_run_id,failure_policy_snapshot,created_at,finished_at FROM workflow_runs WHERE id=$1 AND project_id=$2", id, project).Scan(&workflow, &status, &version, &retryOf, &failurePolicy, &created, &finished)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), "SELECT id,node_key,status,job_run_id,created_at,finished_at FROM workflow_node_runs WHERE workflow_run_id=$1 ORDER BY created_at,id", id)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	nodes := []map[string]any{}
	for rows.Next() {
		var node uuid.UUID
		var key, state string
		var jobRun *uuid.UUID
		var nodeCreated, nodeFinished any
		if err = rows.Scan(&node, &key, &state, &jobRun, &nodeCreated, &nodeFinished); err != nil {
			handleErr(w, r, err)
			return
		}
		nodes = append(nodes, map[string]any{"id": node, "key": key, "status": state, "job_run_id": jobRun, "created_at": nodeCreated, "finished_at": nodeFinished})
	}
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprint(version)))
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "workflow_id": workflow, "status": status, "version": version, "retry_of_run_id": retryOf, "failure_policy": failurePolicy, "created_at": created, "finished_at": finished, "nodes": nodes})
}
func (a *API) cancelWorkflowRun(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer", "operator") {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid workflow run ID", false)
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	out, err := (controlplane.WorkflowService{Store: a.Store}).CancelRun(r.Context(), project, id, version, func(ctx context.Context, tx pgx.Tx, runID uuid.UUID, before, after json.RawMessage) error {
		return a.auditChangeTx(ctx, tx, principal(r), project, "workflow_run.cancel", "workflow_run", runID, before, after)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 404, "NOT_FOUND", "Workflow run not found", false)
		return
	}
	if errors.Is(err, controlplane.ErrOptimisticLock) {
		problem(w, r, 412, "PRECONDITION_FAILED", "Workflow run changed; reload and retry", true)
		return
	}
	if errors.Is(err, controlplane.ErrWorkflowRunActive) {
		problem(w, r, 409, "WORKFLOW_RUN_ACTIVE", "A workflow handler is still running; cancellation is safe only between nodes", true)
		return
	}
	if errors.Is(err, controlplane.ErrWorkflowRunTerminal) {
		problem(w, r, 409, "WORKFLOW_RUN_TERMINAL", "Workflow run is already terminal", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	a.emit(r.Context(), project, "workflow.run.cancelled", "workflow_run", id, map[string]any{"id": id, "version": out.Version})
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprint(out.Version)))
	writeJSON(w, http.StatusOK, map[string]any{"id": out.ID, "status": out.Status, "version": out.Version})
}
func (a *API) retryWorkflowRun(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer", "operator") {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid workflow run ID", false)
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	out, err := (controlplane.WorkflowService{Store: a.Store}).RetryRun(r.Context(), project, id, version, func(ctx context.Context, tx pgx.Tx, child uuid.UUID, before, after json.RawMessage) error {
		return a.auditChangeTx(ctx, tx, principal(r), project, "workflow_run.retry", "workflow_run", child, before, after)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 404, "NOT_FOUND", "Workflow run not found", false)
		return
	}
	if errors.Is(err, controlplane.ErrOptimisticLock) {
		problem(w, r, 412, "PRECONDITION_FAILED", "Workflow run changed; reload and retry", true)
		return
	}
	if errors.Is(err, controlplane.ErrWorkflowRunActive) {
		problem(w, r, 409, "WORKFLOW_RUN_ACTIVE", "A workflow handler is still running; wait before retrying", true)
		return
	}
	if errors.Is(err, controlplane.ErrWorkflowRunNotRetryable) {
		problem(w, r, 409, "WORKFLOW_RUN_NOT_RETRYABLE", "Only failed, cancelled, or manually-paused workflow runs can be retried", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	if out.Created {
		a.emit(r.Context(), project, "workflow.run.retried", "workflow_run", out.ID, map[string]any{"id": out.ID, "retry_of_run_id": id})
	}
	status := http.StatusOK
	if out.Created {
		status = http.StatusCreated
	}
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprint(out.Version)))
	writeJSON(w, status, map[string]any{"id": out.ID, "status": out.Status, "version": out.Version, "retry_of_run_id": id, "created": out.Created})
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
