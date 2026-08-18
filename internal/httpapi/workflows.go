package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/example/task-processing/internal/application/workflow"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
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
	nodes := make([]workflow.Node, 0, len(req.Nodes))
	for _, n := range req.Nodes {
		nodes = append(nodes, workflow.Node{ID: n.Key, Key: n.Key})
	}
	edges := make([]workflow.Edge, 0, len(req.Edges))
	for _, e := range req.Edges {
		edges = append(edges, workflow.Edge{From: e.From, To: e.To})
	}
	if _, err := workflow.Validate(nodes, edges); err != nil {
		problem(w, r, 400, "INVALID_WORKFLOW_DAG", err.Error(), false)
		return
	}
	tx, err := a.Store.Pool.Begin(r.Context())
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer tx.Rollback(r.Context())
	for _, n := range req.Nodes {
		var ok bool
		if err = tx.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM job_definitions WHERE id=$1 AND project_id=$2 AND status='ACTIVE' AND deleted_at IS NULL FOR UPDATE)", n.JobDefinitionID, req.ProjectID).Scan(&ok); err != nil || !ok {
			problem(w, r, 404, "JOB_DEFINITION_NOT_FOUND", "Workflow step has no active job definition", false)
			return
		}
	}
	var id uuid.UUID
	var version int64
	if err = tx.QueryRow(r.Context(), "INSERT INTO workflow_definitions(project_id,name) VALUES($1,$2) RETURNING id,row_version", req.ProjectID, req.Name).Scan(&id, &version); err != nil {
		handleErr(w, r, err)
		return
	}
	ids := map[string]uuid.UUID{}
	for _, n := range req.Nodes {
		var node uuid.UUID
		if err = tx.QueryRow(r.Context(), "INSERT INTO workflow_nodes(workflow_id,node_key,job_definition_id) VALUES($1,$2,$3) RETURNING id", id, n.Key, n.JobDefinitionID).Scan(&node); err != nil {
			handleErr(w, r, err)
			return
		}
		ids[n.Key] = node
	}
	for _, e := range req.Edges {
		if _, err = tx.Exec(r.Context(), "INSERT INTO workflow_edges(workflow_id,from_node_id,to_node_id) VALUES($1,$2,$3)", id, ids[e.From], ids[e.To]); err != nil {
			handleErr(w, r, err)
			return
		}
	}
	if err = a.auditChangeTx(r.Context(), tx, principal(r), req.ProjectID, "workflow.create", "workflow_definition", id, nil, req); err != nil {
		handleErr(w, r, err)
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		handleErr(w, r, err)
		return
	}
	a.emit(r.Context(), req.ProjectID, "workflow.changed", "workflow_definition", id, map[string]any{"id": id, "version": version})
	writeJSON(w, 201, map[string]any{"id": id, "version": version})
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
	run, err := a.Store.StartWorkflow(r.Context(), project, id, req.Input)
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
	tx, err := a.Store.Pool.Begin(r.Context())
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer tx.Rollback(r.Context())
	var running bool
	if err = tx.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM workflow_runs WHERE workflow_id=$1 AND status IN ('PENDING','RUNNING'))", id).Scan(&running); err != nil {
		handleErr(w, r, err)
		return
	}
	if running {
		problem(w, r, 409, "WORKFLOW_RUNNING", "Workflow has active runs", false)
		return
	}
	var name string
	err = tx.QueryRow(r.Context(), "UPDATE workflow_definitions SET deleted_at=now(),deleted_by=$3 WHERE id=$1 AND project_id=$2 AND row_version=$4 AND deleted_at IS NULL RETURNING name", id, project, principal(r).Actor, version).Scan(&name)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	if err = a.auditChangeTx(r.Context(), tx, principal(r), project, "workflow.soft_delete", "workflow_definition", id, map[string]any{"name": name}, map[string]any{"deleted": true}); err != nil {
		handleErr(w, r, err)
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
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
	tag, err := a.Store.Pool.Exec(r.Context(), "UPDATE workflow_definitions SET deleted_at=NULL,deleted_by=NULL WHERE id=$1 AND project_id=$2 AND row_version=$3 AND deleted_at IS NOT NULL", id, project, version)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	if tag.RowsAffected() != 1 {
		problem(w, r, 412, "PRECONDITION_FAILED", "Workflow changed or cannot be restored", false)
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
