package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/example/task-processing/internal/application/controlplane"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type controlSpec struct {
	resource string
	table    string
	role     []string
	scope    string
	event    string
}

var controlSpecs = map[string]controlSpec{
	"queues":               {resource: "queue", table: "queues", role: []string{"admin"}, scope: "t.project_id=$1", event: "queue.changed"},
	"retry-policies":       {resource: "retry_policy", table: "retry_policies", role: []string{"admin", "developer"}, scope: "t.project_id=$1", event: "retry_policy.changed"},
	"rate-limit-policies":  {resource: "rate_limit_policy", table: "rate_limit_policies", role: []string{"admin", "developer"}, scope: "t.project_id=$1", event: "rate_limit_policy.changed"},
	"function-definitions": {resource: "function_definition", table: "function_definitions", role: []string{"admin", "developer"}, scope: "t.project_id=$1", event: "function_definition.changed"},
	"job-definitions":      {resource: "job_definition", table: "job_definitions", role: []string{"admin", "developer"}, scope: "t.project_id=$1", event: "job_definition.changed"},
	"schedules":            {resource: "schedule", table: "schedules", role: []string{"admin", "developer"}, scope: "EXISTS(SELECT 1 FROM job_definitions jd WHERE jd.id=t.job_definition_id AND jd.project_id=$1)", event: "schedule.changed"},
	"retention-policies":   {resource: "retention_policy", table: "retention_policies", role: []string{"admin", "developer"}, scope: "t.project_id=$1", event: "retention_policy.changed"},
}

func controlSpecFor(r *http.Request) (controlSpec, bool) {
	spec, ok := controlSpecs[chi.URLParam(r, "aggregate")]
	return spec, ok
}

func controlRow() string {
	return "((to_jsonb(t) - 'row_version') || jsonb_build_object('version', t.row_version))"
}

func (a *API) controlList(w http.ResponseWriter, r *http.Request) {
	spec, ok := controlSpecFor(r)
	if !ok {
		problem(w, r, 404, "NOT_FOUND", "Control-plane aggregate not found", false)
		return
	}
	if !requireRole(w, r, spec.role...) {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	items, err := (controlplane.ResourceService{Store: a.Store}).List(r.Context(), spec.resource, project, r.URL.Query().Get("include_deleted") == "true")
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *API) controlGet(w http.ResponseWriter, r *http.Request) {
	spec, ok := controlSpecFor(r)
	if !ok {
		problem(w, r, 404, "NOT_FOUND", "Control-plane aggregate not found", false)
		return
	}
	if !requireRole(w, r, spec.role...) {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, ok := controlID(w, r)
	if !ok {
		return
	}
	item, err := (controlplane.ResourceService{Store: a.Store}).Snapshot(r.Context(), spec.resource, project, id, r.URL.Query().Get("include_deleted") == "true")
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 404, "NOT_FOUND", "Resource not found", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	var versioned map[string]any
	_ = json.Unmarshal(item, &versioned)
	if version, ok := versioned["version"].(float64); ok {
		w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprintf("%.0f", version)))
	}
	writeJSON(w, http.StatusOK, item)
}

func (a *API) controlUpdate(w http.ResponseWriter, r *http.Request) {
	spec, ok := controlSpecFor(r)
	if !ok {
		problem(w, r, 404, "NOT_FOUND", "Control-plane aggregate not found", false)
		return
	}
	if !requireRole(w, r, spec.role...) {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, ok := controlID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	patch, ok := controlPatch(w, r)
	if !ok {
		return
	}
	audit := func(ctx context.Context, tx pgx.Tx, resourceID uuid.UUID, before, after json.RawMessage) error {
		return a.auditChangeTx(ctx, tx, principal(r), project, spec.resource+".update", spec.resource, resourceID, before, after)
	}
	after, err := (controlplane.ResourceService{Store: a.Store}).Update(r.Context(), spec.resource, project, id, version, patch, audit)
	if errors.Is(err, controlplane.ErrResourceNotFound) {
		problem(w, r, 404, "NOT_FOUND", "Resource not found", false)
		return
	}
	if errors.Is(err, controlplane.ErrOptimisticLock) {
		a.controlPrecondition(w, r, spec, project, id)
		return
	}
	if err != nil {
		problem(w, r, 400, "INVALID_CONTROL_PLANE_PATCH", err.Error(), false)
		return
	}
	a.emit(r.Context(), project, spec.event, spec.resource, id, map[string]any{"id": id, "version": version + 1})
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprint(version+1)))
	writeJSON(w, http.StatusOK, after)
}

func (a *API) controlDelete(w http.ResponseWriter, r *http.Request) {
	spec, ok := controlSpecFor(r)
	if !ok {
		problem(w, r, 404, "NOT_FOUND", "Control-plane aggregate not found", false)
		return
	}
	if !requireRole(w, r, spec.role...) {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, ok := controlID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	audit := func(ctx context.Context, tx pgx.Tx, resourceID uuid.UUID, before, after json.RawMessage) error {
		return a.auditChangeTx(ctx, tx, principal(r), project, spec.resource+".soft_delete", spec.resource, resourceID, before, after)
	}
	after, err := (controlplane.ResourceService{Store: a.Store}).SoftDelete(r.Context(), spec.resource, project, id, version, principal(r).Actor, audit)
	if errors.Is(err, controlplane.ErrResourceNotFound) {
		problem(w, r, 404, "NOT_FOUND", "Resource not found", false)
		return
	}
	if errors.Is(err, controlplane.ErrDependencyBlocked) {
		problem(w, r, 409, "DEPENDENCY_EXISTS", "Resource has active control-plane dependents and cannot be deleted", false)
		return
	}
	if errors.Is(err, controlplane.ErrOptimisticLock) {
		a.controlPrecondition(w, r, spec, project, id)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	a.emit(r.Context(), project, spec.event, spec.resource, id, map[string]any{"id": id, "deleted": true, "version": version + 1})
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprint(version+1)))
	writeJSON(w, http.StatusOK, after)
}

func (a *API) controlRestore(w http.ResponseWriter, r *http.Request) {
	spec, ok := controlSpecFor(r)
	if !ok {
		problem(w, r, 404, "NOT_FOUND", "Control-plane aggregate not found", false)
		return
	}
	if !requireRole(w, r, spec.role...) {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	id, ok := controlID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	audit := func(ctx context.Context, tx pgx.Tx, resourceID uuid.UUID, before, after json.RawMessage) error {
		return a.auditChangeTx(ctx, tx, principal(r), project, spec.resource+".restore", spec.resource, resourceID, before, after)
	}
	after, err := (controlplane.ResourceService{Store: a.Store}).Restore(r.Context(), spec.resource, project, id, version, audit)
	if errors.Is(err, controlplane.ErrResourceNotFound) {
		problem(w, r, 404, "NOT_FOUND", "Resource not found", false)
		return
	}
	if errors.Is(err, controlplane.ErrRestoreConflict) {
		problem(w, r, 409, "RESTORE_CONFLICT", "An active resource already uses this identity", false)
		return
	}
	if errors.Is(err, controlplane.ErrOptimisticLock) {
		a.controlPrecondition(w, r, spec, project, id)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	a.emit(r.Context(), project, spec.event, spec.resource, id, map[string]any{"id": id, "restored": true, "version": version + 1})
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprint(version+1)))
	writeJSON(w, http.StatusOK, after)
}

func (a *API) auditChangeTx(ctx context.Context, tx pgx.Tx, p Principal, project uuid.UUID, action, typ string, id uuid.UUID, before, after any) error {
	beforeJSON, err := json.Marshal(before)
	if err != nil {
		return err
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		return err
	}
	meta := requestMeta(ctx)
	var auditID uuid.UUID
	err = tx.QueryRow(ctx, "INSERT INTO audit_logs(project_id,tenant_id,actor_id,action,resource_type,resource_id,before_data,after_data,request_id,trace_id,metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id", project, p.TenantID, p.Actor, action, typ, id, beforeJSON, afterJSON, meta.RequestID, meta.TraceID, json.RawMessage("{\"source\":\"api\"}")).Scan(&auditID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO audit_outbox_events(audit_log_id,project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,$2,$3,$4,$5,$6)", auditID, project, "audit.control_plane", typ, id, afterJSON)
	return err
}

func controlID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid resource ID", false)
		return uuid.Nil, false
	}
	return id, true
}

func controlPatch(w http.ResponseWriter, r *http.Request) (controlplane.Patch, bool) {
	var patch controlplane.Patch
	if !decode(w, r, &patch) {
		return nil, false
	}
	return patch, true
}

func (a *API) controlPrecondition(w http.ResponseWriter, r *http.Request, spec controlSpec, project, id uuid.UUID) {
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM " + spec.table + " t WHERE " + spec.scope + " AND t.id=$2)"
	err := a.Store.Pool.QueryRow(r.Context(), query, project, id).Scan(&exists)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	if !exists {
		problem(w, r, 404, "NOT_FOUND", "Resource not found", false)
		return
	}
	problem(w, r, http.StatusPreconditionFailed, "PRECONDITION_FAILED", "Resource changed, was deleted, or no longer satisfies its dependencies", false)
}
