package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/example/task-processing/internal/application/controlplane"
	"github.com/example/task-processing/internal/persistence/postgres"
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
	includeDeleted := r.URL.Query().Get("include_deleted") == "true"
	query := "SELECT " + controlRow() + " FROM " + spec.table + " t WHERE " + spec.scope + " AND ($2 OR t.deleted_at IS NULL) ORDER BY t.created_at DESC, t.id DESC LIMIT 200"
	rows, err := a.Store.Pool.Query(r.Context(), query, project, includeDeleted)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	items := make([]json.RawMessage, 0)
	for rows.Next() {
		var item json.RawMessage
		if err := rows.Scan(&item); err != nil {
			handleErr(w, r, err)
			return
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
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
	item, err := a.controlSnapshot(r.Context(), spec, project, id, r.URL.Query().Get("include_deleted") == "true")
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
	if err := controlplane.ValidatePatch(spec.resource, patch); err != nil {
		problem(w, r, 400, "INVALID_CONTROL_PLANE_PATCH", err.Error(), false)
		return
	}
	if err := a.validateSchedulePatch(r, spec, project, id, patch); err != nil {
		problem(w, r, 400, "INVALID_SCHEDULE", err.Error(), false)
		return
	}
	if err := a.validateRetryPolicyPatch(r, spec, project, id, patch); err != nil {
		problem(w, r, 400, "INVALID_RETRY_POLICY", err.Error(), false)
		return
	}
	if err := a.validateFunctionSchemaPatch(spec, patch); err != nil {
		problem(w, r, 400, "INVALID_INPUT_SCHEMA", err.Error(), false)
		return
	}
	if err := a.validateJobDefinitionPatch(r, spec, project, id, patch); err != nil {
		problem(w, r, 409, "BATCH_BACKEND_UNSUPPORTED", err.Error(), false)
		return
	}
	if err := a.validatePatchDependencies(r, spec, project, id, patch); err != nil {
		problem(w, r, 409, "DEPENDENCY_NOT_FOUND", err.Error(), false)
		return
	}
	tx, err := a.Store.Pool.Begin(r.Context())
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer tx.Rollback(r.Context())
	before, err := controlSnapshotWith(r.Context(), tx, spec, project, id, false)
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 404, "NOT_FOUND", "Resource not found", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	payload, _ := json.Marshal(patch)
	fields := make([]string, 0, len(patch))
	for field := range patch {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	sets := make([]string, 0, len(fields))
	for _, field := range fields {
		sets = append(sets, field+"=p."+field)
	}
	query := "UPDATE " + spec.table + " t SET " + strings.Join(sets, ",") + " FROM jsonb_populate_record(NULL::" + spec.table + ",$3::jsonb) AS p WHERE " + spec.scope + " AND t.id=$2 AND t.row_version=$4 AND t.deleted_at IS NULL" + a.patchDependencySQL(spec) + " RETURNING " + controlRow()
	var after json.RawMessage
	err = tx.QueryRow(r.Context(), query, project, id, payload, version).Scan(&after)
	if errors.Is(err, pgx.ErrNoRows) {
		a.controlPrecondition(w, r, spec, project, id)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	if err := a.auditChangeTx(r.Context(), tx, principal(r), project, spec.resource+".update", spec.resource, id, before, after); err != nil {
		handleErr(w, r, err)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		handleErr(w, r, err)
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
	if blocked, err := a.deleteBlocked(r, spec, project, id); err != nil {
		handleErr(w, r, err)
		return
	} else if blocked {
		problem(w, r, 409, "DEPENDENCY_EXISTS", "Resource has active control-plane dependents and cannot be deleted", false)
		return
	}
	tx, err := a.Store.Pool.Begin(r.Context())
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer tx.Rollback(r.Context())
	before, err := controlSnapshotWith(r.Context(), tx, spec, project, id, false)
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 404, "NOT_FOUND", "Resource not found", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	query := "UPDATE " + spec.table + " t SET deleted_at=now(),deleted_by=$3 WHERE " + spec.scope + " AND t.id=$2 AND t.row_version=$4 AND t.deleted_at IS NULL" + a.deleteDependencySQL(spec) + " RETURNING " + controlRow()
	var after json.RawMessage
	err = tx.QueryRow(r.Context(), query, project, id, principal(r).Actor, version).Scan(&after)
	if errors.Is(err, pgx.ErrNoRows) {
		a.controlPrecondition(w, r, spec, project, id)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	if err := a.auditChangeTx(r.Context(), tx, principal(r), project, spec.resource+".soft_delete", spec.resource, id, before, after); err != nil {
		handleErr(w, r, err)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
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
	tx, err := a.Store.Pool.Begin(r.Context())
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer tx.Rollback(r.Context())
	before, err := controlSnapshotWith(r.Context(), tx, spec, project, id, true)
	if errors.Is(err, pgx.ErrNoRows) {
		problem(w, r, 404, "NOT_FOUND", "Resource not found", false)
		return
	}
	if err != nil {
		handleErr(w, r, err)
		return
	}
	query := "UPDATE " + spec.table + " t SET deleted_at=NULL,deleted_by=NULL WHERE " + spec.scope + " AND t.id=$2 AND t.row_version=$3 AND t.deleted_at IS NOT NULL" + a.restoreDependencySQL(spec) + " RETURNING " + controlRow()
	var after json.RawMessage
	err = tx.QueryRow(r.Context(), query, project, id, version).Scan(&after)
	if errors.Is(err, pgx.ErrNoRows) {
		a.controlPrecondition(w, r, spec, project, id)
		return
	}
	if err != nil {
		if strings.Contains(err.Error(), "ux_") || strings.Contains(err.Error(), "duplicate key") {
			problem(w, r, 409, "RESTORE_CONFLICT", "An active resource already uses this identity", false)
			return
		}
		handleErr(w, r, err)
		return
	}
	if err := a.auditChangeTx(r.Context(), tx, principal(r), project, spec.resource+".restore", spec.resource, id, before, after); err != nil {
		handleErr(w, r, err)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		handleErr(w, r, err)
		return
	}
	a.emit(r.Context(), project, spec.event, spec.resource, id, map[string]any{"id": id, "restored": true, "version": version + 1})
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprint(version+1)))
	writeJSON(w, http.StatusOK, after)
}

type controlQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func controlSnapshotWith(ctx context.Context, db controlQueryer, spec controlSpec, project, id uuid.UUID, includeDeleted bool) (json.RawMessage, error) {
	query := "SELECT " + controlRow() + " FROM " + spec.table + " t WHERE " + spec.scope + " AND t.id=$2 AND ($3 OR t.deleted_at IS NULL)"
	var item json.RawMessage
	err := db.QueryRow(ctx, query, project, id, includeDeleted).Scan(&item)
	return item, err
}

func (a *API) controlSnapshot(ctx context.Context, spec controlSpec, project, id uuid.UUID, includeDeleted bool) (json.RawMessage, error) {
	return controlSnapshotWith(ctx, a.Store.Pool, spec, project, id, includeDeleted)
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

func (a *API) validatePatchDependencies(r *http.Request, spec controlSpec, project, id uuid.UUID, patch controlplane.Patch) error {
	switch spec.resource {
	case "queue":
		raw, changed := patch["backend_id"]
		if !changed || strings.TrimSpace(string(raw)) == "null" {
			return nil
		}
		var backendID uuid.UUID
		if err := json.Unmarshal(raw, &backendID); err != nil {
			return fmt.Errorf("backend_id must be a UUID or null")
		}
		var exists bool
		err := a.Store.Pool.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM queue_backends WHERE id=$1 AND status='ACTIVE' AND deleted_at IS NULL)", backendID).Scan(&exists)
		if err != nil || !exists {
			return fmt.Errorf("queue backend is not active")
		}
		var incompatible bool
		err = a.Store.Pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM job_definitions jd JOIN queue_backends qb ON qb.id=$1 WHERE jd.queue_id=$2 AND jd.deleted_at IS NULL AND jd.execution_mode='BATCH' AND qb.backend_type <> 'POSTGRES')`, backendID, id).Scan(&incompatible)
		if err != nil {
			return err
		}
		if incompatible {
			return fmt.Errorf("queue has active BATCH job definitions and cannot use an external backend")
		}
	case "job_definition":
		raw, changed := patch["retry_policy_id"]
		if !changed || strings.TrimSpace(string(raw)) == "null" {
			return nil
		}
		var policyID uuid.UUID
		if err := json.Unmarshal(raw, &policyID); err != nil {
			return fmt.Errorf("retry_policy_id must be a UUID or null")
		}
		var exists bool
		err := a.Store.Pool.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM retry_policies WHERE id=$1 AND project_id=$2 AND deleted_at IS NULL)", policyID, project).Scan(&exists)
		if err != nil || !exists {
			return fmt.Errorf("retry policy is outside this project or deleted")
		}
	}
	return nil
}

func (a *API) validateSchedulePatch(r *http.Request, spec controlSpec, project, id uuid.UUID, patch controlplane.Patch) error {
	if spec.resource != "schedule" {
		return nil
	}
	item, err := a.controlSnapshot(r.Context(), spec, project, id, false)
	if err != nil {
		return err
	}
	var current map[string]json.RawMessage
	if err := json.Unmarshal(item, &current); err != nil {
		return err
	}
	for key, value := range patch {
		current[key] = value
	}
	var scheduleType, expression, timezoneName string
	var seconds bool
	if err := json.Unmarshal(current["schedule_type"], &scheduleType); err != nil {
		return fmt.Errorf("schedule_type is invalid")
	}
	if scheduleType != "CRON" {
		return nil
	}
	if err := json.Unmarshal(current["cron_expression"], &expression); err != nil {
		return fmt.Errorf("cron_expression is invalid")
	}
	if err := json.Unmarshal(current["timezone"], &timezoneName); err != nil {
		return fmt.Errorf("timezone is invalid")
	}
	if err := json.Unmarshal(current["with_seconds"], &seconds); err != nil {
		return fmt.Errorf("with_seconds is invalid")
	}
	return controlplane.ValidateScheduleExpression(expression, timezoneName, seconds)
}

func (a *API) validateRetryPolicyPatch(r *http.Request, spec controlSpec, project, id uuid.UUID, patch controlplane.Patch) error {
	if spec.resource != "retry_policy" {
		return nil
	}
	item, err := a.controlSnapshot(r.Context(), spec, project, id, false)
	if err != nil {
		return err
	}
	var current map[string]json.RawMessage
	if err := json.Unmarshal(item, &current); err != nil {
		return err
	}
	for key, value := range patch {
		current[key] = value
	}
	var strategy string
	var initialDelay, maxDelay int64
	var multiplier float64
	if json.Unmarshal(current["strategy"], &strategy) != nil || json.Unmarshal(current["initial_delay_ms"], &initialDelay) != nil || json.Unmarshal(current["max_delay_ms"], &maxDelay) != nil || json.Unmarshal(current["multiplier"], &multiplier) != nil {
		return fmt.Errorf("retry policy state is invalid")
	}
	return controlplane.ValidateRetryPolicyValues(strategy, initialDelay, maxDelay, multiplier)
}

func (a *API) validateFunctionSchemaPatch(spec controlSpec, patch controlplane.Patch) error {
	if spec.resource != "function_definition" {
		return nil
	}
	raw, changed := patch["input_schema"]
	if !changed {
		return nil
	}
	return postgres.ValidateInputSchema(raw)
}

// External transports currently implement a single-job delivery contract.
// Reject every create/update combination that would leave a BATCH definition
// on such a queue rather than accepting work that no worker can claim.
func (a *API) validateJobDefinitionPatch(r *http.Request, spec controlSpec, project, id uuid.UUID, patch controlplane.Patch) error {
	if spec.resource != "job_definition" {
		return nil
	}
	item, err := a.controlSnapshot(r.Context(), spec, project, id, false)
	if err != nil {
		return err
	}
	var state struct {
		ExecutionMode string    `json:"execution_mode"`
		QueueID       uuid.UUID `json:"queue_id"`
	}
	if err = json.Unmarshal(item, &state); err != nil {
		return err
	}
	if raw, ok := patch["execution_mode"]; ok {
		if err = json.Unmarshal(raw, &state.ExecutionMode); err != nil {
			return fmt.Errorf("execution_mode is invalid")
		}
	}
	if raw, ok := patch["queue_id"]; ok {
		if err = json.Unmarshal(raw, &state.QueueID); err != nil {
			return fmt.Errorf("queue_id is invalid")
		}
	}
	if state.ExecutionMode != "BATCH" {
		return nil
	}
	var external bool
	err = a.Store.Pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM queues q JOIN queue_backends qb ON qb.id=q.backend_id WHERE q.id=$1 AND qb.backend_type <> 'POSTGRES' AND qb.status='ACTIVE' AND qb.deleted_at IS NULL)`, state.QueueID).Scan(&external)
	if err != nil {
		return err
	}
	if external {
		return fmt.Errorf("BATCH execution requires a PostgreSQL queue backend")
	}
	return nil
}

func (a *API) patchDependencySQL(spec controlSpec) string {
	switch spec.resource {
	case "queue":
		return " AND (NOT ($3::jsonb ? 'backend_id') OR p.backend_id IS NULL OR EXISTS(SELECT 1 FROM queue_backends b WHERE b.id=p.backend_id AND b.status='ACTIVE' AND b.deleted_at IS NULL FOR UPDATE))"
	case "job_definition":
		return " AND (NOT ($3::jsonb ? 'retry_policy_id') OR p.retry_policy_id IS NULL OR EXISTS(SELECT 1 FROM retry_policies rp WHERE rp.id=p.retry_policy_id AND rp.project_id=$1 AND rp.deleted_at IS NULL FOR UPDATE))"
	}
	return ""
}

func (a *API) deleteBlocked(r *http.Request, spec controlSpec, project, id uuid.UUID) (bool, error) {
	var query string
	switch spec.resource {
	case "queue":
		query = "SELECT EXISTS(SELECT 1 FROM job_definitions WHERE project_id=$1 AND queue_id=$2 AND deleted_at IS NULL)"
	case "retry_policy":
		query = "SELECT EXISTS(SELECT 1 FROM job_definitions WHERE project_id=$1 AND retry_policy_id=$2 AND deleted_at IS NULL)"
	case "function_definition":
		query = "SELECT EXISTS(SELECT 1 FROM job_definitions WHERE project_id=$1 AND function_id=$2 AND deleted_at IS NULL)"
	case "job_definition":
		query = "SELECT EXISTS(SELECT 1 FROM schedules WHERE job_definition_id=$2 AND deleted_at IS NULL)"
	default:
		return false, nil
	}
	var blocked bool
	err := a.Store.Pool.QueryRow(r.Context(), query, project, id).Scan(&blocked)
	return blocked, err
}

func (a *API) deleteDependencySQL(spec controlSpec) string {
	switch spec.resource {
	case "queue":
		return " AND NOT EXISTS(SELECT 1 FROM job_definitions jd WHERE jd.project_id=$1 AND jd.queue_id=t.id AND jd.deleted_at IS NULL)"
	case "retry_policy":
		return " AND NOT EXISTS(SELECT 1 FROM job_definitions jd WHERE jd.project_id=$1 AND jd.retry_policy_id=t.id AND jd.deleted_at IS NULL)"
	case "function_definition":
		return " AND NOT EXISTS(SELECT 1 FROM job_definitions jd WHERE jd.project_id=$1 AND jd.function_id=t.id AND jd.deleted_at IS NULL)"
	case "job_definition":
		return " AND NOT EXISTS(SELECT 1 FROM schedules s WHERE s.job_definition_id=t.id AND s.deleted_at IS NULL)"
	}
	return ""
}

func (a *API) restoreDependencySQL(spec controlSpec) string {
	switch spec.resource {
	case "queue":
		return " AND (t.backend_id IS NULL OR EXISTS(SELECT 1 FROM queue_backends b WHERE b.id=t.backend_id AND b.status='ACTIVE' AND b.deleted_at IS NULL))"
	case "job_definition":
		return " AND EXISTS(SELECT 1 FROM function_definitions f WHERE f.id=t.function_id AND f.project_id=$1 AND f.deleted_at IS NULL) AND EXISTS(SELECT 1 FROM queues q WHERE q.id=t.queue_id AND q.project_id=$1 AND q.deleted_at IS NULL) AND (t.retry_policy_id IS NULL OR EXISTS(SELECT 1 FROM retry_policies rp WHERE rp.id=t.retry_policy_id AND rp.project_id=$1 AND rp.deleted_at IS NULL))"
	case "schedule":
		return " AND EXISTS(SELECT 1 FROM job_definitions jd WHERE jd.id=t.job_definition_id AND jd.project_id=$1 AND jd.deleted_at IS NULL)"
	}
	return ""
}
