package httpapi

import (
	"net/http"

	"github.com/example/task-processing/internal/application/controlplane"
	"github.com/google/uuid"
)

func (a *API) previewRetention(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
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
	out, err := a.Store.PreviewRetention(r.Context(), project, id, 1000)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
func (a *API) executeRetention(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
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
	var req struct {
		Limit int `json:"limit"`
	}
	if !decode(w, r, &req) {
		return
	}
	meta := requestMeta(r.Context())
	out, err := a.Store.ExecuteRetention(r.Context(), project, id, req.Limit, principal(r).Actor, meta.RequestID, meta.TraceID)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	a.audit(r.Context(), principal(r), project, "retention.execute", "retention_policy", id, map[string]any{"run_id": out.ID, "deleted_count": out.DeletedCount})
	writeJSON(w, http.StatusOK, out)
}
func (a *API) listRetentionRuns(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
		return
	}
	project, ok := projectParam(w, r)
	if !ok {
		return
	}
	items, err := a.Store.RetentionRuns(r.Context(), project)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *API) createRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	if !requireRole(w, r, "admin", "developer") {
		return
	}
	var req struct {
		ProjectID     uuid.UUID `json:"project_id"`
		ResourceType  string    `json:"resource_type"`
		RetentionDays int       `json:"retention_days"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !a.projectOK(r.Context(), principal(r), req.ProjectID) {
		problem(w, r, http.StatusNotFound, "PROJECT_NOT_FOUND", "Project not found", false)
		return
	}
	out, err := (controlplane.RetentionPolicyService{Store: a.Store}).Create(r.Context(), controlplane.CreateRetentionPolicy{ProjectID: req.ProjectID, ResourceType: req.ResourceType, RetentionDays: req.RetentionDays})
	if err != nil {
		problem(w, r, http.StatusBadRequest, "INVALID_RETENTION_POLICY", err.Error(), false)
		return
	}
	a.audit(r.Context(), principal(r), req.ProjectID, "retention_policy.create", "retention_policy", out.ID, req)
	a.emit(r.Context(), req.ProjectID, "retention_policy.changed", "retention_policy", out.ID, map[string]any{"id": out.ID, "version": out.Version})
	writeJSON(w, http.StatusCreated, map[string]any{"id": out.ID, "status": out.Status, "version": out.Version})
}
