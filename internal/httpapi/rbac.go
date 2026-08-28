package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/example/task-processing/internal/application/controlplane"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func requirePlatformAdmin(w http.ResponseWriter, r *http.Request) bool {
	p := principal(r)
	if p.Permissions != nil && p.Permissions["platform:admin"] {
		return true
	}
	if p.Permissions == nil && p.Role == "admin" {
		return true
	}
	problem(w, r, http.StatusForbidden, "FORBIDDEN", "platform:admin is required", false)
	return false
}
func (a *API) rbacPermissions(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), "SELECT id,permission_key,description FROM permissions ORDER BY permission_key")
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id uuid.UUID
		var key, description string
		if err = rows.Scan(&id, &key, &description); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": id, "permission_key": key, "description": description})
	}
	if err = rows.Err(); err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
func (a *API) platformAudits(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), `SELECT id,tenant_id,actor_id,action,resource_type,resource_id,before_data,after_data,request_id,trace_id,metadata,created_at
		FROM platform_audit_logs WHERE tenant_id=$1 ORDER BY created_at DESC,id DESC LIMIT 200`, principal(r).TenantID)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		var id, resourceID uuid.UUID
		var tenant *uuid.UUID
		var actor, action, typ string
		var before, after, metadata json.RawMessage
		var requestID, traceID *string
		var created any
		if err = rows.Scan(&id, &tenant, &actor, &action, &typ, &resourceID, &before, &after, &requestID, &traceID, &metadata, &created); err != nil {
			handleErr(w, r, err)
			return
		}
		items = append(items, map[string]any{"id": id, "tenant_id": tenant, "actor_id": actor, "action": action, "resource_type": typ, "resource_id": resourceID, "before_data": before, "after_data": after, "request_id": requestID, "trace_id": traceID, "metadata": metadata, "created_at": created})
	}
	if err = rows.Err(); err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}
func rbacIncludeDeleted(r *http.Request) bool {
	value, _ := strconv.ParseBool(r.URL.Query().Get("include_deleted"))
	return value
}
func (a *API) rbacUsers(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	items, err := (controlplane.RBACService{Store: a.Store}).ListUsers(r.Context(), principal(r).TenantID, rbacIncludeDeleted(r))
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}
func (a *API) rbacRoles(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	items, err := (controlplane.RBACService{Store: a.Store}).ListRoles(r.Context(), principal(r).TenantID, rbacIncludeDeleted(r))
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *API) createRBACUser(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	var req struct {
		Subject     string `json:"subject"`
		Email       string `json:"email"`
		DisplayName string `json:"display_name"`
		Status      string `json:"status"`
	}
	if !decode(w, r, &req) {
		return
	}
	p := principal(r)
	out, err := (controlplane.RBACService{Store: a.Store}).CreateUser(r.Context(), p.TenantID, controlplane.UserInput{Subject: req.Subject, Email: req.Email, DisplayName: req.DisplayName, Status: req.Status}, a.rbacAudit(r, "rbac.user.create", "user"))
	if err != nil {
		rbacError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}
func (a *API) createRBACRole(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	var req struct {
		Key         string `json:"role_key"`
		Name        string `json:"display_name"`
		Description string `json:"description"`
		Status      string `json:"status"`
	}
	if !decode(w, r, &req) {
		return
	}
	p := principal(r)
	out, err := (controlplane.RBACService{Store: a.Store}).CreateRole(r.Context(), p.TenantID, controlplane.RoleInput{RoleKey: req.Key, DisplayName: req.Name, Description: req.Description, Status: req.Status}, a.rbacAudit(r, "rbac.role.create", "role"))
	if err != nil {
		rbacError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}
func (a *API) updateRBACUser(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	id, ok := rbacID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	var req struct {
		Subject     *string `json:"subject"`
		Email       *string `json:"email"`
		DisplayName *string `json:"display_name"`
		Status      *string `json:"status"`
	}
	if !decode(w, r, &req) {
		return
	}
	p := principal(r)
	out, err := (controlplane.RBACService{Store: a.Store}).UpdateUser(r.Context(), p.TenantID, id, version, controlplane.UserPatch{Subject: req.Subject, Email: req.Email, DisplayName: req.DisplayName, Status: req.Status}, a.rbacAudit(r, "rbac.user.update", "user"))
	if err != nil {
		rbacError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteVersion(out))
	writeJSON(w, http.StatusOK, out)
}
func (a *API) updateRBACRole(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	id, ok := rbacID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	var req struct {
		Key         *string `json:"role_key"`
		Name        *string `json:"display_name"`
		Description *string `json:"description"`
		Status      *string `json:"status"`
	}
	if !decode(w, r, &req) {
		return
	}
	p := principal(r)
	out, err := (controlplane.RBACService{Store: a.Store}).UpdateRole(r.Context(), p.TenantID, id, version, controlplane.RolePatch{RoleKey: req.Key, DisplayName: req.Name, Description: req.Description, Status: req.Status}, a.rbacAudit(r, "rbac.role.update", "role"))
	if err != nil {
		rbacError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteVersion(out))
	writeJSON(w, http.StatusOK, out)
}
func (a *API) replaceRolePermissions(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	id, ok := rbacID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	var req struct {
		PermissionIDs []uuid.UUID `json:"permission_ids"`
	}
	if !decode(w, r, &req) {
		return
	}
	p := principal(r)
	out, err := (controlplane.RBACService{Store: a.Store}).ReplaceRolePermissions(r.Context(), p.TenantID, id, version, req.PermissionIDs, a.rbacAudit(r, "rbac.role.permissions.replace", "role"))
	if err != nil {
		rbacError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteVersion(out))
	writeJSON(w, http.StatusOK, out)
}
func (a *API) replaceUserRoles(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	id, ok := rbacID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	var req struct {
		RoleIDs []uuid.UUID `json:"role_ids"`
	}
	if !decode(w, r, &req) {
		return
	}
	p := principal(r)
	out, err := (controlplane.RBACService{Store: a.Store}).ReplaceUserRoles(r.Context(), p.TenantID, id, version, req.RoleIDs, a.rbacAudit(r, "rbac.user.roles.replace", "user"))
	if err != nil {
		rbacError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteVersion(out))
	writeJSON(w, http.StatusOK, out)
}
func (a *API) deleteRBACUser(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	id, ok := rbacID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	p := principal(r)
	out, err := (controlplane.RBACService{Store: a.Store}).SoftDeleteUser(r.Context(), p.TenantID, id, version, p.Actor, a.rbacAudit(r, "rbac.user.soft_delete", "user"))
	if err != nil {
		rbacError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteVersion(out))
	writeJSON(w, http.StatusOK, out)
}
func (a *API) restoreRBACUser(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	id, ok := rbacID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	p := principal(r)
	out, err := (controlplane.RBACService{Store: a.Store}).RestoreUser(r.Context(), p.TenantID, id, version, a.rbacAudit(r, "rbac.user.restore", "user"))
	if err != nil {
		rbacError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteVersion(out))
	writeJSON(w, http.StatusOK, out)
}
func (a *API) deleteRBACRole(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	id, ok := rbacID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	p := principal(r)
	out, err := (controlplane.RBACService{Store: a.Store}).SoftDeleteRole(r.Context(), p.TenantID, id, version, p.Actor, a.rbacAudit(r, "rbac.role.soft_delete", "role"))
	if err != nil {
		rbacError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteVersion(out))
	writeJSON(w, http.StatusOK, out)
}
func (a *API) restoreRBACRole(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	id, ok := rbacID(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	p := principal(r)
	out, err := (controlplane.RBACService{Store: a.Store}).RestoreRole(r.Context(), p.TenantID, id, version, a.rbacAudit(r, "rbac.role.restore", "role"))
	if err != nil {
		rbacError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteVersion(out))
	writeJSON(w, http.StatusOK, out)
}

func (a *API) rbacAudit(r *http.Request, action, typ string) controlplane.RBACMutationAudit {
	return func(ctx context.Context, tx pgx.Tx, id uuid.UUID, before, after json.RawMessage) error {
		return a.platformAuditChangeTx(ctx, tx, principal(r), action, typ, id, before, after)
	}
}
func rbacID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, http.StatusBadRequest, "INVALID_ID", "Invalid RBAC ID", false)
		return uuid.Nil, false
	}
	return id, true
}
func quoteVersion(raw json.RawMessage) string {
	var v struct {
		Version int64 `json:"version"`
	}
	_ = json.Unmarshal(raw, &v)
	return `"` + strconv.FormatInt(v.Version, 10) + `"`
}
func rbacError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, controlplane.ErrResourceNotFound):
		problem(w, r, http.StatusNotFound, "NOT_FOUND", "RBAC resource not found", false)
	case errors.Is(err, controlplane.ErrOptimisticLock):
		problem(w, r, http.StatusPreconditionFailed, "PRECONDITION_FAILED", "RBAC resource changed or is not active", false)
	case errors.Is(err, controlplane.ErrRestoreConflict):
		problem(w, r, http.StatusConflict, "RESTORE_CONFLICT", "An active resource already uses this identity", false)
	case errors.Is(err, controlplane.ErrDependencyBlocked):
		problem(w, r, http.StatusConflict, "DEPENDENCY_BLOCKED", "RBAC resource is still in use", false)
	default:
		problem(w, r, http.StatusBadRequest, "INVALID_RBAC", err.Error(), false)
	}
}
