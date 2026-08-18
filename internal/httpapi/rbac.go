package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
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
	writeJSON(w, http.StatusOK, out)
}
func (a *API) rbacUsers(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	p := principal(r)
	rows, err := a.Store.Pool.Query(r.Context(), "SELECT id,subject,email,display_name,status,row_version,deleted_at FROM users WHERE tenant_id=$1 ORDER BY created_at DESC", p.TenantID)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id uuid.UUID
		var subject, name, status string
		var email *string
		var version int64
		var deleted any
		if err = rows.Scan(&id, &subject, &email, &name, &status, &version, &deleted); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": id, "subject": subject, "email": email, "display_name": name, "status": status, "version": version, "deleted_at": deleted})
	}
	writeJSON(w, 200, out)
}
func (a *API) createRBACUser(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	var req struct {
		Subject     string `json:"subject"`
		Email       string `json:"email"`
		DisplayName string `json:"display_name"`
	}
	if !decode(w, r, &req) {
		return
	}
	req.Subject = strings.TrimSpace(req.Subject)
	req.DisplayName = strings.TrimSpace(req.DisplayName)
	if req.Subject == "" || req.DisplayName == "" {
		problem(w, r, 400, "INVALID_USER", "subject and display_name are required", false)
		return
	}
	var id uuid.UUID
	err := a.Store.Pool.QueryRow(r.Context(), "INSERT INTO users(tenant_id,subject,email,display_name) VALUES($1,$2,NULLIF($3,''),$4) RETURNING id", principal(r).TenantID, req.Subject, req.Email, req.DisplayName).Scan(&id)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, 201, map[string]any{"id": id})
}
func (a *API) rbacRoles(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	rows, err := a.Store.Pool.Query(r.Context(), "SELECT r.id,r.role_key,r.display_name,r.description,r.status,r.row_version,COALESCE(jsonb_agg(p.permission_key) FILTER (WHERE p.id IS NOT NULL),'[]'::jsonb) FROM roles r LEFT JOIN role_permissions rp ON rp.role_id=r.id LEFT JOIN permissions p ON p.id=rp.permission_id WHERE r.tenant_id=$1 AND r.deleted_at IS NULL GROUP BY r.id ORDER BY r.role_key", principal(r).TenantID)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id uuid.UUID
		var key, name, status string
		var description *string
		var version int64
		var permissions json.RawMessage
		if err = rows.Scan(&id, &key, &name, &description, &status, &version, &permissions); err != nil {
			handleErr(w, r, err)
			return
		}
		out = append(out, map[string]any{"id": id, "role_key": key, "display_name": name, "description": description, "status": status, "version": version, "permissions": permissions})
	}
	writeJSON(w, 200, out)
}
func (a *API) createRBACRole(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	var req struct {
		Key         string `json:"role_key"`
		Name        string `json:"display_name"`
		Description string `json:"description"`
	}
	if !decode(w, r, &req) {
		return
	}
	req.Key = strings.TrimSpace(req.Key)
	req.Name = strings.TrimSpace(req.Name)
	if req.Key == "" || req.Name == "" {
		problem(w, r, 400, "INVALID_ROLE", "role_key and display_name are required", false)
		return
	}
	var id uuid.UUID
	err := a.Store.Pool.QueryRow(r.Context(), "INSERT INTO roles(tenant_id,role_key,display_name,description) VALUES($1,$2,$3,NULLIF($4,'')) RETURNING id", principal(r).TenantID, req.Key, req.Name, req.Description).Scan(&id)
	if err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, 201, map[string]any{"id": id})
}
func (a *API) replaceRolePermissions(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	role, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid role ID", false)
		return
	}
	var req struct {
		PermissionIDs []uuid.UUID `json:"permission_ids"`
	}
	if !decode(w, r, &req) {
		return
	}
	tx, err := a.Store.Pool.Begin(r.Context())
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer tx.Rollback(r.Context())
	var exists bool
	if err = tx.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM roles WHERE id=$1 AND tenant_id=$2 AND deleted_at IS NULL)", role, principal(r).TenantID).Scan(&exists); err != nil || !exists {
		problem(w, r, 404, "NOT_FOUND", "Role not found", false)
		return
	}
	if _, err = tx.Exec(r.Context(), "DELETE FROM role_permissions WHERE role_id=$1", role); err != nil {
		handleErr(w, r, err)
		return
	}
	for _, permission := range req.PermissionIDs {
		tag, e := tx.Exec(r.Context(), "INSERT INTO role_permissions(role_id,permission_id) SELECT $1,id FROM permissions WHERE id=$2 ON CONFLICT DO NOTHING", role, permission)
		if e != nil || tag.RowsAffected() == 0 {
			problem(w, r, 400, "INVALID_PERMISSION", "Unknown permission", false)
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": role, "permission_ids": req.PermissionIDs})
}
func (a *API) replaceUserRoles(w http.ResponseWriter, r *http.Request) {
	if !requirePlatformAdmin(w, r) {
		return
	}
	user, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		problem(w, r, 400, "INVALID_ID", "Invalid user ID", false)
		return
	}
	var req struct {
		RoleIDs []uuid.UUID `json:"role_ids"`
	}
	if !decode(w, r, &req) {
		return
	}
	tx, err := a.Store.Pool.Begin(r.Context())
	if err != nil {
		handleErr(w, r, err)
		return
	}
	defer tx.Rollback(r.Context())
	var exists bool
	if err = tx.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND tenant_id=$2 AND deleted_at IS NULL)", user, principal(r).TenantID).Scan(&exists); err != nil || !exists {
		problem(w, r, 404, "NOT_FOUND", "User not found", false)
		return
	}
	if _, err = tx.Exec(r.Context(), "DELETE FROM user_roles WHERE user_id=$1", user); err != nil {
		handleErr(w, r, err)
		return
	}
	for _, role := range req.RoleIDs {
		tag, e := tx.Exec(r.Context(), "INSERT INTO user_roles(user_id,role_id) SELECT $1,id FROM roles WHERE id=$2 AND tenant_id=$3 AND deleted_at IS NULL ON CONFLICT DO NOTHING", user, role, principal(r).TenantID)
		if e != nil || tag.RowsAffected() == 0 {
			problem(w, r, 400, "INVALID_ROLE", "Unknown tenant role", false)
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		handleErr(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": user, "role_ids": req.RoleIDs})
}
