package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RBACService owns tenant-scoped identities and roles. Mapping changes bump
// their parent's version, so two administrators cannot overwrite each other.
type RBACService struct{ Store *postgres.Store }

type RBACMutationAudit func(context.Context, pgx.Tx, uuid.UUID, json.RawMessage, json.RawMessage) error

type UserInput struct {
	Subject, Email, DisplayName, Status string
}

type RoleInput struct {
	RoleKey, DisplayName, Description, Status string
}

type UserPatch struct {
	Subject, Email, DisplayName, Status *string
}

type RolePatch struct {
	RoleKey, DisplayName, Description, Status *string
}

func (s RBACService) ListUsers(ctx context.Context, tenant uuid.UUID, includeDeleted bool) ([]json.RawMessage, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	rows, err := s.Store.Pool.Query(ctx, `SELECT ((to_jsonb(u) - 'row_version') || jsonb_build_object('version',u.row_version) || jsonb_build_object('role_ids',COALESCE((SELECT jsonb_agg(ur.role_id ORDER BY ur.role_id) FROM user_roles ur WHERE ur.user_id=u.id),'[]'::jsonb))) FROM users u WHERE u.tenant_id=$1 AND ($2 OR u.deleted_at IS NULL) ORDER BY u.created_at DESC,u.id DESC`, tenant, includeDeleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var item json.RawMessage
		if err = rows.Scan(&item); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s RBACService) ListRoles(ctx context.Context, tenant uuid.UUID, includeDeleted bool) ([]json.RawMessage, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	rows, err := s.Store.Pool.Query(ctx, `SELECT ((to_jsonb(ro) - 'row_version') || jsonb_build_object('version',ro.row_version) || jsonb_build_object('permission_ids',COALESCE((SELECT jsonb_agg(rp.permission_id ORDER BY rp.permission_id) FROM role_permissions rp WHERE rp.role_id=ro.id),'[]'::jsonb))) FROM roles ro WHERE ro.tenant_id=$1 AND ($2 OR ro.deleted_at IS NULL) ORDER BY ro.role_key,ro.id`, tenant, includeDeleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var item json.RawMessage
		if err = rows.Scan(&item); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s RBACService) CreateUser(ctx context.Context, tenant uuid.UUID, in UserInput, audit RBACMutationAudit) (json.RawMessage, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if err := validateUser(in); err != nil {
		return nil, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var id uuid.UUID
	if err = tx.QueryRow(ctx, `INSERT INTO users(tenant_id,subject,email,display_name,status) VALUES($1,$2,NULLIF($3,''),$4,$5) RETURNING id`, tenant, strings.TrimSpace(in.Subject), strings.TrimSpace(in.Email), strings.TrimSpace(in.DisplayName), defaultStatus(in.Status)).Scan(&id); err != nil {
		return nil, err
	}
	after, err := rbacUserSnapshot(ctx, tx, tenant, id, true)
	if err != nil {
		return nil, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, nil, after); err != nil {
			return nil, err
		}
	}
	return after, tx.Commit(ctx)
}

func (s RBACService) CreateRole(ctx context.Context, tenant uuid.UUID, in RoleInput, audit RBACMutationAudit) (json.RawMessage, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if err := validateRole(in); err != nil {
		return nil, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var id uuid.UUID
	if err = tx.QueryRow(ctx, `INSERT INTO roles(tenant_id,role_key,display_name,description,status) VALUES($1,$2,$3,NULLIF($4,''),$5) RETURNING id`, tenant, strings.TrimSpace(in.RoleKey), strings.TrimSpace(in.DisplayName), strings.TrimSpace(in.Description), defaultStatus(in.Status)).Scan(&id); err != nil {
		return nil, err
	}
	after, err := rbacRoleSnapshot(ctx, tx, tenant, id, true)
	if err != nil {
		return nil, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, nil, after); err != nil {
			return nil, err
		}
	}
	return after, tx.Commit(ctx)
}

func (s RBACService) UpdateUser(ctx context.Context, tenant, id uuid.UUID, version int64, patch UserPatch, audit RBACMutationAudit) (json.RawMessage, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if err := validateUserPatch(patch); err != nil {
		return nil, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	before, err := rbacUserSnapshot(ctx, tx, tenant, id, false)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrResourceNotFound
	}
	if err != nil {
		return nil, err
	}
	var subject, email, name, status *string = patch.Subject, patch.Email, patch.DisplayName, patch.Status
	var ignored uuid.UUID
	err = tx.QueryRow(ctx, `UPDATE users SET subject=CASE WHEN $4 THEN $5 ELSE subject END,email=CASE WHEN $6 THEN NULLIF($7,'') ELSE email END,display_name=CASE WHEN $8 THEN $9 ELSE display_name END,status=CASE WHEN $10 THEN $11 ELSE status END WHERE tenant_id=$1 AND id=$2 AND row_version=$3 AND deleted_at IS NULL RETURNING id`, tenant, id, version, subject != nil, value(subject), email != nil, value(email), name != nil, value(name), status != nil, value(status)).Scan(&ignored)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOptimisticLock
	}
	if err != nil {
		return nil, err
	}
	after, err := rbacUserSnapshot(ctx, tx, tenant, id, false)
	if err != nil {
		return nil, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, before, after); err != nil {
			return nil, err
		}
	}
	return after, tx.Commit(ctx)
}

func (s RBACService) UpdateRole(ctx context.Context, tenant, id uuid.UUID, version int64, patch RolePatch, audit RBACMutationAudit) (json.RawMessage, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	if err := validateRolePatch(patch); err != nil {
		return nil, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	before, err := rbacRoleSnapshot(ctx, tx, tenant, id, false)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrResourceNotFound
	}
	if err != nil {
		return nil, err
	}
	var key, name, description, status *string = patch.RoleKey, patch.DisplayName, patch.Description, patch.Status
	var ignored uuid.UUID
	err = tx.QueryRow(ctx, `UPDATE roles SET role_key=CASE WHEN $4 THEN $5 ELSE role_key END,display_name=CASE WHEN $6 THEN $7 ELSE display_name END,description=CASE WHEN $8 THEN NULLIF($9,'') ELSE description END,status=CASE WHEN $10 THEN $11 ELSE status END WHERE tenant_id=$1 AND id=$2 AND row_version=$3 AND deleted_at IS NULL RETURNING id`, tenant, id, version, key != nil, value(key), name != nil, value(name), description != nil, value(description), status != nil, value(status)).Scan(&ignored)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOptimisticLock
	}
	if err != nil {
		return nil, err
	}
	after, err := rbacRoleSnapshot(ctx, tx, tenant, id, false)
	if err != nil {
		return nil, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, before, after); err != nil {
			return nil, err
		}
	}
	return after, tx.Commit(ctx)
}

func (s RBACService) ReplaceRolePermissions(ctx context.Context, tenant, id uuid.UUID, version int64, permissionIDs []uuid.UUID, audit RBACMutationAudit) (json.RawMessage, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	permissionIDs = uniqueUUIDs(permissionIDs)
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	before, err := rbacRoleSnapshot(ctx, tx, tenant, id, false)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrResourceNotFound
	}
	if err != nil {
		return nil, err
	}
	if err = requireIDs(ctx, tx, "permissions", "id", permissionIDs); err != nil {
		return nil, fmt.Errorf("invalid permission: %w", err)
	}
	if _, err = tx.Exec(ctx, "DELETE FROM role_permissions WHERE role_id=$1", id); err != nil {
		return nil, err
	}
	if len(permissionIDs) > 0 {
		if _, err = tx.Exec(ctx, "INSERT INTO role_permissions(role_id,permission_id) SELECT $1,unnest($2::uuid[])", id, permissionIDs); err != nil {
			return nil, err
		}
	}
	if err = bumpRBACParent(ctx, tx, "roles", tenant, id, version); err != nil {
		return nil, err
	}
	after, err := rbacRoleSnapshot(ctx, tx, tenant, id, false)
	if err != nil {
		return nil, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, before, after); err != nil {
			return nil, err
		}
	}
	return after, tx.Commit(ctx)
}

func (s RBACService) ReplaceUserRoles(ctx context.Context, tenant, id uuid.UUID, version int64, roleIDs []uuid.UUID, audit RBACMutationAudit) (json.RawMessage, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	roleIDs = uniqueUUIDs(roleIDs)
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	before, err := rbacUserSnapshot(ctx, tx, tenant, id, false)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrResourceNotFound
	}
	if err != nil {
		return nil, err
	}
	if err = requireTenantRoleIDs(ctx, tx, tenant, roleIDs); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, "DELETE FROM user_roles WHERE user_id=$1", id); err != nil {
		return nil, err
	}
	if len(roleIDs) > 0 {
		if _, err = tx.Exec(ctx, "INSERT INTO user_roles(user_id,role_id) SELECT $1,unnest($2::uuid[])", id, roleIDs); err != nil {
			return nil, err
		}
	}
	if err = bumpRBACParent(ctx, tx, "users", tenant, id, version); err != nil {
		return nil, err
	}
	after, err := rbacUserSnapshot(ctx, tx, tenant, id, false)
	if err != nil {
		return nil, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, before, after); err != nil {
			return nil, err
		}
	}
	return after, tx.Commit(ctx)
}

func (s RBACService) SoftDeleteUser(ctx context.Context, tenant, id uuid.UUID, version int64, actor string, audit RBACMutationAudit) (json.RawMessage, error) {
	return s.setUserDeletion(ctx, tenant, id, version, actor, true, audit)
}
func (s RBACService) RestoreUser(ctx context.Context, tenant, id uuid.UUID, version int64, audit RBACMutationAudit) (json.RawMessage, error) {
	return s.setUserDeletion(ctx, tenant, id, version, "", false, audit)
}
func (s RBACService) setUserDeletion(ctx context.Context, tenant, id uuid.UUID, version int64, actor string, deleted bool, audit RBACMutationAudit) (json.RawMessage, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	before, err := rbacUserSnapshot(ctx, tx, tenant, id, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrResourceNotFound
	}
	if err != nil {
		return nil, err
	}
	where := "deleted_at IS NOT NULL"
	set := "deleted_at=NULL,deleted_by=NULL"
	if deleted {
		where = "deleted_at IS NULL"
		set = "deleted_at=now(),deleted_by=$4"
	}
	args := []any{tenant, id, version}
	if deleted {
		args = append(args, actor)
	}
	var ignored uuid.UUID
	err = tx.QueryRow(ctx, "UPDATE users SET "+set+" WHERE tenant_id=$1 AND id=$2 AND row_version=$3 AND "+where+" RETURNING id", args...).Scan(&ignored)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOptimisticLock
	}
	if err != nil {
		if isUnique(err) {
			return nil, ErrRestoreConflict
		}
		return nil, err
	}
	after, err := rbacUserSnapshot(ctx, tx, tenant, id, true)
	if err != nil {
		return nil, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, before, after); err != nil {
			return nil, err
		}
	}
	return after, tx.Commit(ctx)
}

func (s RBACService) SoftDeleteRole(ctx context.Context, tenant, id uuid.UUID, version int64, actor string, audit RBACMutationAudit) (json.RawMessage, error) {
	return s.setRoleDeletion(ctx, tenant, id, version, actor, true, audit)
}
func (s RBACService) RestoreRole(ctx context.Context, tenant, id uuid.UUID, version int64, audit RBACMutationAudit) (json.RawMessage, error) {
	return s.setRoleDeletion(ctx, tenant, id, version, "", false, audit)
}
func (s RBACService) setRoleDeletion(ctx context.Context, tenant, id uuid.UUID, version int64, actor string, deleted bool, audit RBACMutationAudit) (json.RawMessage, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	before, err := rbacRoleSnapshot(ctx, tx, tenant, id, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrResourceNotFound
	}
	if err != nil {
		return nil, err
	}
	if deleted {
		var used bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM user_roles ur JOIN users u ON u.id=ur.user_id WHERE ur.role_id=$1 AND u.tenant_id=$2 AND u.deleted_at IS NULL)", id, tenant).Scan(&used); err != nil {
			return nil, err
		}
		if used {
			return nil, ErrDependencyBlocked
		}
	}
	where := "deleted_at IS NOT NULL"
	set := "deleted_at=NULL,deleted_by=NULL"
	if deleted {
		where = "deleted_at IS NULL"
		set = "deleted_at=now(),deleted_by=$4"
	}
	args := []any{tenant, id, version}
	if deleted {
		args = append(args, actor)
	}
	var ignored uuid.UUID
	err = tx.QueryRow(ctx, "UPDATE roles SET "+set+" WHERE tenant_id=$1 AND id=$2 AND row_version=$3 AND "+where+" RETURNING id", args...).Scan(&ignored)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOptimisticLock
	}
	if err != nil {
		if isUnique(err) {
			return nil, ErrRestoreConflict
		}
		return nil, err
	}
	after, err := rbacRoleSnapshot(ctx, tx, tenant, id, true)
	if err != nil {
		return nil, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, before, after); err != nil {
			return nil, err
		}
	}
	return after, tx.Commit(ctx)
}

func (s RBACService) ready() error {
	if s.Store == nil || s.Store.Pool == nil {
		return fmt.Errorf("RBAC store is required")
	}
	return nil
}
func defaultStatus(s string) string {
	if strings.TrimSpace(s) == "" {
		return "ACTIVE"
	}
	return strings.TrimSpace(s)
}
func validStatus(s string) bool { return s == "ACTIVE" || s == "DISABLED" }
func validateUser(in UserInput) error {
	if strings.TrimSpace(in.Subject) == "" || strings.TrimSpace(in.DisplayName) == "" || !validStatus(defaultStatus(in.Status)) {
		return fmt.Errorf("subject, display_name and status are invalid")
	}
	return nil
}
func validateRole(in RoleInput) error {
	if strings.TrimSpace(in.RoleKey) == "" || strings.TrimSpace(in.DisplayName) == "" || !validStatus(defaultStatus(in.Status)) {
		return fmt.Errorf("role_key, display_name and status are invalid")
	}
	return nil
}
func validateUserPatch(p UserPatch) error {
	if p.Subject == nil && p.Email == nil && p.DisplayName == nil && p.Status == nil {
		return fmt.Errorf("at least one user field is required")
	}
	if p.Subject != nil && strings.TrimSpace(*p.Subject) == "" || p.DisplayName != nil && strings.TrimSpace(*p.DisplayName) == "" || p.Status != nil && !validStatus(strings.TrimSpace(*p.Status)) {
		return fmt.Errorf("user patch is invalid")
	}
	return nil
}
func validateRolePatch(p RolePatch) error {
	if p.RoleKey == nil && p.DisplayName == nil && p.Description == nil && p.Status == nil {
		return fmt.Errorf("at least one role field is required")
	}
	if p.RoleKey != nil && strings.TrimSpace(*p.RoleKey) == "" || p.DisplayName != nil && strings.TrimSpace(*p.DisplayName) == "" || p.Status != nil && !validStatus(strings.TrimSpace(*p.Status)) {
		return fmt.Errorf("role patch is invalid")
	}
	return nil
}
func value(p *string) string {
	if p == nil {
		return ""
	}
	return strings.TrimSpace(*p)
}
func uniqueUUIDs(ids []uuid.UUID) []uuid.UUID {
	set := map[uuid.UUID]struct{}{}
	for _, id := range ids {
		if id == uuid.Nil {
			continue
		}
		set[id] = struct{}{}
	}
	out := make([]uuid.UUID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}
func requireIDs(ctx context.Context, tx pgx.Tx, table, column string, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	var count int
	err := tx.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE "+column+"=ANY($1)", ids).Scan(&count)
	if err != nil {
		return err
	}
	if count != len(ids) {
		return fmt.Errorf("unknown identifier")
	}
	return nil
}
func requireTenantRoleIDs(ctx context.Context, tx pgx.Tx, tenant uuid.UUID, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	var count int
	err := tx.QueryRow(ctx, "SELECT count(*) FROM roles WHERE tenant_id=$1 AND id=ANY($2) AND deleted_at IS NULL", tenant, ids).Scan(&count)
	if err != nil {
		return err
	}
	if count != len(ids) {
		return fmt.Errorf("role is outside this tenant or deleted")
	}
	return nil
}
func bumpRBACParent(ctx context.Context, tx pgx.Tx, table string, tenant, id uuid.UUID, version int64) error {
	var ignored uuid.UUID
	err := tx.QueryRow(ctx, "UPDATE "+table+" SET status=status WHERE tenant_id=$1 AND id=$2 AND row_version=$3 AND deleted_at IS NULL RETURNING id", tenant, id, version).Scan(&ignored)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOptimisticLock
	}
	return err
}
func rbacUserSnapshot(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, tenant, id uuid.UUID, includeDeleted bool) (json.RawMessage, error) {
	var out json.RawMessage
	err := db.QueryRow(ctx, `SELECT ((to_jsonb(u) - 'row_version') || jsonb_build_object('version',u.row_version) || jsonb_build_object('role_ids',COALESCE((SELECT jsonb_agg(ur.role_id ORDER BY ur.role_id) FROM user_roles ur WHERE ur.user_id=u.id),'[]'::jsonb))) FROM users u WHERE u.tenant_id=$1 AND u.id=$2 AND ($3 OR u.deleted_at IS NULL)`, tenant, id, includeDeleted).Scan(&out)
	return out, err
}
func rbacRoleSnapshot(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, tenant, id uuid.UUID, includeDeleted bool) (json.RawMessage, error) {
	var out json.RawMessage
	err := db.QueryRow(ctx, `SELECT ((to_jsonb(ro) - 'row_version') || jsonb_build_object('version',ro.row_version) || jsonb_build_object('permission_ids',COALESCE((SELECT jsonb_agg(rp.permission_id ORDER BY rp.permission_id) FROM role_permissions rp WHERE rp.role_id=ro.id),'[]'::jsonb))) FROM roles ro WHERE ro.tenant_id=$1 AND ro.id=$2 AND ($3 OR ro.deleted_at IS NULL)`, tenant, id, includeDeleted).Scan(&out)
	return out, err
}
func isUnique(err error) bool {
	return strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "ux_")
}
