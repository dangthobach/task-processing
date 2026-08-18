package postgres

import (
	"context"

	"github.com/google/uuid"
)

// EffectivePermissions resolves only active, non-expired grants. The query is
// deliberately tenant-scoped so a subject cannot inherit a role from another tenant.
func (s *Store) EffectivePermissions(ctx context.Context, tenant uuid.UUID, subject string) (map[string]bool, error) {
	rows, err := s.Pool.Query(ctx, "SELECT DISTINCT p.permission_key FROM users u JOIN user_roles ur ON ur.user_id=u.id JOIN roles r ON r.id=ur.role_id JOIN role_permissions rp ON rp.role_id=r.id JOIN permissions p ON p.id=rp.permission_id WHERE u.tenant_id=$1 AND u.subject=$2 AND u.status='ACTIVE' AND u.deleted_at IS NULL AND r.status='ACTIVE' AND r.deleted_at IS NULL AND (ur.expires_at IS NULL OR ur.expires_at>now())", tenant, subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]bool{}
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			return nil, err
		}
		result[key] = true
	}
	return result, rows.Err()
}
