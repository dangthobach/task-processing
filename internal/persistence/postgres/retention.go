package postgres

import (
	"context"
	"fmt"
)

// ApplyRetention deletes bounded chunks, never a whole history table in one
// transaction. Partition pruning still applies to job_logs; this is the safe
// fallback for default/legacy partitions and the other retained resources.
func (s *Store) ApplyRetention(ctx context.Context, chunk int) (int64, error) {
	if chunk <= 0 || chunk > 10000 {
		chunk = 1000
	}
	rows, err := s.Pool.Query(ctx, `SELECT project_id,resource_type,retention_days FROM retention_policies WHERE status='ACTIVE' AND deleted_at IS NULL ORDER BY project_id,resource_type`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var total int64
	for rows.Next() {
		var project string
		var resource string
		var days int
		if err = rows.Scan(&project, &resource, &days); err != nil {
			return total, err
		}
		var query string
		switch resource {
		case "JOB_LOG":
			query = `DELETE FROM job_logs WHERE ctid IN (SELECT ctid FROM job_logs WHERE project_id=$1 AND created_at < now()-($2 * interval '1 day') LIMIT $3)`
		case "SCHEDULER_LOG":
			query = `DELETE FROM scheduler_logs WHERE ctid IN (SELECT ctid FROM scheduler_logs WHERE project_id=$1 AND occurred_at < now()-($2 * interval '1 day') LIMIT $3)`
		case "REALTIME_EVENT":
			query = `DELETE FROM realtime_events WHERE ctid IN (SELECT ctid FROM realtime_events WHERE project_id=$1 AND created_at < now()-($2 * interval '1 day') LIMIT $3)`
		case "AUDIT_LOG":
			query = `DELETE FROM audit_logs WHERE ctid IN (SELECT ctid FROM audit_logs WHERE project_id=$1 AND created_at < now()-($2 * interval '1 day') LIMIT $3)`
		case "TERMINAL_RUN":
			query = `DELETE FROM job_runs WHERE ctid IN (SELECT r.ctid FROM job_runs r WHERE r.project_id=$1 AND r.status IN ('SUCCEEDED','DEAD_LETTER','CANCELLED') AND r.finished_at < now()-($2 * interval '1 day') AND NOT EXISTS(SELECT 1 FROM workflow_node_runs nr WHERE nr.job_run_id=r.id) LIMIT $3)`
		default:
			return total, fmt.Errorf("unsupported retention resource %q", resource)
		}
		tag, execErr := s.Pool.Exec(ctx, query, project, days, chunk)
		if execErr != nil {
			return total, execErr
		}
		total += tag.RowsAffected()
	}
	return total, rows.Err()
}
