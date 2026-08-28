package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type RetentionPreview struct {
	PolicyID      uuid.UUID `json:"policy_id"`
	ResourceType  string    `json:"resource_type"`
	RetentionDays int       `json:"retention_days"`
	Candidates    int       `json:"candidates"`
	Capped        bool      `json:"capped"`
}
type RetentionRun struct {
	ID           uuid.UUID `json:"id"`
	DeletedCount int       `json:"deleted_count"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
}

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

func (s *Store) PreviewRetention(ctx context.Context, project, policy uuid.UUID, limit int) (RetentionPreview, error) {
	if limit < 1 || limit > 10000 {
		limit = 1000
	}
	var out RetentionPreview
	if err := s.Pool.QueryRow(ctx, "SELECT id,resource_type,retention_days FROM retention_policies WHERE id=$1 AND project_id=$2 AND status='ACTIVE' AND deleted_at IS NULL", policy, project).Scan(&out.PolicyID, &out.ResourceType, &out.RetentionDays); err != nil {
		return out, err
	}
	query, err := retentionCandidateQuery(out.ResourceType)
	if err != nil {
		return out, err
	}
	var count int
	if err = s.Pool.QueryRow(ctx, "SELECT count(*) FROM ("+query+") candidates", project, out.RetentionDays, limit+1).Scan(&count); err != nil {
		return out, err
	}
	out.Capped = count > limit
	if out.Capped {
		count = limit
	}
	out.Candidates = count
	return out, nil
}
func (s *Store) ExecuteRetention(ctx context.Context, project, policy uuid.UUID, limit int, actor, requestID, traceID string) (RetentionRun, error) {
	if limit < 1 || limit > 10000 {
		limit = 1000
	}
	if actor == "" {
		actor = "system:retention"
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return RetentionRun{}, err
	}
	defer tx.Rollback(ctx)
	var resource string
	var days int
	if err = tx.QueryRow(ctx, "SELECT resource_type,retention_days FROM retention_policies WHERE id=$1 AND project_id=$2 AND status='ACTIVE' AND deleted_at IS NULL FOR UPDATE", policy, project).Scan(&resource, &days); err != nil {
		return RetentionRun{}, err
	}
	query, err := retentionDeleteQuery(resource)
	if err != nil {
		return RetentionRun{}, err
	}
	var out RetentionRun
	if err = tx.QueryRow(ctx, "INSERT INTO retention_runs(project_id,retention_policy_id,source,actor_id,status,requested_limit,request_id,trace_id) VALUES($1,$2,'API',$3,'SUCCEEDED',$4,NULLIF($5,''),NULLIF($6,'')) RETURNING id,started_at,finished_at", project, policy, actor, limit, requestID, traceID).Scan(&out.ID, &out.StartedAt, &out.FinishedAt); err != nil {
		return out, err
	}
	tag, err := tx.Exec(ctx, query, project, days, limit)
	if err != nil {
		return out, err
	}
	out.DeletedCount = int(tag.RowsAffected())
	if _, err = tx.Exec(ctx, "UPDATE retention_runs SET deleted_count=$2,finished_at=now() WHERE id=$1", out.ID, out.DeletedCount); err != nil {
		return out, err
	}
	if err = tx.QueryRow(ctx, "SELECT finished_at FROM retention_runs WHERE id=$1", out.ID).Scan(&out.FinishedAt); err != nil {
		return out, err
	}
	return out, tx.Commit(ctx)
}
func (s *Store) RetentionRuns(ctx context.Context, project uuid.UUID) ([]RetentionRun, error) {
	rows, err := s.Pool.Query(ctx, "SELECT id,deleted_count,started_at,finished_at FROM retention_runs WHERE project_id=$1 ORDER BY started_at DESC LIMIT 100", project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RetentionRun{}
	for rows.Next() {
		var r RetentionRun
		if err = rows.Scan(&r.ID, &r.DeletedCount, &r.StartedAt, &r.FinishedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func retentionCandidateQuery(resource string) (string, error) {
	switch resource {
	case "JOB_LOG":
		return `SELECT ctid FROM job_logs WHERE project_id=$1 AND created_at < now()-($2 * interval '1 day') LIMIT $3`, nil
	case "SCHEDULER_LOG":
		return `SELECT ctid FROM scheduler_logs WHERE project_id=$1 AND occurred_at < now()-($2 * interval '1 day') LIMIT $3`, nil
	case "REALTIME_EVENT":
		return `SELECT ctid FROM realtime_events WHERE project_id=$1 AND created_at < now()-($2 * interval '1 day') LIMIT $3`, nil
	case "TERMINAL_RUN":
		return `SELECT r.ctid FROM job_runs r WHERE r.project_id=$1 AND r.status IN ('SUCCEEDED','DEAD_LETTER','CANCELLED') AND r.finished_at < now()-($2 * interval '1 day') AND NOT EXISTS(SELECT 1 FROM workflow_node_runs nr WHERE nr.job_run_id=r.id) LIMIT $3`, nil
	case "AUDIT_LOG":
		return "", fmt.Errorf("AUDIT_LOG retention is blocked while audit outbox evidence references the log")
	}
	return "", fmt.Errorf("unsupported retention resource %q", resource)
}
func retentionDeleteQuery(resource string) (string, error) {
	switch resource {
	case "JOB_LOG":
		return `DELETE FROM job_logs WHERE ctid IN (SELECT ctid FROM job_logs WHERE project_id=$1 AND created_at < now()-($2 * interval '1 day') LIMIT $3)`, nil
	case "SCHEDULER_LOG":
		return `DELETE FROM scheduler_logs WHERE ctid IN (SELECT ctid FROM scheduler_logs WHERE project_id=$1 AND occurred_at < now()-($2 * interval '1 day') LIMIT $3)`, nil
	case "REALTIME_EVENT":
		return `DELETE FROM realtime_events WHERE ctid IN (SELECT ctid FROM realtime_events WHERE project_id=$1 AND created_at < now()-($2 * interval '1 day') LIMIT $3)`, nil
	case "TERMINAL_RUN":
		return `DELETE FROM job_runs WHERE ctid IN (SELECT r.ctid FROM job_runs r WHERE r.project_id=$1 AND r.status IN ('SUCCEEDED','DEAD_LETTER','CANCELLED') AND r.finished_at < now()-($2 * interval '1 day') AND NOT EXISTS(SELECT 1 FROM workflow_node_runs nr WHERE nr.job_run_id=r.id) LIMIT $3)`, nil
	case "AUDIT_LOG":
		return "", fmt.Errorf("AUDIT_LOG retention is blocked while audit outbox evidence references the log")
	}
	return "", fmt.Errorf("unsupported retention resource %q", resource)
}
