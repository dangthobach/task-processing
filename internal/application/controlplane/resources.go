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

var ErrResourceNotFound = errors.New("control-plane resource not found")
var ErrOptimisticLock = errors.New("control-plane optimistic lock lost")
var ErrRestoreConflict = errors.New("control-plane restore conflict")
var ErrDependencyBlocked = errors.New("control-plane dependency blocked")

type ResourceService struct{ Store *postgres.Store }
type ResourceMutationAudit func(context.Context, pgx.Tx, uuid.UUID, json.RawMessage, json.RawMessage) error

type resourceSpec struct{ table, scope string }

var resourceSpecs = map[string]resourceSpec{
	"queue":               {"queues", "t.project_id=$1"},
	"retry_policy":        {"retry_policies", "t.project_id=$1"},
	"rate_limit_policy":   {"rate_limit_policies", "t.project_id=$1"},
	"function_definition": {"function_definitions", "t.project_id=$1"},
	"job_definition":      {"job_definitions", "t.project_id=$1"},
	"schedule":            {"schedules", "EXISTS(SELECT 1 FROM job_definitions jd WHERE jd.id=t.job_definition_id AND jd.project_id=$1)"},
	"retention_policy":    {"retention_policies", "t.project_id=$1"},
}

func resourceSpecFor(resource string) (resourceSpec, error) {
	spec, ok := resourceSpecs[resource]
	if !ok {
		return resourceSpec{}, fmt.Errorf("unsupported control-plane resource")
	}
	return spec, nil
}
func resourceRow() string {
	return "((to_jsonb(t) - 'row_version') || jsonb_build_object('version', t.row_version))"
}

func (s ResourceService) List(ctx context.Context, resource string, project uuid.UUID, includeDeleted bool) ([]json.RawMessage, error) {
	spec, err := resourceSpecFor(resource)
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.Pool.Query(ctx, "SELECT "+resourceRow()+" FROM "+spec.table+" t WHERE "+spec.scope+" AND ($2 OR t.deleted_at IS NULL) ORDER BY t.created_at DESC,t.id DESC LIMIT 200", project, includeDeleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []json.RawMessage{}
	for rows.Next() {
		var item json.RawMessage
		if err = rows.Scan(&item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
func (s ResourceService) Snapshot(ctx context.Context, resource string, project, id uuid.UUID, includeDeleted bool) (json.RawMessage, error) {
	spec, err := resourceSpecFor(resource)
	if err != nil {
		return nil, err
	}
	return snapshotResource(ctx, s.Store.Pool, spec, project, id, includeDeleted)
}
func snapshotResource(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, spec resourceSpec, project, id uuid.UUID, includeDeleted bool) (json.RawMessage, error) {
	var out json.RawMessage
	err := db.QueryRow(ctx, "SELECT "+resourceRow()+" FROM "+spec.table+" t WHERE "+spec.scope+" AND t.id=$2 AND ($3 OR t.deleted_at IS NULL)", project, id, includeDeleted).Scan(&out)
	return out, err
}

func (s ResourceService) Update(ctx context.Context, resource string, project, id uuid.UUID, version int64, patch Patch, audit ResourceMutationAudit) (json.RawMessage, error) {
	spec, err := resourceSpecFor(resource)
	if err != nil {
		return nil, err
	}
	if err = ValidatePatch(resource, patch); err != nil {
		return nil, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	before, err := snapshotResource(ctx, tx, spec, project, id, false)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrResourceNotFound
	}
	if err != nil {
		return nil, err
	}
	if blocked, err := hasActiveDependents(ctx, tx, resource, project, id); err != nil {
		return nil, err
	} else if blocked {
		return nil, ErrDependencyBlocked
	}
	if err = s.validateUpdateTx(ctx, tx, resource, spec, project, id, before, patch); err != nil {
		return nil, err
	}
	fields := make([]string, 0, len(patch))
	for field := range patch {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	sets := make([]string, 0, len(fields))
	for _, field := range fields {
		sets = append(sets, field+"=p."+field)
	}
	payload, _ := json.Marshal(patch)
	var after json.RawMessage
	query := "UPDATE " + spec.table + " t SET " + strings.Join(sets, ",") + " FROM jsonb_populate_record(NULL::" + spec.table + ",$3::jsonb) AS p WHERE " + spec.scope + " AND t.id=$2 AND t.row_version=$4 AND t.deleted_at IS NULL" + patchDependencySQL(resource) + " RETURNING " + resourceRow()
	err = tx.QueryRow(ctx, query, project, id, payload, version).Scan(&after)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOptimisticLock
	}
	if err != nil {
		return nil, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, before, after); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return after, nil
}

func (s ResourceService) SoftDelete(ctx context.Context, resource string, project, id uuid.UUID, version int64, actor string, audit ResourceMutationAudit) (json.RawMessage, error) {
	spec, err := resourceSpecFor(resource)
	if err != nil {
		return nil, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	before, err := snapshotResource(ctx, tx, spec, project, id, false)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrResourceNotFound
	}
	if err != nil {
		return nil, err
	}
	var after json.RawMessage
	query := "UPDATE " + spec.table + " t SET deleted_at=now(),deleted_by=$3 WHERE " + spec.scope + " AND t.id=$2 AND t.row_version=$4 AND t.deleted_at IS NULL" + deleteDependencySQL(resource) + " RETURNING " + resourceRow()
	err = tx.QueryRow(ctx, query, project, id, actor, version).Scan(&after)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOptimisticLock
	}
	if err != nil {
		return nil, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, before, after); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return after, nil
}
func (s ResourceService) Restore(ctx context.Context, resource string, project, id uuid.UUID, version int64, audit ResourceMutationAudit) (json.RawMessage, error) {
	spec, err := resourceSpecFor(resource)
	if err != nil {
		return nil, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	before, err := snapshotResource(ctx, tx, spec, project, id, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrResourceNotFound
	}
	if err != nil {
		return nil, err
	}
	var after json.RawMessage
	query := "UPDATE " + spec.table + " t SET deleted_at=NULL,deleted_by=NULL WHERE " + spec.scope + " AND t.id=$2 AND t.row_version=$3 AND t.deleted_at IS NOT NULL" + restoreDependencySQL(resource) + " RETURNING " + resourceRow()
	err = tx.QueryRow(ctx, query, project, id, version).Scan(&after)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOptimisticLock
	}
	if err != nil {
		if strings.Contains(err.Error(), "ux_") || strings.Contains(err.Error(), "duplicate key") {
			return nil, ErrRestoreConflict
		}
		return nil, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, before, after); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return after, nil
}

func (s ResourceService) validateUpdateTx(ctx context.Context, tx pgx.Tx, resource string, spec resourceSpec, project, id uuid.UUID, before json.RawMessage, patch Patch) error {
	if resource == "function_definition" {
		if raw, ok := patch["input_schema"]; ok {
			if err := postgres.ValidateInputSchema(raw); err != nil {
				return err
			}
		}
	}
	var state map[string]json.RawMessage
	if err := json.Unmarshal(before, &state); err != nil {
		return err
	}
	for k, v := range patch {
		state[k] = v
	}
	if resource == "retry_policy" {
		var strategy string
		var initial, max int64
		var mult float64
		if json.Unmarshal(state["strategy"], &strategy) != nil || json.Unmarshal(state["initial_delay_ms"], &initial) != nil || json.Unmarshal(state["max_delay_ms"], &max) != nil || json.Unmarshal(state["multiplier"], &mult) != nil {
			return fmt.Errorf("retry policy state is invalid")
		}
		if err := ValidateRetryPolicyValues(strategy, initial, max, mult); err != nil {
			return err
		}
	}
	if resource == "schedule" {
		var kind, cron, tz string
		var seconds bool
		if json.Unmarshal(state["schedule_type"], &kind) != nil {
			return fmt.Errorf("schedule_type is invalid")
		}
		if kind == "CRON" {
			if json.Unmarshal(state["cron_expression"], &cron) != nil || json.Unmarshal(state["timezone"], &tz) != nil || json.Unmarshal(state["with_seconds"], &seconds) != nil {
				return fmt.Errorf("schedule state is invalid")
			}
			if err := ValidateScheduleExpression(cron, tz, seconds); err != nil {
				return err
			}
		}
	}
	if resource == "job_definition" {
		var mode string
		var queue uuid.UUID
		if json.Unmarshal(state["execution_mode"], &mode) != nil || json.Unmarshal(state["queue_id"], &queue) != nil {
			return fmt.Errorf("job definition state is invalid")
		}
		if mode == "BATCH" {
			var external bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM queues q JOIN queue_backends qb ON qb.id=q.backend_id WHERE q.id=$1 AND qb.backend_type <> 'POSTGRES' AND qb.status='ACTIVE' AND qb.deleted_at IS NULL)", queue).Scan(&external); err != nil {
				return err
			}
			if external {
				return fmt.Errorf("BATCH execution requires a PostgreSQL queue backend")
			}
		}
	}
	return validateDependenciesTx(ctx, tx, resource, project, id, patch)
}
func validateDependenciesTx(ctx context.Context, tx pgx.Tx, resource string, project, id uuid.UUID, patch Patch) error {
	if resource == "queue" {
		if raw, ok := patch["backend_id"]; ok && strings.TrimSpace(string(raw)) != "null" {
			var backend uuid.UUID
			if err := json.Unmarshal(raw, &backend); err != nil {
				return fmt.Errorf("backend_id must be a UUID or null")
			}
			var exists, incompatible bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM queue_backends WHERE id=$1 AND status='ACTIVE' AND deleted_at IS NULL)", backend).Scan(&exists); err != nil || !exists {
				return fmt.Errorf("queue backend is not active")
			}
			if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM job_definitions jd JOIN queue_backends qb ON qb.id=$1 WHERE jd.queue_id=$2 AND jd.deleted_at IS NULL AND jd.execution_mode='BATCH' AND qb.backend_type <> 'POSTGRES')", backend, id).Scan(&incompatible); err != nil {
				return err
			}
			if incompatible {
				return fmt.Errorf("queue has active BATCH job definitions and cannot use an external backend")
			}
		}
	}
	if resource == "job_definition" {
		if raw, ok := patch["retry_policy_id"]; ok && strings.TrimSpace(string(raw)) != "null" {
			var policy uuid.UUID
			if err := json.Unmarshal(raw, &policy); err != nil {
				return fmt.Errorf("retry_policy_id must be a UUID or null")
			}
			var exists bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM retry_policies WHERE id=$1 AND project_id=$2 AND deleted_at IS NULL)", policy, project).Scan(&exists); err != nil || !exists {
				return fmt.Errorf("retry policy is outside this project or deleted")
			}
		}
	}
	return nil
}
func patchDependencySQL(resource string) string {
	if resource == "queue" {
		return " AND (NOT ($3::jsonb ? 'backend_id') OR p.backend_id IS NULL OR EXISTS(SELECT 1 FROM queue_backends b WHERE b.id=p.backend_id AND b.status='ACTIVE' AND b.deleted_at IS NULL FOR UPDATE))"
	}
	if resource == "job_definition" {
		return " AND (NOT ($3::jsonb ? 'retry_policy_id') OR p.retry_policy_id IS NULL OR EXISTS(SELECT 1 FROM retry_policies rp WHERE rp.id=p.retry_policy_id AND rp.project_id=$1 AND rp.deleted_at IS NULL FOR UPDATE))"
	}
	return ""
}
func deleteDependencySQL(resource string) string {
	switch resource {
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

func hasActiveDependents(ctx context.Context, tx pgx.Tx, resource string, project, id uuid.UUID) (bool, error) {
	query := ""
	switch resource {
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
	if err := tx.QueryRow(ctx, query, project, id).Scan(&blocked); err != nil {
		return false, err
	}
	return blocked, nil
}
func restoreDependencySQL(resource string) string {
	switch resource {
	case "queue":
		return " AND (t.backend_id IS NULL OR EXISTS(SELECT 1 FROM queue_backends b WHERE b.id=t.backend_id AND b.status='ACTIVE' AND b.deleted_at IS NULL))"
	case "job_definition":
		return " AND EXISTS(SELECT 1 FROM function_definitions f WHERE f.id=t.function_id AND f.project_id=$1 AND f.deleted_at IS NULL) AND EXISTS(SELECT 1 FROM queues q WHERE q.id=t.queue_id AND q.project_id=$1 AND q.deleted_at IS NULL) AND (t.retry_policy_id IS NULL OR EXISTS(SELECT 1 FROM retry_policies rp WHERE rp.id=t.retry_policy_id AND rp.project_id=$1 AND rp.deleted_at IS NULL))"
	case "schedule":
		return " AND EXISTS(SELECT 1 FROM job_definitions jd WHERE jd.id=t.job_definition_id AND jd.project_id=$1 AND jd.deleted_at IS NULL)"
	}
	return ""
}
