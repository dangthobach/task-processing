package migration

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// baselineRequirement is intentionally conservative. A database is only
// baselined when it proves it already has the schema shape claimed by its
// operator. This prevents a partially migrated database from being marked as
// healthy and permanently skipping the missing migrations.
type baselineRequirement struct {
	introduced int64
	table      string
	column     string
	index      string
}

var baselineRequirements = []baselineRequirement{
	{1, "tenants", "id", ""}, {1, "projects", "tenant_id", ""},
	{1, "queues", "max_concurrency", ""}, {1, "job_definitions", "function_id", ""},
	{1, "schedules", "run_at", ""}, {1, "job_runs", "policy_snapshot", ""},
	{2, "scheduler_logs", "occurred_at", ""},
	{3, "function_definitions", "max_concurrency", ""},
	{5, "job_batches", "status", ""}, {5, "batch_items", "job_run_id", ""},
	{6, "queues", "row_version", ""}, {6, "job_runs", "row_version", ""},
	{7, "audit_outbox_events", "aggregate_id", ""}, {7, "queues", "deleted_at", ""},
	{9, "", "", "ux_queues_active_name"}, {9, "", "", "ux_retry_policies_active_name"},
	{10, "job_logs", "created_at", ""}, {10, "job_logs_default", "", ""},
	{10, "workflow_definitions", "status", ""}, {10, "workflow_node_runs", "status", ""},
	{11, "users", "subject", ""}, {11, "roles", "role_key", ""}, {11, "permissions", "permission_key", ""},
	{12, "rate_limit_policies", "scope", ""}, {12, "rate_limit_buckets", "tokens", ""},
	{12, "", "", "ux_rate_limit_active_name"},
}

// VerifyBaseline validates the minimum relational fingerprint for an existing
// schema before migration versions are recorded without execution.
func VerifyBaseline(ctx context.Context, conn *pgxpool.Conn, through int64) error {
	if through <= 0 {
		return fmt.Errorf("unsafe baseline version %d", through)
	}
	for _, requirement := range baselineRequirements {
		if requirement.introduced > through {
			continue
		}
		if requirement.table != "" {
			var exists bool
			if err := conn.QueryRow(ctx, `SELECT EXISTS(
				SELECT 1 FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
				WHERE n.nspname=current_schema() AND c.relname=$1 AND c.relkind IN ('r','p')
			)`, requirement.table).Scan(&exists); err != nil {
				return fmt.Errorf("inspect baseline table %s: %w", requirement.table, err)
			}
			if !exists {
				return fmt.Errorf("unsafe baseline through %d: required table %q is missing", through, requirement.table)
			}
		}
		if requirement.column != "" {
			var exists bool
			if err := conn.QueryRow(ctx, `SELECT EXISTS(
				SELECT 1 FROM information_schema.columns
				WHERE table_schema=current_schema() AND table_name=$1 AND column_name=$2
			)`, requirement.table, requirement.column).Scan(&exists); err != nil {
				return fmt.Errorf("inspect baseline column %s.%s: %w", requirement.table, requirement.column, err)
			}
			if !exists {
				return fmt.Errorf("unsafe baseline through %d: required column %q.%q is missing", through, requirement.table, requirement.column)
			}
		}
		if requirement.index != "" {
			var exists bool
			if err := conn.QueryRow(ctx, `SELECT EXISTS(
				SELECT 1 FROM pg_catalog.pg_class c
				JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
				WHERE n.nspname=current_schema() AND c.relname=$1 AND c.relkind IN ('i','r','p')
			)`, requirement.index).Scan(&exists); err != nil {
				return fmt.Errorf("inspect baseline index %s: %w", requirement.index, err)
			}
			if !exists {
				return fmt.Errorf("unsafe baseline through %d: required index %q is missing", through, requirement.index)
			}
		}
	}
	return nil
}

// baselineConnection is compile-time guarded to document the session-scoped
// advisory-lock requirement: verification and all migration transactions use
// the acquired pool connection rather than a new pool session.
var _ interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
} = (*pgxpool.Conn)(nil)
