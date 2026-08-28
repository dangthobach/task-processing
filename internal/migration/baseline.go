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
	constraint string
	trigger    string
}

var baselineRequirements = []baselineRequirement{
	{introduced: 1, table: "tenants", column: "id"}, {introduced: 1, table: "projects", column: "tenant_id"},
	{introduced: 1, table: "queues", column: "max_concurrency"}, {introduced: 1, table: "job_definitions", column: "function_id"},
	{introduced: 1, table: "schedules", column: "run_at"}, {introduced: 1, table: "job_runs", column: "policy_snapshot"},
	{introduced: 2, table: "scheduler_logs", column: "occurred_at"},
	{introduced: 3, table: "function_definitions", column: "max_concurrency"},
	{introduced: 5, table: "job_batches", column: "status"}, {introduced: 5, table: "job_batch_items", column: "job_run_id"},
	{introduced: 6, table: "queues", column: "row_version"}, {introduced: 6, table: "job_runs", column: "row_version"},
	{introduced: 7, table: "audit_outbox_events", column: "aggregate_id"}, {introduced: 7, table: "queues", column: "deleted_at"},
	{introduced: 9, index: "ux_queues_active_name"}, {introduced: 9, index: "ux_retry_policies_active_name"},
	{introduced: 10, table: "job_logs", column: "created_at"}, {introduced: 10, table: "job_logs_default"},
	{introduced: 10, table: "workflow_definitions", column: "status"}, {introduced: 10, table: "workflow_node_runs", column: "status"},
	{introduced: 11, table: "users", column: "subject"}, {introduced: 11, table: "roles", column: "role_key"}, {introduced: 11, table: "permissions", column: "permission_key"},
	{introduced: 12, table: "rate_limit_policies", column: "scope"}, {introduced: 12, table: "rate_limit_buckets", column: "tokens"},
	{introduced: 12, index: "ux_rate_limit_active_name"},
	{introduced: 21, table: "audit_outbox_events", column: "claim_token"},
	{introduced: 22, table: "job_runs", column: "payload_rewrapped_at"},
	{introduced: 23, table: "job_runs", column: "traceparent"}, {introduced: 23, table: "job_runs", column: "tracestate"},
	{introduced: 23, table: "job_runs", constraint: "ck_job_runs_traceparent_length"},
	{introduced: 23, table: "job_runs", constraint: "ck_job_runs_tracestate_length"},
	{introduced: 24, table: "worker_function_capabilities", column: "function_version"},
	{introduced: 24, index: "ix_worker_capabilities_lookup"},
	{introduced: 24, table: "job_definitions", trigger: "trg_job_definitions_require_worker_capability"},
	{introduced: 25, table: "job_batches", column: "lease_token"}, {introduced: 25, index: "ix_job_batches_recovery_lease"},
	{introduced: 26, table: "outbox_events", column: "expired_at"}, {introduced: 26, index: "ix_outbox_claimable_live"},
	{introduced: 27, table: "platform_audit_logs", column: "resource_type"}, {introduced: 27, index: "ix_platform_audit_tenant_created"},
	{introduced: 28, index: "ux_queue_backends_active_name"},
	{introduced: 29, table: "platform_audit_outbox_events", column: "claim_token"}, {introduced: 29, index: "ix_platform_audit_outbox_claimable"},
	{introduced: 30, table: "retention_runs", column: "deleted_count"}, {introduced: 30, index: "ix_retention_runs_project_created"},
	{introduced: 31, table: "workflow_runs", column: "row_version"}, {introduced: 31, index: "ix_workflow_runs_project_workflow_created"},
	{introduced: 31, table: "workflow_runs", trigger: "trg_workflow_runs_version"},
	{introduced: 32, table: "workflow_runs", column: "retry_of_run_id"}, {introduced: 32, index: "ux_workflow_runs_retry_source"},
	{introduced: 33, table: "workflow_definitions", column: "failure_policy"}, {introduced: 33, table: "workflow_runs", column: "failure_policy_snapshot"},
	{introduced: 33, table: "workflow_run_edges", column: "condition_type"}, {introduced: 33, table: "workflow_node_runs", constraint: "workflow_node_runs_status_check"},
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
		if requirement.constraint != "" {
			var exists bool
			if err := conn.QueryRow(ctx, `SELECT EXISTS(
				SELECT 1 FROM pg_catalog.pg_constraint c
				JOIN pg_catalog.pg_class r ON r.oid=c.conrelid
				JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
				WHERE n.nspname=current_schema() AND r.relname=$1 AND c.conname=$2
			)`, requirement.table, requirement.constraint).Scan(&exists); err != nil {
				return fmt.Errorf("inspect baseline constraint %s.%s: %w", requirement.table, requirement.constraint, err)
			}
			if !exists {
				return fmt.Errorf("unsafe baseline through %d: required constraint %q.%q is missing", through, requirement.table, requirement.constraint)
			}
		}
		if requirement.trigger != "" {
			var exists bool
			if err := conn.QueryRow(ctx, `SELECT EXISTS(
				SELECT 1 FROM pg_catalog.pg_trigger t
				JOIN pg_catalog.pg_class r ON r.oid=t.tgrelid
				JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
				WHERE n.nspname=current_schema() AND r.relname=$1 AND t.tgname=$2 AND NOT t.tgisinternal
			)`, requirement.table, requirement.trigger).Scan(&exists); err != nil {
				return fmt.Errorf("inspect baseline trigger %s.%s: %w", requirement.table, requirement.trigger, err)
			}
			if !exists {
				return fmt.Errorf("unsafe baseline through %d: required trigger %q.%q is missing", through, requirement.table, requirement.trigger)
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
