CREATE TABLE IF NOT EXISTS rate_limit_policies (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES projects(id),
  name text NOT NULL,
  scope text NOT NULL CHECK(scope IN ('PROJECT','QUEUE','FUNCTION')),
  target_id uuid,
  capacity integer NOT NULL CHECK(capacity > 0),
  refill_tokens integer NOT NULL CHECK(refill_tokens > 0),
  refill_period_ms bigint NOT NULL CHECK(refill_period_ms BETWEEN 1 AND 86400000),
  status text NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','DISABLED')),
  row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  deleted_by text,
  CHECK((scope='PROJECT' AND target_id IS NULL) OR (scope IN ('QUEUE','FUNCTION') AND target_id IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_rate_limit_active_name ON rate_limit_policies(project_id,name) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_rate_limit_active_scope ON rate_limit_policies(project_id,scope,target_id) WHERE deleted_at IS NULL AND status='ACTIVE';
CREATE TABLE IF NOT EXISTS rate_limit_buckets (
  policy_id uuid PRIMARY KEY REFERENCES rate_limit_policies(id) ON DELETE CASCADE,
  tokens numeric NOT NULL,
  last_refilled_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
DROP TRIGGER IF EXISTS trg_rate_limit_policies_version ON rate_limit_policies;
CREATE TRIGGER trg_rate_limit_policies_version BEFORE UPDATE ON rate_limit_policies FOR EACH ROW EXECUTE FUNCTION task_processing_bump_version();

-- BRD vocabulary aliases. Existing workflow_nodes/workflow_node_runs are
-- retained as physical tables for compatibility; views expose step terminology.
CREATE OR REPLACE VIEW workflow_steps AS SELECT id,workflow_id,node_key AS step_key,job_definition_id,created_at FROM workflow_nodes;
CREATE OR REPLACE VIEW workflow_step_runs AS SELECT id,workflow_run_id,node_id AS step_id,job_run_id,status,created_at,finished_at FROM workflow_node_runs;
CREATE INDEX IF NOT EXISTS ix_workflow_runs_project_created ON workflow_runs(project_id,created_at DESC);
CREATE INDEX IF NOT EXISTS ix_workflow_node_runs_status ON workflow_node_runs(workflow_run_id,status);
