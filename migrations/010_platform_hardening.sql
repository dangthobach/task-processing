-- Structured runtime logs are intentionally separate from application stdout.
-- They are range-partitioned so retention can drop old partitions without
-- blocking the worker hot path.
CREATE TABLE IF NOT EXISTS job_logs (
  id bigint GENERATED ALWAYS AS IDENTITY,
  project_id uuid NOT NULL REFERENCES projects(id),
  job_run_id uuid NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
  attempt_id uuid REFERENCES job_attempts(id) ON DELETE SET NULL,
  level text NOT NULL CHECK(level IN ('DEBUG','INFO','WARN','ERROR')),
  message text NOT NULL,
  fields jsonb NOT NULL DEFAULT '{}'::jsonb,
  trace_id text,
  created_at timestamptz NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);
CREATE TABLE IF NOT EXISTS job_logs_default PARTITION OF job_logs DEFAULT;
CREATE INDEX IF NOT EXISTS ix_job_logs_project_time ON job_logs(project_id,created_at DESC);
CREATE INDEX IF NOT EXISTS ix_job_logs_run_time ON job_logs(job_run_id,created_at DESC);
CREATE INDEX IF NOT EXISTS ix_job_runs_search ON job_runs(project_id,status,updated_at DESC);
CREATE INDEX IF NOT EXISTS ix_attempts_trace ON job_attempts(trace_id) WHERE trace_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS retention_policies (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES projects(id),
  resource_type text NOT NULL CHECK(resource_type IN ('JOB_LOG','SCHEDULER_LOG','REALTIME_EVENT','AUDIT_LOG','TERMINAL_RUN')),
  retention_days integer NOT NULL CHECK(retention_days BETWEEN 1 AND 3650),
  status text NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','DISABLED')),
  created_at timestamptz NOT NULL DEFAULT now(),
  row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0),
  deleted_at timestamptz,
  deleted_by text
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_retention_active_resource ON retention_policies(project_id,resource_type) WHERE deleted_at IS NULL;
DROP TRIGGER IF EXISTS trg_retention_policies_version ON retention_policies;
CREATE TRIGGER trg_retention_policies_version BEFORE UPDATE ON retention_policies FOR EACH ROW EXECUTE FUNCTION task_processing_bump_version();

-- Workflow DAG control plane. Nodes reference immutable job definitions;
-- execution state is append-only in workflow_runs/workflow_node_runs.
CREATE TABLE IF NOT EXISTS workflow_definitions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES projects(id),
  name text NOT NULL,
  status text NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','DISABLED')),
  row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  deleted_by text
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_workflow_active_name ON workflow_definitions(project_id,name) WHERE deleted_at IS NULL;
CREATE TABLE IF NOT EXISTS workflow_nodes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_id uuid NOT NULL REFERENCES workflow_definitions(id) ON DELETE CASCADE,
  node_key text NOT NULL,
  job_definition_id uuid NOT NULL REFERENCES job_definitions(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(workflow_id,node_key)
);
CREATE TABLE IF NOT EXISTS workflow_edges (
  workflow_id uuid NOT NULL REFERENCES workflow_definitions(id) ON DELETE CASCADE,
  from_node_id uuid NOT NULL REFERENCES workflow_nodes(id) ON DELETE CASCADE,
  to_node_id uuid NOT NULL REFERENCES workflow_nodes(id) ON DELETE CASCADE,
  PRIMARY KEY(workflow_id,from_node_id,to_node_id),
  CHECK(from_node_id <> to_node_id)
);
CREATE TABLE IF NOT EXISTS workflow_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES projects(id),
  workflow_id uuid NOT NULL REFERENCES workflow_definitions(id),
  status text NOT NULL CHECK(status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
  input jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);
CREATE TABLE IF NOT EXISTS workflow_node_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_run_id uuid NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  node_id uuid NOT NULL REFERENCES workflow_nodes(id),
  job_run_id uuid REFERENCES job_runs(id),
  status text NOT NULL CHECK(status IN ('PENDING','BLOCKED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  UNIQUE(workflow_run_id,node_id)
);
DROP TRIGGER IF EXISTS trg_workflow_definitions_version ON workflow_definitions;
CREATE TRIGGER trg_workflow_definitions_version BEFORE UPDATE ON workflow_definitions FOR EACH ROW EXECUTE FUNCTION task_processing_bump_version();
