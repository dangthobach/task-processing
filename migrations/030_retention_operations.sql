CREATE TABLE IF NOT EXISTS retention_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES projects(id),
  retention_policy_id uuid REFERENCES retention_policies(id),
  source text NOT NULL CHECK(source IN ('WORKER','API')),
  actor_id text NOT NULL,
  status text NOT NULL CHECK(status IN ('SUCCEEDED','FAILED')),
  requested_limit integer NOT NULL CHECK(requested_limit BETWEEN 1 AND 10000),
  deleted_count integer NOT NULL DEFAULT 0 CHECK(deleted_count >= 0),
  error_message text,
  request_id text,
  trace_id text,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_retention_runs_project_created ON retention_runs(project_id,started_at DESC);
