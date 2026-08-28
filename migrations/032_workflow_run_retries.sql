-- A workflow retry is a new immutable execution generation. Keeping the
-- source link prevents historical node/job evidence from being overwritten.
ALTER TABLE workflow_runs
  ADD COLUMN IF NOT EXISTS retry_of_run_id uuid REFERENCES workflow_runs(id) ON DELETE RESTRICT;

CREATE UNIQUE INDEX IF NOT EXISTS ux_workflow_runs_retry_source
  ON workflow_runs(retry_of_run_id) WHERE retry_of_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_workflow_runs_retry_of
  ON workflow_runs(retry_of_run_id) WHERE retry_of_run_id IS NOT NULL;
