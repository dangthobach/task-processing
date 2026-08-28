-- Workflow runs are operational resources.  A cancellation is a versioned
-- command, so a stale UI or an automation cannot overwrite a terminal state.
ALTER TABLE workflow_runs
  ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1
  CHECK (row_version > 0);

DROP TRIGGER IF EXISTS trg_workflow_runs_version ON workflow_runs;
CREATE TRIGGER trg_workflow_runs_version
  BEFORE UPDATE ON workflow_runs
  FOR EACH ROW EXECUTE FUNCTION task_processing_bump_version();

CREATE INDEX IF NOT EXISTS ix_workflow_runs_project_workflow_created
  ON workflow_runs(project_id, workflow_id, created_at DESC);
