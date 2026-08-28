-- Workflow 6C. Conditions and failure policy are copied into each run so an
-- edit to a definition cannot change an execution already in progress.
ALTER TABLE workflow_definitions
  ADD COLUMN IF NOT EXISTS failure_policy text NOT NULL DEFAULT 'FAIL_FAST';
ALTER TABLE workflow_definitions
  ADD CONSTRAINT ck_workflow_definitions_failure_policy
  CHECK (failure_policy IN ('FAIL_FAST','CONTINUE','MANUAL_INTERVENTION'));

ALTER TABLE workflow_edges
  ADD COLUMN IF NOT EXISTS condition_type text NOT NULL DEFAULT 'ON_SUCCESS';
ALTER TABLE workflow_edges
  ADD CONSTRAINT ck_workflow_edges_condition_type
  CHECK (condition_type IN ('ON_SUCCESS','ON_FAILURE','ALWAYS'));

ALTER TABLE workflow_runs
  ADD COLUMN IF NOT EXISTS failure_policy_snapshot text NOT NULL DEFAULT 'FAIL_FAST';
ALTER TABLE workflow_runs
  ADD CONSTRAINT ck_workflow_runs_failure_policy_snapshot
  CHECK (failure_policy_snapshot IN ('FAIL_FAST','CONTINUE','MANUAL_INTERVENTION'));

ALTER TABLE workflow_run_edges
  ADD COLUMN IF NOT EXISTS condition_type text NOT NULL DEFAULT 'ON_SUCCESS';
ALTER TABLE workflow_run_edges
  ADD CONSTRAINT ck_workflow_run_edges_condition_type
  CHECK (condition_type IN ('ON_SUCCESS','ON_FAILURE','ALWAYS'));

ALTER TABLE workflow_node_runs DROP CONSTRAINT IF EXISTS workflow_node_runs_status_check;
ALTER TABLE workflow_node_runs
  ADD CONSTRAINT workflow_node_runs_status_check
  CHECK (status IN ('PENDING','BLOCKED','RUNNING','SUCCEEDED','FAILED','CANCELLED','SKIPPED'));

ALTER TABLE workflow_runs DROP CONSTRAINT IF EXISTS workflow_runs_status_check;
ALTER TABLE workflow_runs
  ADD CONSTRAINT workflow_runs_status_check
  CHECK (status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED','AWAITING_INTERVENTION'));
