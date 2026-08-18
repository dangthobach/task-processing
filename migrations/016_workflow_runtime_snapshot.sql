-- Runtime workflow data is immutable by value. Definition nodes/edges remain
-- editable control-plane records, while a run uses only these snapshots.
ALTER TABLE workflow_node_runs ADD COLUMN IF NOT EXISTS node_key text;
ALTER TABLE workflow_node_runs ADD COLUMN IF NOT EXISTS job_definition_id uuid;
ALTER TABLE workflow_node_runs ADD COLUMN IF NOT EXISTS execution_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE TABLE IF NOT EXISTS workflow_run_edges (
  workflow_run_id uuid NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
  from_node_run_id uuid NOT NULL REFERENCES workflow_node_runs(id) ON DELETE CASCADE,
  to_node_run_id uuid NOT NULL REFERENCES workflow_node_runs(id) ON DELETE CASCADE,
  PRIMARY KEY(workflow_run_id,from_node_run_id,to_node_run_id),
  CHECK(from_node_run_id <> to_node_run_id)
);
CREATE INDEX IF NOT EXISTS ix_workflow_run_edges_to ON workflow_run_edges(workflow_run_id,to_node_run_id);

-- Upgrade pre-existing runtime rows using the definition values visible at the
-- migration boundary; subsequent definition edits cannot alter those runs.
UPDATE workflow_node_runs nr
SET node_key=n.node_key,
    job_definition_id=jd.id,
    execution_snapshot=jsonb_build_object(
      'definition_id',jd.id,'queue_id',jd.queue_id,'function_key',fd.function_key,'function_version',fd.version,'priority',jd.default_priority,
      'policy',jsonb_build_object('retry',jsonb_build_object('max_attempts',COALESCE(rp.max_attempts,1),'strategy',COALESCE(rp.strategy,'FIXED'),'initial_delay_ms',COALESCE(rp.initial_delay_ms,0),'multiplier',COALESCE(rp.multiplier,1),'max_delay_ms',COALESCE(rp.max_delay_ms,0),'jitter_pct',COALESCE(rp.jitter_pct,0),'retry_timeout',COALESCE(rp.retry_timeout,false),'retry_rate_limited',COALESCE(rp.retry_rate_limited,false),'retry_dependency_error',COALESCE(rp.retry_dependency_error,false),'retry_validation_error',COALESCE(rp.retry_validation_error,false)),'timeout_ms',jd.timeout_ms)
    )
FROM workflow_nodes n JOIN job_definitions jd ON jd.id=n.job_definition_id
JOIN function_definitions fd ON fd.id=jd.function_id LEFT JOIN retry_policies rp ON rp.id=jd.retry_policy_id
WHERE nr.node_id=n.id AND (nr.job_definition_id IS NULL OR nr.execution_snapshot='{}'::jsonb);

INSERT INTO workflow_run_edges(workflow_run_id,from_node_run_id,to_node_run_id)
SELECT wr.id,from_run.id,to_run.id FROM workflow_runs wr
JOIN workflow_edges e ON e.workflow_id=wr.workflow_id
JOIN workflow_node_runs from_run ON from_run.workflow_run_id=wr.id AND from_run.node_id=e.from_node_id
JOIN workflow_node_runs to_run ON to_run.workflow_run_id=wr.id AND to_run.node_id=e.to_node_id
ON CONFLICT DO NOTHING;
