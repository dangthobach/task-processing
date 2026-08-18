ALTER TABLE rate_limit_policies ADD COLUMN IF NOT EXISTS enforcement_point text NOT NULL DEFAULT 'WORKER_START'
  CHECK(enforcement_point IN ('SUBMISSION','WORKER_START'));
CREATE INDEX IF NOT EXISTS ix_rate_limit_active_admission
  ON rate_limit_policies(project_id,enforcement_point,scope,target_id)
  WHERE deleted_at IS NULL AND status='ACTIVE';
