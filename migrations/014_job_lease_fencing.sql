-- A worker incarnation must prove the exact lease it acquired. Owner strings
-- alone are vulnerable to ABA when an owner identity is reused.
ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS lease_token uuid;
CREATE INDEX IF NOT EXISTS ix_job_runs_active_lease
  ON job_runs(id,lease_token,lease_expires_at)
  WHERE status IN ('RESERVED','RUNNING');
