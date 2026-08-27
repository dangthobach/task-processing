-- A batch is a leased execution aggregate. Its ownership must be fenced just
-- like an individual job run: a worker that loses the lease must never report
-- progress or finish items after another worker/recovery has taken over.
ALTER TABLE job_batches
  ADD COLUMN IF NOT EXISTS lease_token uuid;

CREATE INDEX IF NOT EXISTS ix_job_batches_recovery_lease
  ON job_batches(lease_expires_at,id)
  WHERE status IN ('RESERVED','RUNNING');
