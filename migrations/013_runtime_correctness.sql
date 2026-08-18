-- Runtime ownership is fenced: a stale dispatcher callback cannot acknowledge
-- a row claimed by a newer dispatcher.
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS claimed_by text;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS claim_token uuid;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS claim_expires_at timestamptz;
CREATE INDEX IF NOT EXISTS ix_outbox_claimable
  ON outbox_events(available_at,id)
  WHERE published_at IS NULL;

-- Function concurrency is shared by all job definitions referring to the
-- same function definition.
CREATE INDEX IF NOT EXISTS ix_job_runs_running_definition
  ON job_runs(job_definition_id)
  WHERE status = 'RUNNING';
