-- Queue publication is durable and retried with backoff. A permanently broken
-- backend/configuration must not leave its job run ENQUEUE_PENDING forever.
ALTER TABLE outbox_events
  ADD COLUMN IF NOT EXISTS expired_at timestamptz;

CREATE INDEX IF NOT EXISTS ix_outbox_claimable_live
  ON outbox_events(available_at,id)
  WHERE published_at IS NULL AND expired_at IS NULL;
