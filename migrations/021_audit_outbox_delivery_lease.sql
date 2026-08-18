-- Audit delivery is at-least-once. A lease keeps slow or failed external sinks
-- from holding database transactions open, while bounded retry eventually
-- expires a poison event without blocking later audit records.
ALTER TABLE audit_outbox_events ADD COLUMN IF NOT EXISTS claim_token uuid;
ALTER TABLE audit_outbox_events ADD COLUMN IF NOT EXISTS claimed_by text;
ALTER TABLE audit_outbox_events ADD COLUMN IF NOT EXISTS claim_expires_at timestamptz;
ALTER TABLE audit_outbox_events ADD COLUMN IF NOT EXISTS expired_at timestamptz;
CREATE INDEX IF NOT EXISTS ix_audit_outbox_claimable
  ON audit_outbox_events(available_at,id)
  WHERE published_at IS NULL AND expired_at IS NULL;
