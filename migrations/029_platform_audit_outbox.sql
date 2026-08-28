-- Platform/tenant aggregates have no project_id, therefore they need an
-- independent external-audit hand-off instead of abusing audit_outbox_events.
CREATE TABLE IF NOT EXISTS platform_audit_outbox_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  platform_audit_log_id uuid NOT NULL UNIQUE REFERENCES platform_audit_logs(id),
  tenant_id uuid REFERENCES tenants(id),
  event_type text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id uuid NOT NULL,
  payload jsonb NOT NULL,
  available_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  last_error text,
  claimed_by text,
  claim_token uuid,
  claim_expires_at timestamptz,
  expired_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_platform_audit_outbox_claimable
  ON platform_audit_outbox_events(available_at,id)
  WHERE published_at IS NULL AND expired_at IS NULL;
