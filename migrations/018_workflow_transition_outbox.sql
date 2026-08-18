-- Workflow progress is durable: a completed job must never depend on a
-- best-effort worker call to unlock its downstream nodes.
CREATE TABLE IF NOT EXISTS workflow_dispatch_outbox (
  workflow_run_id uuid PRIMARY KEY REFERENCES workflow_runs(id) ON DELETE CASCADE,
  available_at timestamptz NOT NULL DEFAULT now(),
  attempts integer NOT NULL DEFAULT 0 CHECK(attempts >= 0),
  lease_owner text,
  lease_token uuid,
  lease_expires_at timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_workflow_dispatch_outbox_ready
  ON workflow_dispatch_outbox(available_at, created_at);
