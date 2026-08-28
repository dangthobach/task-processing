-- Queue backends are platform-scoped and dynamic RBAC is tenant-scoped; neither
-- can truthfully be attached to an arbitrary project audit record.
CREATE TABLE IF NOT EXISTS platform_audit_logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid REFERENCES tenants(id),
  actor_id text NOT NULL,
  action text NOT NULL,
  resource_type text NOT NULL,
  resource_id uuid NOT NULL,
  before_data jsonb,
  after_data jsonb,
  request_id text,
  trace_id text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_platform_audit_tenant_created
  ON platform_audit_logs(tenant_id,created_at DESC);
CREATE INDEX IF NOT EXISTS ix_platform_audit_resource
  ON platform_audit_logs(resource_type,resource_id,created_at DESC);
