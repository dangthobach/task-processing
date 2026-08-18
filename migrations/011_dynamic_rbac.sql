CREATE TABLE IF NOT EXISTS users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  subject text NOT NULL,
  email text,
  display_name text NOT NULL,
  status text NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','DISABLED')),
  row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  deleted_by text
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_users_active_subject ON users(tenant_id,subject) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS permissions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  permission_key text NOT NULL,
  description text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(permission_key)
);
CREATE TABLE IF NOT EXISTS roles (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id uuid NOT NULL REFERENCES tenants(id),
  role_key text NOT NULL,
  display_name text NOT NULL,
  description text,
  status text NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','DISABLED')),
  row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz,
  deleted_by text
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_roles_active_key ON roles(tenant_id,role_key) WHERE deleted_at IS NULL;
CREATE TABLE IF NOT EXISTS user_roles (
  user_id uuid NOT NULL REFERENCES users(id),
  role_id uuid NOT NULL REFERENCES roles(id),
  expires_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(user_id,role_id)
);
CREATE TABLE IF NOT EXISTS role_permissions (
  role_id uuid NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
  permission_id uuid NOT NULL REFERENCES permissions(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(role_id,permission_id)
);

INSERT INTO permissions(permission_key,description) VALUES
  ('platform:admin','Unrestricted tenant administration'),
  ('control:read','Read queues, functions, definitions and schedules'),
  ('control:write','Mutate control-plane configuration'),
  ('job:read','Read runs, attempts and job logs'),
  ('job:submit','Submit task runs and batches'),
  ('job:operate','Cancel or retry task runs'),
  ('batch:read','Read batch execution state'),
  ('operations:read','Read workers, scheduler state and DLQ'),
  ('operations:write','Replay DLQ and execute operational actions'),
  ('audit:read','Read audit trails'),
  ('event:read','Read realtime event streams')
ON CONFLICT(permission_key) DO NOTHING;

DROP TRIGGER IF EXISTS trg_users_version ON users;
CREATE TRIGGER trg_users_version BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION task_processing_bump_version();
DROP TRIGGER IF EXISTS trg_roles_version ON roles;
CREATE TRIGGER trg_roles_version BEFORE UPDATE ON roles FOR EACH ROW EXECUTE FUNCTION task_processing_bump_version();
