CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS tenants (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text NOT NULL, status text NOT NULL DEFAULT 'ACTIVE', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS projects (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id uuid NOT NULL REFERENCES tenants(id), name text NOT NULL, key text NOT NULL, status text NOT NULL DEFAULT 'ACTIVE', created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id, key)
);
CREATE TABLE IF NOT EXISTS queue_backends (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text NOT NULL UNIQUE, backend_type text NOT NULL CHECK (backend_type IN ('POSTGRES','REDIS_STREAMS','JETSTREAM')), encrypted_config bytea, capabilities jsonb NOT NULL DEFAULT '{}'::jsonb, status text NOT NULL DEFAULT 'ACTIVE', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS retry_policies (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), project_id uuid NOT NULL REFERENCES projects(id), name text NOT NULL, max_attempts integer NOT NULL CHECK(max_attempts > 0), strategy text NOT NULL CHECK(strategy IN ('FIXED','EXPONENTIAL')), initial_delay_ms bigint NOT NULL CHECK(initial_delay_ms >= 0), multiplier numeric NOT NULL DEFAULT 2, max_delay_ms bigint NOT NULL CHECK(max_delay_ms >= 0), jitter_pct numeric NOT NULL DEFAULT 0 CHECK(jitter_pct BETWEEN 0 AND 100), retry_timeout boolean NOT NULL DEFAULT true, retry_rate_limited boolean NOT NULL DEFAULT true, retry_dependency_error boolean NOT NULL DEFAULT true, retry_validation_error boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(project_id,name)
);
CREATE TABLE IF NOT EXISTS queues (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), project_id uuid NOT NULL REFERENCES projects(id), backend_id uuid REFERENCES queue_backends(id), name text NOT NULL, status text NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','PAUSED','DRAINING','DISABLED')), default_priority smallint NOT NULL DEFAULT 3 CHECK(default_priority BETWEEN 1 AND 5), max_concurrency integer NOT NULL DEFAULT 10 CHECK(max_concurrency > 0), created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(project_id,name)
);
CREATE TABLE IF NOT EXISTS function_definitions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), project_id uuid NOT NULL REFERENCES projects(id), function_key text NOT NULL, version text NOT NULL, input_schema jsonb NOT NULL DEFAULT '{}'::jsonb, status text NOT NULL DEFAULT 'ACTIVE', created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(project_id,function_key,version)
);
CREATE TABLE IF NOT EXISTS job_definitions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), project_id uuid NOT NULL REFERENCES projects(id), function_id uuid NOT NULL REFERENCES function_definitions(id), queue_id uuid NOT NULL REFERENCES queues(id), retry_policy_id uuid REFERENCES retry_policies(id), name text NOT NULL, default_priority smallint NOT NULL DEFAULT 3 CHECK(default_priority BETWEEN 1 AND 5), timeout_ms bigint NOT NULL DEFAULT 30000 CHECK(timeout_ms > 0), status text NOT NULL DEFAULT 'ACTIVE', created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(project_id,name)
);
CREATE TABLE IF NOT EXISTS schedules (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), job_definition_id uuid NOT NULL REFERENCES job_definitions(id), schedule_type text NOT NULL CHECK(schedule_type IN ('CRON','ONE_TIME')), cron_expression text, timezone text NOT NULL DEFAULT 'UTC', with_seconds boolean NOT NULL DEFAULT false, run_at timestamptz, status text NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','PAUSED','DISABLED')), next_run_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(), CHECK((schedule_type = 'CRON' AND cron_expression IS NOT NULL) OR (schedule_type = 'ONE_TIME' AND run_at IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS job_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), project_id uuid NOT NULL REFERENCES projects(id), job_definition_id uuid NOT NULL REFERENCES job_definitions(id), queue_id uuid NOT NULL REFERENCES queues(id), schedule_id uuid REFERENCES schedules(id), idempotency_key text, status text NOT NULL CHECK(status IN ('CREATED','ENQUEUE_PENDING','QUEUED','RESERVED','RUNNING','RETRY_WAIT','SUCCEEDED','DEAD_LETTER','CANCELLED','TIMED_OUT')), priority smallint NOT NULL CHECK(priority BETWEEN 1 AND 5), scheduled_for timestamptz, available_at timestamptz NOT NULL DEFAULT now(), payload jsonb, payload_ciphertext bytea, encryption_key_ref text, function_version text NOT NULL, policy_snapshot jsonb NOT NULL, backend_message_id text, lease_owner text, lease_expires_at timestamptz, reserved_at timestamptz, started_at timestamptz, finished_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), CHECK((payload IS NOT NULL AND payload_ciphertext IS NULL) OR (payload IS NULL AND payload_ciphertext IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_job_runs_idempotency ON job_runs(project_id,idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_schedule_occurrence ON job_runs(schedule_id,scheduled_for) WHERE schedule_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_job_runs_claim ON job_runs(queue_id,priority DESC,available_at ASC,id ASC) WHERE status = 'QUEUED';
CREATE INDEX IF NOT EXISTS ix_job_runs_retry ON job_runs(available_at) WHERE status = 'RETRY_WAIT';
CREATE TABLE IF NOT EXISTS workers (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), worker_key text NOT NULL UNIQUE, hostname text NOT NULL, version text NOT NULL, status text NOT NULL DEFAULT 'ONLINE', heartbeat_at timestamptz NOT NULL DEFAULT now(), created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS job_attempts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), job_run_id uuid NOT NULL REFERENCES job_runs(id), worker_id uuid REFERENCES workers(id), attempt_number integer NOT NULL, status text NOT NULL, error_class text, error_code text, error_message text, trace_id text, started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz, UNIQUE(job_run_id,attempt_number)
);
CREATE INDEX IF NOT EXISTS ix_attempts_run ON job_attempts(job_run_id,attempt_number);
CREATE TABLE IF NOT EXISTS dlq_entries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), job_run_id uuid NOT NULL UNIQUE REFERENCES job_runs(id), reason text NOT NULL, entered_at timestamptz NOT NULL DEFAULT now(), replayed_at timestamptz
);
CREATE INDEX IF NOT EXISTS ix_dlq_open ON dlq_entries(entered_at DESC) WHERE replayed_at IS NULL;
CREATE TABLE IF NOT EXISTS outbox_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), project_id uuid NOT NULL REFERENCES projects(id), event_type text NOT NULL, aggregate_id uuid NOT NULL, payload jsonb NOT NULL DEFAULT '{}'::jsonb, available_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz, attempts integer NOT NULL DEFAULT 0, last_error text, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON outbox_events(available_at,id) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS audit_logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), project_id uuid NOT NULL REFERENCES projects(id), actor_id text NOT NULL, action text NOT NULL, resource_type text NOT NULL, resource_id uuid NOT NULL, before_data jsonb, after_data jsonb, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_audit_project_created ON audit_logs(project_id,created_at DESC);
