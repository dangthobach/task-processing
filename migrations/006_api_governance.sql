-- API governance: every mutable aggregate has an optimistic-lock version.
-- The control-plane API must require If-Match for state-changing operations.
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);
ALTER TABLE projects ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);
ALTER TABLE queue_backends ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);
ALTER TABLE retry_policies ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);
ALTER TABLE queues ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);
ALTER TABLE function_definitions ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);
ALTER TABLE job_definitions ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);
ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);
ALTER TABLE workers ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);
ALTER TABLE dlq_entries ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);
ALTER TABLE job_batches ADD COLUMN IF NOT EXISTS row_version bigint NOT NULL DEFAULT 1 CHECK(row_version > 0);

-- Audit entries are first-class correlation records, not only a best-effort
-- payload snapshot.  Actor identity, request and distributed trace are kept
-- with every externally initiated mutation.
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS tenant_id uuid REFERENCES tenants(id);
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS request_id text;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS trace_id text;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS metadata jsonb NOT NULL DEFAULT '{}'::jsonb;
CREATE INDEX IF NOT EXISTS ix_audit_request_id ON audit_logs(request_id) WHERE request_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_audit_trace_id ON audit_logs(trace_id) WHERE trace_id IS NOT NULL;

CREATE OR REPLACE FUNCTION task_processing_bump_version() RETURNS trigger AS $$
BEGIN
  NEW.row_version := OLD.row_version + 1;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE table_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY['tenants','projects','queue_backends','retry_policies','queues','function_definitions','job_definitions','schedules','job_runs','workers','dlq_entries','job_batches']
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', 'trg_' || table_name || '_version', table_name);
    EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION task_processing_bump_version()', 'trg_' || table_name || '_version', table_name);
  END LOOP;
END;
$$;

-- A mutation is auditable only when both sides can be represented. Existing
-- creation events use a null before_data; subsequent state changes are updated
-- by the application service with an explicit before/after snapshot.
