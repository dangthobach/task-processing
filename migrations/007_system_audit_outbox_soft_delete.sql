-- Configuration aggregates are soft-deletable. Runtime execution, audit and
-- outbox records are immutable operational evidence and intentionally have no
-- deleted_at column.
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS deleted_by text;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS deleted_by text;
ALTER TABLE queue_backends ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE queue_backends ADD COLUMN IF NOT EXISTS deleted_by text;
ALTER TABLE retry_policies ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE retry_policies ADD COLUMN IF NOT EXISTS deleted_by text;
ALTER TABLE queues ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE queues ADD COLUMN IF NOT EXISTS deleted_by text;
ALTER TABLE function_definitions ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE function_definitions ADD COLUMN IF NOT EXISTS deleted_by text;
ALTER TABLE job_definitions ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE job_definitions ADD COLUMN IF NOT EXISTS deleted_by text;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS deleted_by text;

CREATE INDEX IF NOT EXISTS ix_projects_active ON projects(tenant_id,id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_queues_active ON queues(project_id,id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_functions_active ON function_definitions(project_id,id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_job_definitions_active ON job_definitions(project_id,id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_schedules_active ON schedules(job_definition_id,id) WHERE deleted_at IS NULL;

-- A transactional audit outbox is the hand-off point for a future Kafka/NATS/
-- SIEM publisher. It is intentionally separate from queue outbox_events, whose
-- dispatcher has job-enqueue semantics.
CREATE TABLE IF NOT EXISTS audit_outbox_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  audit_log_id uuid NOT NULL REFERENCES audit_logs(id),
  project_id uuid NOT NULL REFERENCES projects(id),
  event_type text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id uuid NOT NULL,
  payload jsonb NOT NULL,
  available_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  attempts integer NOT NULL DEFAULT 0,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_audit_outbox_pending ON audit_outbox_events(available_at,id) WHERE published_at IS NULL;

-- Worker transitions are recorded in the same transaction as the state change,
-- so a committed state cannot exist without an audit event and durable outbox
-- hand-off. API-initiated state transitions retain their user audit record too.
CREATE OR REPLACE FUNCTION task_processing_record_system_transition() RETURNS trigger AS $$
DECLARE
  v_project_id uuid;
  v_tenant_id uuid;
  v_audit_id uuid;
  v_resource_type text;
  v_action text;
  v_payload jsonb;
BEGIN
  IF OLD.status IS NOT DISTINCT FROM NEW.status THEN
    RETURN NEW;
  END IF;
  IF TG_TABLE_NAME = 'job_runs' THEN
    v_project_id := NEW.project_id;
    v_resource_type := 'job_run';
  ELSIF TG_TABLE_NAME = 'job_batches' THEN
    v_project_id := NEW.project_id;
    v_resource_type := 'job_batch';
  ELSE
    RETURN NEW;
  END IF;
  SELECT tenant_id INTO v_tenant_id FROM projects WHERE id = v_project_id;
  v_action := 'system.' || v_resource_type || '.status_changed';
  v_payload := jsonb_build_object(
    'source', 'transition-engine',
    'previous_status', OLD.status,
    'status', NEW.status,
    'previous_version', OLD.row_version,
    'version', NEW.row_version
  );
  INSERT INTO audit_logs(project_id,tenant_id,actor_id,action,resource_type,resource_id,before_data,after_data,trace_id,metadata)
  VALUES(v_project_id,v_tenant_id,'system:transition-engine',v_action,v_resource_type,NEW.id,
    jsonb_build_object('status',OLD.status,'version',OLD.row_version),
    jsonb_build_object('status',NEW.status,'version',NEW.row_version),
    NULLIF(current_setting('app.trace_id', true),''),
    jsonb_build_object('source','worker'))
  RETURNING id INTO v_audit_id;
  INSERT INTO audit_outbox_events(audit_log_id,project_id,event_type,aggregate_type,aggregate_id,payload)
  VALUES(v_audit_id,v_project_id,'audit.system_transition',v_resource_type,NEW.id,v_payload);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_job_runs_system_audit ON job_runs;
CREATE TRIGGER trg_job_runs_system_audit
AFTER UPDATE OF status ON job_runs
FOR EACH ROW EXECUTE FUNCTION task_processing_record_system_transition();

DROP TRIGGER IF EXISTS trg_job_batches_system_audit ON job_batches;
CREATE TRIGGER trg_job_batches_system_audit
AFTER UPDATE OF status ON job_batches
FOR EACH ROW EXECUTE FUNCTION task_processing_record_system_transition();
