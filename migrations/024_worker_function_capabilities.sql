CREATE TABLE IF NOT EXISTS worker_function_capabilities (
  worker_id uuid NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
  function_key text NOT NULL CHECK (char_length(function_key) BETWEEN 1 AND 256),
  function_version text NOT NULL CHECK (char_length(function_version) BETWEEN 1 AND 128),
  execution_mode text NOT NULL CHECK (execution_mode IN ('SINGLE','BATCH')),
  registered_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(worker_id,function_key,function_version,execution_mode)
);

CREATE INDEX IF NOT EXISTS ix_worker_capabilities_lookup
  ON worker_function_capabilities(function_key,function_version,execution_mode,worker_id);

-- Keep the capability guard below HTTP/application services. Generic PATCH
-- endpoints can change execution_mode, so enforcing it only in CreateJob
-- would leave a bypass path.
CREATE OR REPLACE FUNCTION task_processing_require_worker_capability()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  key_value text;
  version_value text;
BEGIN
  SELECT function_key,version INTO key_value,version_value FROM function_definitions
  WHERE id=NEW.function_id AND deleted_at IS NULL;
  IF key_value IS NULL OR NOT EXISTS (
    SELECT 1 FROM worker_function_capabilities c
    JOIN workers w ON w.id=c.worker_id
    WHERE c.function_key=key_value
      AND (c.function_version=version_value OR c.function_version='*')
      AND c.execution_mode=NEW.execution_mode
      AND w.status='ONLINE'
      AND w.heartbeat_at>now()-interval '30 seconds'
  ) THEN
    RAISE EXCEPTION 'no live worker capability for function % version % mode %', key_value, version_value, NEW.execution_mode
      USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_job_definitions_require_worker_capability ON job_definitions;
CREATE TRIGGER trg_job_definitions_require_worker_capability
  BEFORE INSERT OR UPDATE OF function_id,execution_mode ON job_definitions
  FOR EACH ROW EXECUTE FUNCTION task_processing_require_worker_capability();
