ALTER TABLE job_definitions ADD COLUMN IF NOT EXISTS execution_mode text NOT NULL DEFAULT 'SINGLE' CHECK(execution_mode IN ('SINGLE','BATCH'));
ALTER TABLE job_definitions ADD COLUMN IF NOT EXISTS batch_size integer NOT NULL DEFAULT 100 CHECK(batch_size BETWEEN 2 AND 1000);
ALTER TABLE job_definitions ADD COLUMN IF NOT EXISTS batch_max_wait_ms integer NOT NULL DEFAULT 5000 CHECK(batch_max_wait_ms BETWEEN 0 AND 3600000);

CREATE TABLE IF NOT EXISTS job_batches (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES projects(id),
  queue_id uuid NOT NULL REFERENCES queues(id),
  job_definition_id uuid NOT NULL REFERENCES job_definitions(id),
  worker_id uuid REFERENCES workers(id),
  lease_owner text,
  lease_expires_at timestamptz,
  status text NOT NULL CHECK(status IN ('RESERVED','RUNNING','SUCCEEDED','PARTIAL_FAILED','FAILED')),
  total_items integer NOT NULL CHECK(total_items > 0),
  processed_items integer NOT NULL DEFAULT 0,
  succeeded_items integer NOT NULL DEFAULT 0,
  failed_items integer NOT NULL DEFAULT 0,
  retry_scheduled_items integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  finished_at timestamptz
);
CREATE INDEX IF NOT EXISTS ix_job_batches_project_created ON job_batches(project_id,created_at DESC);

CREATE TABLE IF NOT EXISTS job_batch_items (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id uuid NOT NULL REFERENCES job_batches(id) ON DELETE CASCADE,
  job_run_id uuid NOT NULL REFERENCES job_runs(id),
  ordinal integer NOT NULL,
  status text NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING','SUCCEEDED','FAILED','RETRY_SCHEDULED','DEAD_LETTER')),
  error_class text,
  error_code text,
  error_message text,
  completed_at timestamptz,
  UNIQUE(batch_id,job_run_id),
  UNIQUE(batch_id,ordinal)
);
CREATE INDEX IF NOT EXISTS ix_job_batch_items_batch ON job_batch_items(batch_id,ordinal);

CREATE TABLE IF NOT EXISTS job_batch_attempts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id uuid NOT NULL REFERENCES job_batches(id) ON DELETE CASCADE,
  worker_id uuid REFERENCES workers(id),
  attempt_number integer NOT NULL,
  status text NOT NULL CHECK(status IN ('RUNNING','SUCCEEDED','PARTIAL_FAILED','FAILED')),
  total_items integer NOT NULL,
  processed_items integer NOT NULL DEFAULT 0,
  succeeded_items integer NOT NULL DEFAULT 0,
  failed_items integer NOT NULL DEFAULT 0,
  progress_pct smallint NOT NULL DEFAULT 0 CHECK(progress_pct BETWEEN 0 AND 100),
  progress_message text,
  trace_id text,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  UNIQUE(batch_id,attempt_number)
);
ALTER TABLE job_attempts ADD COLUMN IF NOT EXISTS batch_attempt_id uuid REFERENCES job_batch_attempts(id);

CREATE TABLE IF NOT EXISTS job_batch_logs (
  id bigserial PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES projects(id),
  batch_id uuid NOT NULL REFERENCES job_batches(id) ON DELETE CASCADE,
  batch_attempt_id uuid REFERENCES job_batch_attempts(id) ON DELETE SET NULL,
  level text NOT NULL CHECK(level IN ('DEBUG','INFO','WARN','ERROR')),
  message text NOT NULL,
  fields jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_job_batch_logs_batch ON job_batch_logs(batch_id,id);
