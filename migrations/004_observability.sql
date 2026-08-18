ALTER TABLE job_attempts ADD COLUMN IF NOT EXISTS progress_pct smallint NOT NULL DEFAULT 0 CHECK(progress_pct BETWEEN 0 AND 100);
ALTER TABLE job_attempts ADD COLUMN IF NOT EXISTS progress_message text;
ALTER TABLE job_attempts ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS realtime_events (
  id bigserial PRIMARY KEY,
  project_id uuid NOT NULL REFERENCES projects(id),
  event_type text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id uuid NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_realtime_events_project_id ON realtime_events(project_id,id);
