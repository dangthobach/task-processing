CREATE TABLE IF NOT EXISTS scheduler_logs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  project_id uuid NOT NULL REFERENCES projects(id),
  schedule_id uuid REFERENCES schedules(id) ON DELETE SET NULL,
  scheduler_instance_id text NOT NULL,
  level text NOT NULL CHECK(level IN ('DEBUG','INFO','WARN','ERROR')),
  event_type text NOT NULL,
  message text NOT NULL,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  occurred_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_scheduler_logs_project_time ON scheduler_logs(project_id,occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_scheduler_logs_schedule_time ON scheduler_logs(schedule_id,occurred_at DESC);
