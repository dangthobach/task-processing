ALTER TABLE schedules ADD COLUMN IF NOT EXISTS misfire_policy text NOT NULL DEFAULT 'SKIP'
  CHECK (misfire_policy IN ('SKIP','FIRE_ONCE'));

-- evaluated_through records the last schedule boundary that was decided,
-- including boundaries deliberately skipped during downtime.
CREATE TABLE IF NOT EXISTS schedule_cursors (
  schedule_id uuid PRIMARY KEY REFERENCES schedules(id) ON DELETE CASCADE,
  evaluated_through timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
