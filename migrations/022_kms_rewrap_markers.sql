ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS payload_rewrapped_at timestamptz;
ALTER TABLE queue_backends ADD COLUMN IF NOT EXISTS config_rewrapped_at timestamptz;
