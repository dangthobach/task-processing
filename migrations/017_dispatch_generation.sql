ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS current_dispatch_id uuid;
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS dispatch_id uuid;
UPDATE job_runs SET current_dispatch_id=gen_random_uuid() WHERE current_dispatch_id IS NULL;
UPDATE outbox_events o SET dispatch_id=r.current_dispatch_id FROM job_runs r
WHERE o.event_type='job.enqueue' AND o.aggregate_id=r.id AND o.dispatch_id IS NULL;
ALTER TABLE job_runs ALTER COLUMN current_dispatch_id SET NOT NULL;
ALTER TABLE outbox_events ADD CONSTRAINT ck_outbox_enqueue_dispatch
  CHECK(event_type <> 'job.enqueue' OR dispatch_id IS NOT NULL) NOT VALID;
ALTER TABLE outbox_events VALIDATE CONSTRAINT ck_outbox_enqueue_dispatch;
CREATE INDEX IF NOT EXISTS ix_job_runs_dispatch_ownership ON job_runs(id,current_dispatch_id,status,available_at);
