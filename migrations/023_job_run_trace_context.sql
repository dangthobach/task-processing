-- Persist only W3C trace-context headers needed to reconnect the submit span
-- to the worker attempt. Baggage is deliberately not persisted: it is often
-- unbounded and may contain tenant or user data that is not job state.
ALTER TABLE job_runs
  ADD COLUMN IF NOT EXISTS traceparent text,
  ADD COLUMN IF NOT EXISTS tracestate text;

ALTER TABLE job_runs
  ADD CONSTRAINT ck_job_runs_traceparent_length
  CHECK (traceparent IS NULL OR char_length(traceparent) <= 512) NOT VALID;

ALTER TABLE job_runs
  ADD CONSTRAINT ck_job_runs_tracestate_length
  CHECK (tracestate IS NULL OR char_length(tracestate) <= 512) NOT VALID;

ALTER TABLE job_runs VALIDATE CONSTRAINT ck_job_runs_traceparent_length;
ALTER TABLE job_runs VALIDATE CONSTRAINT ck_job_runs_tracestate_length;
