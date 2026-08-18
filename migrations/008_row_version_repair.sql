-- Repair databases that applied the original governance migration, where the
-- generic `version` column collided with handler and worker build versions.
-- Semantic text versions are preserved; optimistic locking is always row_version.
DO $$
DECLARE target_table text;
BEGIN
  FOREACH target_table IN ARRAY ARRAY['tenants','projects','queue_backends','retry_policies','queues','job_definitions','schedules','job_runs','dlq_entries','job_batches']
  LOOP
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='public' AND table_name=target_table AND column_name='version' AND data_type='bigint'
    ) THEN
      EXECUTE format('UPDATE %I SET row_version=GREATEST(row_version, version)', target_table);
      EXECUTE format('ALTER TABLE %I DROP COLUMN version', target_table);
    END IF;
  END LOOP;
END;
$$;
