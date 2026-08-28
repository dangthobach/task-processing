-- Queue backends are soft-deletable configuration. A deleted name must not
-- block a replacement backend, while restore remains conflict checked.
ALTER TABLE queue_backends DROP CONSTRAINT IF EXISTS queue_backends_name_key;
CREATE UNIQUE INDEX IF NOT EXISTS ux_queue_backends_active_name
  ON queue_backends(name) WHERE deleted_at IS NULL;
