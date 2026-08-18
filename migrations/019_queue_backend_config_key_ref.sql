-- Ciphertext alone is not decryptable after KMS key rotation. Store its key
-- reference explicitly and make runtime backend configuration versioned.
ALTER TABLE queue_backends ADD COLUMN IF NOT EXISTS encryption_key_ref text;
