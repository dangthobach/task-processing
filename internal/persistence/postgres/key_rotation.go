package postgres

import (
	"context"
	"fmt"

	"github.com/example/task-processing/internal/security/kms"
	"github.com/google/uuid"
)

// RewrapEncryptedData incrementally advances ciphertext to the provider's
// current key version. It reads a bounded page and uses conditional writes, so
// it neither holds a database transaction during a KMS call nor overwrites a
// concurrent configuration update.
func (s *Store) RewrapEncryptedData(ctx context.Context, limit int) (int, error) {
	rewrapper, ok := s.PayloadProtector.(kms.Rewrapper)
	if !ok {
		return 0, fmt.Errorf("configured KMS provider does not support rewrap")
	}
	if limit < 1 || limit > 1000 {
		limit = 100
	}
	count, err := s.rewrapJobPayloads(ctx, rewrapper, limit)
	if err != nil {
		return count, err
	}
	if count >= limit {
		return count, nil
	}
	more, err := s.rewrapQueueBackendConfigs(ctx, rewrapper, limit-count)
	return count + more, err
}

// ResetKMSRewrapMarkers starts a new, explicitly authorized rotation campaign.
// It is intentionally not performed automatically after changing key config.
func (s *Store) ResetKMSRewrapMarkers(ctx context.Context) error {
	if _, err := s.Pool.Exec(ctx, "UPDATE job_runs SET payload_rewrapped_at=NULL WHERE payload_ciphertext IS NOT NULL AND octet_length(payload_ciphertext)>0"); err != nil {
		return err
	}
	_, err := s.Pool.Exec(ctx, "UPDATE queue_backends SET config_rewrapped_at=NULL WHERE encrypted_config IS NOT NULL AND octet_length(encrypted_config)>0")
	return err
}
func (s *Store) rewrapJobPayloads(ctx context.Context, r kms.Rewrapper, limit int) (int, error) {
	rows, err := s.Pool.Query(ctx, "SELECT id,project_id,job_definition_id,payload_ciphertext,encryption_key_ref FROM job_runs WHERE payload_ciphertext IS NOT NULL AND octet_length(payload_ciphertext)>0 AND payload_rewrapped_at IS NULL ORDER BY created_at,id LIMIT $1", limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	changed := 0
	for rows.Next() {
		var id, project, definition uuid.UUID
		var cipher []byte
		var ref string
		if err = rows.Scan(&id, &project, &definition, &cipher, &ref); err != nil {
			return changed, err
		}
		next, nextRef, err := r.Rewrap(ctx, cipher, ref, []byte(project.String()+":"+definition.String()))
		if err != nil {
			return changed, fmt.Errorf("rewrap job run %s: %w", id, err)
		}
		tag, err := s.Pool.Exec(ctx, "UPDATE job_runs SET payload_ciphertext=$2,encryption_key_ref=$3,payload_rewrapped_at=now() WHERE id=$1 AND payload_ciphertext=$4 AND encryption_key_ref=$5 AND payload_rewrapped_at IS NULL", id, next, nextRef, cipher, ref)
		if err != nil {
			return changed, err
		}
		changed += int(tag.RowsAffected())
	}
	return changed, rows.Err()
}
func (s *Store) rewrapQueueBackendConfigs(ctx context.Context, r kms.Rewrapper, limit int) (int, error) {
	if limit < 1 {
		return 0, nil
	}
	rows, err := s.Pool.Query(ctx, "SELECT id,encrypted_config,encryption_key_ref FROM queue_backends WHERE encrypted_config IS NOT NULL AND octet_length(encrypted_config)>0 AND config_rewrapped_at IS NULL ORDER BY created_at,id LIMIT $1", limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	changed := 0
	for rows.Next() {
		var id uuid.UUID
		var cipher []byte
		var ref string
		if err = rows.Scan(&id, &cipher, &ref); err != nil {
			return changed, err
		}
		next, nextRef, err := r.Rewrap(ctx, cipher, ref, []byte("queue-backend/"+id.String()))
		if err != nil {
			return changed, fmt.Errorf("rewrap queue backend %s: %w", id, err)
		}
		tag, err := s.Pool.Exec(ctx, "UPDATE queue_backends SET encrypted_config=$2,encryption_key_ref=$3,config_rewrapped_at=now() WHERE id=$1 AND encrypted_config=$4 AND encryption_key_ref=$5 AND config_rewrapped_at IS NULL", id, next, nextRef, cipher, ref)
		if err != nil {
			return changed, err
		}
		changed += int(tag.RowsAffected())
	}
	return changed, rows.Err()
}
