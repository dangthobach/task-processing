package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

type QueueBackendConfig struct {
	ID               uuid.UUID
	Type             string
	EncryptedConfig  []byte
	EncryptionKeyRef string
	RowVersion       int64
}

func (s *Store) ActiveQueueBackendConfigs(ctx context.Context) ([]QueueBackendConfig, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id,backend_type,encrypted_config,encryption_key_ref,row_version
		FROM queue_backends WHERE status='ACTIVE' AND deleted_at IS NULL AND backend_type <> 'POSTGRES' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]QueueBackendConfig, 0)
	for rows.Next() {
		var item QueueBackendConfig
		if err = rows.Scan(&item.ID, &item.Type, &item.EncryptedConfig, &item.EncryptionKeyRef, &item.RowVersion); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) DecryptQueueBackendConfig(ctx context.Context, id uuid.UUID, ciphertext []byte, keyRef string) ([]byte, error) {
	if s.PayloadProtector == nil {
		return nil, fmt.Errorf("queue backend %s configuration requires TASK_PAYLOAD_MASTER_KEY", id)
	}
	if len(ciphertext) == 0 || keyRef == "" {
		return nil, fmt.Errorf("queue backend %s has no encrypted configuration/key reference", id)
	}
	return s.PayloadProtector.Decrypt(ctx, ciphertext, keyRef, []byte("queue-backend/"+id.String()))
}

func (s *Store) EncryptQueueBackendConfig(ctx context.Context, id uuid.UUID, plaintext []byte) ([]byte, string, error) {
	if s.PayloadProtector == nil {
		return nil, "", fmt.Errorf("queue backend configuration encryption requires TASK_PAYLOAD_MASTER_KEY")
	}
	return s.PayloadProtector.Encrypt(ctx, plaintext, []byte("queue-backend/"+id.String()))
}
