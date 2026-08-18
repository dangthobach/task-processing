package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
)

// QueueBackendService owns the platform-level configuration aggregate. HTTP
// only translates DTOs; KMS and persistence details stay behind this service.
type QueueBackendService struct{ Store *postgres.Store }

type CreateQueueBackend struct {
	Name, BackendType string
	Config            json.RawMessage
}

type QueueBackend struct {
	ID                        uuid.UUID
	Name, BackendType, Status string
	Version                   int64
}

func (s QueueBackendService) Create(ctx context.Context, in CreateQueueBackend) (QueueBackend, error) {
	if s.Store == nil {
		return QueueBackend{}, fmt.Errorf("queue backend store is required")
	}
	in.Name, in.BackendType = strings.TrimSpace(in.Name), strings.TrimSpace(in.BackendType)
	if in.Name == "" || (in.BackendType != "REDIS_STREAMS" && in.BackendType != "JETSTREAM") || !json.Valid(in.Config) {
		return QueueBackend{}, fmt.Errorf("name, supported backend_type and JSON config are required")
	}
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(in.Config, &cfg); err != nil {
		return QueueBackend{}, fmt.Errorf("config must be an object: %w", err)
	}
	for _, key := range requiredBackendFields(in.BackendType) {
		var value string
		if raw, ok := cfg[key]; !ok || json.Unmarshal(raw, &value) != nil || strings.TrimSpace(value) == "" {
			return QueueBackend{}, fmt.Errorf("config.%s is required", key)
		}
	}
	id := uuid.New()
	ciphertext, keyRef, err := s.Store.EncryptQueueBackendConfig(ctx, id, in.Config)
	if err != nil {
		return QueueBackend{}, err
	}
	result := QueueBackend{ID: id, Name: in.Name, BackendType: in.BackendType, Status: "ACTIVE"}
	err = s.Store.Pool.QueryRow(ctx, `INSERT INTO queue_backends(id,name,backend_type,encrypted_config,encryption_key_ref,capabilities,status)
		VALUES($1,$2,$3,$4,$5,'{}'::jsonb,'ACTIVE') RETURNING row_version`, id, in.Name, in.BackendType, ciphertext, keyRef).Scan(&result.Version)
	return result, err
}

func requiredBackendFields(kind string) []string {
	if kind == "REDIS_STREAMS" {
		return []string{"url", "stream", "group"}
	}
	return []string{"url", "stream", "subject", "consumer"}
}
