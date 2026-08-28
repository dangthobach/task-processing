package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// QueueBackendService owns the platform-level configuration aggregate. HTTP
// only translates DTOs; KMS and persistence details stay behind this service.
type QueueBackendService struct{ Store *postgres.Store }
type QueueBackendMutationAudit func(context.Context, pgx.Tx, uuid.UUID, json.RawMessage, json.RawMessage) error

type CreateQueueBackend struct {
	Name, BackendType string
	Config            json.RawMessage
}

type QueueBackend struct {
	ID           uuid.UUID       `json:"id"`
	Name         string          `json:"name"`
	BackendType  string          `json:"backend_type"`
	Capabilities json.RawMessage `json:"capabilities"`
	Status       string          `json:"status"`
	Version      int64           `json:"version"`
	CreatedAt    string          `json:"created_at,omitempty"`
	DeletedAt    any             `json:"deleted_at,omitempty"`
}
type UpdateQueueBackend struct {
	Name, Status *string
	Config       *json.RawMessage
}

func (s QueueBackendService) Create(ctx context.Context, in CreateQueueBackend, audit QueueBackendMutationAudit) (QueueBackend, error) {
	if s.Store == nil {
		return QueueBackend{}, fmt.Errorf("queue backend store is required")
	}
	in.Name, in.BackendType = strings.TrimSpace(in.Name), strings.TrimSpace(in.BackendType)
	if err := validateBackendConfig(in.Name, in.BackendType, in.Config); err != nil {
		return QueueBackend{}, err
	}
	id := uuid.New()
	ciphertext, keyRef, err := s.Store.EncryptQueueBackendConfig(ctx, id, in.Config)
	if err != nil {
		return QueueBackend{}, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return QueueBackend{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO queue_backends(id,name,backend_type,encrypted_config,encryption_key_ref,capabilities,status) VALUES($1,$2,$3,$4,$5,'{}'::jsonb,'ACTIVE')`, id, in.Name, in.BackendType, ciphertext, keyRef); err != nil {
		return QueueBackend{}, err
	}
	result, err := backendSnapshot(ctx, tx, id, true)
	if err != nil {
		return QueueBackend{}, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, nil, backendAudit(result)); err != nil {
			return QueueBackend{}, err
		}
	}
	return result, tx.Commit(ctx)
}

func (s QueueBackendService) List(ctx context.Context, includeDeleted bool) ([]QueueBackend, error) {
	if s.Store == nil || s.Store.Pool == nil {
		return nil, fmt.Errorf("queue backend store is required")
	}
	rows, err := s.Store.Pool.Query(ctx, backendSelect+` WHERE ($1 OR b.deleted_at IS NULL) ORDER BY b.created_at DESC,b.id DESC`, includeDeleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []QueueBackend{}
	for rows.Next() {
		var b QueueBackend
		if err = rows.Scan(&b.ID, &b.Name, &b.BackendType, &b.Capabilities, &b.Status, &b.Version, &b.CreatedAt, &b.DeletedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
func (s QueueBackendService) Get(ctx context.Context, id uuid.UUID, includeDeleted bool) (QueueBackend, error) {
	if s.Store == nil || s.Store.Pool == nil {
		return QueueBackend{}, fmt.Errorf("queue backend store is required")
	}
	return backendSnapshot(ctx, s.Store.Pool, id, includeDeleted)
}

func (s QueueBackendService) Update(ctx context.Context, id uuid.UUID, version int64, in UpdateQueueBackend, audit QueueBackendMutationAudit) (QueueBackend, error) {
	if s.Store == nil || s.Store.Pool == nil {
		return QueueBackend{}, fmt.Errorf("queue backend store is required")
	}
	if in.Name == nil && in.Status == nil && in.Config == nil {
		return QueueBackend{}, fmt.Errorf("at least one backend field is required")
	}
	if in.Name != nil && strings.TrimSpace(*in.Name) == "" {
		return QueueBackend{}, fmt.Errorf("name is required")
	}
	if in.Status != nil && (*in.Status != "ACTIVE" && *in.Status != "DISABLED") {
		return QueueBackend{}, fmt.Errorf("status must be ACTIVE or DISABLED")
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return QueueBackend{}, err
	}
	defer tx.Rollback(ctx)
	before, err := backendSnapshot(ctx, tx, id, false)
	if errors.Is(err, pgx.ErrNoRows) {
		return QueueBackend{}, ErrResourceNotFound
	}
	if err != nil {
		return QueueBackend{}, err
	}
	ciphertext, keyRef := []byte(nil), ""
	if in.Config != nil {
		if err = validateBackendConfig(before.Name, before.BackendType, *in.Config); err != nil {
			return QueueBackend{}, err
		}
		ciphertext, keyRef, err = s.Store.EncryptQueueBackendConfig(ctx, id, *in.Config)
		if err != nil {
			return QueueBackend{}, err
		}
	}
	name, status := before.Name, before.Status
	if in.Name != nil {
		name = strings.TrimSpace(*in.Name)
	}
	if in.Status != nil {
		status = *in.Status
	}
	var ignored uuid.UUID
	err = tx.QueryRow(ctx, `UPDATE queue_backends SET name=$3,status=$4,encrypted_config=CASE WHEN $5 THEN $6 ELSE encrypted_config END,encryption_key_ref=CASE WHEN $5 THEN $7 ELSE encryption_key_ref END WHERE id=$1 AND row_version=$2 AND deleted_at IS NULL RETURNING id`, id, version, name, status, in.Config != nil, ciphertext, keyRef).Scan(&ignored)
	if errors.Is(err, pgx.ErrNoRows) {
		return QueueBackend{}, ErrOptimisticLock
	}
	if err != nil {
		return QueueBackend{}, err
	}
	after, err := backendSnapshot(ctx, tx, id, false)
	if err != nil {
		return QueueBackend{}, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, backendAudit(before), backendAudit(after)); err != nil {
			return QueueBackend{}, err
		}
	}
	return after, tx.Commit(ctx)
}
func (s QueueBackendService) SoftDelete(ctx context.Context, id uuid.UUID, version int64, actor string, audit QueueBackendMutationAudit) (QueueBackend, error) {
	return s.setDeletion(ctx, id, version, actor, true, audit)
}
func (s QueueBackendService) Restore(ctx context.Context, id uuid.UUID, version int64, audit QueueBackendMutationAudit) (QueueBackend, error) {
	return s.setDeletion(ctx, id, version, "", false, audit)
}
func (s QueueBackendService) setDeletion(ctx context.Context, id uuid.UUID, version int64, actor string, deleted bool, audit QueueBackendMutationAudit) (QueueBackend, error) {
	if s.Store == nil || s.Store.Pool == nil {
		return QueueBackend{}, fmt.Errorf("queue backend store is required")
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return QueueBackend{}, err
	}
	defer tx.Rollback(ctx)
	before, err := backendSnapshot(ctx, tx, id, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return QueueBackend{}, ErrResourceNotFound
	}
	if err != nil {
		return QueueBackend{}, err
	}
	if deleted {
		var used bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM queues WHERE backend_id=$1 AND deleted_at IS NULL)", id).Scan(&used); err != nil {
			return QueueBackend{}, err
		}
		if used {
			return QueueBackend{}, ErrDependencyBlocked
		}
	}
	where := "deleted_at IS NOT NULL"
	set := "deleted_at=NULL,deleted_by=NULL"
	args := []any{id, version}
	if deleted {
		where = "deleted_at IS NULL"
		set = "deleted_at=now(),deleted_by=$3"
		args = append(args, actor)
	}
	var ignored uuid.UUID
	err = tx.QueryRow(ctx, "UPDATE queue_backends SET "+set+" WHERE id=$1 AND row_version=$2 AND "+where+" RETURNING id", args...).Scan(&ignored)
	if errors.Is(err, pgx.ErrNoRows) {
		return QueueBackend{}, ErrOptimisticLock
	}
	if err != nil {
		if isUnique(err) {
			return QueueBackend{}, ErrRestoreConflict
		}
		return QueueBackend{}, err
	}
	after, err := backendSnapshot(ctx, tx, id, true)
	if err != nil {
		return QueueBackend{}, err
	}
	if audit != nil {
		if err = audit(ctx, tx, id, backendAudit(before), backendAudit(after)); err != nil {
			return QueueBackend{}, err
		}
	}
	return after, tx.Commit(ctx)
}

const backendSelect = `SELECT b.id,b.name,b.backend_type,b.capabilities,b.status,b.row_version,b.created_at::text,b.deleted_at FROM queue_backends b`

func backendSnapshot(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id uuid.UUID, includeDeleted bool) (QueueBackend, error) {
	var b QueueBackend
	err := db.QueryRow(ctx, backendSelect+` WHERE b.id=$1 AND ($2 OR b.deleted_at IS NULL)`, id, includeDeleted).Scan(&b.ID, &b.Name, &b.BackendType, &b.Capabilities, &b.Status, &b.Version, &b.CreatedAt, &b.DeletedAt)
	return b, err
}
func validateBackendConfig(name, kind string, config json.RawMessage) error {
	if strings.TrimSpace(name) == "" || (kind != "REDIS_STREAMS" && kind != "JETSTREAM") || !json.Valid(config) {
		return fmt.Errorf("name, supported backend_type and JSON config are required")
	}
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("config must be an object: %w", err)
	}
	for _, key := range requiredBackendFields(kind) {
		var value string
		if raw, ok := cfg[key]; !ok || json.Unmarshal(raw, &value) != nil || strings.TrimSpace(value) == "" {
			return fmt.Errorf("config.%s is required", key)
		}
	}
	return nil
}
func backendAudit(b QueueBackend) json.RawMessage {
	out, _ := json.Marshal(map[string]any{"id": b.ID, "name": b.Name, "backend_type": b.BackendType, "status": b.Status, "version": b.Version, "capabilities": b.Capabilities, "deleted_at": b.DeletedAt})
	return out
}

func requiredBackendFields(kind string) []string {
	if kind == "REDIS_STREAMS" {
		return []string{"url", "stream", "group"}
	}
	return []string{"url", "stream", "subject", "consumer"}
}
