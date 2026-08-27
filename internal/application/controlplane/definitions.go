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

var ErrDependencyNotFound = errors.New("active dependency not found")

type FunctionDefinitionInput struct {
	ProjectID            uuid.UUID
	FunctionKey, Version string
	InputSchema          json.RawMessage
}
type JobDefinitionInput struct {
	ProjectID, FunctionID, QueueID uuid.UUID
	RetryPolicyID                  *uuid.UUID
	Name                           string
	DefaultPriority                int
	TimeoutMS                      int64
	ExecutionMode                  string
	BatchSize, BatchMaxWaitMS      int
}
type DefinitionService struct{ Store *postgres.Store }

func (s DefinitionService) CreateFunction(ctx context.Context, in FunctionDefinitionInput, audit AuditInTransaction) (VersionedResource, error) {
	if s.Store == nil {
		return VersionedResource{}, fmt.Errorf("definition store is required")
	}
	in.FunctionKey, in.Version = strings.TrimSpace(in.FunctionKey), strings.TrimSpace(in.Version)
	if in.ProjectID == uuid.Nil || in.FunctionKey == "" || in.Version == "" {
		return VersionedResource{}, fmt.Errorf("project_id, function_key and version are required")
	}
	if len(in.FunctionKey) > 255 || len(in.Version) > 255 {
		return VersionedResource{}, fmt.Errorf("function_key and version must be at most 255 characters")
	}
	if len(in.InputSchema) == 0 {
		in.InputSchema = json.RawMessage(`{}`)
	}
	if err := postgres.ValidateInputSchema(in.InputSchema); err != nil {
		return VersionedResource{}, err
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return VersionedResource{}, err
	}
	defer tx.Rollback(ctx)
	out := VersionedResource{Status: "ACTIVE"}
	if err = tx.QueryRow(ctx, "INSERT INTO function_definitions(project_id,function_key,version,input_schema) VALUES($1,$2,$3,$4) RETURNING id,row_version", in.ProjectID, in.FunctionKey, in.Version, in.InputSchema).Scan(&out.ID, &out.Version); err != nil {
		return VersionedResource{}, err
	}
	if audit != nil {
		if err = audit(ctx, tx, out.ID); err != nil {
			return VersionedResource{}, err
		}
	}
	return out, tx.Commit(ctx)
}
func (s DefinitionService) CreateJob(ctx context.Context, in JobDefinitionInput, audit AuditInTransaction) (VersionedResource, error) {
	if s.Store == nil {
		return VersionedResource{}, fmt.Errorf("definition store is required")
	}
	in.Name = strings.TrimSpace(in.Name)
	if in.ProjectID == uuid.Nil || in.FunctionID == uuid.Nil || in.QueueID == uuid.Nil || in.Name == "" {
		return VersionedResource{}, fmt.Errorf("project_id, function_id, queue_id and name are required")
	}
	if in.DefaultPriority == 0 {
		in.DefaultPriority = 3
	}
	if in.DefaultPriority < 1 || in.DefaultPriority > 5 {
		return VersionedResource{}, fmt.Errorf("default_priority must be 1..5")
	}
	if in.TimeoutMS == 0 {
		in.TimeoutMS = 30000
	}
	if in.TimeoutMS < 1 || in.TimeoutMS > 86400000 {
		return VersionedResource{}, fmt.Errorf("timeout_ms must be 1..86400000")
	}
	if in.ExecutionMode == "" {
		in.ExecutionMode = "SINGLE"
	}
	if in.ExecutionMode != "SINGLE" && in.ExecutionMode != "BATCH" {
		return VersionedResource{}, fmt.Errorf("execution_mode must be SINGLE or BATCH")
	}
	if in.BatchSize == 0 {
		in.BatchSize = 100
	}
	if in.BatchSize < 2 || in.BatchSize > 1000 {
		return VersionedResource{}, fmt.Errorf("batch_size must be between 2 and 1000")
	}
	if in.BatchMaxWaitMS < 0 || in.BatchMaxWaitMS > 3600000 {
		return VersionedResource{}, fmt.Errorf("batch_max_wait_ms must be 0..3600000")
	}
	tx, err := s.Store.Pool.Begin(ctx)
	if err != nil {
		return VersionedResource{}, err
	}
	defer tx.Rollback(ctx)
	out := VersionedResource{Status: "ACTIVE"}
	err = tx.QueryRow(ctx, `INSERT INTO job_definitions(project_id,function_id,queue_id,retry_policy_id,name,default_priority,timeout_ms,execution_mode,batch_size,batch_max_wait_ms)
		SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10
		WHERE EXISTS(SELECT 1 FROM function_definitions WHERE id=$2 AND project_id=$1 AND status='ACTIVE' AND deleted_at IS NULL FOR UPDATE)
		AND EXISTS(SELECT 1 FROM queues WHERE id=$3 AND project_id=$1 AND status='ACTIVE' AND deleted_at IS NULL FOR UPDATE)
		AND ($4::uuid IS NULL OR EXISTS(SELECT 1 FROM retry_policies WHERE id=$4 AND project_id=$1 AND status='ACTIVE' AND deleted_at IS NULL FOR UPDATE))
		AND ($8 <> 'BATCH' OR NOT EXISTS(SELECT 1 FROM queues q JOIN queue_backends qb ON qb.id=q.backend_id WHERE q.id=$3 AND qb.backend_type <> 'POSTGRES' AND qb.status='ACTIVE' AND qb.deleted_at IS NULL))
		AND EXISTS(SELECT 1 FROM function_definitions fd JOIN worker_function_capabilities c ON c.function_key=fd.function_key AND (c.function_version=fd.version OR c.function_version='*') AND c.execution_mode=$8 JOIN workers w ON w.id=c.worker_id WHERE fd.id=$2 AND w.status='ONLINE' AND w.heartbeat_at>now()-interval '30 seconds')
		RETURNING id,row_version`, in.ProjectID, in.FunctionID, in.QueueID, in.RetryPolicyID, in.Name, in.DefaultPriority, in.TimeoutMS, in.ExecutionMode, in.BatchSize, in.BatchMaxWaitMS).Scan(&out.ID, &out.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return VersionedResource{}, ErrDependencyNotFound
	}
	if err != nil {
		return VersionedResource{}, err
	}
	if audit != nil {
		if err = audit(ctx, tx, out.ID); err != nil {
			return VersionedResource{}, err
		}
	}
	return out, tx.Commit(ctx)
}
