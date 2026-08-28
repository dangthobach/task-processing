package controlplane

import (
	"context"
	"fmt"
	"strings"

	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
)

type RetentionPolicyService struct{ Store *postgres.Store }
type CreateRetentionPolicy struct {
	ProjectID     uuid.UUID
	ResourceType  string
	RetentionDays int
}
type RetentionPolicy struct {
	ID      uuid.UUID
	Version int64
	Status  string
}

func (s RetentionPolicyService) Create(ctx context.Context, in CreateRetentionPolicy) (RetentionPolicy, error) {
	if s.Store == nil {
		return RetentionPolicy{}, fmt.Errorf("retention policy store is required")
	}
	in.ResourceType = strings.TrimSpace(in.ResourceType)
	if in.ProjectID == uuid.Nil || in.RetentionDays < 1 || in.RetentionDays > 3650 {
		return RetentionPolicy{}, fmt.Errorf("project_id and retention_days (1..3650) are required")
	}
	switch in.ResourceType {
	case "JOB_LOG", "SCHEDULER_LOG", "REALTIME_EVENT", "TERMINAL_RUN":
	default:
		return RetentionPolicy{}, fmt.Errorf("resource_type is invalid or protected from retention")
	}
	var out RetentionPolicy
	err := s.Store.Pool.QueryRow(ctx, "INSERT INTO retention_policies(project_id,resource_type,retention_days) VALUES($1,$2,$3) RETURNING id,row_version,status", in.ProjectID, in.ResourceType, in.RetentionDays).Scan(&out.ID, &out.Version, &out.Status)
	return out, err
}
