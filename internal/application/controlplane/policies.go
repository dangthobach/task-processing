package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func jsonString(value string) json.RawMessage { out, _ := json.Marshal(value); return out }
func jsonNumber(value any) json.RawMessage { out, _ := json.Marshal(value); return out }
func jsonBool(value bool) json.RawMessage { out, _ := json.Marshal(value); return out }

type RetryPolicyInput struct { ProjectID uuid.UUID; Name, Strategy string; MaxAttempts int; InitialDelayMS, MaxDelayMS int64; Multiplier, JitterPct float64; RetryTimeout, RetryRateLimited, RetryDependencyError, RetryValidationError bool }
type RateLimitPolicyInput struct { ProjectID uuid.UUID; Name, Scope string; TargetID *uuid.UUID; Capacity, RefillTokens int; RefillPeriodMS int64; EnforcementPoint string }
type VersionedResource struct { ID uuid.UUID; Version int64; Status string }
type AuditInTransaction func(context.Context, pgx.Tx, uuid.UUID) error

// PolicyService owns validation, dependency guards and the rate bucket's
// atomic creation. HTTP contributes only identity-aware audit metadata via the
// callback, which is executed before the service commits.
type PolicyService struct { Store *postgres.Store }
func (s PolicyService) CreateRetry(ctx context.Context,in RetryPolicyInput,audit AuditInTransaction)(VersionedResource,error){
	if s.Store==nil{return VersionedResource{},fmt.Errorf("policy store is required")};in.Name=strings.TrimSpace(in.Name);patch:=Patch{"name":jsonString(in.Name),"max_attempts":jsonNumber(in.MaxAttempts),"strategy":jsonString(in.Strategy),"initial_delay_ms":jsonNumber(in.InitialDelayMS),"multiplier":jsonNumber(in.Multiplier),"max_delay_ms":jsonNumber(in.MaxDelayMS),"jitter_pct":jsonNumber(in.JitterPct),"retry_timeout":jsonBool(in.RetryTimeout),"retry_rate_limited":jsonBool(in.RetryRateLimited),"retry_dependency_error":jsonBool(in.RetryDependencyError),"retry_validation_error":jsonBool(in.RetryValidationError)};if err:=ValidatePatch("retry_policy",patch);err!=nil{return VersionedResource{},err};if err:=ValidateRetryPolicyValues(in.Strategy,in.InitialDelayMS,in.MaxDelayMS,in.Multiplier);err!=nil{return VersionedResource{},err};tx,err:=s.Store.Pool.Begin(ctx);if err!=nil{return VersionedResource{},err};defer tx.Rollback(ctx);var out VersionedResource;out.Status="ACTIVE";if err=tx.QueryRow(ctx,"INSERT INTO retry_policies(project_id,name,max_attempts,strategy,initial_delay_ms,multiplier,max_delay_ms,jitter_pct,retry_timeout,retry_rate_limited,retry_dependency_error,retry_validation_error) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id,row_version",in.ProjectID,in.Name,in.MaxAttempts,in.Strategy,in.InitialDelayMS,in.Multiplier,in.MaxDelayMS,in.JitterPct,in.RetryTimeout,in.RetryRateLimited,in.RetryDependencyError,in.RetryValidationError).Scan(&out.ID,&out.Version);err!=nil{return VersionedResource{},err};if audit!=nil{if err=audit(ctx,tx,out.ID);err!=nil{return VersionedResource{},err}};return out,tx.Commit(ctx)
}
func (s PolicyService) CreateRateLimit(ctx context.Context,in RateLimitPolicyInput,audit AuditInTransaction)(VersionedResource,error){
	if s.Store==nil{return VersionedResource{},fmt.Errorf("policy store is required")};in.Name=strings.TrimSpace(in.Name);if in.EnforcementPoint==""{in.EnforcementPoint="WORKER_START"};if in.ProjectID==uuid.Nil||in.Name==""||in.Capacity<1||in.RefillTokens<1||in.RefillPeriodMS<1||in.RefillPeriodMS>86400000{return VersionedResource{},fmt.Errorf("name, capacity, refill_tokens and refill_period_ms are invalid")};if in.Scope!="PROJECT"&&in.Scope!="QUEUE"&&in.Scope!="FUNCTION"{return VersionedResource{},fmt.Errorf("scope must be PROJECT, QUEUE or FUNCTION")};if in.EnforcementPoint!="SUBMISSION"&&in.EnforcementPoint!="WORKER_START"{return VersionedResource{},fmt.Errorf("enforcement_point must be SUBMISSION or WORKER_START")};if (in.Scope=="PROJECT")!=(in.TargetID==nil){return VersionedResource{},fmt.Errorf("PROJECT has no target; QUEUE/FUNCTION require target_id")};if in.TargetID!=nil{table:="queues";if in.Scope=="FUNCTION"{table="function_definitions"};var ok bool;if err:=s.Store.Pool.QueryRow(ctx,"SELECT EXISTS(SELECT 1 FROM "+table+" WHERE id=$1 AND project_id=$2 AND deleted_at IS NULL)",*in.TargetID,in.ProjectID).Scan(&ok);err!=nil{return VersionedResource{},err};if !ok{return VersionedResource{},fmt.Errorf("rate-limit target is not active in this project")}};tx,err:=s.Store.Pool.Begin(ctx);if err!=nil{return VersionedResource{},err};defer tx.Rollback(ctx);var out VersionedResource;out.Status="ACTIVE";if err=tx.QueryRow(ctx,"INSERT INTO rate_limit_policies(project_id,name,scope,target_id,capacity,refill_tokens,refill_period_ms,enforcement_point) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id,row_version",in.ProjectID,in.Name,in.Scope,in.TargetID,in.Capacity,in.RefillTokens,in.RefillPeriodMS,in.EnforcementPoint).Scan(&out.ID,&out.Version);err!=nil{return VersionedResource{},err};if _,err=tx.Exec(ctx,"INSERT INTO rate_limit_buckets(policy_id,tokens) VALUES($1,$2)",out.ID,in.Capacity);err!=nil{return VersionedResource{},err};if audit!=nil{if err=audit(ctx,tx,out.ID);err!=nil{return VersionedResource{},err}};return out,tx.Commit(ctx)
}
