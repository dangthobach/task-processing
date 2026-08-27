// Package tasks contains application services: orchestration and business rules
// independent from HTTP transport and PostgreSQL SQL details.
package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/example/task-processing/internal/domain/job"
	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

var (
	ErrInvalidInput = errors.New("invalid task input")
	ErrInvalidState = errors.New("invalid state transition")
	ErrNotFound     = errors.New("resource not found")
)

type Service struct {
	Jobs      Jobs
	Queues    Queues
	Schedules Schedules
	DLQ       DLQ
}

func New(store Repository) *Service {
	return &Service{Jobs: Jobs{repo: store}, Queues: Queues{repo: store}, Schedules: Schedules{repo: store}, DLQ: DLQ{repo: store}}
}

// Repository is the infrastructure boundary used by all task business services.
// PostgreSQL is its baseline implementation; another adapter can implement the same contract.
type Repository interface {
	Submit(context.Context, postgres.Submit) (job.Run, error)
	SubmitBulk(context.Context, []postgres.Submit) ([]job.Run, error)
	CancelRun(context.Context, uuid.UUID, uuid.UUID, int64) (bool, error)
	RetryRun(context.Context, uuid.UUID, uuid.UUID, int64) (bool, error)
	CreateQueue(context.Context, postgres.CreateQueue) (uuid.UUID, error)
	TransitionQueue(context.Context, uuid.UUID, uuid.UUID, string, []string, int64) (bool, error)
	CreateSchedule(context.Context, postgres.CreateSchedule) (uuid.UUID, error)
	TransitionSchedule(context.Context, uuid.UUID, uuid.UUID, string, int64) (bool, error)
	ReplayDLQ(context.Context, uuid.UUID, uuid.UUID, int64) (uuid.UUID, bool, error)
}

type SubmitInput struct {
	ProjectID, DefinitionID uuid.UUID
	Payload                 json.RawMessage
	IdempotencyKey          string
	Priority                *job.Priority
}
type Jobs struct{ repo Repository }

func (s Jobs) Submit(ctx context.Context, in SubmitInput) (job.Run, error) {
	if err := validateSubmit(in); err != nil {
		return job.Run{}, err
	}
	return s.repo.Submit(ctx, postgres.Submit{ProjectID: in.ProjectID, DefinitionID: in.DefinitionID, Payload: in.Payload, IdempotencyKey: in.IdempotencyKey, Priority: in.Priority})
}
func (s Jobs) SubmitBulk(ctx context.Context, project, definition uuid.UUID, items []SubmitInput) ([]job.Run, error) {
	if len(items) == 0 || len(items) > 1000 {
		return nil, fmt.Errorf("%w: bulk size must be between 1 and 1000", ErrInvalidInput)
	}
	commands := make([]postgres.Submit, 0, len(items))
	for _, item := range items {
		item.ProjectID = project
		item.DefinitionID = definition
		if err := validateSubmit(item); err != nil {
			return nil, err
		}
		commands = append(commands, postgres.Submit{ProjectID: project, DefinitionID: definition, Payload: item.Payload, IdempotencyKey: item.IdempotencyKey, Priority: item.Priority})
	}
	return s.repo.SubmitBulk(ctx, commands)
}
func (s Jobs) Cancel(ctx context.Context, project, run uuid.UUID, version int64) error {
	ok, err := s.repo.CancelRun(ctx, project, run, version)
	if err != nil {
		return err
	}
	if !ok {
		return ErrInvalidState
	}
	return nil
}
func (s Jobs) Retry(ctx context.Context, project, run uuid.UUID, version int64) error {
	ok, err := s.repo.RetryRun(ctx, project, run, version)
	if err != nil {
		return err
	}
	if !ok {
		return ErrInvalidState
	}
	return nil
}
func validateSubmit(in SubmitInput) error {
	if in.ProjectID == uuid.Nil || in.DefinitionID == uuid.Nil {
		return fmt.Errorf("%w: project_id and job_definition_id are required", ErrInvalidInput)
	}
	if in.Priority != nil && (*in.Priority < job.Bulk || *in.Priority > job.Critical) {
		return fmt.Errorf("%w: priority must be 1..5", ErrInvalidInput)
	}
	if len(in.Payload) > 0 && !json.Valid(in.Payload) {
		return fmt.Errorf("%w: payload must be JSON", ErrInvalidInput)
	}
	return nil
}

type QueueInput struct {
	ProjectID                       uuid.UUID
	Name                            string
	MaxConcurrency, DefaultPriority int
}
type Queues struct{ repo Repository }

func (s Queues) Create(ctx context.Context, in QueueInput) (uuid.UUID, error) {
	if in.ProjectID == uuid.Nil || in.Name == "" {
		return uuid.Nil, fmt.Errorf("%w: project_id and name are required", ErrInvalidInput)
	}
	if in.MaxConcurrency == 0 {
		in.MaxConcurrency = 10
	}
	if in.MaxConcurrency < 1 {
		return uuid.Nil, fmt.Errorf("%w: max_concurrency must be positive", ErrInvalidInput)
	}
	if in.DefaultPriority == 0 {
		in.DefaultPriority = 3
	}
	if in.DefaultPriority < 1 || in.DefaultPriority > 5 {
		return uuid.Nil, fmt.Errorf("%w: default_priority must be 1..5", ErrInvalidInput)
	}
	return s.repo.CreateQueue(ctx, postgres.CreateQueue{ProjectID: in.ProjectID, Name: in.Name, MaxConcurrency: in.MaxConcurrency, DefaultPriority: in.DefaultPriority})
}
func (s Queues) Pause(ctx context.Context, p, q uuid.UUID, version int64) error {
	return s.transition(ctx, p, q, "PAUSED", []string{"ACTIVE"}, version)
}
func (s Queues) Resume(ctx context.Context, p, q uuid.UUID, version int64) error {
	return s.transition(ctx, p, q, "ACTIVE", []string{"PAUSED", "DRAINING"}, version)
}
func (s Queues) Drain(ctx context.Context, p, q uuid.UUID, version int64) error {
	return s.transition(ctx, p, q, "DRAINING", []string{"ACTIVE", "PAUSED"}, version)
}
func (s Queues) transition(ctx context.Context, p, q uuid.UUID, to string, from []string, version int64) error {
	ok, err := s.repo.TransitionQueue(ctx, p, q, to, from, version)
	if err != nil {
		return err
	}
	if !ok {
		return ErrInvalidState
	}
	return nil
}

type ScheduleInput struct {
	ProjectID, DefinitionID uuid.UUID
	Type, Cron, Timezone    string
	MisfirePolicy           string
	WithSeconds             bool
	RunAt                   *time.Time
}
type Schedules struct{ repo Repository }

func (s Schedules) Create(ctx context.Context, in ScheduleInput) (uuid.UUID, error) {
	if in.ProjectID == uuid.Nil || in.DefinitionID == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: project_id and job_definition_id are required", ErrInvalidInput)
	}
	in.Type = strings.ToUpper(strings.TrimSpace(in.Type))
	if in.Type == "" {
		in.Type = "CRON"
	}
	in.MisfirePolicy = strings.ToUpper(strings.TrimSpace(in.MisfirePolicy))
	if in.Timezone == "" {
		in.Timezone = "UTC"
	}
	switch in.Type {
	case "CRON":
		if in.Cron == "" {
			return uuid.Nil, fmt.Errorf("%w: cron_expression is required for CRON", ErrInvalidInput)
		}
		if in.RunAt != nil {
			return uuid.Nil, fmt.Errorf("%w: run_at is only valid for ONE_TIME", ErrInvalidInput)
		}
		upperCron := strings.ToUpper(strings.TrimSpace(in.Cron))
		if strings.HasPrefix(upperCron, "TZ=") || strings.HasPrefix(upperCron, "CRON_TZ=") {
			return uuid.Nil, fmt.Errorf("%w: cron expression must not include a timezone prefix", ErrInvalidInput)
		}
		if _, err := time.LoadLocation(in.Timezone); err != nil {
			return uuid.Nil, fmt.Errorf("%w: timezone: %v", ErrInvalidInput, err)
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		if in.WithSeconds {
			parser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		}
		if _, err := parser.Parse(in.Cron); err != nil {
			return uuid.Nil, fmt.Errorf("%w: cron expression: %v", ErrInvalidInput, err)
		}
		if in.MisfirePolicy == "" {
			in.MisfirePolicy = "FIRE_ONCE"
		}
	case "ONE_TIME":
		if in.Cron != "" || in.WithSeconds {
			return uuid.Nil, fmt.Errorf("%w: cron_expression and with_seconds are not valid for ONE_TIME", ErrInvalidInput)
		}
		if in.RunAt == nil || !in.RunAt.UTC().After(time.Now().UTC()) {
			return uuid.Nil, fmt.Errorf("%w: run_at must be a future RFC3339 timestamp", ErrInvalidInput)
		}
		runAt := in.RunAt.UTC()
		in.RunAt = &runAt
		in.Timezone = "UTC"
		if in.MisfirePolicy == "" {
			in.MisfirePolicy = "FIRE_ONCE"
		}
		if in.MisfirePolicy != "FIRE_ONCE" {
			return uuid.Nil, fmt.Errorf("%w: ONE_TIME requires FIRE_ONCE to guarantee a durable occurrence", ErrInvalidInput)
		}
	default:
		return uuid.Nil, fmt.Errorf("%w: schedule_type must be CRON or ONE_TIME", ErrInvalidInput)
	}
	if in.MisfirePolicy != "SKIP" && in.MisfirePolicy != "FIRE_ONCE" {
		return uuid.Nil, fmt.Errorf("%w: misfire_policy must be SKIP or FIRE_ONCE", ErrInvalidInput)
	}
	return s.repo.CreateSchedule(ctx, postgres.CreateSchedule{ProjectID: in.ProjectID, DefinitionID: in.DefinitionID, Type: in.Type, Cron: in.Cron, Timezone: in.Timezone, WithSeconds: in.WithSeconds, RunAt: in.RunAt, MisfirePolicy: in.MisfirePolicy})
}
func (s Schedules) Pause(ctx context.Context, p, id uuid.UUID, version int64) error {
	return s.state(ctx, p, id, "PAUSED", version)
}
func (s Schedules) Resume(ctx context.Context, p, id uuid.UUID, version int64) error {
	return s.state(ctx, p, id, "ACTIVE", version)
}
func (s Schedules) state(ctx context.Context, p, id uuid.UUID, state string, version int64) error {
	ok, err := s.repo.TransitionSchedule(ctx, p, id, state, version)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}
	return nil
}

type DLQ struct{ repo Repository }

func (s DLQ) Replay(ctx context.Context, p, id uuid.UUID, version int64) (uuid.UUID, error) {
	run, ok, err := s.repo.ReplayDLQ(ctx, p, id, version)
	if err != nil {
		return uuid.Nil, err
	}
	if !ok {
		return uuid.Nil, ErrNotFound
	}
	return run, nil
}
