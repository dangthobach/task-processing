package scheduler

import (
	"context"
	"errors"
	"fmt"
	"github.com/example/task-processing/internal/application/scheduling"
	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"log/slog"
	"sync"
	"time"
)

type Service struct {
	Store      *postgres.Store
	Log        *slog.Logger
	InstanceID string
}
type scheduleRow struct {
	ID, DefinitionID, ProjectID uuid.UUID
	Type, Cron, Timezone        string
	WithSeconds                 bool
	RunAt                       *time.Time
}
type registeredJob struct {
	jobID       uuid.UUID
	fingerprint string
}

func (s *Service) Run(ctx context.Context) error {
	if s.InstanceID == "" {
		s.InstanceID = uuid.NewString()
	}
	// The advisory lock is held on a dedicated connection for this service's lifetime.
	// Database occurrence uniqueness remains the second line of defense against duplicate triggers.
	conn, err := s.Store.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var leader bool
	for !leader {
		if err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext('task-processing-scheduler-leader'))").Scan(&leader); err != nil {
			return err
		}
		if !leader {
			s.Log.Debug("scheduler standby: leader lock is held by another replica")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
	}
	defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock(hashtext('task-processing-scheduler-leader'))")
	s.Log.Info("scheduler leader elected", "instance_id", s.InstanceID)
	sch, err := gocron.NewScheduler()
	if err != nil {
		return err
	}
	registry := map[uuid.UUID]registeredJob{}
	var registryMu sync.Mutex
	if err = s.reconcile(ctx, sch, registry, &registryMu); err != nil {
		return err
	}
	sch.Start()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return sch.Shutdown()
		case <-ticker.C:
			if reconcileErr := s.reconcile(ctx, sch, registry, &registryMu); reconcileErr != nil && !errors.Is(reconcileErr, context.Canceled) {
				s.Log.Error("schedule reconciliation failed", "error", reconcileErr)
			}
		}
	}
}

func (s *Service) reconcile(ctx context.Context, sch gocron.Scheduler, registry map[uuid.UUID]registeredJob, registryMu *sync.Mutex) error {
	states, err := s.Store.ListActiveSchedulePlans(ctx)
	if err != nil {
		return err
	}
	active := make(map[uuid.UUID]struct{}, len(states))
	registryMu.Lock()
	defer registryMu.Unlock()
	for _, state := range states {
		active[state.ID] = struct{}{}
		if err = s.evaluate(ctx, state.ID); err != nil {
			s.Log.Error("schedule evaluation failed", "schedule_id", state.ID, "error", err)
			continue
		}
		fingerprint := fingerprint(state)
		if current, exists := registry[state.ID]; exists && current.fingerprint == fingerprint {
			continue
		}
		if current, exists := registry[state.ID]; exists {
			if err = sch.RemoveJob(current.jobID); err != nil {
				return fmt.Errorf("remove changed schedule %s: %w", state.ID, err)
			}
			delete(registry, state.ID)
		}
		jobID, registerErr := s.registerWakeup(ctx, sch, state)
		if registerErr != nil {
			s.writeLog(ctx, state.ProjectID, &state.ID, "ERROR", "scheduler.schedule_invalid", registerErr.Error(), nil)
			continue
		}
		if jobID != uuid.Nil {
			registry[state.ID] = registeredJob{jobID: jobID, fingerprint: fingerprint}
		}
	}
	for scheduleID, current := range registry {
		if _, exists := active[scheduleID]; exists {
			continue
		}
		if err = sch.RemoveJob(current.jobID); err != nil {
			return fmt.Errorf("remove deleted schedule %s: %w", scheduleID, err)
		}
		delete(registry, scheduleID)
	}
	return nil
}

func (s *Service) registerWakeup(ctx context.Context, sch gocron.Scheduler, state postgres.SchedulePlanState) (uuid.UUID, error) {
	var definition gocron.JobDefinition
	if state.Type == "ONE_TIME" {
		if state.RunAt == nil || !state.RunAt.After(time.Now()) {
			return uuid.Nil, nil
		}
		definition = gocron.OneTimeJob(gocron.OneTimeJobStartDateTime(*state.RunAt))
	} else {
		expr, err := (scheduling.ScheduleSpec{Cron: state.Cron, Timezone: state.Timezone}).CronExpression()
		if err != nil {
			return uuid.Nil, err
		}
		definition = gocron.CronJob(expr, state.WithSeconds)
	}
	trigger := state
	registered, err := sch.NewJob(definition, gocron.NewTask(func() {
		if e := s.evaluate(ctx, trigger.ID); e != nil && !errors.Is(e, context.Canceled) {
			s.Log.Error("schedule evaluation failed", "schedule_id", trigger.ID, "error", e)
		}
	}))
	if err != nil {
		return uuid.Nil, err
	}
	s.writeLog(ctx, state.ProjectID, &state.ID, "INFO", "scheduler.schedule_registered", "schedule registered as planner wake-up", map[string]any{"schedule_type": state.Type})
	return registered.ID(), nil
}

func fingerprint(state postgres.SchedulePlanState) string {
	runAt := ""
	if state.RunAt != nil {
		runAt = state.RunAt.UTC().Format(time.RFC3339Nano)
	}
	return state.ID.String() + "|" + state.ProjectID.String() + "|" + state.DefinitionID.String() + "|" + state.Type + "|" + state.Cron + "|" + state.Timezone + fmt.Sprintf("|%t|", state.WithSeconds) + runAt + "|" + state.MisfirePolicy
}

func (s *Service) evaluate(ctx context.Context, id uuid.UUID) error {
	state, err := s.Store.LoadSchedulePlan(ctx, id)
	if err != nil {
		return err
	}
	spec := scheduling.ScheduleSpec{ID: state.ID, DefinitionID: state.DefinitionID, ProjectID: state.ProjectID, Type: state.Type, Cron: state.Cron, WithSeconds: state.WithSeconds, Timezone: state.Timezone, RunAt: state.RunAt, MisfirePolicy: state.MisfirePolicy, CreatedAt: state.CreatedAt}
	decision, err := scheduling.Evaluate(spec, state.Cursor, time.Now())
	if err != nil {
		return err
	}
	if decision.EvaluatedThrough.Equal(state.CreatedAt) && state.Cursor == nil {
		return nil
	}
	// Submit before cursor advancement: a crash may repeat submission, which is
	// safe due to occurrence idempotency; advancing first could lose a run.
	if decision.Occurrence != nil {
		run, submitErr := s.Store.Submit(ctx, postgres.Submit{ProjectID: decision.Occurrence.ProjectID, DefinitionID: decision.Occurrence.DefinitionID, ScheduleID: &decision.Occurrence.ScheduleID, ScheduledFor: &decision.Occurrence.ScheduledFor})
		if submitErr != nil {
			return submitErr
		}
		s.writeLog(ctx, state.ProjectID, &state.ID, "INFO", "scheduler.triggered", "scheduled occurrence accepted", map[string]any{"scheduled_for": decision.Occurrence.ScheduledFor, "job_run_id": run.ID})
	}
	if err = s.Store.AdvanceScheduleCursor(ctx, state.ID, decision.EvaluatedThrough); err != nil {
		return err
	}
	if state.Type == "ONE_TIME" && state.RunAt != nil && !decision.EvaluatedThrough.Before(state.RunAt.UTC()) {
		return s.Store.CompleteOneTimeSchedule(ctx, state.ID)
	}
	return nil
}

func (s *Service) writeLog(ctx context.Context, project uuid.UUID, schedule *uuid.UUID, level, event, message string, details any) {
	if err := s.Store.WriteSchedulerLog(ctx, postgres.SchedulerLogInput{ProjectID: project, ScheduleID: schedule, SchedulerInstanceID: s.InstanceID, Level: level, EventType: event, Message: message, Details: details}); err != nil {
		s.Log.Error("scheduler log persistence failed", "event_type", event, "error", err)
	}
}
