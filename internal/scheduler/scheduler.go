package scheduler

import (
	"context"
	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"log/slog"
	"time"
)

type Service struct {
	Store      *postgres.Store
	Log        *slog.Logger
	InstanceID string
}
type scheduleRow struct {
	ID, DefinitionID, ProjectID uuid.UUID
	Cron                        string
	WithSeconds                 bool
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
	rows, err := s.Store.Pool.Query(ctx, `SELECT s.id,s.job_definition_id,jd.project_id,s.cron_expression,s.with_seconds FROM schedules s JOIN job_definitions jd ON jd.id=s.job_definition_id WHERE s.status='ACTIVE' AND s.deleted_at IS NULL AND s.schedule_type='CRON' AND jd.deleted_at IS NULL`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var r scheduleRow
		if err = rows.Scan(&r.ID, &r.DefinitionID, &r.ProjectID, &r.Cron, &r.WithSeconds); err != nil {
			return err
		}
		trigger := r
		s.writeLog(ctx, trigger.ProjectID, &trigger.ID, "INFO", "scheduler.schedule_registered", "schedule registered with gocron", map[string]any{"cron_expression": trigger.Cron, "with_seconds": trigger.WithSeconds})
		_, err = sch.NewJob(gocron.CronJob(trigger.Cron, trigger.WithSeconds), gocron.NewTask(func() {
			now := time.Now().UTC()
			if !trigger.WithSeconds {
				now = now.Truncate(time.Minute)
			}
			run, e := s.Store.Submit(context.Background(), postgres.Submit{ProjectID: trigger.ProjectID, DefinitionID: trigger.DefinitionID, ScheduleID: &trigger.ID, ScheduledFor: &now})
			if e != nil {
				s.Log.Error("schedule trigger failed", "schedule_id", trigger.ID, "error", e)
				s.writeLog(context.Background(), trigger.ProjectID, &trigger.ID, "ERROR", "scheduler.trigger_failed", "could not create scheduled run", map[string]any{"scheduled_for": now, "error": e.Error()})
				return
			}
			s.writeLog(context.Background(), trigger.ProjectID, &trigger.ID, "INFO", "scheduler.triggered", "scheduled run was accepted", map[string]any{"scheduled_for": now, "job_run_id": run.ID, "status": run.Status})
		}))
		if err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	sch.Start()
	<-ctx.Done()
	return sch.Shutdown()
}

func (s *Service) writeLog(ctx context.Context, project uuid.UUID, schedule *uuid.UUID, level, event, message string, details any) {
	if err := s.Store.WriteSchedulerLog(ctx, postgres.SchedulerLogInput{ProjectID: project, ScheduleID: schedule, SchedulerInstanceID: s.InstanceID, Level: level, EventType: event, Message: message, Details: details}); err != nil {
		s.Log.Error("scheduler log persistence failed", "event_type", event, "error", err)
	}
}
