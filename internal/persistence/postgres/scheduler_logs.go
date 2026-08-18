package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type SchedulerLog struct {
	ID                  uuid.UUID       `json:"id"`
	ProjectID           uuid.UUID       `json:"project_id"`
	ScheduleID          *uuid.UUID      `json:"schedule_id,omitempty"`
	SchedulerInstanceID string          `json:"scheduler_instance_id"`
	Level               string          `json:"level"`
	EventType           string          `json:"event_type"`
	Message             string          `json:"message"`
	Details             json.RawMessage `json:"details"`
	OccurredAt          time.Time       `json:"occurred_at"`
}
type SchedulerLogInput struct {
	ProjectID                                      uuid.UUID
	ScheduleID                                     *uuid.UUID
	SchedulerInstanceID, Level, EventType, Message string
	Details                                        any
}

func (s *Store) WriteSchedulerLog(ctx context.Context, in SchedulerLogInput) error {
	if in.ProjectID == uuid.Nil {
		return fmt.Errorf("scheduler log requires project ID")
	}
	if in.Level == "" {
		in.Level = "INFO"
	}
	if in.Details == nil {
		in.Details = map[string]any{}
	}
	details, err := json.Marshal(in.Details)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, `INSERT INTO scheduler_logs(project_id,schedule_id,scheduler_instance_id,level,event_type,message,details) VALUES($1,$2,$3,$4,$5,$6,$7)`, in.ProjectID, in.ScheduleID, in.SchedulerInstanceID, in.Level, in.EventType, in.Message, details)
	return err
}

type SchedulerLogFilter struct {
	ProjectID  uuid.UUID
	ScheduleID *uuid.UUID
	Limit      int
	Before     *time.Time
	Level      string
}

func (s *Store) ListSchedulerLogs(ctx context.Context, f SchedulerLogFilter) ([]SchedulerLog, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	args := []any{f.ProjectID}
	where := []string{"project_id=$1"}
	if f.ScheduleID != nil {
		args = append(args, *f.ScheduleID)
		where = append(where, fmt.Sprintf("schedule_id=$%d", len(args)))
	}
	if f.Before != nil {
		args = append(args, *f.Before)
		where = append(where, fmt.Sprintf("occurred_at<$%d", len(args)))
	}
	if f.Level != "" {
		args = append(args, f.Level)
		where = append(where, fmt.Sprintf("level=$%d", len(args)))
	}
	args = append(args, f.Limit)
	rows, err := s.Pool.Query(ctx, `SELECT id,project_id,schedule_id,scheduler_instance_id,level,event_type,message,details,occurred_at FROM scheduler_logs WHERE `+strings.Join(where, " AND ")+fmt.Sprintf(" ORDER BY occurred_at DESC,id DESC LIMIT $%d", len(args)), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SchedulerLog{}
	for rows.Next() {
		var l SchedulerLog
		if err = rows.Scan(&l.ID, &l.ProjectID, &l.ScheduleID, &l.SchedulerInstanceID, &l.Level, &l.EventType, &l.Message, &l.Details, &l.OccurredAt); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}
