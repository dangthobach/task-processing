package postgres

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"time"
)

type RealtimeEvent struct {
	ID            int64           `json:"id"`
	ProjectID     uuid.UUID       `json:"project_id"`
	EventType     string          `json:"event_type"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   uuid.UUID       `json:"aggregate_id"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
}

func (s *Store) WriteRealtimeEvent(ctx context.Context, project uuid.UUID, eventType, aggregateType string, aggregateID uuid.UUID, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,$2,$3,$4,$5)", project, eventType, aggregateType, aggregateID, raw)
	return err
}
func (s *Store) ListRealtimeEvents(ctx context.Context, project uuid.UUID, after int64, limit int) ([]RealtimeEvent, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	rows, err := s.Pool.Query(ctx, "SELECT id,project_id,event_type,aggregate_type,aggregate_id,payload,created_at FROM realtime_events WHERE project_id=$1 AND id>$2 ORDER BY id LIMIT $3", project, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RealtimeEvent{}
	for rows.Next() {
		var e RealtimeEvent
		if err = rows.Scan(&e.ID, &e.ProjectID, &e.EventType, &e.AggregateType, &e.AggregateID, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
