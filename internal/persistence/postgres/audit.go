package postgres

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/trace"
)

// setSystemAuditContext makes the active worker trace available to database
// triggers for the lifetime of this transaction. It never creates a trace: a
// null trace is correct for maintenance/recovery work with no parent request.
func setSystemAuditContext(ctx context.Context, tx pgx.Tx) error {
	span := trace.SpanContextFromContext(ctx)
	if !span.IsValid() {
		return nil
	}
	_, err := tx.Exec(ctx, "SELECT set_config('app.trace_id',$1,true)", span.TraceID().String())
	return err
}

// DispatchAuditOutbox hands system audit records to the durable realtime event
// stream. A future Kafka/NATS/SIEM adapter can consume this table with the same
// SKIP LOCKED contract instead of this local publisher.
func (s *Store) DispatchAuditOutbox(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		limit = 100
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id,project_id,event_type,aggregate_type,aggregate_id,payload FROM audit_outbox_events WHERE published_at IS NULL AND available_at<=now() ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT $1`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	type event struct {
		id, project, aggregate   uuid.UUID
		eventType, aggregateType string
		payload                  json.RawMessage
	}
	items := []event{}
	for rows.Next() {
		var item event
		if err = rows.Scan(&item.id, &item.project, &item.eventType, &item.aggregateType, &item.aggregate, &item.payload); err != nil {
			return 0, err
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	for _, item := range items {
		if _, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,$2,$3,$4,$5)", item.project, item.eventType, item.aggregateType, item.aggregate, item.payload); err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, "UPDATE audit_outbox_events SET published_at=now(),attempts=attempts+1,last_error=NULL WHERE id=$1", item.id); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(items), nil
}
