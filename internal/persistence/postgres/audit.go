package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/trace"
)

type AuditOutboxEvent struct {
	ID, ProjectID, AggregateID uuid.UUID
	Token                      uuid.UUID
	EventType, AggregateType   string
	Payload                    json.RawMessage
	CreatedAt                  time.Time
}

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

// ClaimAuditOutbox only holds a transaction while assigning ownership. Network
// delivery happens after commit, so a slow SIEM cannot starve PostgreSQL locks.
func (s *Store) ClaimAuditOutbox(ctx context.Context, owner string, limit int, lease time.Duration) ([]AuditOutboxEvent, error) {
	if limit < 1 {
		limit = 100
	}
	if lease < time.Second {
		lease = time.Second
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `WITH candidates AS (SELECT id FROM audit_outbox_events WHERE published_at IS NULL AND expired_at IS NULL AND available_at<=now() AND (claim_expires_at IS NULL OR claim_expires_at<=now()) ORDER BY available_at,id FOR UPDATE SKIP LOCKED LIMIT $1) UPDATE audit_outbox_events e SET claim_token=gen_random_uuid(),claimed_by=$2,claim_expires_at=now()+$3::interval FROM candidates c WHERE e.id=c.id RETURNING e.id,e.project_id,e.event_type,e.aggregate_type,e.aggregate_id,e.payload,e.claim_token,e.created_at`, limit, owner, lease.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []AuditOutboxEvent{}
	for rows.Next() {
		var item AuditOutboxEvent
		if err = rows.Scan(&item.ID, &item.ProjectID, &item.EventType, &item.AggregateType, &item.AggregateID, &item.Payload, &item.Token, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) MarkAuditOutboxPublished(ctx context.Context, item AuditOutboxEvent) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, "UPDATE audit_outbox_events SET published_at=now(),attempts=attempts+1,last_error=NULL,claim_token=NULL,claimed_by=NULL,claim_expires_at=NULL WHERE id=$1 AND claim_token=$2 AND published_at IS NULL", item.ID, item.Token)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("audit outbox lease lost")
	}
	if _, err = tx.Exec(ctx, "INSERT INTO realtime_events(project_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,$2,$3,$4,$5)", item.ProjectID, item.EventType, item.AggregateType, item.AggregateID, item.Payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) RecordAuditOutboxFailure(ctx context.Context, item AuditOutboxEvent, cause error) error {
	// Exponential retry is capped at five minutes. At 20 attempts the immutable
	// audit_log remains available while this poison delivery is explicitly expired.
	tag, err := s.Pool.Exec(ctx, `UPDATE audit_outbox_events SET attempts=attempts+1,last_error=$3,available_at=now()+make_interval(secs => LEAST(300, power(2,LEAST(attempts,8))::int)),claim_token=NULL,claimed_by=NULL,claim_expires_at=NULL,expired_at=CASE WHEN attempts+1>=20 THEN now() ELSE NULL END WHERE id=$1 AND claim_token=$2 AND published_at IS NULL`, item.ID, item.Token, truncateAuditError(cause))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("audit outbox lease lost")
	}
	return nil
}
func truncateAuditError(err error) string {
	if err == nil {
		return "unknown audit delivery failure"
	}
	value := err.Error()
	if len(value) > 1024 {
		return value[:1024]
	}
	return value
}
