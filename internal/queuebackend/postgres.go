package queuebackend

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/example/task-processing/internal/domain/job"
	store "github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
)

// PostgresBackend is the MVP adapter. It uses SKIP LOCKED in the store for
// reserve and preserves the same at-least-once lifecycle as external backends.
type PostgresBackend struct{ Store *store.Store }

func NewPostgres(store *store.Store) *PostgresBackend { return &PostgresBackend{Store: store} }
func (b *PostgresBackend) Type() Type                 { return Postgres }
func (b *PostgresBackend) Capabilities() Capabilities {
	return Capabilities{NativePriority: true, NativeDelay: true, NativeDLQ: true, NativePause: true, AtomicBatch: true, ConsumerGroups: false}
}
func (b *PostgresBackend) Enqueue(ctx context.Context, message Message) error {
	if err := message.Validate(); err != nil {
		return err
	}
	return b.Store.EnqueueDispatches(ctx, []store.DispatchRef{{RunID: message.RunID, DispatchID: message.DispatchID}})
}
func (b *PostgresBackend) EnqueueBatch(ctx context.Context, messages []Message) error {
	dispatches := make([]store.DispatchRef, 0, len(messages))
	for _, message := range messages {
		if err := message.Validate(); err != nil {
			return err
		}
		dispatches = append(dispatches, store.DispatchRef{RunID: message.RunID, DispatchID: message.DispatchID})
	}
	return b.Store.EnqueueDispatches(ctx, dispatches)
}
func (b *PostgresBackend) Reserve(ctx context.Context, req ReserveRequest) ([]Delivery, error) {
	if req.WorkerID == uuid.Nil || req.Owner == "" || req.Limit < 1 {
		return nil, fmt.Errorf("%w: worker_id, owner and positive limit are required", ErrInvalidMessage)
	}
	if req.Lease <= 0 {
		req.Lease = time.Minute
	}
	runs, err := b.Store.Claim(ctx, req.WorkerID, req.Owner, req.Limit, req.Lease)
	if err != nil {
		return nil, err
	}
	deliveries := make([]Delivery, 0, len(runs))
	for _, run := range runs {
		execution, startErr := b.Store.StartAttempt(ctx, run.ID, req.WorkerID, req.Owner, run.LeaseToken, req.Lease)
		if errors.Is(startErr, store.ErrConcurrencyLimited) {
			_ = b.Store.ReleaseReservation(ctx, run.ID, req.Owner, run.LeaseToken)
			continue
		}
		if startErr != nil {
			_ = b.Store.ReleaseReservation(ctx, run.ID, req.Owner, run.LeaseToken)
			return deliveries, startErr
		}
		deliveries = append(deliveries, Delivery{Message: Message{DispatchID: run.DispatchID, RunID: run.ID, ProjectID: run.ProjectID, QueueID: run.QueueID, Priority: run.Priority}, Execution: execution, Owner: req.Owner})
	}
	return deliveries, nil
}
func (b *PostgresBackend) Ack(ctx context.Context, delivery Delivery) error {
	return b.Store.CompleteSuccess(ctx, delivery.Execution, delivery.Owner)
}
func (b *PostgresBackend) Nack(ctx context.Context, delivery Delivery, reason error) error {
	if reason == nil {
		reason = errors.New("backend nack without an error")
	}
	classified := job.Classify(reason)
	return b.Store.CompleteFailure(ctx, delivery.Execution, delivery.Owner, classified, delivery.Execution.Policy.Retry, time.Now())
}
func (b *PostgresBackend) Depth(ctx context.Context, queue uuid.UUID) (QueueStats, error) {
	stats, err := b.Store.QueueDepth(ctx, queue)
	return QueueStats{Queued: stats.Queued, Running: stats.Running, Retry: stats.Retry, Oldest: stats.Oldest}, err
}
func (b *PostgresBackend) Health(ctx context.Context) error { return b.Store.Pool.Ping(ctx) }
