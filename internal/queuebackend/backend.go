// Package queuebackend defines vendor-neutral delivery semantics. PostgreSQL
// remains the control-plane source of truth regardless of the selected backend.
package queuebackend

import (
	"context"
	"errors"
	"time"

	"github.com/example/task-processing/internal/domain/job"
	"github.com/google/uuid"
)

type Type string

const (
	Postgres     Type = "POSTGRES"
	RedisStreams Type = "REDIS_STREAMS"
	JetStream    Type = "JETSTREAM"
)

var (
	ErrUnknownBackend = errors.New("queue backend is not registered")
	ErrInvalidMessage = errors.New("queue message is invalid")
)

// TransportAckError means PostgreSQL already accepted the terminal transition
// but the broker acknowledgement was not confirmed. Callers may continue
// durable follow-up work; the broker will redeliver and the ownership fence
// will discard the duplicate safely.
type TransportAckError struct{ Err error }

func (e *TransportAckError) Error() string { return "transport acknowledgement: " + e.Err.Error() }
func (e *TransportAckError) Unwrap() error { return e.Err }

// Capabilities tell the control plane which feature must be emulated instead
// of assuming vendor primitives exist.
type Capabilities struct {
	NativePriority bool `json:"native_priority"`
	NativeDelay    bool `json:"native_delay"`
	NativeDLQ      bool `json:"native_dlq"`
	NativePause    bool `json:"native_pause"`
	AtomicBatch    bool `json:"atomic_batch"`
	ConsumerGroups bool `json:"consumer_groups"`
}

type Message struct {
	DispatchID  uuid.UUID    `json:"dispatch_id"`
	RunID       uuid.UUID    `json:"run_id"`
	ProjectID   uuid.UUID    `json:"project_id"`
	QueueID     uuid.UUID    `json:"queue_id"`
	Priority    job.Priority `json:"priority"`
	AvailableAt time.Time    `json:"available_at"`
}

type ReserveRequest struct {
	WorkerID uuid.UUID
	Owner    string
	Limit    int
	Lease    time.Duration
}

type Delivery struct {
	Message   Message       `json:"message"`
	Execution job.Execution `json:"execution"`
	Owner     string        `json:"-"`
	// Receipt is transport-specific acknowledgement metadata. It is opaque to
	// callers and never persisted as job state; PostgreSQL delivery leaves it
	// empty while stream adapters store the stream/message sequence here.
	Receipt string `json:"-"`
}

// BatchReserveRequest is a protocol extension, separate from Reserve so an
// adapter cannot accidentally claim a BATCH definition as independent jobs.
// A conforming adapter must atomically reserve every message in a delivery and
// retain receipts until PostgreSQL persists every item result.
type BatchReserveRequest struct {
	ReserveRequest
	QueueID        uuid.UUID
	MaxItems       int
	MaxWait        time.Duration
	BatchGeneration uuid.UUID
}
type BatchDelivery struct {
	Messages   []Message
	Receipts   []string
	Generation uuid.UUID
}

// BatchBackend is deliberately optional. Redis Streams and JetStream remain
// ineligible for BATCH execution until their adapter implements this contract
// and its acknowledgement/lease integration tests. This is a safety gate, not
// a capability flag that can be toggled by configuration.
type BatchBackend interface {
	ReserveBatch(context.Context, BatchReserveRequest) ([]BatchDelivery, error)
	AckBatch(context.Context, BatchDelivery) error
	NackBatch(context.Context, BatchDelivery, error) error
}

type QueueStats struct {
	Queued  int64      `json:"queued"`
	Running int64      `json:"running"`
	Retry   int64      `json:"retry"`
	Oldest  *time.Time `json:"oldest_available_at,omitempty"`
}

// Backend is intentionally narrow and semantic. It supports at-least-once
// delivery; callers must make handlers idempotent using the job run key.
type Backend interface {
	Type() Type
	Enqueue(context.Context, Message) error
	EnqueueBatch(context.Context, []Message) error
	Reserve(context.Context, ReserveRequest) ([]Delivery, error)
	Ack(context.Context, Delivery) error
	Nack(context.Context, Delivery, error) error
	Depth(context.Context, uuid.UUID) (QueueStats, error)
	Health(context.Context) error
	Capabilities() Capabilities
}

func (m Message) Validate() error {
	if m.DispatchID == uuid.Nil || m.RunID == uuid.Nil || m.ProjectID == uuid.Nil || m.QueueID == uuid.Nil {
		return ErrInvalidMessage
	}
	return nil
}
