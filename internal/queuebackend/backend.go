// Package queuebackend defines vendor-neutral delivery semantics. PostgreSQL
// remains the control-plane source of truth regardless of the selected backend.
package queuebackend

import (
	"context"
	"encoding/json"
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
	RunID       uuid.UUID       `json:"run_id"`
	ProjectID   uuid.UUID       `json:"project_id"`
	QueueID     uuid.UUID       `json:"queue_id"`
	Priority    job.Priority    `json:"priority"`
	AvailableAt time.Time       `json:"available_at"`
	Payload     json.RawMessage `json:"payload,omitempty"`
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
	if m.RunID == uuid.Nil || m.ProjectID == uuid.Nil || m.QueueID == uuid.Nil {
		return ErrInvalidMessage
	}
	return nil
}
