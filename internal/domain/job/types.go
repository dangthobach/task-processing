package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	Created        Status = "CREATED"
	EnqueuePending Status = "ENQUEUE_PENDING"
	Queued         Status = "QUEUED"
	Reserved       Status = "RESERVED"
	Running        Status = "RUNNING"
	RetryWait      Status = "RETRY_WAIT"
	Succeeded      Status = "SUCCEEDED"
	DeadLetter     Status = "DEAD_LETTER"
	Cancelled      Status = "CANCELLED"
	TimedOut       Status = "TIMED_OUT"
)

type ErrorClass string

const (
	Transient             ErrorClass = "TRANSIENT"
	RateLimited           ErrorClass = "RATE_LIMITED"
	DependencyUnavailable ErrorClass = "DEPENDENCY_UNAVAILABLE"
	Timeout               ErrorClass = "TIMEOUT"
	Validation            ErrorClass = "VALIDATION"
	Authorization         ErrorClass = "AUTHORIZATION"
	Permanent             ErrorClass = "PERMANENT"
	Panic                 ErrorClass = "PANIC"
	CancelledError        ErrorClass = "CANCELLED"
)

type Priority int16

const (
	Bulk     Priority = 1
	Low      Priority = 2
	Normal   Priority = 3
	High     Priority = 4
	Critical Priority = 5
)

type RetryPolicy struct {
	MaxAttempts          int     `json:"max_attempts"`
	Strategy             string  `json:"strategy"`
	InitialDelayMS       int64   `json:"initial_delay_ms"`
	Multiplier           float64 `json:"multiplier"`
	MaxDelayMS           int64   `json:"max_delay_ms"`
	JitterPct            float64 `json:"jitter_pct"`
	RetryTimeout         bool    `json:"retry_timeout"`
	RetryRateLimited     bool    `json:"retry_rate_limited"`
	RetryDependencyError bool    `json:"retry_dependency_error"`
	RetryValidationError bool    `json:"retry_validation_error"`
}
type PolicySnapshot struct {
	Retry     RetryPolicy `json:"retry"`
	TimeoutMS int64       `json:"timeout_ms"`
}
type Run struct {
	ID              uuid.UUID       `json:"id"`
	ProjectID       uuid.UUID       `json:"project_id"`
	DefinitionID    uuid.UUID       `json:"job_definition_id"`
	QueueID         uuid.UUID       `json:"queue_id"`
	FunctionKey     string          `json:"function_key"`
	FunctionVersion string          `json:"function_version"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	Status          Status          `json:"status"`
	Priority        Priority        `json:"priority"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	Policy          PolicySnapshot  `json:"policy"`
	ScheduledFor    *time.Time      `json:"scheduled_for,omitempty"`
	AttemptNumber   int             `json:"attempt_number,omitempty"`
}
type Execution struct {
	RunID           uuid.UUID
	AttemptID       uuid.UUID
	TenantID        uuid.UUID
	ProjectID       uuid.UUID
	QueueID         uuid.UUID
	DefinitionID    uuid.UUID
	FunctionKey     string
	FunctionVersion string
	IdempotencyKey  string
	Payload         json.RawMessage
	Policy          PolicySnapshot
	Attempt         int
}

// ExecutionContext is the typed, per-attempt context supplied to every handler.
// It carries stable platform IDs for correlation and is the supported place for
// cancellation/deadline propagation. Side effects should use IdempotencyKey.
type ExecutionContext struct {
	context.Context
	Execution *Execution
	progress  ProgressReporter
}
type ProgressReporter interface {
	ReportProgress(context.Context, *Execution, int, string) error
}

func (c *ExecutionContext) ReportProgress(percent int, message string) error {
	if percent < 0 || percent > 100 {
		return fmt.Errorf("progress must be between 0 and 100")
	}
	if c.progress == nil {
		return errors.New("progress reporting is unavailable")
	}
	return c.progress.ReportProgress(c.Context, c.Execution, percent, message)
}
func NewExecutionContext(ctx context.Context, ex *Execution, progress ProgressReporter) *ExecutionContext {
	return &ExecutionContext{Context: ctx, Execution: ex, progress: progress}
}

type Handler func(*ExecutionContext) error

type BatchStatus string

const (
	BatchReserved      BatchStatus = "RESERVED"
	BatchRunning       BatchStatus = "RUNNING"
	BatchSucceeded     BatchStatus = "SUCCEEDED"
	BatchPartialFailed BatchStatus = "PARTIAL_FAILED"
	BatchFailed        BatchStatus = "FAILED"
)

type BatchItem struct {
	ItemID  uuid.UUID
	Run     Execution
	Ordinal int
}
type BatchExecution struct {
	BatchID, AttemptID               uuid.UUID
	ProjectID, QueueID, DefinitionID uuid.UUID
	FunctionKey                      string
	Policy                           PolicySnapshot
	Items                            []BatchItem
	Attempt                          int
}
type BatchItemResult struct {
	ItemID  uuid.UUID
	Success bool
	Error   error
}
type BatchHandler func(*BatchExecutionContext) ([]BatchItemResult, error)
type BatchProgressReporter interface {
	ReportBatchProgress(context.Context, *BatchExecution, int, string) error
	WriteBatchLog(context.Context, *BatchExecution, string, string, map[string]any) error
}
type BatchExecutionContext struct {
	context.Context
	Batch    *BatchExecution
	reporter BatchProgressReporter
}

func NewBatchExecutionContext(ctx context.Context, batch *BatchExecution, reporter BatchProgressReporter) *BatchExecutionContext {
	return &BatchExecutionContext{Context: ctx, Batch: batch, reporter: reporter}
}
func (c *BatchExecutionContext) ReportProgress(processed int, message string) error {
	if processed < 0 || processed > len(c.Batch.Items) {
		return fmt.Errorf("processed must be between 0 and %d", len(c.Batch.Items))
	}
	if c.reporter == nil {
		return errors.New("batch progress reporting is unavailable")
	}
	return c.reporter.ReportBatchProgress(c.Context, c.Batch, processed, message)
}
func (c *BatchExecutionContext) Log(level, message string, fields map[string]any) error {
	if c.reporter == nil {
		return errors.New("batch logging is unavailable")
	}
	return c.reporter.WriteBatchLog(c.Context, c.Batch, level, message, fields)
}

type ClassifiedError struct {
	Class ErrorClass
	Code  string
	Err   error
}

func (e *ClassifiedError) Error() string { return e.Err.Error() }
func (e *ClassifiedError) Unwrap() error { return e.Err }
func Classify(err error) *ClassifiedError {
	var c *ClassifiedError
	if errors.As(err, &c) {
		return c
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &ClassifiedError{Class: Timeout, Code: "DEADLINE_EXCEEDED", Err: err}
	}
	return &ClassifiedError{Class: Transient, Code: "UNCLASSIFIED", Err: err}
}
func Retryable(class ErrorClass, p RetryPolicy) bool {
	switch class {
	case Validation:
		return p.RetryValidationError
	case Timeout:
		return p.RetryTimeout
	case RateLimited:
		return p.RetryRateLimited
	case DependencyUnavailable:
		return p.RetryDependencyError
	case Authorization, Permanent, CancelledError:
		return false
	default:
		return true
	}
}
func MustPayload(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	if !json.Valid(raw) {
		panic(fmt.Sprintf("invalid JSON payload: %s", raw))
	}
	return raw
}
