package worker

import (
	"context"
	"errors"
	"fmt"
	"github.com/example/task-processing/internal/domain/job"
	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/example/task-processing/internal/queuebackend"
	"github.com/example/task-processing/internal/registry"
	"github.com/example/task-processing/internal/telemetry"
	workermiddleware "github.com/example/task-processing/internal/worker/middleware"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"sync"
	"time"
)

type Worker struct {
	Store       *postgres.Store
	Registry    *registry.Registry
	ID          uuid.UUID
	Owner       string
	Concurrency int
	Lease       time.Duration
	Log         *slog.Logger
	Middleware  []workermiddleware.Middleware
	Backends    *queuebackend.Registry
}

func (w *Worker) Run(ctx context.Context) error {
	if w.Concurrency < 1 {
		w.Concurrency = 1
	}
	if w.Lease <= 0 {
		w.Lease = 60 * time.Second
	}
	if w.Backends == nil {
		w.Backends = queuebackend.NewRegistry(queuebackend.NewPostgres(w.Store))
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()
	defer w.Store.SetWorkerStatus(context.Background(), w.ID, "OFFLINE")
	sem := make(chan struct{}, w.Concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeat.C:
			if err := w.Store.Heartbeat(ctx, w.ID); err != nil {
				w.Log.Error("worker heartbeat failed", "error", err)
			}
			w.refreshMetrics(ctx)
		case <-ticker.C:
			w.dispatchOutbox(ctx)
			_, _ = w.Store.DispatchAuditOutbox(ctx, 100)
			_, _ = w.Store.PromoteRetries(ctx)
			_, _ = w.Store.RecoverExpiredBatches(ctx)
			_, _ = w.Store.RecoverExpiredLeases(ctx)
			capacity := w.Concurrency - len(sem)
			batches, err := w.Store.ClaimBatches(ctx, w.ID, w.Owner, capacity, w.Lease)
			if err != nil {
				w.Log.Error("batch claim failed", "error", err)
			}
			for _, batchID := range batches {
				sem <- struct{}{}
				wg.Add(1)
				go func(id uuid.UUID) { defer func() { <-sem; wg.Done() }(); w.executeBatch(ctx, id) }(batchID)
			}
			capacity = w.Concurrency - len(sem)
			runs, err := w.Store.Claim(ctx, w.ID, w.Owner, capacity, w.Lease)
			if err != nil {
				w.Log.Error("claim failed", "error", err)
				continue
			}
			for _, r := range runs {
				sem <- struct{}{}
				wg.Add(1)
				go func(r job.Run) { defer func() { <-sem; wg.Done() }(); w.execute(ctx, r.ID) }(r)
			}
		}
	}
}
func (w *Worker) refreshMetrics(parent context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), time.Second)
	defer cancel()
	telemetry.Metrics.WorkerHeartbeat.WithLabelValues(w.ID.String()).Set(0)
	queues, err := w.Store.QueueAges(ctx)
	if err != nil {
		w.Log.Warn("queue metrics query failed", "error", err)
		return
	}
	for _, queue := range queues {
		telemetry.Metrics.QueueOldest.WithLabelValues(queue.Name).Set(queue.AgeSeconds)
	}
}

func (w *Worker) dispatchOutbox(ctx context.Context) {
	items, err := w.Store.PendingOutbox(ctx, 100)
	if err != nil {
		w.Log.Error("outbox query failed", "error", err)
		return
	}
	for _, item := range items {
		backend, lookupErr := w.Backends.Get(queuebackend.Type(item.BackendType))
		if lookupErr == nil {
			lookupErr = backend.Enqueue(ctx, queuebackend.Message{RunID: item.RunID, ProjectID: item.ProjectID, QueueID: item.QueueID, Priority: job.Priority(item.Priority), AvailableAt: item.AvailableAt, Payload: item.Payload})
		}
		if lookupErr != nil {
			telemetry.Metrics.OutboxFailures.Inc()
			_ = w.Store.RecordOutboxFailure(ctx, item.ID, lookupErr)
			w.Log.Error("outbox enqueue failed", "outbox_id", item.ID, "run_id", item.RunID, "backend", item.BackendType, "error", lookupErr)
			continue
		}
		if err = w.Store.MarkOutboxPublished(ctx, item.ID); err != nil {
			w.Log.Error("outbox publish acknowledgement failed", "outbox_id", item.ID, "error", err)
		}
	}
}
func (w *Worker) executeBatch(parent context.Context, batchID uuid.UUID) {
	spanCtx, span := otel.Tracer("task-processing/worker").Start(parent, "task.batch", trace.WithAttributes(attribute.String("task.batch_id", batchID.String())))
	defer span.End()
	batch, err := w.Store.StartBatch(spanCtx, batchID, w.ID, w.Owner, w.Lease)
	if err != nil {
		w.Log.Error("start batch failed", "batch_id", batchID, "error", err)
		return
	}
	telemetry.Metrics.Inflight.Inc()
	defer telemetry.Metrics.Inflight.Dec()
	for range batch.Items {
		telemetry.Metrics.Attempts.WithLabelValues(batch.FunctionKey).Inc()
	}
	ctx, cancel := context.WithTimeout(spanCtx, time.Duration(batch.Policy.TimeoutMS)*time.Millisecond)
	defer cancel()
	stopLease := make(chan struct{})
	defer close(stopLease)
	go w.renewBatchLease(ctx, batchID, stopLease)
	h, err := w.Registry.GetBatch(batch.FunctionKey)
	var results []job.BatchItemResult
	if err == nil {
		results, err = w.recoveringBatchCall(job.NewBatchExecutionContext(ctx, &batch, w), h)
	}
	if completeErr := w.Store.CompleteBatch(spanCtx, &batch, w.Owner, results, err); completeErr != nil {
		w.Log.Error("complete batch failed", "batch_id", batchID, "error", completeErr)
		return
	}
	failed := 0
	for _, result := range results {
		if !result.Success {
			failed++
		}
	}
	if err != nil {
		failed = len(batch.Items)
	}
	status := "SUCCEEDED"
	if failed > 0 {
		status = "FAILED"
	}
	telemetry.Metrics.Completed.WithLabelValues(status, "").Add(float64(len(batch.Items)))
}
func (w *Worker) recoveringBatchCall(ctx *job.BatchExecutionContext, h job.BatchHandler) (results []job.BatchItemResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &job.ClassifiedError{Class: job.Panic, Code: "PANIC", Err: fmt.Errorf("batch panic: %v", r)}
		}
	}()
	return h(ctx)
}
func (w *Worker) ReportBatchProgress(ctx context.Context, b *job.BatchExecution, processed int, message string) error {
	return w.Store.ReportBatchProgress(ctx, b, processed, message)
}
func (w *Worker) WriteBatchLog(ctx context.Context, b *job.BatchExecution, level, message string, fields map[string]any) error {
	return w.Store.WriteBatchLog(ctx, b, level, message, fields)
}
func (w *Worker) execute(parent context.Context, runID uuid.UUID) {
	spanCtx, span := otel.Tracer("task-processing/worker").Start(parent, "task.attempt", trace.WithAttributes(attribute.String("task.run_id", runID.String())))
	defer span.End()
	ex, err := w.Store.StartAttempt(spanCtx, runID, w.ID, w.Owner, w.Lease)
	if err != nil {
		if errors.Is(err, postgres.ErrConcurrencyLimited) {
			_ = w.Store.ReleaseReservation(parent, runID, w.Owner)
			return
		}
		if errors.Is(err, postgres.ErrRateLimited) {
			_ = w.Store.ReleaseReservation(parent, runID, w.Owner)
			return
		}
		w.Log.Error("start attempt failed", "run_id", runID, "error", err)
		return
	}
	telemetry.Metrics.Attempts.WithLabelValues(ex.FunctionKey).Inc()
	w.writeJobLog(spanCtx, ex, "INFO", "job handler started", nil)
	telemetry.Metrics.Inflight.Inc()
	defer telemetry.Metrics.Inflight.Dec()
	span.SetAttributes(attribute.String("task.function", ex.FunctionKey), attribute.Int("task.attempt", ex.Attempt))
	h, err := w.Registry.Get(ex.FunctionKey)
	if err == nil {
		ctx, cancel := context.WithTimeout(spanCtx, time.Duration(ex.Policy.TimeoutMS)*time.Millisecond)
		defer cancel()
		stopLease := make(chan struct{})
		defer close(stopLease)
		go w.renewLease(ctx, runID, stopLease)
		h = workermiddleware.Chain(h, append([]workermiddleware.Middleware{workermiddleware.Recover, workermiddleware.StructuredLog(w.Log)}, w.Middleware...)...)
		err = h(job.NewExecutionContext(ctx, &ex, w))
	}
	if err == nil {
		err = w.Store.CompleteSuccess(spanCtx, ex, w.Owner)
		if err != nil {
			w.Log.Error("complete success failed", "run_id", runID, "error", err)
		} else if err = w.Store.AdvanceWorkflowForJob(spanCtx, ex.RunID, true); err != nil {
			w.Log.Error("advance workflow failed", "run_id", runID, "error", err)
		}
		telemetry.Metrics.Completed.WithLabelValues("SUCCEEDED", "").Inc()
		w.writeJobLog(spanCtx, ex, "INFO", "job handler completed", nil)
		return
	}
	c := job.Classify(err)
	span.RecordError(err)
	if e := w.Store.CompleteFailure(spanCtx, ex, w.Owner, c, ex.Policy.Retry, time.Now()); e != nil {
		w.Log.Error("complete failure failed", "run_id", runID, "error", e)
	} else if ex.Attempt >= ex.Policy.Retry.MaxAttempts || !job.Retryable(c.Class, ex.Policy.Retry) {
		if advanceErr := w.Store.AdvanceWorkflowForJob(spanCtx, ex.RunID, false); advanceErr != nil {
			w.Log.Error("advance workflow failed", "run_id", runID, "error", advanceErr)
		}
	}
	telemetry.Metrics.Completed.WithLabelValues("FAILED", string(c.Class)).Inc()
	w.writeJobLog(spanCtx, ex, "ERROR", "job handler failed", map[string]any{"error_class": c.Class, "error_code": c.Code})
}
func (w *Worker) writeJobLog(ctx context.Context, ex job.Execution, level, message string, fields map[string]any) {
	logCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	traceID := ""
	if span := trace.SpanContextFromContext(ctx); span.IsValid() {
		traceID = span.TraceID().String()
	}
	if err := w.Store.WriteJobLog(logCtx, ex.ProjectID, ex.RunID, ex.AttemptID, level, message, traceID, fields); err != nil {
		w.Log.Warn("job log persistence failed", "run_id", ex.RunID, "error", err)
	}
}
func (w *Worker) ReportProgress(ctx context.Context, ex *job.Execution, percent int, message string) error {
	return w.Store.ReportProgress(ctx, ex, w.Owner, percent, message)
}
func (w *Worker) renewLease(ctx context.Context, runID uuid.UUID, stop <-chan struct{}) {
	interval := w.Lease / 3
	if interval < time.Second {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.Store.ExtendLease(ctx, runID, w.Owner, w.Lease); err != nil {
				w.Log.Error("lease renewal failed", "run_id", runID, "error", err)
				return
			}
		}
	}
}
func (w *Worker) renewBatchLease(ctx context.Context, batchID uuid.UUID, stop <-chan struct{}) {
	interval := w.Lease / 3
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.Store.ExtendBatchLease(ctx, batchID, w.Owner, w.Lease); err != nil {
				w.Log.Error("batch lease renewal failed", "batch_id", batchID, "error", err)
				return
			}
		}
	}
}
