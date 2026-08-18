package main

import (
	"context"
	"github.com/example/task-processing/internal/domain/job"
	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/example/task-processing/internal/queuebackend"
	"github.com/example/task-processing/internal/registry"
	"github.com/example/task-processing/internal/telemetry"
	"github.com/example/task-processing/internal/worker"
	"github.com/google/uuid"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	telemetry.RegisterMetrics()
	shutdownTracing, err := telemetry.InitTracing(ctx, "task-processing-worker")
	if err != nil {
		panic(err)
	}
	defer shutdownTracing(context.Background())
	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	metricsServer := &http.Server{Addr: metricsAddr, Handler: promhttp.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if e := metricsServer.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			slog.Error("metrics server failed", "error", e)
		}
	}()
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdown)
	}()
	store, err := postgres.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	backends, err := queuebackend.FromEnv(store, os.Getenv)
	if err != nil {
		panic(err)
	}
	config, err := worker.LoadConfig(os.Getenv)
	if err != nil {
		panic(err)
	}
	reg := registry.New()
	reg.Register("example.echo", func(e *job.ExecutionContext) error {
		slog.Info("echo handled", "run_id", e.Execution.RunID, "payload", string(e.Execution.Payload))
		return nil
	})
	reg.RegisterBatch("example.batch_echo", func(ctx *job.BatchExecutionContext) ([]job.BatchItemResult, error) {
		results := make([]job.BatchItemResult, 0, len(ctx.Batch.Items))
		for index, item := range ctx.Batch.Items {
			_ = ctx.Log("INFO", "batch item processed", map[string]any{"ordinal": item.Ordinal, "job_run_id": item.Run.RunID})
			results = append(results, job.BatchItemResult{ItemID: item.ItemID, Success: true})
			_ = ctx.ReportProgress(index+1, "items processed")
		}
		return results, nil
	})
	id := uuid.New()
	var workerID uuid.UUID
	err = store.Pool.QueryRow(ctx, `INSERT INTO workers(worker_key,hostname,version) VALUES($1,$2,'dev') ON CONFLICT(worker_key) DO UPDATE SET heartbeat_at=now(),status='ONLINE' RETURNING id`, id.String(), "local").Scan(&workerID)
	if err != nil {
		panic(err)
	}
	w := &worker.Worker{Store: store, Registry: reg, ID: workerID, Owner: id.String(), Config: config, Backends: backends, Log: slog.Default()}
	if err = w.Run(ctx); err != nil && ctx.Err() == nil {
		panic(err)
	}
}
