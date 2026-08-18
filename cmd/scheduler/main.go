package main

import (
	"context"
	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/example/task-processing/internal/scheduler"
	"github.com/example/task-processing/internal/telemetry"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownTracing, err := telemetry.InitTracing(ctx, "task-processing-scheduler")
	if err != nil {
		panic(err)
	}
	defer shutdownTracing(context.Background())
	store, err := postgres.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	if err = (&scheduler.Service{Store: store, Log: slog.Default()}).Run(ctx); err != nil && ctx.Err() == nil {
		panic(err)
	}
}
