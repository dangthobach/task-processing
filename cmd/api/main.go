package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/task-processing/internal/httpapi"
	"github.com/example/task-processing/internal/identity"
	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/example/task-processing/internal/telemetry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	telemetry.RegisterMetrics()
	shutdownTracing, err := telemetry.InitTracing(ctx, "task-processing-api")
	if err != nil {
		panic(err)
	}
	defer shutdownTracing(context.Background())
	store, err := postgres.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	api := &httpapi.API{Store: store, Events: httpapi.NewEventHub()}
	if os.Getenv("TASK_DEV_HEADER_IDENTITY") == "true" { api.Identity = identity.HeaderProvider{} }
	srv := &http.Server{Addr: ":8080", Handler: api.Router(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	slog.Info("API listening", "address", srv.Addr)
	if err = srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}
