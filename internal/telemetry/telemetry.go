package telemetry

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

var Metrics = newMetrics()

type metrics struct {
	Attempts            *prometheus.CounterVec
	Completed           *prometheus.CounterVec
	Inflight            prometheus.Gauge
	QueueOldest         *prometheus.GaugeVec
	WorkerHeartbeat     *prometheus.GaugeVec
	OutboxFailures      prometheus.Counter
	MaintenanceFailures *prometheus.CounterVec
	RetentionDeleted    prometheus.Counter
}

func newMetrics() *metrics {
	return &metrics{Attempts: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "task_attempts_total", Help: "Task attempts started"}, []string{"function"}), Completed: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "task_runs_completed_total", Help: "Terminal task runs"}, []string{"status", "error_class"}), Inflight: prometheus.NewGauge(prometheus.GaugeOpts{Name: "task_worker_inflight", Help: "Attempts currently executing in this worker process"}), QueueOldest: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "task_queue_oldest_age_seconds", Help: "Age of the oldest queued item"}, []string{"queue"}), WorkerHeartbeat: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "task_worker_heartbeat_age_seconds", Help: "Age of latest worker heartbeat"}, []string{"worker"}), OutboxFailures: prometheus.NewCounter(prometheus.CounterOpts{Name: "task_outbox_failures_total", Help: "Failed outbox backend publications"}), MaintenanceFailures: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "task_maintenance_failures_total", Help: "Worker maintenance loop failures"}, []string{"operation"}), RetentionDeleted: prometheus.NewCounter(prometheus.CounterOpts{Name: "task_retention_rows_deleted_total", Help: "Rows deleted by retention policies"})}
}

var registerOnce sync.Once

func RegisterMetrics() {
	registerOnce.Do(func() {
		prometheus.MustRegister(Metrics.Attempts, Metrics.Completed, Metrics.Inflight, Metrics.QueueOldest, Metrics.WorkerHeartbeat, Metrics.OutboxFailures, Metrics.MaintenanceFailures, Metrics.RetentionDeleted)
	})
}

// InitTracing enables OTLP/HTTP exporting when OTEL_EXPORTER_OTLP_ENDPOINT is set.
// Without it the global provider remains a no-op, keeping local development dependency-free.
func InitTracing(ctx context.Context, service string) (func(context.Context) error, error) {
	// Always install W3C propagation and an SDK provider.  This keeps trace and
	// span identifiers available in local development too; exporting remains
	// opt-in through OTEL_EXPORTER_OTLP_ENDPOINT.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		provider := trace.NewTracerProvider(trace.WithResource(resource.Default()))
		otel.SetTracerProvider(provider)
		return provider.Shutdown, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(service)))
	if err != nil {
		return nil, err
	}
	provider := trace.NewTracerProvider(trace.WithBatcher(exp), trace.WithResource(res))
	otel.SetTracerProvider(provider)
	slog.Info("OpenTelemetry OTLP tracing enabled", "service", service)
	return provider.Shutdown, nil
}
