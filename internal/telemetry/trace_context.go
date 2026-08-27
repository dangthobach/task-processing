package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TraceContext is the bounded W3C state that may safely travel with a durable
// job. Baggage is intentionally excluded because it is not durable job state
// and can contain unbounded or sensitive request metadata.
type TraceContext struct {
	TraceParent string
	TraceState  string
}

// CaptureTraceContext serializes only a valid remote-parent candidate. Invalid
// contexts become empty fields rather than poison a future worker attempt.
func CaptureTraceContext(ctx context.Context) TraceContext {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return ValidateTraceContext(TraceContext{TraceParent: carrier.Get("traceparent"), TraceState: carrier.Get("tracestate")})
}

// ValidateTraceContext rejects malformed W3C values before durable storage.
// Tracing must never reject an otherwise valid task submission.
func ValidateTraceContext(value TraceContext) TraceContext {
	if len(value.TraceParent) == 0 || len(value.TraceParent) > 512 || len(value.TraceState) > 512 {
		return TraceContext{}
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": value.TraceParent, "tracestate": value.TraceState})
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return TraceContext{}
	}
	return value
}

// ExtractTraceContext retains worker cancellation/deadline semantics while
// restoring a durable parent. It has no database, lock, or network operation.
func ExtractTraceContext(fallback context.Context, value TraceContext) context.Context {
	value = ValidateTraceContext(value)
	if value.TraceParent == "" {
		return fallback
	}
	return propagation.TraceContext{}.Extract(fallback, propagation.MapCarrier{"traceparent": value.TraceParent, "tracestate": value.TraceState})
}
