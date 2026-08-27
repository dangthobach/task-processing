package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestValidateAndExtractTraceContext(t *testing.T) {
	value := ValidateTraceContext(TraceContext{TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", TraceState: "vendor=value"})
	if value.TraceParent == "" {
		t.Fatal("valid W3C trace context was rejected")
	}
	ctx := ExtractTraceContext(context.Background(), value)
	span := trace.SpanContextFromContext(ctx)
	if !span.IsValid() || span.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" || !span.IsRemote() {
		t.Fatalf("span=%+v", span)
	}
}

func TestValidateTraceContextRejectsMalformedInput(t *testing.T) {
	if value := ValidateTraceContext(TraceContext{TraceParent: "not-a-trace"}); value.TraceParent != "" {
		t.Fatalf("value=%+v", value)
	}
}
