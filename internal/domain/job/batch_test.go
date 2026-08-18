package job

import (
	"context"
	"testing"
)

type batchSpy struct {
	processed int
	message   string
	level     string
}

func (s *batchSpy) ReportBatchProgress(_ context.Context, _ *BatchExecution, p int, m string) error {
	s.processed = p
	s.message = m
	return nil
}
func (s *batchSpy) WriteBatchLog(_ context.Context, _ *BatchExecution, l, m string, _ map[string]any) error {
	s.level = l
	s.message = m
	return nil
}
func TestBatchContextProgressIsBounded(t *testing.T) {
	batch := &BatchExecution{Items: []BatchItem{{}, {}}}
	spy := &batchSpy{}
	ctx := NewBatchExecutionContext(context.Background(), batch, spy)
	if err := ctx.ReportProgress(2, "done"); err != nil {
		t.Fatal(err)
	}
	if spy.processed != 2 {
		t.Fatal("progress not delegated")
	}
	if err := ctx.ReportProgress(3, "bad"); err == nil {
		t.Fatal("expected bounds error")
	}
}
func TestBatchContextPersistsHandlerLog(t *testing.T) {
	batch := &BatchExecution{}
	spy := &batchSpy{}
	ctx := NewBatchExecutionContext(context.Background(), batch, spy)
	if err := ctx.Log("INFO", "hello", nil); err != nil {
		t.Fatal(err)
	}
	if spy.level != "INFO" || spy.message != "hello" {
		t.Fatalf("log=%+v", spy)
	}
}
