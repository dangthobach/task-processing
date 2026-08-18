package job

import (
	"context"
	"testing"
)

type progressSpy struct {
	percent int
	message string
}

func (s *progressSpy) ReportProgress(_ context.Context, _ *Execution, p int, m string) error {
	s.percent = p
	s.message = m
	return nil
}
func TestExecutionContextReportsBoundedProgress(t *testing.T) {
	spy := &progressSpy{}
	ctx := NewExecutionContext(context.Background(), &Execution{}, spy)
	if err := ctx.ReportProgress(45, "halfway"); err != nil {
		t.Fatal(err)
	}
	if spy.percent != 45 || spy.message != "halfway" {
		t.Fatalf("spy=%+v", spy)
	}
	if err := ctx.ReportProgress(101, "bad"); err == nil {
		t.Fatal("expected validation error")
	}
}
