package middleware

import (
	"context"
	"errors"
	"github.com/example/task-processing/internal/domain/job"
	"testing"
)

func TestRecoverConvertsPanic(t *testing.T) {
	h := Chain(func(*job.ExecutionContext) error { panic("boom") }, Recover)
	err := h(&job.ExecutionContext{Context: context.Background(), Execution: &job.Execution{}})
	var ce *job.ClassifiedError
	if !errors.As(err, &ce) || ce.Class != job.Panic {
		t.Fatalf("err=%v", err)
	}
}
func TestChainOrder(t *testing.T) {
	var got []string
	mw := func(name string) Middleware {
		return func(next job.Handler) job.Handler {
			return func(ctx *job.ExecutionContext) error {
				got = append(got, name+"-before")
				err := next(ctx)
				got = append(got, name+"-after")
				return err
			}
		}
	}
	h := Chain(func(*job.ExecutionContext) error { got = append(got, "handler"); return nil }, mw("a"), mw("b"))
	_ = h(&job.ExecutionContext{Context: context.Background(), Execution: &job.Execution{}})
	want := "a-before,b-before,handler,b-after,a-after"
	actual := ""
	for i, v := range got {
		if i > 0 {
			actual += ","
		}
		actual += v
	}
	if actual != want {
		t.Fatalf("got %s", actual)
	}
}
