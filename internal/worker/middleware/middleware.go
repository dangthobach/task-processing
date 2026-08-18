// Package middleware contains composable handler middleware. It deliberately has
// no dependency on PostgreSQL or HTTP and can be reused by any worker runtime.
package middleware

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/example/task-processing/internal/domain/job"
)

type Middleware func(job.Handler) job.Handler

func Chain(handler job.Handler, middlewares ...Middleware) job.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

func Recover(next job.Handler) job.Handler {
	return func(ctx *job.ExecutionContext) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = &job.ClassifiedError{Class: job.Panic, Code: "PANIC", Err: fmt.Errorf("panic: %v", r)}
			}
		}()
		return next(ctx)
	}
}
func StructuredLog(log *slog.Logger) Middleware {
	if log == nil {
		log = slog.Default()
	}
	return func(next job.Handler) job.Handler {
		return func(ctx *job.ExecutionContext) error {
			started := time.Now()
			log.Info("job handler started", "run_id", ctx.Execution.RunID, "attempt", ctx.Execution.Attempt, "function", ctx.Execution.FunctionKey)
			err := next(ctx)
			if err != nil {
				log.Error("job handler failed", "run_id", ctx.Execution.RunID, "attempt", ctx.Execution.Attempt, "duration", time.Since(started), "error", err)
			} else {
				log.Info("job handler completed", "run_id", ctx.Execution.RunID, "attempt", ctx.Execution.Attempt, "duration", time.Since(started))
			}
			return err
		}
	}
}
