package postgres

import (
	"context"
	"fmt"

	"github.com/example/task-processing/internal/registry"
	"github.com/google/uuid"
)

// ReplaceWorkerCapabilities atomically publishes the capabilities compiled in
// one worker binary. It is called before the worker claims work; failure keeps
// the worker offline rather than allowing unsupported jobs to dispatch.
func (s *Store) ReplaceWorkerCapabilities(ctx context.Context, workerID uuid.UUID, values []registry.Capability) error {
	if workerID == uuid.Nil {
		return fmt.Errorf("worker id is required")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "DELETE FROM worker_function_capabilities WHERE worker_id=$1", workerID); err != nil {
		return err
	}
	for _, value := range values {
		if value.FunctionKey == "" || value.FunctionVersion == "" || (value.ExecutionMode != "SINGLE" && value.ExecutionMode != "BATCH") {
			return fmt.Errorf("invalid worker capability")
		}
		if _, err = tx.Exec(ctx, `INSERT INTO worker_function_capabilities(worker_id,function_key,function_version,execution_mode)
			VALUES($1,$2,$3,$4)`, workerID, value.FunctionKey, value.FunctionVersion, value.ExecutionMode); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
