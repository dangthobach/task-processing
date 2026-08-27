package postgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/example/task-processing/internal/domain/job"
	"github.com/example/task-processing/internal/migration"
	"github.com/example/task-processing/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestBatchLeaseRecoveryFencesStaleWorker proves that a recovered batch cannot
// be mutated by the former worker. This is intentionally end-to-end against a
// migrated schema because the safety property spans the batch aggregate and
// its child job runs.
func TestBatchLeaseRecoveryFencesStaleWorker(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if testing.Short() || dsn == "" {
		t.Skip("requires TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "batch_fencing_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	all, err := migration.Load(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = migration.Run(ctx, pool, all, migration.Options{}); err != nil {
		t.Fatal(err)
	}
	store := &Store{Pool: pool}

	var tenant, project, queue, function, retryPolicy, workerID, definition uuid.UUID
	if err = pool.QueryRow(ctx, "INSERT INTO tenants(name) VALUES('batch-fencing') RETURNING id").Scan(&tenant); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO projects(tenant_id,name,key) VALUES($1,'batch-fencing','batch-fencing') RETURNING id", tenant).Scan(&project); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO queues(project_id,name,max_concurrency) VALUES($1,'batch-fencing',10) RETURNING id", project).Scan(&queue); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO function_definitions(project_id,function_key,version,input_schema) VALUES($1,'integration.batch-fencing','v1','{}') RETURNING id", project).Scan(&function); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO retry_policies(project_id,name,max_attempts,strategy,initial_delay_ms,max_delay_ms) VALUES($1,'batch-fencing',2,'FIXED',0,0) RETURNING id", project).Scan(&retryPolicy); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO workers(worker_key,hostname,version,status) VALUES('batch-fencing','test','v1','ONLINE') RETURNING id").Scan(&workerID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "INSERT INTO worker_function_capabilities(worker_id,function_key,function_version,execution_mode) VALUES($1,'integration.batch-fencing','v1','BATCH')", workerID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO job_definitions(project_id,function_id,queue_id,retry_policy_id,name,execution_mode,batch_size,batch_max_wait_ms) VALUES($1,$2,$3,$4,'batch-fencing','BATCH',2,0) RETURNING id", project, function, queue, retryPolicy).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		run, submitErr := store.Submit(ctx, Submit{ProjectID: project, DefinitionID: definition, Payload: []byte(`{}`)})
		if submitErr != nil {
			t.Fatal(submitErr)
		}
		if submitErr = store.EnqueueDispatches(ctx, []DispatchRef{{RunID: run.ID, DispatchID: run.DispatchID}}); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	batches, err := store.ClaimBatches(ctx, workerID, "former-worker", 1, time.Second)
	if err != nil || len(batches) != 1 {
		t.Fatalf("claim batches=%v err=%v", batches, err)
	}
	execution, err := store.StartBatch(ctx, batches[0], workerID, "former-worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if execution.LeaseToken == uuid.Nil || execution.LeaseExpiresAt.IsZero() {
		t.Fatal("batch execution has no fenced lease")
	}
	if _, err = pool.Exec(ctx, "UPDATE job_batches SET lease_expires_at=now()-interval '1 second' WHERE id=$1", execution.BatchID); err != nil {
		t.Fatal(err)
	}
	if recovered, recoverErr := store.RecoverExpiredBatches(ctx); recoverErr != nil || recovered != 1 {
		t.Fatalf("recover=%d err=%v", recovered, recoverErr)
	}
	if err = store.ReportBatchProgress(ctx, &execution, 1, "late progress"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale progress err=%v, want ErrLeaseLost", err)
	}
	results := make([]job.BatchItemResult, 0, len(execution.Items))
	for _, item := range execution.Items {
		results = append(results, job.BatchItemResult{ItemID: item.ItemID, Success: true})
	}
	if err = store.CompleteBatch(ctx, &execution, "former-worker", results, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale completion err=%v, want ErrLeaseLost", err)
	}
	var queued int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM job_runs WHERE job_definition_id=$1 AND status='QUEUED'", definition).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 2 {
		t.Fatalf("recovered queued runs=%d want=2", queued)
	}
}
