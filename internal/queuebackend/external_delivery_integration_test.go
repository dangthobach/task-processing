package queuebackend

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/example/task-processing/internal/domain/job"
	"github.com/example/task-processing/internal/migration"
	storepkg "github.com/example/task-processing/internal/persistence/postgres"
	"github.com/example/task-processing/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests execute the complete external delivery state machine against a
// migrated throw-away schema. They are opt-in because they require PostgreSQL
// plus the relevant broker, but never read or mutate the application's schema.
func TestRedisStreamsDeliveryIntegration(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("requires TEST_REDIS_URL")
	}
	name := "task.delivery." + uuid.NewString()
	exerciseExternalDelivery(t, func(s *storepkg.Store) Backend {
		backend, err := NewRedisStreams(s, RedisStreamsConfig{URL: url, Stream: name, Group: "workers", Block: 100 * time.Millisecond, ClaimIdle: 25 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		return backend
	})
}

func TestJetStreamDeliveryIntegration(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("requires TEST_NATS_URL")
	}
	name := strings.ToUpper(strings.ReplaceAll(uuid.NewString()[:12], "-", ""))
	exerciseExternalDelivery(t, func(s *storepkg.Store) Backend {
		backend, err := NewJetStream(s, JetStreamConfig{URL: url, Stream: "TASKDELIVERY" + name, Subject: "task.delivery." + name, Consumer: "workers", FetchWait: 100 * time.Millisecond, AckWait: 25 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		return backend
	})
}

func exerciseExternalDelivery(t *testing.T, newBackend func(*storepkg.Store) Backend) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, workerID, project, queue, definition := isolatedExternalStore(t, ctx)
	backend := newBackend(store)
	if closable, ok := backend.(ClosableBackend); ok {
		t.Cleanup(func() { _ = closable.Close() })
	}
	owner1, owner2 := "delivery-a", "delivery-b"

	// Success persists before transport acknowledgement.
	run := externalRun(t, ctx, store, project, definition)
	message := externalDispatch(t, ctx, store, run, project, queue)
	if err := backend.Enqueue(ctx, message); err != nil {
		t.Fatalf("enqueue success message: %v", err)
	}
	deliveries, err := backend.Reserve(ctx, ReserveRequest{WorkerID: workerID, Owner: owner1, Limit: 1, Lease: time.Second, PollWait: 100 * time.Millisecond})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("reserve deliveries=%d err=%v", len(deliveries), err)
	}
	if err = backend.Ack(ctx, deliveries[0]); err != nil {
		t.Fatalf("ack success message: %v", err)
	}
	assertRunStatus(t, ctx, store, run.ID, "SUCCEEDED")

	// Failure is persisted as retry state before the broker receipt is acked.
	run = externalRun(t, ctx, store, project, definition)
	message = externalDispatch(t, ctx, store, run, project, queue)
	if err = backend.Enqueue(ctx, message); err != nil {
		t.Fatalf("enqueue nack message: %v", err)
	}
	deliveries, err = backend.Reserve(ctx, ReserveRequest{WorkerID: workerID, Owner: owner1, Limit: 1, Lease: time.Second, PollWait: 100 * time.Millisecond})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("reserve for nack deliveries=%d err=%v", len(deliveries), err)
	}
	if err = backend.Nack(ctx, deliveries[0], &job.ClassifiedError{Class: job.DependencyUnavailable, Code: "TEST_DOWN", Err: errors.New("dependency down")}); err != nil {
		t.Fatalf("nack message: %v", err)
	}
	assertRunStatus(t, ctx, store, run.ID, "RETRY_WAIT")

	// A message redelivered after its original lease expired has an old dispatch
	// generation. It is transport-acked but cannot reclaim the recovered run.
	run = externalRun(t, ctx, store, project, definition)
	message = externalDispatch(t, ctx, store, run, project, queue)
	if err = backend.Enqueue(ctx, message); err != nil {
		t.Fatalf("enqueue stale message: %v", err)
	}
	deliveries, err = backend.Reserve(ctx, ReserveRequest{WorkerID: workerID, Owner: owner1, Limit: 1, Lease: 40 * time.Millisecond, PollWait: 100 * time.Millisecond})
	if err != nil || len(deliveries) != 1 {
		t.Fatalf("reserve stale deliveries=%d err=%v", len(deliveries), err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err = store.RecoverExpiredLeases(ctx); err != nil {
		t.Fatalf("recover expired lease: %v", err)
	}
	// Both Redis XAUTOCLAIM and JetStream AckWait need a short redelivery window.
	time.Sleep(100 * time.Millisecond)
	deliveries, err = backend.Reserve(ctx, ReserveRequest{WorkerID: workerID, Owner: owner2, Limit: 1, Lease: time.Second, PollWait: 100 * time.Millisecond})
	if err != nil || len(deliveries) != 0 {
		t.Fatalf("stale delivery was accepted count=%d err=%v", len(deliveries), err)
	}
	assertRunStatus(t, ctx, store, run.ID, "ENQUEUE_PENDING")
}

func isolatedExternalStore(t *testing.T, ctx context.Context) (*storepkg.Store, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires TEST_DATABASE_URL")
	}
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "delivery_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.Exec(cleanup, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	all, err := migration.Load(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = migration.Run(ctx, pool, all, migration.Options{}); err != nil {
		t.Fatal(err)
	}
	store := &storepkg.Store{Pool: pool}
	var tenant, project, queue, function, retryPolicy, workerID, definition uuid.UUID
	if err = pool.QueryRow(ctx, "INSERT INTO tenants(name) VALUES($1) RETURNING id", "delivery-tenant").Scan(&tenant); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO projects(tenant_id,name,key) VALUES($1,$2,$3) RETURNING id", tenant, "delivery-project", "delivery").Scan(&project); err != nil {
		t.Fatalf("seed project: %v", err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO queues(project_id,name,max_concurrency) VALUES($1,$2,10) RETURNING id", project, "delivery").Scan(&queue); err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO function_definitions(project_id,function_key,version,input_schema) VALUES($1,$2,$3,'{}') RETURNING id", project, "integration.delivery", "v1").Scan(&function); err != nil {
		t.Fatalf("seed function: %v", err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO retry_policies(project_id,name,max_attempts,strategy,initial_delay_ms,max_delay_ms) VALUES($1,$2,3,'FIXED',0,0) RETURNING id", project, "delivery-retry").Scan(&retryPolicy); err != nil {
		t.Fatalf("seed retry policy: %v", err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO workers(worker_key,hostname,version,status) VALUES($1,$2,$3,'ONLINE') RETURNING id", "delivery-worker", "integration", "v1").Scan(&workerID); err != nil {
		t.Fatalf("seed worker: %v", err)
	}
	if _, err = pool.Exec(ctx, "INSERT INTO worker_function_capabilities(worker_id,function_key,function_version,execution_mode) VALUES($1,$2,$3,'SINGLE')", workerID, "integration.delivery", "v1"); err != nil {
		t.Fatalf("seed capability: %v", err)
	}
	if err = pool.QueryRow(ctx, "INSERT INTO job_definitions(project_id,function_id,queue_id,retry_policy_id,name,default_priority,timeout_ms,execution_mode,batch_size) VALUES($1,$2,$3,$4,$5,3,30000,'SINGLE',2) RETURNING id", project, function, queue, retryPolicy, "delivery-job").Scan(&definition); err != nil {
		t.Fatalf("seed job definition: %v", err)
	}
	return store, workerID, project, queue, definition
}

func externalRun(t *testing.T, ctx context.Context, store *storepkg.Store, project, definition uuid.UUID) job.Run {
	t.Helper()
	run, err := store.Submit(ctx, storepkg.Submit{ProjectID: project, DefinitionID: definition, Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("submit run: %v", err)
	}
	return run
}

func externalDispatch(t *testing.T, ctx context.Context, store *storepkg.Store, run job.Run, project, queue uuid.UUID) Message {
	t.Helper()
	if err := store.EnqueueDispatches(ctx, []storepkg.DispatchRef{{RunID: run.ID, DispatchID: run.DispatchID}}); err != nil {
		t.Fatalf("make dispatch claimable: %v", err)
	}
	return Message{DispatchID: run.DispatchID, RunID: run.ID, ProjectID: project, QueueID: queue, Priority: run.Priority, AvailableAt: time.Now().UTC()}
}

func assertRunStatus(t *testing.T, ctx context.Context, store *storepkg.Store, run uuid.UUID, want string) {
	t.Helper()
	var got string
	if err := store.Pool.QueryRow(ctx, "SELECT status FROM job_runs WHERE id=$1", run).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("run %s status=%s want=%s", run, got, want)
	}
}
