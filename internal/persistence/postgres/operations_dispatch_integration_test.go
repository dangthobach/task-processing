package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRetryRunCreatesNewDispatchAndOutboxIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if testing.Short() || dsn == "" {
		t.Skip("requires TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "operations_test_" + uuid.NewString()[:12]
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE") })
	for _, ddl := range []string{
		"CREATE TABLE " + identifier + ".job_runs(id uuid PRIMARY KEY,project_id uuid NOT NULL,row_version bigint NOT NULL,status text NOT NULL,current_dispatch_id uuid NOT NULL,available_at timestamptz NOT NULL DEFAULT now(),finished_at timestamptz,updated_at timestamptz NOT NULL DEFAULT now())",
		"CREATE TABLE " + identifier + ".outbox_events(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),project_id uuid NOT NULL,event_type text NOT NULL,aggregate_id uuid NOT NULL,dispatch_id uuid NOT NULL,payload jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now())",
		"CREATE TABLE " + identifier + ".dlq_entries(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),job_run_id uuid NOT NULL,row_version bigint NOT NULL DEFAULT 1,replayed_at timestamptz)",
	} {
		if _, err = admin.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET search_path TO "+identifier+", public")
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	project, run, oldDispatch := uuid.New(), uuid.New(), uuid.New()
	if _, err = pool.Exec(ctx, "INSERT INTO job_runs(id,project_id,row_version,status,current_dispatch_id) VALUES($1,$2,1,'DEAD_LETTER',$3)", run, project, oldDispatch); err != nil {
		t.Fatal(err)
	}
	ok, err := (&Store{Pool: pool}).RetryRun(ctx, project, run, 1)
	if err != nil || !ok {
		t.Fatalf("retry=%v err=%v", ok, err)
	}
	var status string
	var dispatch uuid.UUID
	if err = pool.QueryRow(ctx, "SELECT status,current_dispatch_id FROM job_runs WHERE id=$1", run).Scan(&status, &dispatch); err != nil {
		t.Fatal(err)
	}
	if status != "ENQUEUE_PENDING" || dispatch == oldDispatch {
		t.Fatalf("status=%s dispatch=%s old=%s", status, dispatch, oldDispatch)
	}
	var outboxDispatch uuid.UUID
	if err = pool.QueryRow(ctx, "SELECT dispatch_id FROM outbox_events WHERE aggregate_id=$1", run).Scan(&outboxDispatch); err != nil {
		t.Fatal(err)
	}
	if outboxDispatch != dispatch {
		t.Fatalf("outbox dispatch=%s run dispatch=%s", outboxDispatch, dispatch)
	}
}
