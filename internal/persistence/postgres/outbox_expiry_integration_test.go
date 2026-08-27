package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestQueueOutboxExpiryMovesOnlyCurrentPendingRunToDLQ(t *testing.T) {
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
	schema := "outbox_expiry_" + uuid.NewString()[:12]
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE") })
	for _, ddl := range []string{
		"CREATE TABLE " + identifier + ".job_runs(id uuid PRIMARY KEY,project_id uuid NOT NULL,status text NOT NULL,current_dispatch_id uuid NOT NULL,finished_at timestamptz,lease_owner text,lease_token uuid,lease_expires_at timestamptz,updated_at timestamptz NOT NULL DEFAULT now())",
		"CREATE TABLE " + identifier + ".outbox_events(id uuid PRIMARY KEY,aggregate_id uuid NOT NULL,dispatch_id uuid NOT NULL,event_type text NOT NULL,published_at timestamptz,attempts integer NOT NULL DEFAULT 0,last_error text,available_at timestamptz NOT NULL DEFAULT now(),claimed_by text,claim_token uuid,claim_expires_at timestamptz,expired_at timestamptz)",
		"CREATE TABLE " + identifier + ".dlq_entries(job_run_id uuid PRIMARY KEY,reason text NOT NULL)",
		"CREATE TABLE " + identifier + ".realtime_events(id bigserial PRIMARY KEY,project_id uuid NOT NULL,event_type text NOT NULL,aggregate_type text NOT NULL,aggregate_id uuid NOT NULL,payload jsonb NOT NULL)",
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
		_, err := conn.Exec(ctx, "SET search_path TO "+identifier)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := &Store{Pool: pool}
	project, run, dispatch, outbox, token := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	if _, err = pool.Exec(ctx, "INSERT INTO job_runs(id,project_id,status,current_dispatch_id) VALUES($1,$2,'ENQUEUE_PENDING',$3)", run, project, dispatch); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "INSERT INTO outbox_events(id,aggregate_id,dispatch_id,event_type,attempts,claim_token,claim_expires_at) VALUES($1,$2,$3,'job.enqueue',$4,$5,now()+interval '1 minute')", outbox, run, dispatch, maxQueueOutboxAttempts-1, token); err != nil {
		t.Fatal(err)
	}
	if err = store.RecordOutboxFailure(ctx, outbox, token, errors.New("backend unavailable")); err != nil {
		t.Fatal(err)
	}
	var runStatus, reason string
	var expiredAt *time.Time
	if err = pool.QueryRow(ctx, "SELECT status FROM job_runs WHERE id=$1", run).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, "SELECT expired_at FROM outbox_events WHERE id=$1", outbox).Scan(&expiredAt); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, "SELECT reason FROM dlq_entries WHERE job_run_id=$1", run).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if runStatus != "DEAD_LETTER" || expiredAt == nil || reason != "OUTBOX_DELIVERY_EXPIRED" {
		t.Fatalf("status=%s expired=%v dlq_reason=%q", runStatus, expiredAt, reason)
	}
	if err = store.RecordOutboxFailure(ctx, outbox, token, errors.New("duplicate callback")); err == nil {
		t.Fatal("expired outbox was accepted again")
	}
}
