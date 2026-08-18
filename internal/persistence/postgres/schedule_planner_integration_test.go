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

// TestScheduleCursorSurvivesEvaluatorFailover exercises the durable cursor
// contract against PostgreSQL. TEST_DATABASE_URL is deliberately optional for
// local source-only work but required in CI's integration job.
func TestScheduleCursorSurvivesEvaluatorFailover(t *testing.T) {
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
	schema := "cursor_test_" + uuid.NewString()[:12]
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE") })
	if _, err = admin.Exec(ctx, "CREATE TABLE "+identifier+".schedules(id uuid PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "CREATE TABLE "+identifier+".schedule_cursors(schedule_id uuid PRIMARY KEY REFERENCES "+identifier+".schedules(id) ON DELETE CASCADE,evaluated_through timestamptz NOT NULL,updated_at timestamptz NOT NULL DEFAULT now())"); err != nil {
		t.Fatal(err)
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
	id := uuid.New()
	if _, err = pool.Exec(ctx, "INSERT INTO schedules(id) VALUES($1)", id); err != nil {
		t.Fatal(err)
	}
	newer := time.Now().UTC().Truncate(time.Second)
	if err = store.AdvanceScheduleCursor(ctx, id, newer); err != nil {
		t.Fatal(err)
	}
	if err = store.AdvanceScheduleCursor(ctx, id, newer.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	var got time.Time
	if err = pool.QueryRow(ctx, "SELECT evaluated_through FROM schedule_cursors WHERE schedule_id=$1", id).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(newer) {
		t.Fatalf("cursor moved backwards: got %s want %s", got, newer)
	}
}
