package migration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	appmigrations "github.com/example/task-processing/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestRunIsIdempotentIntegration proves the migration ledger rather than the
// individual SQL files is responsible for the second invocation being safe.
// Supply TEST_DATABASE_URL in CI or a local PostgreSQL environment.
func TestRunIsIdempotentIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if testing.Short() || dsn == "" {
		t.Skip("requires TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "migration_test_" + uuid.NewString()[0:12]
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	})

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET search_path TO "+pgx.Identifier{schema}.Sanitize()+", public")
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	loaded, err := Load(appmigrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	first, err := Run(ctx, pool, loaded, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Applied) != len(loaded) {
		t.Fatalf("applied %d migrations, want %d", len(first.Applied), len(loaded))
	}
	second, err := Run(ctx, pool, loaded, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Applied) != 0 || len(second.Skipped) != len(loaded) {
		t.Fatalf("second run result = %+v", second)
	}
	var count int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != len(loaded) {
		t.Fatal(fmt.Errorf("migration ledger count %d, want %d", count, len(loaded)))
	}
}
