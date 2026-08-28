package controlplane_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/example/task-processing/internal/application/controlplane"
	"github.com/example/task-processing/internal/migration"
	"github.com/example/task-processing/internal/persistence/postgres"
	appmigrations "github.com/example/task-processing/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests use a disposable schema. They specifically exercise the two
// control-plane aggregates that used to mutate directly from HTTP handlers.
func TestRBACMappingAndQueueBackendOptimisticConcurrencyIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if testing.Short() || dsn == "" {
		t.Skip("requires TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := integrationControlPlanePool(t, ctx, dsn)
	defer pool.Close()
	store := &postgres.Store{Pool: pool}
	var tenant uuid.UUID
	if err := pool.QueryRow(ctx, "INSERT INTO tenants(name) VALUES('rbac-concurrency') RETURNING id").Scan(&tenant); err != nil {
		t.Fatal(err)
	}
	rbac := controlplane.RBACService{Store: store}
	role, err := rbac.CreateRole(ctx, tenant, controlplane.RoleInput{RoleKey: "operator", DisplayName: "Operator"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var roleView struct {
		ID      uuid.UUID `json:"id"`
		Version int64     `json:"version"`
	}
	if err = json.Unmarshal(role, &roleView); err != nil {
		t.Fatal(err)
	}
	var permission uuid.UUID
	if err = pool.QueryRow(ctx, "SELECT id FROM permissions WHERE permission_key='job:read'").Scan(&permission); err != nil {
		t.Fatal(err)
	}

	results := runTwice(func() error {
		_, err := rbac.ReplaceRolePermissions(ctx, tenant, roleView.ID, roleView.Version, []uuid.UUID{permission}, nil)
		return err
	})
	assertOneOptimisticWinner(t, results)

	var backend uuid.UUID
	if err = pool.QueryRow(ctx, `INSERT INTO queue_backends(name,backend_type,capabilities,status) VALUES('rbac-backend','REDIS_STREAMS','{}','ACTIVE') RETURNING id`).Scan(&backend); err != nil {
		t.Fatal(err)
	}
	backendService := controlplane.QueueBackendService{Store: store}
	results = runTwice(func() error {
		name := "rbac-backend-updated"
		_, err := backendService.Update(ctx, backend, 1, controlplane.UpdateQueueBackend{Name: &name}, nil)
		return err
	})
	assertOneOptimisticWinner(t, results)
}

func runTwice(fn func() error) []error {
	start := make(chan struct{})
	out := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; out <- fn() }()
	}
	close(start)
	wg.Wait()
	close(out)
	results := make([]error, 0, 2)
	for err := range out {
		results = append(results, err)
	}
	return results
}
func assertOneOptimisticWinner(t *testing.T, results []error) {
	t.Helper()
	successes := 0
	locks := 0
	for _, err := range results {
		if err == nil {
			successes++
			continue
		}
		if errors.Is(err, controlplane.ErrOptimisticLock) {
			locks++
			continue
		}
		t.Fatalf("unexpected concurrent result: %v", err)
	}
	if successes != 1 || locks != 1 {
		t.Fatalf("results successes=%d optimistic_locks=%d", successes, locks)
	}
}

func integrationControlPlanePool(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	schema := "control_plane_test_" + uuid.NewString()[:12]
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE") })
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
	loaded, err := migration.Load(appmigrations.FS)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if _, err = migration.Run(ctx, pool, loaded, migration.Options{}); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return pool
}
