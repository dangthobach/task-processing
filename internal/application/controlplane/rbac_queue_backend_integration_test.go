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

func TestWorkflowRunCancellationFencingIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if testing.Short() || dsn == "" {
		t.Skip("requires TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := integrationControlPlanePool(t, ctx, dsn)
	defer pool.Close()
	store := &postgres.Store{Pool: pool}
	service := controlplane.WorkflowService{Store: store}
	project, run := seedWorkflowRun(t, ctx, pool)
	if _, err := pool.Exec(ctx, "INSERT INTO workflow_dispatch_outbox(workflow_run_id) VALUES($1)", run); err != nil {
		t.Fatal(err)
	}
	result, err := service.CancelRun(ctx, project, run, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "CANCELLED" || result.Version != 2 {
		t.Fatalf("unexpected cancellation result: %+v", result)
	}
	var status string
	var signals int
	if err = pool.QueryRow(ctx, "SELECT status FROM workflow_runs WHERE id=$1", run).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM workflow_dispatch_outbox WHERE workflow_run_id=$1", run).Scan(&signals); err != nil {
		t.Fatal(err)
	}
	if status != "CANCELLED" || signals != 0 {
		t.Fatalf("workflow cancellation left status=%s signals=%d", status, signals)
	}

	project, concurrentRun := seedWorkflowRun(t, ctx, pool)
	results := runTwice(func() error {
		_, err := service.CancelRun(ctx, project, concurrentRun, 1, nil)
		return err
	})
	assertOneOptimisticWinner(t, results)

	project, activeRun := seedWorkflowRun(t, ctx, pool)
	activeNode := seedWorkflowNode(t, ctx, pool, project, activeRun)
	if _, err = pool.Exec(ctx, "INSERT INTO workflow_node_runs(workflow_run_id,node_id,status) VALUES($1,$2,'RUNNING')", activeRun, activeNode); err != nil {
		t.Fatal(err)
	}
	if _, err = service.CancelRun(ctx, project, activeRun, 1, nil); !errors.Is(err, controlplane.ErrWorkflowRunActive) {
		t.Fatalf("active workflow cancellation error = %v, want ErrWorkflowRunActive", err)
	}
}

func TestWorkflowRetryCopiesImmutableSnapshotIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if testing.Short() || dsn == "" {
		t.Skip("requires TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := integrationControlPlanePool(t, ctx, dsn)
	defer pool.Close()
	store := &postgres.Store{Pool: pool}
	service := controlplane.WorkflowService{Store: store}
	project, source := seedWorkflowRun(t, ctx, pool)
	node := seedWorkflowNode(t, ctx, pool, project, source)
	if _, err := pool.Exec(ctx, "INSERT INTO workflow_node_runs(workflow_run_id,node_id,execution_snapshot,status) VALUES($1,$2,$3::jsonb,'FAILED')", source, node, `{"captured":"v1"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE workflow_runs SET status='FAILED',finished_at=now() WHERE id=$1", source); err != nil {
		t.Fatal(err)
	}
	retry, err := service.RetryRun(ctx, project, source, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Created || retry.Status != "RUNNING" {
		t.Fatalf("unexpected retry result: %+v", retry)
	}
	var linked uuid.UUID
	var snapshot string
	var nodeStatus string
	if err = pool.QueryRow(ctx, `SELECT wr.retry_of_run_id,nr.execution_snapshot::text,nr.status
		FROM workflow_runs wr JOIN workflow_node_runs nr ON nr.workflow_run_id=wr.id WHERE wr.id=$1`, retry.ID).Scan(&linked, &snapshot, &nodeStatus); err != nil {
		t.Fatal(err)
	}
	if linked != source || snapshot != `{"captured": "v1"}` || nodeStatus != "PENDING" {
		t.Fatalf("retry did not preserve snapshot: linked=%s snapshot=%s status=%s", linked, snapshot, nodeStatus)
	}
	if _, err = pool.Exec(ctx, "UPDATE workflow_runs SET status='SUCCEEDED',finished_at=now() WHERE id=$1", retry.ID); err != nil {
		t.Fatal(err)
	}
	again, err := service.RetryRun(ctx, project, source, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if again.Created || again.ID != retry.ID || again.Status != "SUCCEEDED" || again.Version != 2 {
		t.Fatalf("retry was not idempotent: first=%+v second=%+v", retry, again)
	}
	if _, err = service.RetryRun(ctx, project, source, 1, nil); !errors.Is(err, controlplane.ErrOptimisticLock) {
		t.Fatalf("stale retry error = %v, want ErrOptimisticLock", err)
	}
}

func TestWorkflowContinueDispatchesFailureBranchIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if testing.Short() || dsn == "" {
		t.Skip("requires TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := integrationControlPlanePool(t, ctx, dsn)
	defer pool.Close()
	store := &postgres.Store{Pool: pool}
	project, seedRun := seedWorkflowRun(t, ctx, pool)
	root := seedWorkflowNode(t, ctx, pool, project, seedRun)
	failureBranch := seedWorkflowNode(t, ctx, pool, project, seedRun)
	var workflow uuid.UUID
	if err := pool.QueryRow(ctx, "SELECT workflow_id FROM workflow_runs WHERE id=$1", seedRun).Scan(&workflow); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE workflow_definitions SET failure_policy='CONTINUE' WHERE id=$1", workflow); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO workflow_edges(workflow_id,from_node_id,to_node_id,condition_type) VALUES($1,$2,$3,'ON_FAILURE')", workflow, root, failureBranch); err != nil {
		t.Fatal(err)
	}
	run, err := store.StartWorkflow(ctx, project, workflow, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.DispatchReadyWorkflowNodes(ctx, run); err != nil {
		t.Fatal(err)
	}
	var rootJob uuid.UUID
	if err = pool.QueryRow(ctx, "SELECT job_run_id FROM workflow_node_runs WHERE workflow_run_id=$1 AND node_id=$2", run, root).Scan(&rootJob); err != nil {
		t.Fatal(err)
	}
	if err = store.AdvanceWorkflowForJob(ctx, rootJob, false); err != nil {
		t.Fatal(err)
	}
	if err = store.DispatchReadyWorkflowNodes(ctx, run); err != nil {
		t.Fatal(err)
	}
	var rootState, branchState, runState string
	if err = pool.QueryRow(ctx, `SELECT
		(SELECT status FROM workflow_node_runs WHERE workflow_run_id=$1 AND node_id=$2),
		(SELECT status FROM workflow_node_runs WHERE workflow_run_id=$1 AND node_id=$3),
		(SELECT status FROM workflow_runs WHERE id=$1)`, run, root, failureBranch).Scan(&rootState, &branchState, &runState); err != nil {
		t.Fatal(err)
	}
	if rootState != "FAILED" || branchState != "RUNNING" || runState != "RUNNING" {
		t.Fatalf("failure branch state root=%s branch=%s run=%s", rootState, branchState, runState)
	}
}

func TestWorkflowSkippedBranchCompletesIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if testing.Short() || dsn == "" {
		t.Skip("requires TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := integrationControlPlanePool(t, ctx, dsn)
	defer pool.Close()
	store := &postgres.Store{Pool: pool}
	project, seedRun := seedWorkflowRun(t, ctx, pool)
	root := seedWorkflowNode(t, ctx, pool, project, seedRun)
	failureBranch := seedWorkflowNode(t, ctx, pool, project, seedRun)
	var workflow uuid.UUID
	if err := pool.QueryRow(ctx, "SELECT workflow_id FROM workflow_runs WHERE id=$1", seedRun).Scan(&workflow); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO workflow_edges(workflow_id,from_node_id,to_node_id,condition_type) VALUES($1,$2,$3,'ON_FAILURE')", workflow, root, failureBranch); err != nil {
		t.Fatal(err)
	}
	run, err := store.StartWorkflow(ctx, project, workflow, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.DispatchReadyWorkflowNodes(ctx, run); err != nil {
		t.Fatal(err)
	}
	var rootJob uuid.UUID
	if err = pool.QueryRow(ctx, "SELECT job_run_id FROM workflow_node_runs WHERE workflow_run_id=$1 AND node_id=$2", run, root).Scan(&rootJob); err != nil {
		t.Fatal(err)
	}
	if err = store.AdvanceWorkflowForJob(ctx, rootJob, true); err != nil {
		t.Fatal(err)
	}
	if err = store.DispatchReadyWorkflowNodes(ctx, run); err != nil {
		t.Fatal(err)
	}
	var branchState, runState string
	if err = pool.QueryRow(ctx, `SELECT
		(SELECT status FROM workflow_node_runs WHERE workflow_run_id=$1 AND node_id=$2),
		(SELECT status FROM workflow_runs WHERE id=$1)`, run, failureBranch).Scan(&branchState, &runState); err != nil {
		t.Fatal(err)
	}
	if branchState != "SKIPPED" || runState != "SUCCEEDED" {
		t.Fatalf("skipped branch did not complete workflow: branch=%s run=%s", branchState, runState)
	}
}

func TestWorkflowManualInterventionBlocksDispatchIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if testing.Short() || dsn == "" {
		t.Skip("requires TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := integrationControlPlanePool(t, ctx, dsn)
	defer pool.Close()
	store := &postgres.Store{Pool: pool}
	project, seedRun := seedWorkflowRun(t, ctx, pool)
	root := seedWorkflowNode(t, ctx, pool, project, seedRun)
	pending := seedWorkflowNode(t, ctx, pool, project, seedRun)
	var workflow uuid.UUID
	if err := pool.QueryRow(ctx, "SELECT workflow_id FROM workflow_runs WHERE id=$1", seedRun).Scan(&workflow); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE workflow_definitions SET failure_policy='MANUAL_INTERVENTION' WHERE id=$1", workflow); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO workflow_edges(workflow_id,from_node_id,to_node_id,condition_type) VALUES($1,$2,$3,'ALWAYS')", workflow, root, pending); err != nil {
		t.Fatal(err)
	}
	run, err := store.StartWorkflow(ctx, project, workflow, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.DispatchReadyWorkflowNodes(ctx, run); err != nil {
		t.Fatal(err)
	}
	var rootJob uuid.UUID
	if err = pool.QueryRow(ctx, "SELECT job_run_id FROM workflow_node_runs WHERE workflow_run_id=$1 AND node_id=$2", run, root).Scan(&rootJob); err != nil {
		t.Fatal(err)
	}
	if err = store.AdvanceWorkflowForJob(ctx, rootJob, false); err != nil {
		t.Fatal(err)
	}
	if err = store.DispatchReadyWorkflowNodes(ctx, run); err != nil {
		t.Fatal(err)
	}
	var pendingState, runState string
	if err = pool.QueryRow(ctx, `SELECT
		(SELECT status FROM workflow_node_runs WHERE workflow_run_id=$1 AND node_id=$2),
		(SELECT status FROM workflow_runs WHERE id=$1)`, run, pending).Scan(&pendingState, &runState); err != nil {
		t.Fatal(err)
	}
	if pendingState != "BLOCKED" || runState != "AWAITING_INTERVENTION" {
		t.Fatalf("manual intervention state pending=%s run=%s", pendingState, runState)
	}
	retry, err := (controlplane.WorkflowService{Store: store}).RetryRun(ctx, project, run, 2, nil)
	if err != nil || !retry.Created || retry.Status != "RUNNING" {
		t.Fatalf("manual workflow retry = %+v, %v", retry, err)
	}
}

func seedWorkflowRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (uuid.UUID, uuid.UUID) {
	t.Helper()
	var tenant, project, workflow, run uuid.UUID
	key := uuid.NewString()
	if err := pool.QueryRow(ctx, "INSERT INTO tenants(name) VALUES($1) RETURNING id", "workflow-"+key).Scan(&tenant); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO projects(tenant_id,name,key) VALUES($1,$2,$3) RETURNING id", tenant, "workflow", key).Scan(&project); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO workflow_definitions(project_id,name) VALUES($1,$2) RETURNING id", project, "workflow-"+key).Scan(&workflow); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO workflow_runs(project_id,workflow_id,status) VALUES($1,$2,'RUNNING') RETURNING id", project, workflow).Scan(&run); err != nil {
		t.Fatal(err)
	}
	return project, run
}

func seedWorkflowNode(t *testing.T, ctx context.Context, pool *pgxpool.Pool, project, run uuid.UUID) uuid.UUID {
	t.Helper()
	var workflow, queue, function, worker, definition, node uuid.UUID
	if err := pool.QueryRow(ctx, "SELECT workflow_id FROM workflow_runs WHERE id=$1", run).Scan(&workflow); err != nil {
		t.Fatal(err)
	}
	key := uuid.NewString()
	if err := pool.QueryRow(ctx, "INSERT INTO queues(project_id,name) VALUES($1,$2) RETURNING id", project, "queue-"+key).Scan(&queue); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO function_definitions(project_id,function_key,version,input_schema) VALUES($1,$2,'v1','{}') RETURNING id", project, "workflow.fn."+key).Scan(&function); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO workers(worker_key,hostname,version,status) VALUES($1,'integration','v1','ONLINE') RETURNING id", "workflow-worker-"+key).Scan(&worker); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO worker_function_capabilities(worker_id,function_key,function_version,execution_mode) VALUES($1,$2,'v1','SINGLE')", worker, "workflow.fn."+key); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO job_definitions(project_id,function_id,queue_id,name) VALUES($1,$2,$3,$4) RETURNING id", project, function, queue, "definition-"+key).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO workflow_nodes(workflow_id,node_key,job_definition_id) VALUES($1,$2,$3) RETURNING id", workflow, "active-"+key, definition).Scan(&node); err != nil {
		t.Fatal(err)
	}
	return node
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
