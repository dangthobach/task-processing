package main

import (
	"context"
	"fmt"
	"os"

	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
)

// bootstrap creates a minimal tenant/project/queue/retry policy for local development.
func main() {
	ctx := context.Background()
	store, err := postgres.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	tx, err := store.Pool.Begin(ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(ctx)
	var tenant, project, queue, user, role, permission uuid.UUID
	if err = tx.QueryRow(ctx, "INSERT INTO tenants(name) VALUES('Local development') RETURNING id").Scan(&tenant); err != nil {
		panic(err)
	}
	if err = tx.QueryRow(ctx, "INSERT INTO projects(tenant_id,name,key) VALUES($1,'Default project','default') RETURNING id", tenant).Scan(&project); err != nil {
		panic(err)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO retry_policies(project_id,name,max_attempts,strategy,initial_delay_ms,multiplier,max_delay_ms,jitter_pct) VALUES($1,'default',5,'EXPONENTIAL',1000,2,300000,20)", project); err != nil {
		panic(err)
	}
	if err = tx.QueryRow(ctx, "INSERT INTO queues(project_id,name,max_concurrency) VALUES($1,'default',10) RETURNING id", project).Scan(&queue); err != nil {
		panic(err)
	}
	if err = tx.QueryRow(ctx, "INSERT INTO users(tenant_id,subject,display_name) VALUES($1,'admin','Local administrator') RETURNING id", tenant).Scan(&user); err != nil {
		panic(err)
	}
	if err = tx.QueryRow(ctx, "INSERT INTO roles(tenant_id,role_key,display_name) VALUES($1,'admin','Administrator') RETURNING id", tenant).Scan(&role); err != nil {
		panic(err)
	}
	if err = tx.QueryRow(ctx, "SELECT id FROM permissions WHERE permission_key='platform:admin'").Scan(&permission); err != nil {
		panic(err)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO user_roles(user_id,role_id) VALUES($1,$2); INSERT INTO role_permissions(role_id,permission_id) VALUES($2,$3)", user, role, permission); err != nil {
		panic(err)
	}
	if err = tx.Commit(ctx); err != nil {
		panic(err)
	}
	fmt.Printf("tenant_id=%s\nproject_id=%s\nqueue_id=%s\n", tenant, project, queue)
}
