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

func TestPlatformAuditOutboxLeaseRetryAndPublishIntegration(t *testing.T) {
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
	schema := "platform_audit_outbox_" + uuid.NewString()[:12]
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE") })
	if _, err = admin.Exec(ctx, `CREATE TABLE `+identifier+`.platform_audit_outbox_events(
		id uuid PRIMARY KEY,platform_audit_log_id uuid NOT NULL,tenant_id uuid,event_type text NOT NULL,aggregate_type text NOT NULL,aggregate_id uuid NOT NULL,payload jsonb NOT NULL,
		available_at timestamptz NOT NULL DEFAULT now(),published_at timestamptz,attempts integer NOT NULL DEFAULT 0,last_error text,claimed_by text,claim_token uuid,claim_expires_at timestamptz,expired_at timestamptz,created_at timestamptz NOT NULL DEFAULT now())`); err != nil {
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
	id, auditID, tenant, aggregate := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	if _, err = pool.Exec(ctx, `INSERT INTO platform_audit_outbox_events(id,platform_audit_log_id,tenant_id,event_type,aggregate_type,aggregate_id,payload) VALUES($1,$2,$3,'audit.platform_change','role',$4,'{}')`, id, auditID, tenant, aggregate); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimPlatformAuditOutbox(ctx, "worker-a", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != id || claimed[0].TenantID == nil || *claimed[0].TenantID != tenant {
		t.Fatalf("unexpected first claim: %+v", claimed)
	}
	other, err := store.ClaimPlatformAuditOutbox(ctx, "worker-b", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatalf("claimed a fenced event: %+v", other)
	}
	if err = store.RecordPlatformAuditOutboxFailure(ctx, claimed[0], errors.New("sink unavailable")); err != nil {
		t.Fatal(err)
	}
	var attempts int
	var published *time.Time
	if err = pool.QueryRow(ctx, "SELECT attempts,published_at FROM platform_audit_outbox_events WHERE id=$1", id).Scan(&attempts, &published); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || published != nil {
		t.Fatalf("failure state attempts=%d published=%v", attempts, published)
	}
	if _, err = pool.Exec(ctx, "UPDATE platform_audit_outbox_events SET available_at=now() WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	claimed, err = store.ClaimPlatformAuditOutbox(ctx, "worker-b", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("retry was not claimable: %+v", claimed)
	}
	if err = store.MarkPlatformAuditOutboxPublished(ctx, claimed[0]); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, "SELECT published_at FROM platform_audit_outbox_events WHERE id=$1", id).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published == nil {
		t.Fatal("event was not published")
	}
	if err = store.MarkPlatformAuditOutboxPublished(ctx, claimed[0]); err == nil {
		t.Fatal("stale publish acknowledgement was accepted")
	}
}
