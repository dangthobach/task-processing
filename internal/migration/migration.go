// Package migration provides a checksummed, transactional SQL migration runner.
package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var migrationName = regexp.MustCompile(`^([0-9]+)_.+\.sql$`)

type Migration struct {
	Version  int64
	Filename string
	SQL      string
	Checksum string
}

type Result struct {
	Applied   []int64
	Baselined []int64
	Skipped   []int64
}

type Options struct {
	// BaselineThrough records existing migrations without executing them, but
	// only when an untracked database already contains the task-processing
	// schema. Zero requires the operator to make the baseline decision.
	BaselineThrough int64
}

// Load parses numbered SQL files from fsys and verifies that their migration
// numbers are unique. The SQL bytes themselves are checksummed deliberately:
// changing an applied migration must stop deployment rather than mutate history.
func Load(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migration directory: %w", err)
	}
	migrations := make([]Migration, 0, len(entries))
	seen := map[int64]string{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		match := migrationName.FindStringSubmatch(name)
		if match == nil {
			continue
		}
		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("invalid migration filename %q", name)
		}
		if previous, exists := seen[version]; exists {
			return nil, fmt.Errorf("duplicate migration version %d in %q and %q", version, previous, name)
		}
		body, err := fs.ReadFile(fsys, path.Clean(name))
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", name, err)
		}
		if strings.TrimSpace(string(body)) == "" {
			return nil, fmt.Errorf("migration %q is empty", name)
		}
		digest := sha256.Sum256(body)
		seen[version] = name
		migrations = append(migrations, Migration{Version: version, Filename: name, SQL: string(body), Checksum: hex.EncodeToString(digest[:])})
	}
	if len(migrations) == 0 {
		return nil, errors.New("no migrations found")
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	return migrations, nil
}

// Run applies migrations exactly once. It uses a session-level advisory lock
// so concurrent migrators cannot both observe the same pending migration.
func Run(ctx context.Context, pool *pgxpool.Pool, migrations []Migration, opts Options) (Result, error) {
	if pool == nil {
		return Result{}, errors.New("migration pool is required")
	}
	if len(migrations) == 0 {
		return Result{}, errors.New("no migrations supplied")
	}
	if err := validateMigrations(migrations); err != nil {
		return Result{}, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended('task-processing-migrations', 0))"); err != nil {
		return Result{}, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock(hashtextextended('task-processing-migrations', 0))")
	}()

	if _, err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version bigint PRIMARY KEY,
		filename text NOT NULL,
		checksum char(64) NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return Result{}, fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := recorded(ctx, conn)
	if err != nil {
		return Result{}, err
	}
	for _, m := range migrations {
		if old, exists := applied[m.Version]; exists && old != m.Checksum {
			return Result{}, fmt.Errorf("migration checksum mismatch for %s (version %d)", m.Filename, m.Version)
		}
	}

	var hasSchema bool
	if err = conn.QueryRow(ctx, "SELECT to_regclass('job_runs') IS NOT NULL").Scan(&hasSchema); err != nil {
		return Result{}, fmt.Errorf("inspect existing schema: %w", err)
	}
	result := Result{}
	if len(applied) == 0 && hasSchema {
		if opts.BaselineThrough <= 0 {
			return Result{}, errors.New("existing task-processing schema has no migration ledger; set MIGRATION_BASELINE_THROUGH after verifying its applied version")
		}
		if err = VerifyBaseline(ctx, conn, opts.BaselineThrough); err != nil {
			return Result{}, err
		}
		for _, m := range migrations {
			if m.Version > opts.BaselineThrough {
				continue
			}
			if _, err = conn.Exec(ctx, "INSERT INTO schema_migrations(version,filename,checksum) VALUES($1,$2,$3)", m.Version, m.Filename, m.Checksum); err != nil {
				return Result{}, fmt.Errorf("baseline migration %s: %w", m.Filename, err)
			}
			applied[m.Version] = m.Checksum
			result.Baselined = append(result.Baselined, m.Version)
		}
	}

	for _, m := range migrations {
		if _, exists := applied[m.Version]; exists {
			result.Skipped = append(result.Skipped, m.Version)
			continue
		}
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return Result{}, fmt.Errorf("begin migration %s: %w", m.Filename, err)
		}
		if _, err = tx.Exec(ctx, m.SQL); err == nil {
			_, err = tx.Exec(ctx, "INSERT INTO schema_migrations(version,filename,checksum) VALUES($1,$2,$3)", m.Version, m.Filename, m.Checksum)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return Result{}, fmt.Errorf("apply migration %s: %w", m.Filename, err)
		}
		if err = tx.Commit(ctx); err != nil {
			return Result{}, fmt.Errorf("commit migration %s: %w", m.Filename, err)
		}
		applied[m.Version] = m.Checksum
		result.Applied = append(result.Applied, m.Version)
	}
	return result, nil
}

func recorded(ctx context.Context, conn *pgxpool.Conn) (map[int64]string, error) {
	rows, err := conn.Query(ctx, "SELECT version,checksum FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("read migration ledger: %w", err)
	}
	defer rows.Close()
	result := map[int64]string{}
	for rows.Next() {
		var version int64
		var checksum string
		if err = rows.Scan(&version, &checksum); err != nil {
			return nil, fmt.Errorf("scan migration ledger: %w", err)
		}
		result[version] = checksum
	}
	return result, rows.Err()
}

func validateMigrations(migrations []Migration) error {
	seen := map[int64]struct{}{}
	for _, m := range migrations {
		if m.Version <= 0 || m.Filename == "" || strings.TrimSpace(m.SQL) == "" || len(m.Checksum) != sha256.Size*2 {
			return fmt.Errorf("invalid migration %q", m.Filename)
		}
		if _, exists := seen[m.Version]; exists {
			return fmt.Errorf("duplicate migration version %d", m.Version)
		}
		seen[m.Version] = struct{}{}
	}
	return nil
}
