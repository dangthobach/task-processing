package main

import (
	"context"
	"fmt"
	"github.com/example/task-processing/internal/migration"
	"github.com/example/task-processing/internal/persistence/postgres"
	"github.com/example/task-processing/migrations"
	"os"
	"strconv"
)

func main() {
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		panic("DATABASE_URL is required")
	}
	store, err := postgres.Open(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer store.Close()
	baseline := int64(0)
	if raw := os.Getenv("MIGRATION_BASELINE_THROUGH"); raw != "" {
		baseline, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || baseline < 0 {
			panic("MIGRATION_BASELINE_THROUGH must be a non-negative integer")
		}
	}
	loaded, err := migration.Load(migrations.FS)
	if err != nil {
		panic(err)
	}
	result, err := migration.Run(ctx, store.Pool, loaded, migration.Options{BaselineThrough: baseline})
	if err != nil {
		panic(err)
	}
	for _, version := range result.Baselined {
		fmt.Println("baselined", version)
	}
	for _, version := range result.Applied {
		fmt.Println("applied", version)
	}
}
