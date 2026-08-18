package main

import (
	"context"
	"fmt"
	"github.com/example/task-processing/internal/persistence/postgres"
	"os"
	"path/filepath"
	"sort"
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
	files, err := filepath.Glob("migrations/*.sql")
	if err != nil {
		panic(err)
	}
	sort.Strings(files)
	for _, f := range files {
		b, e := os.ReadFile(f)
		if e != nil {
			panic(e)
		}
		if _, e = store.Pool.Exec(ctx, string(b)); e != nil {
			panic(fmt.Errorf("%s: %w", f, e))
		}
		fmt.Println("applied", f)
	}
}
