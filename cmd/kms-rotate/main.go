// kms-rotate performs one bounded rewrap pass. Run it repeatedly from a
// controlled job after rotating a cloud KMS/Vault Transit key.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/example/task-processing/internal/persistence/postgres"
)

func main() {
	ctx := context.Background()
	store, err := postgres.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer store.Close()
	limit := 100
	if raw := os.Getenv("KMS_ROTATE_LIMIT"); raw != "" {
		if value, parseErr := strconv.Atoi(raw); parseErr == nil {
			limit = value
		} else {
			panic("KMS_ROTATE_LIMIT must be an integer")
		}
	}
	if os.Getenv("KMS_ROTATE_RESET") == "true" {
		if err = store.ResetKMSRewrapMarkers(ctx); err != nil {
			panic(err)
		}
	}
	count, err := store.RewrapEncryptedData(ctx, limit)
	if err != nil {
		panic(err)
	}
	fmt.Printf("rewrapped %d encrypted records\n", count)
}
