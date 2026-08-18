package queuebackend

import (
	"fmt"
	"os"
	"strings"

	store "github.com/example/task-processing/internal/persistence/postgres"
)

// FromEnv composes only explicitly configured external transports. This keeps
// PostgreSQL as the safe default while making a missing Redis/NATS service a
// startup error rather than a runtime message-loss path.
func FromEnv(s *store.Store, getenv func(string) string) (*Registry, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	backends := []Backend{NewPostgres(s)}
	if raw := strings.TrimSpace(getenv("TASK_REDIS_STREAMS_URL")); raw != "" {
		backend, err := NewRedisStreams(s, RedisStreamsConfig{
			URL: raw, Stream: valueOr(getenv("TASK_REDIS_STREAMS_STREAM"), "task.dispatch"), Group: valueOr(getenv("TASK_REDIS_STREAMS_GROUP"), "task-workers"),
		})
		if err != nil {
			return nil, fmt.Errorf("configure Redis Streams: %w", err)
		}
		backends = append(backends, backend)
	}
	if raw := strings.TrimSpace(getenv("TASK_NATS_URL")); raw != "" {
		backend, err := NewJetStream(s, JetStreamConfig{
			URL: raw, Stream: valueOr(getenv("TASK_NATS_STREAM"), "TASK_DISPATCH"), Subject: valueOr(getenv("TASK_NATS_SUBJECT"), "task.dispatch"), Consumer: valueOr(getenv("TASK_NATS_CONSUMER"), "task-workers"),
		})
		if err != nil {
			return nil, fmt.Errorf("configure NATS JetStream: %w", err)
		}
		backends = append(backends, backend)
	}
	return NewRegistry(backends...), nil
}

func valueOr(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}

// ClosableBackend is optional because the PostgreSQL adapter shares the Store
// lifecycle; external clients own their own network connections.
type ClosableBackend interface {
	Backend
	Close() error
}
