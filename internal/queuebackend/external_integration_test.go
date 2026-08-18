package queuebackend

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	store "github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

func externalMessage() Message {
	return Message{DispatchID: uuid.New(), RunID: uuid.New(), ProjectID: uuid.New(), QueueID: uuid.New(), Priority: 3, AvailableAt: time.Now().UTC()}
}

func TestRedisStreamsEnqueueIntegration(t *testing.T) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("requires TEST_REDIS_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, group := "task.integration."+uuid.NewString(), "workers"
	backend, err := NewRedisStreams(&store.Store{}, RedisStreamsConfig{URL: url, Stream: stream, Group: group})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	defer backend.Client.Del(context.Background(), stream)
	want := externalMessage()
	if err = backend.Enqueue(ctx, want); err != nil {
		t.Fatal(err)
	}
	streams, err := backend.Client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "integration", Streams: []string{stream, ">"}, Count: 1, Block: time.Second}).Result()
	if err != nil || len(streams) != 1 || len(streams[0].Messages) != 1 {
		t.Fatalf("read streams=%v err=%v", streams, err)
	}
	got, err := decodeRedisMessage(streams[0].Messages[0])
	if err != nil || got.DispatchID != want.DispatchID || got.RunID != want.RunID {
		t.Fatalf("message=%+v err=%v", got, err)
	}
	if err = backend.Client.XAck(ctx, stream, group, streams[0].Messages[0].ID).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestJetStreamEnqueueIntegration(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("requires TEST_NATS_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	name := strings.ToUpper(strings.ReplaceAll(uuid.NewString()[:12], "-", ""))
	stream, subject := "TASK"+name, "task.integration."+name
	backend, err := NewJetStream(&store.Store{}, JetStreamConfig{URL: url, Stream: stream, Subject: subject, Consumer: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	defer backend.JS.DeleteStream(stream, nats.Context(context.Background()))
	want := externalMessage()
	if err = backend.Enqueue(ctx, want); err != nil {
		t.Fatal(err)
	}
	sub, err := backend.JS.PullSubscribe(subject, "workers", nats.BindStream(stream), nats.ManualAck())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	messages, err := sub.Fetch(1, nats.MaxWait(time.Second))
	if err != nil || len(messages) != 1 {
		t.Fatalf("fetch=%d err=%v", len(messages), err)
	}
	var got Message
	if err = json.Unmarshal(messages[0].Data, &got); err != nil || got.DispatchID != want.DispatchID || got.RunID != want.RunID {
		t.Fatalf("message=%+v err=%v", got, err)
	}
	if err = messages[0].Ack(); err != nil {
		t.Fatal(err)
	}
}
