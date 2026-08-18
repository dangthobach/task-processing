package queuebackend

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeBackend struct{ kind Type }

func (f fakeBackend) Type() Type                                                  { return f.kind }
func (f fakeBackend) Enqueue(context.Context, Message) error                      { return nil }
func (f fakeBackend) EnqueueBatch(context.Context, []Message) error               { return nil }
func (f fakeBackend) Reserve(context.Context, ReserveRequest) ([]Delivery, error) { return nil, nil }
func (f fakeBackend) Ack(context.Context, Delivery) error                         { return nil }
func (f fakeBackend) Nack(context.Context, Delivery, error) error                 { return nil }
func (f fakeBackend) Depth(context.Context, uuid.UUID) (QueueStats, error)        { return QueueStats{}, nil }
func (f fakeBackend) Health(context.Context) error                                { return nil }
func (f fakeBackend) Capabilities() Capabilities                                  { return Capabilities{} }

func TestRegistryUsesBackendType(t *testing.T) {
	r := NewRegistry(fakeBackend{kind: Postgres})
	if _, err := r.Get(Postgres); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get(JetStream); err != ErrUnknownBackend {
		t.Fatalf("err=%v", err)
	}
}
func TestMessageRequiresStableControlPlaneIDs(t *testing.T) {
	if err := (Message{}).Validate(); err != ErrInvalidMessage {
		t.Fatalf("err=%v", err)
	}
	if err := (Message{DispatchID: uuid.New(), RunID: uuid.New(), ProjectID: uuid.New(), QueueID: uuid.New()}).Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestRedisWireMessageRoundTripsWithoutPayload(t *testing.T) {
	want := Message{DispatchID: uuid.New(), RunID: uuid.New(), ProjectID: uuid.New(), QueueID: uuid.New(), Priority: 3, AvailableAt: time.Now().UTC().Round(0)}
	encoded := encodeRedisMessage(want)
	got, err := decodeRedisMessage(redisMessageForTest(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if got.DispatchID != want.DispatchID || got.RunID != want.RunID || got.ProjectID != want.ProjectID || got.QueueID != want.QueueID || got.Priority != want.Priority || !got.AvailableAt.Equal(want.AvailableAt) {
		t.Fatalf("message=%+v want=%+v", got, want)
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) == "" || string(data) == "null" {
		t.Fatal("jetstream wire JSON is empty")
	}
	if strings.Contains(string(data), "payload") {
		t.Fatal("transport message must not contain payload")
	}
}
