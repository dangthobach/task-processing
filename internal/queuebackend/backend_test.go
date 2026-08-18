package queuebackend

import (
	"context"
	"testing"

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
