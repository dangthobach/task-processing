package queuebackend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/example/task-processing/internal/domain/job"
	store "github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// JetStreamConfig defines one pull-consumer namespace. JetStream only carries
// routing metadata; PostgreSQL remains the source of payload and state truth.
type JetStreamConfig struct {
	URL       string
	Stream    string
	Subject   string
	Consumer  string
	FetchWait time.Duration
	AckWait   time.Duration
}

type JetStreamBackend struct {
	Store  *store.Store
	Conn   *nats.Conn
	JS     nats.JetStreamContext
	Config JetStreamConfig

	ensureOnce sync.Once
	ensureErr  error
	receiptsMu sync.Mutex
	receipts   map[string]*nats.Msg
}

func NewJetStream(s *store.Store, cfg JetStreamConfig) (*JetStreamBackend, error) {
	if s == nil || cfg.URL == "" || cfg.Stream == "" || cfg.Subject == "" || cfg.Consumer == "" {
		return nil, fmt.Errorf("jetstream store, URL, stream, subject and consumer are required")
	}
	if cfg.FetchWait <= 0 {
		cfg.FetchWait = time.Second
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = time.Minute
	}
	conn, err := nats.Connect(cfg.URL, nats.Name("task-processing-worker"))
	if err != nil {
		return nil, err
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &JetStreamBackend{Store: s, Conn: conn, JS: js, Config: cfg, receipts: make(map[string]*nats.Msg)}, nil
}

func (b *JetStreamBackend) Type() Type { return JetStream }
func (b *JetStreamBackend) Capabilities() Capabilities {
	return Capabilities{NativePriority: false, NativeDelay: false, NativeDLQ: false, NativePause: false, AtomicBatch: false, ConsumerGroups: true}
}
func (b *JetStreamBackend) ensure(ctx context.Context) error {
	b.ensureOnce.Do(func() {
		if _, err := b.JS.StreamInfo(b.Config.Stream, nats.Context(ctx)); err != nil {
			if !errors.Is(err, nats.ErrStreamNotFound) {
				b.ensureErr = err
				return
			}
			_, err = b.JS.AddStream(&nats.StreamConfig{Name: b.Config.Stream, Subjects: []string{b.Config.Subject}, Storage: nats.FileStorage, Retention: nats.WorkQueuePolicy})
			if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
				b.ensureErr = err
				return
			}
		}
		if _, err := b.JS.ConsumerInfo(b.Config.Stream, b.Config.Consumer, nats.Context(ctx)); err != nil {
			if !errors.Is(err, nats.ErrConsumerNotFound) {
				b.ensureErr = err
				return
			}
			_, err = b.JS.AddConsumer(b.Config.Stream, &nats.ConsumerConfig{Durable: b.Config.Consumer, AckPolicy: nats.AckExplicitPolicy, AckWait: b.Config.AckWait, FilterSubject: b.Config.Subject, MaxAckPending: 1000})
			if err != nil && !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
				b.ensureErr = err
			}
		}
	})
	return b.ensureErr
}
func (b *JetStreamBackend) Enqueue(ctx context.Context, message Message) error {
	if err := message.Validate(); err != nil {
		return err
	}
	if err := b.ensure(ctx); err != nil {
		return err
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	_, err = b.JS.Publish(b.Config.Subject, payload, nats.Context(ctx), nats.MsgId(message.DispatchID.String()))
	return err
}
func (b *JetStreamBackend) EnqueueBatch(ctx context.Context, messages []Message) error {
	for _, message := range messages {
		if err := b.Enqueue(ctx, message); err != nil {
			return err
		}
	}
	return nil
}
func (b *JetStreamBackend) Reserve(ctx context.Context, req ReserveRequest) ([]Delivery, error) {
	if req.WorkerID == uuid.Nil || req.Owner == "" || req.Limit < 1 {
		return nil, fmt.Errorf("%w: worker_id, owner and positive limit are required", ErrInvalidMessage)
	}
	if req.Lease <= 0 {
		req.Lease = time.Minute
	}
	if err := b.ensure(ctx); err != nil {
		return nil, err
	}
	sub, err := b.JS.PullSubscribe(b.Config.Subject, b.Config.Consumer, nats.BindStream(b.Config.Stream), nats.ManualAck())
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	messages, err := sub.Fetch(req.Limit, nats.MaxWait(b.Config.FetchWait))
	if errors.Is(err, nats.ErrTimeout) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	deliveries := make([]Delivery, 0, len(messages))
	for _, raw := range messages {
		var message Message
		if err := json.Unmarshal(raw.Data, &message); err != nil || message.Validate() != nil {
			if ackErr := raw.Ack(); ackErr != nil {
				return deliveries, ackErr
			}
			continue
		}
		claim, claimErr := b.Store.TakeDispatchOwnership(ctx, message.RunID, message.DispatchID, req.Owner, req.Lease)
		if errors.Is(claimErr, store.ErrDispatchLost) {
			if ackErr := raw.Ack(); ackErr != nil {
				return deliveries, ackErr
			}
			continue
		}
		if claimErr != nil {
			return deliveries, claimErr
		}
		execution, startErr := b.Store.StartAttempt(ctx, message.RunID, req.WorkerID, req.Owner, claim.LeaseToken, req.Lease)
		if errors.Is(startErr, store.ErrConcurrencyLimited) || errors.Is(startErr, store.ErrRateLimited) {
			_ = b.Store.ReleaseReservation(ctx, message.RunID, req.Owner, claim.LeaseToken)
			continue // preserve transport message for AckWait redelivery.
		}
		if errors.Is(startErr, store.ErrLeaseLost) {
			if ackErr := raw.Ack(); ackErr != nil {
				return deliveries, ackErr
			}
			continue
		}
		if startErr != nil {
			_ = b.Store.ReleaseReservation(ctx, message.RunID, req.Owner, claim.LeaseToken)
			return deliveries, startErr
		}
		receipt := raw.Reply
		b.receiptsMu.Lock()
		b.receipts[receipt] = raw
		b.receiptsMu.Unlock()
		deliveries = append(deliveries, Delivery{Message: message, Execution: execution, Owner: req.Owner, Receipt: receipt})
	}
	return deliveries, nil
}
func (b *JetStreamBackend) Ack(ctx context.Context, delivery Delivery) error {
	err := b.Store.CompleteSuccess(ctx, delivery.Execution, delivery.Owner)
	if err != nil && !errors.Is(err, store.ErrLeaseLost) {
		return err
	}
	if err := b.ackTransport(delivery.Receipt); err != nil {
		return &TransportAckError{Err: err}
	}
	return nil
}
func (b *JetStreamBackend) Nack(ctx context.Context, delivery Delivery, reason error) error {
	if reason == nil {
		reason = errors.New("backend nack without an error")
	}
	err := b.Store.CompleteFailure(ctx, delivery.Execution, delivery.Owner, classify(reason), delivery.Execution.Policy.Retry, time.Now())
	if err != nil && !errors.Is(err, store.ErrLeaseLost) {
		return err
	}
	if err := b.ackTransport(delivery.Receipt); err != nil {
		return &TransportAckError{Err: err}
	}
	return nil
}
func classify(err error) *job.ClassifiedError { return job.Classify(err) }
func (b *JetStreamBackend) ackTransport(receipt string) error {
	b.receiptsMu.Lock()
	message, ok := b.receipts[receipt]
	if ok {
		delete(b.receipts, receipt)
	}
	b.receiptsMu.Unlock()
	if !ok || receipt == "" {
		return ErrInvalidMessage
	}
	return message.Ack()
}
func (b *JetStreamBackend) Depth(ctx context.Context, queue uuid.UUID) (QueueStats, error) {
	stats, err := b.Store.QueueDepth(ctx, queue)
	return QueueStats{Queued: stats.Queued, Running: stats.Running, Retry: stats.Retry, Oldest: stats.Oldest}, err
}
func (b *JetStreamBackend) Health(ctx context.Context) error { return b.Conn.FlushWithContext(ctx) }
func (b *JetStreamBackend) Close() error                     { b.Conn.Drain(); b.Conn.Close(); return nil }
