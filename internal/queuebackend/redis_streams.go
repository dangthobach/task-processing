package queuebackend

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/task-processing/internal/domain/job"
	store "github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RedisStreamsConfig configures one durable stream namespace. Queue routing
// stays in PostgreSQL; each message includes queue_id and no payload is copied
// to Redis.
type RedisStreamsConfig struct {
	URL       string
	Stream    string
	Group     string
	Block     time.Duration
	ClaimIdle time.Duration
}

type RedisStreamsBackend struct {
	Store  *store.Store
	Client redis.UniversalClient
	Config RedisStreamsConfig

	groupMu    sync.Mutex
	groupReady atomic.Bool
}

func NewRedisStreams(s *store.Store, cfg RedisStreamsConfig) (*RedisStreamsBackend, error) {
	if s == nil || cfg.URL == "" || cfg.Stream == "" || cfg.Group == "" {
		return nil, fmt.Errorf("redis streams store, URL, stream and group are required")
	}
	options, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}
	if cfg.Block <= 0 {
		cfg.Block = time.Second
	}
	if cfg.ClaimIdle <= 0 {
		cfg.ClaimIdle = time.Minute
	}
	return &RedisStreamsBackend{Store: s, Client: redis.NewClient(options), Config: cfg}, nil
}

func (b *RedisStreamsBackend) Type() Type { return RedisStreams }
func (b *RedisStreamsBackend) Capabilities() Capabilities {
	return Capabilities{NativePriority: false, NativeDelay: false, NativeDLQ: false, NativePause: false, AtomicBatch: false, ConsumerGroups: true}
}
func (b *RedisStreamsBackend) ensureGroup(ctx context.Context) error {
	if b.groupReady.Load() {
		return nil
	}
	b.groupMu.Lock()
	defer b.groupMu.Unlock()
	if b.groupReady.Load() {
		return nil
	}
	err := b.Client.XGroupCreateMkStream(ctx, b.Config.Stream, b.Config.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	b.groupReady.Store(true)
	return nil
}
func (b *RedisStreamsBackend) Enqueue(ctx context.Context, message Message) error {
	if err := message.Validate(); err != nil {
		return err
	}
	if err := b.ensureGroup(ctx); err != nil {
		return err
	}
	_, err := b.Client.XAdd(ctx, &redis.XAddArgs{Stream: b.Config.Stream, Values: encodeRedisMessage(message)}).Result()
	return err
}
func (b *RedisStreamsBackend) EnqueueBatch(ctx context.Context, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}
	if err := b.ensureGroup(ctx); err != nil {
		return err
	}
	pipe := b.Client.Pipeline()
	for _, message := range messages {
		if err := message.Validate(); err != nil {
			return err
		}
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: b.Config.Stream, Values: encodeRedisMessage(message)})
	}
	_, err := pipe.Exec(ctx)
	return err
}
func encodeRedisMessage(message Message) map[string]any {
	return map[string]any{
		"dispatch_id":  message.DispatchID.String(),
		"run_id":       message.RunID.String(),
		"project_id":   message.ProjectID.String(),
		"queue_id":     message.QueueID.String(),
		"priority":     strconv.Itoa(int(message.Priority)),
		"available_at": message.AvailableAt.UTC().Format(time.RFC3339Nano),
	}
}

func (b *RedisStreamsBackend) Reserve(ctx context.Context, req ReserveRequest) ([]Delivery, error) {
	if req.WorkerID == uuid.Nil || req.Owner == "" || req.Limit < 1 {
		return nil, fmt.Errorf("%w: worker_id, owner and positive limit are required", ErrInvalidMessage)
	}
	if req.Lease <= 0 {
		req.Lease = time.Minute
	}
	if err := b.ensureGroup(ctx); err != nil {
		return nil, err
	}
	messages, err := b.claimIdle(ctx, req.Owner, req.Limit)
	if err != nil {
		return nil, err
	}
	if len(messages) == 0 {
		block := b.Config.Block
		if req.PollWait > 0 && req.PollWait < block {
			block = req.PollWait
		}
		streams, readErr := b.Client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: b.Config.Group, Consumer: req.Owner, Streams: []string{b.Config.Stream, ">"}, Count: int64(req.Limit), Block: block}).Result()
		if errors.Is(readErr, redis.Nil) {
			return nil, nil
		}
		if readErr != nil {
			return nil, readErr
		}
		for _, stream := range streams {
			messages = append(messages, stream.Messages...)
		}
	}
	deliveries := make([]Delivery, 0, len(messages))
	for _, raw := range messages {
		message, decodeErr := decodeRedisMessage(raw)
		if decodeErr != nil {
			// The producer is this service; malformed messages are poison data and
			// must be acknowledged to avoid permanently blocking the group.
			if ackErr := b.ackTransport(ctx, raw.ID); ackErr != nil {
				return deliveries, ackErr
			}
			continue
		}
		claim, claimErr := b.Store.TakeDispatchOwnership(ctx, message.RunID, message.DispatchID, req.Owner, req.Lease)
		if errors.Is(claimErr, store.ErrDispatchLost) {
			if ackErr := b.ackTransport(ctx, raw.ID); ackErr != nil {
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
			continue // leave pending; XAUTOCLAIM retries after ClaimIdle.
		}
		if errors.Is(startErr, store.ErrLeaseLost) {
			if ackErr := b.ackTransport(ctx, raw.ID); ackErr != nil {
				return deliveries, ackErr
			}
			continue
		}
		if startErr != nil {
			_ = b.Store.ReleaseReservation(ctx, message.RunID, req.Owner, claim.LeaseToken)
			return deliveries, startErr
		}
		deliveries = append(deliveries, Delivery{Message: message, Execution: execution, Owner: req.Owner, Receipt: raw.ID})
	}
	return deliveries, nil
}

func (b *RedisStreamsBackend) claimIdle(ctx context.Context, consumer string, limit int) ([]redis.XMessage, error) {
	result, _, err := b.Client.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: b.Config.Stream, Group: b.Config.Group, Consumer: consumer, MinIdle: b.Config.ClaimIdle, Start: "0-0", Count: int64(limit)}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return result, err
}
func decodeRedisMessage(raw redis.XMessage) (Message, error) {
	get := func(key string) (string, error) {
		value, ok := raw.Values[key]
		if !ok {
			return "", fmt.Errorf("redis stream message missing %s", key)
		}
		return fmt.Sprint(value), nil
	}
	dispatchRaw, err := get("dispatch_id")
	if err != nil {
		return Message{}, err
	}
	runRaw, err := get("run_id")
	if err != nil {
		return Message{}, err
	}
	projectRaw, err := get("project_id")
	if err != nil {
		return Message{}, err
	}
	queueRaw, err := get("queue_id")
	if err != nil {
		return Message{}, err
	}
	priorityRaw, err := get("priority")
	if err != nil {
		return Message{}, err
	}
	availableRaw, err := get("available_at")
	if err != nil {
		return Message{}, err
	}
	message := Message{}
	if message.DispatchID, err = uuid.Parse(dispatchRaw); err != nil {
		return Message{}, err
	}
	if message.RunID, err = uuid.Parse(runRaw); err != nil {
		return Message{}, err
	}
	if message.ProjectID, err = uuid.Parse(projectRaw); err != nil {
		return Message{}, err
	}
	if message.QueueID, err = uuid.Parse(queueRaw); err != nil {
		return Message{}, err
	}
	priority, err := strconv.ParseInt(priorityRaw, 10, 16)
	if err != nil {
		return Message{}, err
	}
	message.Priority = job.Priority(priority)
	if message.AvailableAt, err = time.Parse(time.RFC3339Nano, availableRaw); err != nil {
		return Message{}, err
	}
	return message, message.Validate()
}

func (b *RedisStreamsBackend) Ack(ctx context.Context, delivery Delivery) error {
	err := b.Store.CompleteSuccess(ctx, delivery.Execution, delivery.Owner)
	if err != nil && !errors.Is(err, store.ErrLeaseLost) {
		return err
	}
	if err := b.ackTransport(ctx, delivery.Receipt); err != nil {
		return &TransportAckError{Err: err}
	}
	return nil
}
func (b *RedisStreamsBackend) Nack(ctx context.Context, delivery Delivery, reason error) error {
	if reason == nil {
		reason = errors.New("backend nack without an error")
	}
	err := b.Store.CompleteFailure(ctx, delivery.Execution, delivery.Owner, job.Classify(reason), delivery.Execution.Policy.Retry, time.Now())
	if err != nil && !errors.Is(err, store.ErrLeaseLost) {
		return err
	}
	if err := b.ackTransport(ctx, delivery.Receipt); err != nil {
		return &TransportAckError{Err: err}
	}
	return nil
}
func (b *RedisStreamsBackend) ackTransport(ctx context.Context, receipt string) error {
	if receipt == "" {
		return ErrInvalidMessage
	}
	return b.Client.XAck(ctx, b.Config.Stream, b.Config.Group, receipt).Err()
}
func (b *RedisStreamsBackend) Depth(ctx context.Context, queue uuid.UUID) (QueueStats, error) {
	stats, err := b.Store.QueueDepth(ctx, queue)
	return QueueStats{Queued: stats.Queued, Running: stats.Running, Retry: stats.Retry, Oldest: stats.Oldest}, err
}
func (b *RedisStreamsBackend) Health(ctx context.Context) error { return b.Client.Ping(ctx).Err() }
func (b *RedisStreamsBackend) Close() error                     { return b.Client.Close() }
