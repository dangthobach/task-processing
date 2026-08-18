package queuebackend

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	store "github.com/example/task-processing/internal/persistence/postgres"
	"github.com/google/uuid"
)

type Registry struct {
	mu       sync.RWMutex
	backends map[Type]Backend
	dynamic  map[uuid.UUID]versionedBackend
}

type versionedBackend struct {
	backend Backend
	version int64
}

func NewRegistry(backends ...Backend) *Registry {
	r := &Registry{backends: make(map[Type]Backend), dynamic: make(map[uuid.UUID]versionedBackend)}
	for _, backend := range backends {
		r.Register(backend)
	}
	return r
}

func (r *Registry) GetForBackend(id *uuid.UUID, kind Type) (Backend, error) {
	if id != nil {
		r.mu.RLock()
		item, ok := r.dynamic[*id]
		r.mu.RUnlock()
		if ok && item.backend.Type() == kind {
			return item.backend, nil
		}
		return nil, fmt.Errorf("%w: backend_id=%s", ErrUnknownBackend, id.String())
	}
	return r.Get(kind)
}

// Refresh resolves active database backends by ID. Configuration is encrypted
// at rest and recreated only when the optimistic-lock row version changes.
func (r *Registry) Refresh(ctx context.Context, s *store.Store) error {
	configs, err := s.ActiveQueueBackendConfigs(ctx)
	if err != nil {
		return err
	}
	seen := make(map[uuid.UUID]struct{}, len(configs))
	for _, config := range configs {
		seen[config.ID] = struct{}{}
		r.mu.RLock()
		current, ok := r.dynamic[config.ID]
		r.mu.RUnlock()
		if ok && current.version == config.RowVersion {
			continue
		}
		plain, err := s.DecryptQueueBackendConfig(ctx, config.ID, config.EncryptedConfig, config.EncryptionKeyRef)
		if err != nil {
			return err
		}
		backend, err := backendFromConfig(s, Type(config.Type), plain)
		if err != nil {
			return fmt.Errorf("queue backend %s: %w", config.ID, err)
		}
		r.mu.Lock()
		previous, exists := r.dynamic[config.ID]
		r.dynamic[config.ID] = versionedBackend{backend: backend, version: config.RowVersion}
		r.mu.Unlock()
		if exists {
			closeBackend(previous.backend)
		}
	}
	r.mu.Lock()
	removed := make([]Backend, 0)
	for id, item := range r.dynamic {
		if _, ok := seen[id]; !ok {
			delete(r.dynamic, id)
			removed = append(removed, item.backend)
		}
	}
	r.mu.Unlock()
	for _, backend := range removed {
		closeBackend(backend)
	}
	return nil
}

func backendFromConfig(s *store.Store, kind Type, raw []byte) (Backend, error) {
	switch kind {
	case RedisStreams:
		var config struct {
			URL         string `json:"url"`
			Stream      string `json:"stream"`
			Group       string `json:"group"`
			BlockMS     int    `json:"block_ms"`
			ClaimIdleMS int    `json:"claim_idle_ms"`
		}
		if err := json.Unmarshal(raw, &config); err != nil {
			return nil, fmt.Errorf("decode Redis Streams config: %w", err)
		}
		return NewRedisStreams(s, RedisStreamsConfig{URL: config.URL, Stream: config.Stream, Group: config.Group, Block: time.Duration(config.BlockMS) * time.Millisecond, ClaimIdle: time.Duration(config.ClaimIdleMS) * time.Millisecond})
	case JetStream:
		var config struct {
			URL         string `json:"url"`
			Stream      string `json:"stream"`
			Subject     string `json:"subject"`
			Consumer    string `json:"consumer"`
			FetchWaitMS int    `json:"fetch_wait_ms"`
			AckWaitMS   int    `json:"ack_wait_ms"`
		}
		if err := json.Unmarshal(raw, &config); err != nil {
			return nil, fmt.Errorf("decode JetStream config: %w", err)
		}
		return NewJetStream(s, JetStreamConfig{URL: config.URL, Stream: config.Stream, Subject: config.Subject, Consumer: config.Consumer, FetchWait: time.Duration(config.FetchWaitMS) * time.Millisecond, AckWait: time.Duration(config.AckWaitMS) * time.Millisecond})
	default:
		return nil, fmt.Errorf("unsupported dynamic backend type %q", kind)
	}
}

func closeBackend(backend Backend) {
	if closable, ok := backend.(ClosableBackend); ok {
		_ = closable.Close()
	}
}

func (r *Registry) Register(backend Backend) {
	if backend == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backends[backend.Type()] = backend
}

func (r *Registry) Get(kind Type) (Backend, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	backend, ok := r.backends[kind]
	if !ok {
		return nil, ErrUnknownBackend
	}
	return backend, nil
}

// List returns a stable snapshot so the worker can reserve from every enabled
// transport without holding the registry lock during network I/O.
func (r *Registry) List() []Backend {
	r.mu.RLock()
	items := make([]Backend, 0, len(r.backends)+len(r.dynamic))
	for _, backend := range r.backends {
		items = append(items, backend)
	}
	for _, item := range r.dynamic {
		items = append(items, item.backend)
	}
	r.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool { return items[i].Type() < items[j].Type() })
	return items
}
