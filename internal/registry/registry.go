// Package registry owns compiled handler registrations. It is deliberately
// separate from control-plane function definitions: a definition is runnable
// only when at least one live worker advertises the matching capability.
package registry

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/example/task-processing/internal/domain/job"
)

const AnyVersion = "*"

type Capability struct {
	FunctionKey, FunctionVersion, ExecutionMode string
}

type Registry struct {
	mu            sync.RWMutex
	handlers      map[string]map[string]job.Handler
	batchHandlers map[string]map[string]job.BatchHandler
}

func New() *Registry {
	return &Registry{handlers: map[string]map[string]job.Handler{}, batchHandlers: map[string]map[string]job.BatchHandler{}}
}

// Register keeps source compatibility for local development. Production
// workers should use RegisterVersion so the control plane can prevent a
// function definition from dispatching to an incompatible binary.
func (r *Registry) Register(key string, handler job.Handler) error {
	return r.RegisterVersion(key, AnyVersion, handler)
}

func (r *Registry) RegisterVersion(key, version string, handler job.Handler) error {
	key, version = normalize(key, version)
	if key == "" || version == "" || handler == nil {
		return fmt.Errorf("function key, version and handler are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.handlers[key] == nil {
		r.handlers[key] = map[string]job.Handler{}
	}
	if _, exists := r.handlers[key][version]; exists {
		return fmt.Errorf("handler already registered: %s@%s", key, version)
	}
	r.handlers[key][version] = handler
	return nil
}

func (r *Registry) RegisterBatch(key string, handler job.BatchHandler) error {
	return r.RegisterBatchVersion(key, AnyVersion, handler)
}

func (r *Registry) RegisterBatchVersion(key, version string, handler job.BatchHandler) error {
	key, version = normalize(key, version)
	if key == "" || version == "" || handler == nil {
		return fmt.Errorf("function key, version and batch handler are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.batchHandlers[key] == nil {
		r.batchHandlers[key] = map[string]job.BatchHandler{}
	}
	if _, exists := r.batchHandlers[key][version]; exists {
		return fmt.Errorf("batch handler already registered: %s@%s", key, version)
	}
	r.batchHandlers[key][version] = handler
	return nil
}

func (r *Registry) Get(key string) (job.Handler, error) { return r.GetVersion(key, AnyVersion) }

func (r *Registry) GetVersion(key, version string) (job.Handler, error) {
	key, version = normalize(key, version)
	r.mu.RLock()
	defer r.mu.RUnlock()
	if handler := r.handlers[key][version]; handler != nil {
		return handler, nil
	}
	if handler := r.handlers[key][AnyVersion]; handler != nil {
		return handler, nil
	}
	return nil, fmt.Errorf("function handler %q version %q is not registered", key, version)
}

func (r *Registry) GetBatch(key string) (job.BatchHandler, error) {
	return r.GetBatchVersion(key, AnyVersion)
}

func (r *Registry) GetBatchVersion(key, version string) (job.BatchHandler, error) {
	key, version = normalize(key, version)
	r.mu.RLock()
	defer r.mu.RUnlock()
	if handler := r.batchHandlers[key][version]; handler != nil {
		return handler, nil
	}
	if handler := r.batchHandlers[key][AnyVersion]; handler != nil {
		return handler, nil
	}
	return nil, fmt.Errorf("batch handler %q version %q is not registered", key, version)
}

// Capabilities returns a stable snapshot for a worker startup write. No caller
// receives an internal map, avoiding registry races.
func (r *Registry) Capabilities() []Capability {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := make([]Capability, 0, len(r.handlers)+len(r.batchHandlers))
	for key, versions := range r.handlers {
		for version := range versions {
			items = append(items, Capability{FunctionKey: key, FunctionVersion: version, ExecutionMode: "SINGLE"})
		}
	}
	for key, versions := range r.batchHandlers {
		for version := range versions {
			items = append(items, Capability{FunctionKey: key, FunctionVersion: version, ExecutionMode: "BATCH"})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].FunctionKey != items[j].FunctionKey {
			return items[i].FunctionKey < items[j].FunctionKey
		}
		if items[i].FunctionVersion != items[j].FunctionVersion {
			return items[i].FunctionVersion < items[j].FunctionVersion
		}
		return items[i].ExecutionMode < items[j].ExecutionMode
	})
	return items
}

func normalize(key, version string) (string, string) {
	return strings.TrimSpace(key), strings.TrimSpace(version)
}
