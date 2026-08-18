package registry

import (
	"fmt"
	"github.com/example/task-processing/internal/domain/job"
	"sync"
)

type Registry struct {
	mu            sync.RWMutex
	handlers      map[string]job.Handler
	batchHandlers map[string]job.BatchHandler
}

func New() *Registry {
	return &Registry{handlers: map[string]job.Handler{}, batchHandlers: map[string]job.BatchHandler{}}
}
func (r *Registry) Register(key string, h job.Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[key]; ok {
		panic("handler already registered: " + key)
	}
	r.handlers[key] = h
}
func (r *Registry) RegisterBatch(key string, h job.BatchHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.batchHandlers[key]; ok {
		panic("batch handler already registered: " + key)
	}
	r.batchHandlers[key] = h
}
func (r *Registry) Get(key string) (job.Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[key]
	if !ok {
		return nil, fmt.Errorf("function handler %q is not registered", key)
	}
	return h, nil
}
func (r *Registry) GetBatch(key string) (job.BatchHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.batchHandlers[key]
	if !ok {
		return nil, fmt.Errorf("batch handler %q is not registered", key)
	}
	return h, nil
}
