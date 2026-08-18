package queuebackend

import "sync"

type Registry struct {
	mu       sync.RWMutex
	backends map[Type]Backend
}

func NewRegistry(backends ...Backend) *Registry {
	r := &Registry{backends: make(map[Type]Backend)}
	for _, backend := range backends {
		r.Register(backend)
	}
	return r
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
