//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"sync"
)

type IOManager interface {
	LoadInput(ctx context.Context, key string) (map[string]any, error)
	StoreOutput(ctx context.Context, key string, value map[string]any) error
}

var _ IOManager = (*InMemoryIOManager)(nil)

type InMemoryIOManager struct {
	mu   sync.RWMutex
	data map[string]map[string]any
}

func NewInMemoryIOManager() *InMemoryIOManager {
	return &InMemoryIOManager{data: map[string]map[string]any{}}
}

func (m *InMemoryIOManager) LoadInput(_ context.Context, key string) (map[string]any, error) {
	m.mu.RLock()
	value, ok := m.data[key]
	m.mu.RUnlock()
	if !ok {
		return map[string]any{}, nil
	}
	out := make(map[string]any, len(value))
	for k, v := range value {
		out[k] = v
	}
	return out, nil
}

func (m *InMemoryIOManager) StoreOutput(_ context.Context, key string, value map[string]any) error {
	copyValue := make(map[string]any, len(value))
	for k, v := range value {
		copyValue[k] = v
	}
	m.mu.Lock()
	m.data[key] = copyValue
	m.mu.Unlock()
	return nil
}
