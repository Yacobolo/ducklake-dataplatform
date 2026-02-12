package compute

import (
	"database/sql"
	"sync"

	"duck-demo/internal/domain"
)

// RemoteCache manages cached RemoteExecutor instances keyed by endpoint ID.
// It avoids creating duplicate HTTP clients for the same endpoint.
type RemoteCache struct {
	mu      sync.RWMutex
	entries map[int64]*RemoteExecutor
	localDB *sql.DB
}

// NewRemoteCache creates a RemoteCache that materializes remote results into
// the given local DuckDB instance.
func NewRemoteCache(localDB *sql.DB) *RemoteCache {
	return &RemoteCache{
		entries: make(map[int64]*RemoteExecutor),
		localDB: localDB,
	}
}

// GetOrCreate returns an existing RemoteExecutor for the endpoint or creates a
// new one. Uses double-checked locking to minimise lock contention.
func (c *RemoteCache) GetOrCreate(ep *domain.ComputeEndpoint) *RemoteExecutor {
	c.mu.RLock()
	if exec, ok := c.entries[ep.ID]; ok {
		c.mu.RUnlock()
		return exec
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if exec, ok := c.entries[ep.ID]; ok {
		return exec
	}

	exec := NewRemoteExecutor(ep.URL, ep.AuthToken, c.localDB)
	c.entries[ep.ID] = exec
	return exec
}
