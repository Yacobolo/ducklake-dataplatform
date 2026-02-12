package compute

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/duckdb/duckdb-go/v2"

	"duck-demo/internal/domain"
)

func TestRemoteCache_GetOrCreate(t *testing.T) {
	localDB := openTestDuckDB(t)
	cache := NewRemoteCache(localDB)

	ep := &domain.ComputeEndpoint{
		ID:        1,
		Name:      "test-ep",
		URL:       "https://compute-1.example.com:9443",
		AuthToken: "secret",
	}

	t.Run("creates_new_executor", func(t *testing.T) {
		exec := cache.GetOrCreate(ep)
		require.NotNil(t, exec)
		assert.Equal(t, "https://compute-1.example.com:9443", exec.endpointURL)
		assert.Equal(t, "secret", exec.authToken)
	})

	t.Run("returns_cached_executor", func(t *testing.T) {
		exec1 := cache.GetOrCreate(ep)
		exec2 := cache.GetOrCreate(ep)
		assert.Same(t, exec1, exec2) // Same pointer — from cache
	})

	t.Run("different_endpoints_different_executors", func(t *testing.T) {
		ep2 := &domain.ComputeEndpoint{
			ID:        2,
			Name:      "other-ep",
			URL:       "https://compute-2.example.com:9443",
			AuthToken: "other-secret",
		}

		exec1 := cache.GetOrCreate(ep)
		exec2 := cache.GetOrCreate(ep2)
		assert.NotSame(t, exec1, exec2) // Different endpoints
		assert.Equal(t, "https://compute-2.example.com:9443", exec2.endpointURL)
	})

	t.Run("concurrent_access", func(t *testing.T) {
		concurrentCache := NewRemoteCache(localDB)
		var wg sync.WaitGroup
		results := make([]*RemoteExecutor, 10)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				results[idx] = concurrentCache.GetOrCreate(ep)
			}(i)
		}
		wg.Wait()

		// All goroutines should get the same executor
		for i := 1; i < 10; i++ {
			assert.Same(t, results[0], results[i])
		}
	})
}
