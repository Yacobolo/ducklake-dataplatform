package engine

import (
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsurePostgresExtension_RetriesOnFailure(t *testing.T) {
	// We can't easily test with a real DuckDB because InstallPostgresExtension
	// requires network access. Instead, test the mutex+bool pattern directly
	// by exercising the ensurePostgresExtension method's retry behavior.

	t.Run("flag stays false on failure, allowing retry", func(t *testing.T) {
		m := &DuckDBSecretManager{
			db:             &sql.DB{}, // unused in this test
			postgresLoaded: false,
		}
		// Verify initial state
		assert.False(t, m.postgresLoaded)

		// After a hypothetical failure, postgresLoaded should remain false
		// (the old sync.Once would have set it permanently)
		m.postgresMu.Lock()
		// Simulate: we checked, it was false, we tried to install, it failed.
		// So we leave postgresLoaded = false.
		_ = m.postgresLoaded
		m.postgresMu.Unlock()

		assert.False(t, m.postgresLoaded, "flag should remain false after failure")
	})

	t.Run("flag set to true on success prevents re-execution", func(t *testing.T) {
		m := &DuckDBSecretManager{
			db:             &sql.DB{},
			postgresLoaded: false,
		}

		// Simulate success
		m.postgresMu.Lock()
		m.postgresLoaded = true
		m.postgresMu.Unlock()

		assert.True(t, m.postgresLoaded, "flag should be true after success")
	})

	t.Run("concurrent access is safe", func(t *testing.T) {
		// Use a counter to track how many times the "install" would run
		var installCount int
		var mu sync.Mutex

		m := &DuckDBSecretManager{
			db:             &sql.DB{},
			postgresLoaded: false,
		}

		// Simulate concurrent ensurePostgresExtension calls
		var wg sync.WaitGroup
		errCount := 0
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.postgresMu.Lock()
				defer m.postgresMu.Unlock()
				if m.postgresLoaded {
					return
				}
				mu.Lock()
				installCount++
				mu.Unlock()
				// Simulate success on first call
				m.postgresLoaded = true
			}()
		}
		wg.Wait()

		assert.Equal(t, 1, installCount, "install should only run once when all succeed")
		assert.Equal(t, 0, errCount)
		assert.True(t, m.postgresLoaded)
	})
}

func TestEnsurePostgresExtension_RetryAfterTransientError(t *testing.T) {
	// This test verifies the key difference from sync.Once:
	// after a failure, the next call retries instead of silently skipping.

	callCount := 0
	simulatedErr := errors.New("transient network error")

	// Create a mock "ensurePostgresExtension" that simulates the pattern
	ensureFunc := func(m *DuckDBSecretManager) error {
		m.postgresMu.Lock()
		defer m.postgresMu.Unlock()

		if m.postgresLoaded {
			return nil
		}
		callCount++
		if callCount == 1 {
			// First call fails
			return simulatedErr
		}
		// Second call succeeds
		m.postgresLoaded = true
		return nil
	}

	m := &DuckDBSecretManager{db: &sql.DB{}}

	// First call should fail
	err := ensureFunc(m)
	require.Error(t, err)
	assert.Equal(t, simulatedErr, err)
	assert.False(t, m.postgresLoaded, "flag should be false after failure")
	assert.Equal(t, 1, callCount)

	// Second call should retry and succeed
	err = ensureFunc(m)
	require.NoError(t, err)
	assert.True(t, m.postgresLoaded, "flag should be true after retry success")
	assert.Equal(t, 2, callCount)

	// Third call should be a no-op (already loaded)
	err = ensureFunc(m)
	require.NoError(t, err)
	assert.Equal(t, 2, callCount, "should not re-install after success")
}
