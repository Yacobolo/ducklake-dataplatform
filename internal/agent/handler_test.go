package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentHandler_HealthAndMetrics(t *testing.T) {
	db := openTestDuckDB(t)
	h := NewHandler(HandlerConfig{
		DB:             db,
		StartTime:      time.Now().Add(-2 * time.Second),
		MaxMemoryGB:    8,
		QueryResultTTL: 10 * time.Minute,
		MetricsProvider: func() (active, queued, running, completed, stored, cleaned int64) {
			return 1, 2, 3, 4, 5, 6
		},
	})

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	t.Run("health", func(t *testing.T) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/health", nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close() //nolint:errcheck

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var health map[string]interface{}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&health))
		assert.Equal(t, "ok", health["status"])
		assert.Contains(t, health, "duckdb_version")
		assert.Equal(t, float64(1), health["active_queries"])
		assert.Equal(t, float64(2), health["queued_jobs"])
		assert.Equal(t, float64(3), health["running_jobs"])
		assert.Equal(t, float64(4), health["completed_jobs"])
		assert.Equal(t, float64(5), health["stored_jobs"])
		assert.Equal(t, float64(6), health["cleaned_jobs"])
		assert.Equal(t, float64(8), health["max_memory_gb"])
	})

	t.Run("metrics", func(t *testing.T) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/metrics", nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close() //nolint:errcheck

		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "text/plain; version=0.0.4", resp.Header.Get("Content-Type"))
	})
}

func TestAgentHandler_ExecutionRoutesRemoved(t *testing.T) {
	db := openTestDuckDB(t)
	h := NewHandler(HandlerConfig{DB: db, StartTime: time.Now()})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/execute", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func openTestDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}
