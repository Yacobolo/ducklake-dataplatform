package compute

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestRemoteExecutor_QueryContext(t *testing.T) {
	localDB := openTestDuckDB(t)

	t.Run("successful_query", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "POST", r.Method)
			assert.Equal(t, "/execute", r.URL.Path)
			assert.Equal(t, "test-token", r.Header.Get("X-Agent-Token"))
			assert.NotEmpty(t, r.Header.Get("X-Request-ID"))

			var req ExecuteRequest
			assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Equal(t, "SELECT 1", req.SQL)
			assert.NotEmpty(t, req.RequestID)

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(ExecuteResponse{
				Columns:  []string{"id", "name"},
				Rows:     [][]interface{}{{1, "Alice"}, {2, "Bob"}},
				RowCount: 2,
			})
		}))
		defer server.Close()

		exec := NewRemoteExecutor(server.URL, "test-token", localDB)
		rows, err := exec.QueryContext(context.Background(), "SELECT 1")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		cols, err := rows.Columns()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name"}, cols)

		var results [][]string
		for rows.Next() {
			var id, name string
			require.NoError(t, rows.Scan(&id, &name))
			results = append(results, []string{id, name})
		}
		require.NoError(t, rows.Err())
		assert.Len(t, results, 2)
		assert.Equal(t, "1", results[0][0])
		assert.Equal(t, "Alice", results[0][1])
	})

	t.Run("empty_result", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(ExecuteResponse{
				Columns:  []string{},
				Rows:     [][]interface{}{},
				RowCount: 0,
			})
		}))
		defer server.Close()

		exec := NewRemoteExecutor(server.URL, "tok", localDB)
		rows, err := exec.QueryContext(context.Background(), "SELECT 1 WHERE false")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		assert.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})

	t.Run("agent_error_response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": "table not found",
				"code":  "EXECUTION_ERROR",
			})
		}))
		defer server.Close()

		exec := NewRemoteExecutor(server.URL, "tok", localDB)
		_, err := exec.QueryContext(context.Background(), "SELECT * FROM nonexistent") //nolint:sqlclosecheck,rowserrcheck // error path
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table not found")
	})

	t.Run("invalid_json_response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("not json"))
		}))
		defer server.Close()

		exec := NewRemoteExecutor(server.URL, "tok", localDB)
		_, err := exec.QueryContext(context.Background(), "SELECT 1") //nolint:sqlclosecheck,rowserrcheck // error path
		require.Error(t, err)
		assert.Contains(t, err.Error(), "decode response")
	})

	t.Run("connection_refused", func(t *testing.T) {
		exec := NewRemoteExecutor("http://127.0.0.1:1", "tok", localDB)
		_, err := exec.QueryContext(context.Background(), "SELECT 1") //nolint:sqlclosecheck,rowserrcheck // error path
		require.Error(t, err)
		assert.Contains(t, err.Error(), "remote execute")
	})

	t.Run("null_values_in_response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(ExecuteResponse{
				Columns:  []string{"id", "name"},
				Rows:     [][]interface{}{{1, nil}, {2, "Bob"}},
				RowCount: 2,
			})
		}))
		defer server.Close()

		exec := NewRemoteExecutor(server.URL, "tok", localDB)
		rows, err := exec.QueryContext(context.Background(), "SELECT 1")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		var count int
		for rows.Next() {
			var id, name sql.NullString
			require.NoError(t, rows.Scan(&id, &name))
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 2, count)
	})
}

func TestRemoteExecutor_Ping(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "GET", r.Method)
			assert.Equal(t, "/health", r.URL.Path)
			assert.Equal(t, "test-token", r.Header.Get("X-Agent-Token"))

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "ok",
			})
		}))
		defer server.Close()

		exec := NewRemoteExecutor(server.URL, "test-token", openTestDuckDB(t))
		err := exec.Ping(context.Background())
		require.NoError(t, err)
	})

	t.Run("unhealthy_status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		exec := NewRemoteExecutor(server.URL, "tok", openTestDuckDB(t))
		err := exec.Ping(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unhealthy: status 503")
	})

	t.Run("unreachable", func(t *testing.T) {
		exec := NewRemoteExecutor("http://127.0.0.1:1", "tok", openTestDuckDB(t))
		err := exec.Ping(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "health check")
	})
}
