package agent

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/compute"

	_ "github.com/duckdb/duckdb-go/v2"
)

const testToken = "test-agent-token-42"

// setupAgentTest creates an in-memory DuckDB, wires up the handler, and
// returns an httptest.Server. The caller should defer cleanup().
func setupAgentTest(t *testing.T) *httptest.Server {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	handler := NewHandler(HandlerConfig{
		DB:          db,
		AgentToken:  testToken,
		StartTime:   time.Now(),
		MaxMemoryGB: 4,
	})

	srv := httptest.NewServer(handler)
	t.Cleanup(func() {
		srv.Close()
		_ = db.Close()
	})

	return srv
}

// postExecute is a small helper that POSTs a JSON request to /execute.
func postExecute(t *testing.T, srv *httptest.Server, token string, body interface{}) *http.Response {
	t.Helper()

	payload, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/execute", bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-Agent-Token", token)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// === Auth ===

func TestAgentHandler_Auth(t *testing.T) {
	t.Parallel()
	srv := setupAgentTest(t)

	t.Run("missing token returns 401", func(t *testing.T) {
		t.Parallel()
		resp := postExecute(t, srv, "", compute.ExecuteRequest{SQL: "SELECT 1"})
		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

		var errResp compute.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		assert.Equal(t, "AUTH_ERROR", errResp.Code)
	})

	t.Run("wrong token returns 401", func(t *testing.T) {
		t.Parallel()
		resp := postExecute(t, srv, "wrong-token", compute.ExecuteRequest{SQL: "SELECT 1"})
		defer func() { _ = resp.Body.Close() }()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

		var errResp compute.ErrorResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
		assert.Equal(t, "AUTH_ERROR", errResp.Code)
	})
}

// === Bad request ===

func TestAgentHandler_BadRequest(t *testing.T) {
	t.Parallel()
	srv := setupAgentTest(t)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/execute", bytes.NewReader([]byte("{invalid json")))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Token", testToken)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var errResp compute.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	assert.Equal(t, "PARSE_ERROR", errResp.Code)
}

// === Successful query ===

func TestAgentHandler_SuccessfulQuery(t *testing.T) {
	t.Parallel()
	srv := setupAgentTest(t)

	resp := postExecute(t, srv, testToken, compute.ExecuteRequest{
		SQL:       "SELECT 1 AS id",
		RequestID: "req-123",
	})
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result compute.ExecuteResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))

	assert.Equal(t, []string{"id"}, result.Columns)
	assert.Equal(t, 1, result.RowCount)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "req-123", result.RequestID)

	// DuckDB returns int32 for integer literals; JSON decodes as float64.
	val, ok := result.Rows[0][0].(float64)
	require.True(t, ok, "expected float64 from JSON decode, got %T", result.Rows[0][0])
	assert.InDelta(t, float64(1), val, 0.0001)
}

// === SQL error ===

func TestAgentHandler_SQLError(t *testing.T) {
	t.Parallel()
	srv := setupAgentTest(t)

	resp := postExecute(t, srv, testToken, compute.ExecuteRequest{
		SQL:       "SELECT * FROM nonexistent_table_xyz",
		RequestID: "req-err",
	})
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	var errResp compute.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errResp))
	assert.Equal(t, "EXECUTION_ERROR", errResp.Code)
	assert.NotEmpty(t, errResp.Error)
	assert.Equal(t, "req-err", errResp.RequestID)
}

// === Health ===

func TestAgentHandler_Health(t *testing.T) {
	t.Parallel()
	srv := setupAgentTest(t)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/health", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var health map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&health))

	assert.Equal(t, "ok", health["status"])
	assert.NotEmpty(t, health["duckdb_version"], "duckdb_version should be present")
	assert.Contains(t, health, "uptime_seconds")
	assert.Contains(t, health, "max_memory_gb")
}

// === Empty result ===

func TestAgentHandler_EmptyResult(t *testing.T) {
	t.Parallel()
	srv := setupAgentTest(t)

	resp := postExecute(t, srv, testToken, compute.ExecuteRequest{
		SQL: "SELECT 1 WHERE false",
	})
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result compute.ExecuteResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))

	assert.Equal(t, 0, result.RowCount)
	assert.Empty(t, result.Rows)
	assert.NotEmpty(t, result.Columns, "columns should still be present even for empty results")
}
