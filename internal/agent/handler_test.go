package agent

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
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
	return setupAgentTestWithConfig(t, HandlerConfig{CursorMode: true})
}

func setupAgentTestWithConfig(t *testing.T, cfg HandlerConfig) *httptest.Server {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	if cfg.AgentToken == "" {
		cfg.AgentToken = testToken
	}
	if cfg.StartTime.IsZero() {
		cfg.StartTime = time.Now()
	}
	if cfg.MaxMemoryGB == 0 {
		cfg.MaxMemoryGB = 4
	}
	cfg.DB = db

	handler := NewHandler(cfg)

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
	assert.Contains(t, health, "active_queries")
	assert.Contains(t, health, "queued_jobs")
	assert.Contains(t, health, "running_jobs")
	assert.Contains(t, health, "completed_jobs")
	assert.Contains(t, health, "stored_jobs")
	assert.Contains(t, health, "cleaned_jobs")
	assert.Contains(t, health, "query_result_ttl_seconds")
}

func TestAgentHandler_Metrics(t *testing.T) {
	t.Parallel()
	srv := setupAgentTest(t)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/metrics", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "duck_compute_active_queries")
	assert.Contains(t, string(body), "duck_compute_cleaned_jobs_total")
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

func TestAgentHandler_QueryLifecycle(t *testing.T) {
	t.Parallel()
	srv := setupAgentTest(t)

	postBody := compute.SubmitQueryRequest{SQL: "SELECT 1 AS id UNION ALL SELECT 2 AS id"}
	payload, err := json.Marshal(postBody)
	require.NoError(t, err)

	postReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/queries", bytes.NewReader(payload))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	postReq.Header.Set("X-Agent-Token", testToken)

	postResp, err := http.DefaultClient.Do(postReq)
	require.NoError(t, err)
	defer func() { _ = postResp.Body.Close() }()
	require.Equal(t, http.StatusAccepted, postResp.StatusCode)

	var submit compute.SubmitQueryResponse
	require.NoError(t, json.NewDecoder(postResp.Body).Decode(&submit))
	require.NotEmpty(t, submit.QueryID)

	var status compute.QueryStatusResponse
	deadline := time.Now().Add(3 * time.Second)
	for {
		statusReq, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/queries/"+submit.QueryID, nil)
		require.NoError(t, reqErr)
		statusReq.Header.Set("X-Agent-Token", testToken)

		statusResp, doErr := http.DefaultClient.Do(statusReq)
		require.NoError(t, doErr)
		require.Equal(t, http.StatusOK, statusResp.StatusCode)
		require.NoError(t, json.NewDecoder(statusResp.Body).Decode(&status))
		_ = statusResp.Body.Close()

		if status.Status == compute.QueryStatusSucceeded {
			break
		}
		require.True(t, time.Now().Before(deadline), "query did not complete in time, last status=%s", status.Status)
		time.Sleep(50 * time.Millisecond)
	}

	resultsReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, fmt.Sprintf("%s/queries/%s/results?max_results=1", srv.URL, submit.QueryID), nil)
	require.NoError(t, err)
	resultsReq.Header.Set("X-Agent-Token", testToken)

	resultsResp, err := http.DefaultClient.Do(resultsReq)
	require.NoError(t, err)
	defer func() { _ = resultsResp.Body.Close() }()
	require.Equal(t, http.StatusOK, resultsResp.StatusCode)

	var page1 compute.FetchQueryResultsResponse
	require.NoError(t, json.NewDecoder(resultsResp.Body).Decode(&page1))
	require.Equal(t, 2, page1.RowCount)
	require.Len(t, page1.Rows, 1)
	require.NotEmpty(t, page1.NextPageToken)

	resultsReq2, err := http.NewRequestWithContext(context.Background(), http.MethodGet, fmt.Sprintf("%s/queries/%s/results?page_token=%s&max_results=1", srv.URL, submit.QueryID, page1.NextPageToken), nil)
	require.NoError(t, err)
	resultsReq2.Header.Set("X-Agent-Token", testToken)

	resultsResp2, err := http.DefaultClient.Do(resultsReq2)
	require.NoError(t, err)
	defer func() { _ = resultsResp2.Body.Close() }()
	require.Equal(t, http.StatusOK, resultsResp2.StatusCode)

	var page2 compute.FetchQueryResultsResponse
	require.NoError(t, json.NewDecoder(resultsResp2.Body).Decode(&page2))
	require.Len(t, page2.Rows, 1)
	require.Empty(t, page2.NextPageToken)

	cancelReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/queries/"+submit.QueryID+"/cancel", bytes.NewReader([]byte("{}")))
	require.NoError(t, err)
	cancelReq.Header.Set("Content-Type", "application/json")
	cancelReq.Header.Set("X-Agent-Token", testToken)

	cancelResp, err := http.DefaultClient.Do(cancelReq)
	require.NoError(t, err)
	defer func() { _ = cancelResp.Body.Close() }()
	require.Equal(t, http.StatusOK, cancelResp.StatusCode)
}

func TestAgentHandler_QueryLifecycleIdempotentRequestID(t *testing.T) {
	t.Parallel()
	srv := setupAgentTest(t)

	body := compute.SubmitQueryRequest{SQL: "SELECT 42 AS answer", RequestID: "idem-req-1"}
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	post := func() compute.SubmitQueryResponse {
		req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/queries", bytes.NewReader(payload))
		require.NoError(t, reqErr)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Agent-Token", testToken)

		resp, doErr := http.DefaultClient.Do(req)
		require.NoError(t, doErr)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusAccepted, resp.StatusCode)

		var submit compute.SubmitQueryResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&submit))
		require.NotEmpty(t, submit.QueryID)
		return submit
	}

	first := post()
	second := post()

	assert.Equal(t, first.QueryID, second.QueryID)
}

func TestAgentHandler_QueryResultTTL_CleansExpiredJob(t *testing.T) {
	t.Parallel()
	srv := setupAgentTestWithConfig(t, HandlerConfig{QueryResultTTL: 30 * time.Millisecond, CleanupInterval: 10 * time.Millisecond, CursorMode: true})

	payload, err := json.Marshal(compute.SubmitQueryRequest{SQL: "SELECT 1", RequestID: "ttl-req"})
	require.NoError(t, err)

	postReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/queries", bytes.NewReader(payload))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	postReq.Header.Set("X-Agent-Token", testToken)

	postResp, err := http.DefaultClient.Do(postReq)
	require.NoError(t, err)
	defer func() { _ = postResp.Body.Close() }()
	require.Equal(t, http.StatusAccepted, postResp.StatusCode)

	var submit compute.SubmitQueryResponse
	require.NoError(t, json.NewDecoder(postResp.Body).Decode(&submit))
	require.NotEmpty(t, submit.QueryID)

	deadline := time.Now().Add(2 * time.Second)
	for {
		statusReq, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/queries/"+submit.QueryID, nil)
		require.NoError(t, reqErr)
		statusReq.Header.Set("X-Agent-Token", testToken)

		statusResp, doErr := http.DefaultClient.Do(statusReq)
		require.NoError(t, doErr)
		if statusResp.StatusCode == http.StatusOK {
			var status compute.QueryStatusResponse
			require.NoError(t, json.NewDecoder(statusResp.Body).Decode(&status))
			_ = statusResp.Body.Close()
			if status.Status == compute.QueryStatusSucceeded {
				break
			}
		} else {
			_ = statusResp.Body.Close()
		}

		require.True(t, time.Now().Before(deadline), "query did not reach succeeded")
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(70 * time.Millisecond)

	healthReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/health", nil)
	require.NoError(t, err)
	healthResp, err := http.DefaultClient.Do(healthReq)
	require.NoError(t, err)
	_ = healthResp.Body.Close()
	require.Equal(t, http.StatusOK, healthResp.StatusCode)

	statusReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/queries/"+submit.QueryID, nil)
	require.NoError(t, err)
	statusReq.Header.Set("X-Agent-Token", testToken)
	statusResp, err := http.DefaultClient.Do(statusReq)
	require.NoError(t, err)
	defer func() { _ = statusResp.Body.Close() }()

	assert.Equal(t, http.StatusNotFound, statusResp.StatusCode)
}

func TestAgentHandler_QueryLifecycle_Disabled(t *testing.T) {
	t.Parallel()

	srv := setupAgentTestWithConfig(t, HandlerConfig{CursorMode: false})
	payload, err := json.Marshal(compute.SubmitQueryRequest{SQL: "SELECT 1"})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/queries", bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-Token", testToken)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
