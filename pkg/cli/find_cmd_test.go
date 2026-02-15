package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFind_Search(t *testing.T) {
	rec := &requestRecorder{}
	searchResp := `{"data":[{"type":"table","name":"orders","schema_name":"analytics","match_field":"name"},{"type":"column","name":"order_id","schema_name":"analytics","table_name":"orders","match_field":"name"}]}`
	srv := httptest.NewServer(jsonHandler(rec, 200, searchResp))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "find", "order"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	// Verify the search API was called
	captured := rec.last()
	assert.Equal(t, "GET", captured.Method)
	assert.Contains(t, captured.Path, "/v1/search")
	assert.Contains(t, captured.Query, "query=order")

	// Verify JSON output is valid
	var parsed interface{}
	require.NoError(t, json.Unmarshal([]byte(output), &parsed), "output should be valid JSON")
}

func TestFind_WithType(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "find", "test", "--type", "table"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Contains(t, captured.Query, "type=table")
}

func TestFind_Tables(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[{"type":"table","name":"orders","schema_name":"main","match_field":"name"}]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "find", "tables", "orders"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Contains(t, captured.Query, "type=table")
}

func TestFind_Columns(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "find", "columns", "email"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Contains(t, captured.Query, "type=column")
}

func TestFind_APIError(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 401, `{"code":401,"message":"unauthorized"}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "find", "test"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API error")
}

func TestFind_WithCatalog(t *testing.T) {
	rec := &requestRecorder{}
	mux := http.NewServeMux()
	mux.HandleFunc("/", jsonHandler(rec, 200, `{"data":[]}`))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "find", "test", "--catalog", "production"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Contains(t, captured.Query, "catalog=production")
}
