package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribe_Platform(t *testing.T) {
	rec := &requestRecorder{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs", jsonHandler(rec, 200, `{"data":[{"name":"main","status":"active","is_default":true}]}`))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "describe"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result interface{}
	require.NoError(t, json.Unmarshal([]byte(output), &result), "output should be valid JSON")
}

func TestDescribe_Catalog(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs/main", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"name":"main","status":"active"}`))
	})
	mux.HandleFunc("/v1/catalogs/main/schemas", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"name":"analytics"},{"name":"raw"}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "describe", "main"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Contains(t, result, "catalog")
	assert.Contains(t, result, "schemas")
}

func TestDescribe_Schema(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"name":"analytics","comment":"Analytics schema"}`))
	})
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/tables", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"name":"orders","table_type":"MANAGED"}]}`))
	})
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/views", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[]}`))
	})
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/volumes", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "describe", "main.analytics"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Contains(t, result, "schema")
	assert.Contains(t, result, "tables")
}

func TestDescribe_Table(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"name":"orders","table_id":"t1","table_type":"MANAGED","columns":[{"name":"id","type":"BIGINT"}]}`))
	})
	// Row filters and column masks endpoints (best-effort)
	mux.HandleFunc("/v1/tables/t1/row-filters", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[]}`))
	})
	mux.HandleFunc("/v1/tables/t1/column-masks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "describe", "main.analytics.orders"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Contains(t, result, "table")
}

func TestDescribe_InvalidPath(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"describe", "a.b.c.d"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid object path")
}
