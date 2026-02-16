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

func TestDescribe_Catalog_NotFound(t *testing.T) {
	rec := &requestRecorder{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs/nonexistent", jsonHandler(rec, 404, `{"code":404,"message":"catalog not found"}`))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "describe", "nonexistent"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API error")
}

func TestDescribe_Schema_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs/main/schemas/missing", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"code":404,"message":"schema not found"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "describe", "main.missing"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API error")
}

func TestDescribe_Table_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/tables/missing", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"code":404,"message":"table not found"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "describe", "main.analytics.missing"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API error")
}

func TestDescribe_Table_WithRowFiltersAndColumnMasks(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{
			"name":"orders",
			"table_id":"t1",
			"table_type":"MANAGED",
			"columns":[{"name":"id","type":"BIGINT"},{"name":"customer_id","type":"INTEGER"},{"name":"amount","type":"DECIMAL"}]
		}`))
	})
	mux.HandleFunc("/v1/tables/t1/row-filters", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"id":"rf1","filter_sql":"region = 'US'","description":"US-only filter"}]}`))
	})
	mux.HandleFunc("/v1/tables/t1/column-masks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"column_name":"amount","mask_expression":"CASE WHEN role='admin' THEN amount ELSE NULL END","description":"Admin-only amounts"}]}`))
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
	assert.Contains(t, result, "row_filters", "should include row_filters in JSON output")
	assert.Contains(t, result, "column_masks", "should include column_masks in JSON output")

	// Verify row filters data
	rf, ok := result["row_filters"].(map[string]interface{})
	require.True(t, ok, "row_filters should be an object")
	rfData, ok := rf["data"].([]interface{})
	require.True(t, ok)
	assert.Len(t, rfData, 1, "should have one row filter")

	// Verify column masks data
	cm, ok := result["column_masks"].(map[string]interface{})
	require.True(t, ok, "column_masks should be an object")
	cmData, ok := cm["data"].([]interface{})
	require.True(t, ok)
	assert.Len(t, cmData, 1, "should have one column mask")
}

func TestDescribe_Schema_WithViewsAndVolumes(t *testing.T) {
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
		_, _ = w.Write([]byte(`{"data":[{"name":"revenue_summary","comment":"Daily revenue view"}]}`))
	})
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/volumes", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"name":"raw_data","volume_type":"MANAGED"}]}`))
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
	assert.Contains(t, result, "views", "should include views in JSON output")
	assert.Contains(t, result, "volumes", "should include volumes in JSON output")

	// Verify views data is non-nil
	views, ok := result["views"].(map[string]interface{})
	require.True(t, ok, "views should be an object")
	viewsData, ok := views["data"].([]interface{})
	require.True(t, ok)
	assert.Len(t, viewsData, 1, "should have one view")

	// Verify volumes data is non-nil
	volumes, ok := result["volumes"].(map[string]interface{})
	require.True(t, ok, "volumes should be an object")
	volumesData, ok := volumes["data"].([]interface{})
	require.True(t, ok)
	assert.Len(t, volumesData, 1, "should have one volume")
}

func TestDescribe_Table_WithStatisticsAndTags(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{
			"name":"orders",
			"table_id":"t1",
			"table_type":"MANAGED",
			"owner":"admin",
			"comment":"Orders table",
			"columns":[
				{"name":"id","type":"BIGINT","nullable":"false","comment":"Primary key"},
				{"name":"amount","type":"DECIMAL","nullable":"true","comment":"Order amount"}
			],
			"statistics":{
				"row_count":"1000",
				"size_bytes":"50000",
				"column_count":"2",
				"last_profiled_at":"2026-01-15T12:00:00Z"
			},
			"tags":[
				{"key":"env","value":"production"},
				{"key":"team","value":"analytics"}
			]
		}`))
	})
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

	// Test JSON output includes statistics and tags
	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "describe", "main.analytics.orders"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Contains(t, result, "table")

	table, ok := result["table"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "orders", table["name"])
	assert.Equal(t, "MANAGED", table["table_type"])

	// Verify statistics are present
	stats, ok := table["statistics"].(map[string]interface{})
	require.True(t, ok, "table should have statistics")
	assert.Equal(t, "1000", stats["row_count"])
	assert.Equal(t, "50000", stats["size_bytes"])

	// Verify columns are present
	cols, ok := table["columns"].([]interface{})
	require.True(t, ok, "table should have columns")
	assert.Len(t, cols, 2)

	// Verify tags are present
	tags, ok := table["tags"].([]interface{})
	require.True(t, ok, "table should have tags")
	assert.Len(t, tags, 2)
}

func TestDescribe_Table_TextOutput_WithSecurityPolicies(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/tables/orders", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{
			"name":"orders",
			"table_id":"t1",
			"table_type":"MANAGED",
			"owner":"admin",
			"columns":[{"name":"id","type":"BIGINT","nullable":"false","comment":"PK"}],
			"statistics":{"row_count":"500","size_bytes":"25000"},
			"tags":[{"key":"env","value":"dev"}]
		}`))
	})
	mux.HandleFunc("/v1/tables/t1/row-filters", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"id":"rf1","filter_sql":"active = true","description":"Active rows only"}]}`))
	})
	mux.HandleFunc("/v1/tables/t1/column-masks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"column_name":"id","mask_expression":"NULL","description":"masked"}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "describe", "main.analytics.orders"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	// Verify text output contains all sections
	assert.Contains(t, output, "TABLE: main.analytics.orders")
	assert.Contains(t, output, "table_type:")
	assert.Contains(t, output, "MANAGED")
	assert.Contains(t, output, "STATISTICS:")
	assert.Contains(t, output, "row_count:")
	assert.Contains(t, output, "500")
	assert.Contains(t, output, "COLUMNS (1):")
	assert.Contains(t, output, "TAGS (1):")
	assert.Contains(t, output, "ROW FILTERS (1):")
	assert.Contains(t, output, "active = true")
	assert.Contains(t, output, "COLUMN MASKS (1):")
	assert.Contains(t, output, "masked")
}

func TestDescribe_Platform_TextOutput(t *testing.T) {
	rec := &requestRecorder{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs", jsonHandler(rec, 200, `{"data":[{"name":"main","status":"active","is_default":true},{"name":"staging","status":"active","is_default":false}]}`))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "describe"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	assert.Contains(t, output, "PLATFORM OVERVIEW")
	assert.Contains(t, output, "catalogs: 2")
	assert.Contains(t, output, "main")
	assert.Contains(t, output, "staging")
}

func TestDescribe_Schema_ViewsEndpointFails(t *testing.T) {
	// Views endpoint returns 404 (best-effort) — should not fail the overall command
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"name":"analytics"}`))
	})
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/tables", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"data":[{"name":"t1"}]}`))
	})
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/views", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"code":404,"message":"not supported"}`))
	})
	mux.HandleFunc("/v1/catalogs/main/schemas/analytics/volumes", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"code":404,"message":"not supported"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "describe", "main.analytics"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err, "should not fail when views/volumes return 404")

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Contains(t, result, "schema")
	assert.Contains(t, result, "tables")
	// views and volumes should be nil in JSON since endpoints returned 404
	assert.Nil(t, result["views"], "views should be nil when endpoint returns 404")
	assert.Nil(t, result["volumes"], "volumes should be nil when endpoint returns 404")
}
