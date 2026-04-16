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
	assert.Contains(t, captured.Path, "/v1/catalogs/search")
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

func TestFind_SearchIncludesViews(t *testing.T) {
	rec := &requestRecorder{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs/search", jsonHandler(rec, 200, `{"data":[]}`))
	mux.HandleFunc("/v1/catalogs/seeded_local/schemas", jsonHandler(rec, 200, `{"data":[{"name":"nyc_taxi"}]}`))
	mux.HandleFunc("/v1/catalogs/seeded_local/schemas/nyc_taxi/views", jsonHandler(rec, 200, `{"data":[{"name":"trip_daily_metrics","comment":"Daily trip metrics"}]}`))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "find", "trip_daily", "--catalog", "seeded_local"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var result struct {
		Data []struct {
			Type       string  `json:"type"`
			Name       string  `json:"name"`
			SchemaName *string `json:"schema_name"`
			MatchField string  `json:"match_field"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	require.Len(t, result.Data, 1)
	assert.Equal(t, "view", result.Data[0].Type)
	assert.Equal(t, "trip_daily_metrics", result.Data[0].Name)
	require.NotNil(t, result.Data[0].SchemaName)
	assert.Equal(t, "nyc_taxi", *result.Data[0].SchemaName)
	assert.Equal(t, "name", result.Data[0].MatchField)
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

	var searchQuery string
	for _, req := range rec.requests {
		if req.Path == "/v1/catalogs/search" {
			searchQuery = req.Query
			break
		}
	}
	assert.Contains(t, searchQuery, "catalog=production")
}

func TestFind_GlobWildcard(t *testing.T) {
	// Server returns multiple results; client-side glob filtering should narrow them down.
	rec := &requestRecorder{}
	searchResp := `{"data":[
		{"type":"table","name":"orders","schema_name":"analytics","match_field":"name"},
		{"type":"table","name":"order_items","schema_name":"analytics","match_field":"name"},
		{"type":"table","name":"products","schema_name":"analytics","match_field":"name"}
	]}`
	srv := httptest.NewServer(jsonHandler(rec, 200, searchResp))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "find", "order*"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	// Verify API was called with wildcards stripped
	captured := rec.last()
	assert.Contains(t, captured.Query, "query=order", "API query should have wildcards stripped")
	assert.NotContains(t, captured.Query, "*", "API query should not contain wildcards")

	// Verify client-side filtering
	var result struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Len(t, result.Data, 2, "glob 'order*' should match 'orders' and 'order_items' but not 'products'")
	for _, item := range result.Data {
		assert.True(t, item.Name == "orders" || item.Name == "order_items",
			"matched item %q should start with 'order'", item.Name)
	}
}

func TestFind_MaxResults(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "find", "test", "--max-results", "25"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Contains(t, captured.Query, "max_results=25", "should pass max_results to API")
}

func TestFind_ColumnDisplayName(t *testing.T) {
	// Columns should be displayed as tableName.columnName in table output
	rec := &requestRecorder{}
	searchResp := `{"data":[
		{"type":"column","name":"email","schema_name":"users_schema","table_name":"users","match_field":"name"},
		{"type":"table","name":"emails","schema_name":"comms","match_field":"name"}
	]}`
	srv := httptest.NewServer(jsonHandler(rec, 200, searchResp))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "find", "email"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	// In table output, column display name should be "tableName.columnName"
	assert.Contains(t, output, "users.email", "column should be displayed as tableName.columnName")
	assert.Contains(t, output, "emails", "table should be displayed by name")
}

func TestFind_Tables_JSONOutput(t *testing.T) {
	rec := &requestRecorder{}
	searchResp := `{"data":[{"type":"table","name":"orders","schema_name":"main","match_field":"name"}]}`
	srv := httptest.NewServer(jsonHandler(rec, 200, searchResp))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "find", "tables", "orders"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	captured := rec.last()
	assert.Contains(t, captured.Query, "type=table")

	var result struct {
		Data []struct {
			Type string `json:"type"`
			Name string `json:"name"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.NotEmpty(t, result.Data, "should return results")
	assert.Equal(t, "table", result.Data[0].Type)
	assert.Equal(t, "orders", result.Data[0].Name)
}

func TestFind_Columns_JSONOutput(t *testing.T) {
	rec := &requestRecorder{}
	searchResp := `{"data":[{"type":"column","name":"email","schema_name":"main","table_name":"users","match_field":"name"}]}`
	srv := httptest.NewServer(jsonHandler(rec, 200, searchResp))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "find", "columns", "email"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	captured := rec.last()
	assert.Contains(t, captured.Query, "type=column")

	var result struct {
		Data []struct {
			Type      string  `json:"type"`
			Name      string  `json:"name"`
			TableName *string `json:"table_name"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.NotEmpty(t, result.Data, "should return results")
	assert.Equal(t, "column", result.Data[0].Type)
	assert.Equal(t, "email", result.Data[0].Name)
	require.NotNil(t, result.Data[0].TableName)
	assert.Equal(t, "users", *result.Data[0].TableName)
}

func TestFind_DefaultMaxResults(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(jsonHandler(rec, 200, `{"data":[]}`))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "find", "test"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	captured := rec.last()
	assert.Contains(t, captured.Query, "max_results=100", "default max_results should be 100")
}
