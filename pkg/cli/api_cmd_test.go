package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/pkg/cli/gen"
)

func TestAPI_ListAll(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "api", "list"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var endpoints []gen.APIGenEndpoint
	require.NoError(t, json.Unmarshal([]byte(output), &endpoints))
	assert.Greater(t, len(endpoints), 50, "should have many API endpoints")
}

func TestAPI_Search(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "api", "search", "schema"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var endpoints []gen.APIGenEndpoint
	require.NoError(t, json.Unmarshal([]byte(output), &endpoints))
	assert.NotEmpty(t, endpoints, "should find schema-related endpoints")
}

func TestAPI_Describe(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "api", "describe", "listSchemas"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var ep gen.APIGenEndpoint
	require.NoError(t, json.Unmarshal([]byte(output), &ep))
	assert.Equal(t, "listSchemas", ep.OperationID)
	assert.Equal(t, "GET", ep.Method)
	assert.NotEmpty(t, ep.Path)
}

func TestAPI_Describe_NotFound(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"api", "describe", "nonExistentOperation"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestAPI_ListByTag(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "api", "list", "--tag", "Identity"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var endpoints []gen.APIGenEndpoint
	require.NoError(t, json.Unmarshal([]byte(output), &endpoints))
	assert.NotEmpty(t, endpoints, "should find Identity-tagged endpoints")
	for _, ep := range endpoints {
		found := false
		for _, tag := range ep.Tags {
			if strings.EqualFold(tag, "Identity") {
				found = true
				break
			}
		}
		assert.True(t, found, "endpoint %s should have Identity tag", ep.OperationID)
	}
}

func TestAPI_ListByTag_CaseInsensitive(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "api", "list", "--tag", "identity"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var endpoints []gen.APIGenEndpoint
	require.NoError(t, json.Unmarshal([]byte(output), &endpoints))
	assert.NotEmpty(t, endpoints, "case-insensitive tag filter should match Identity")
}

func TestAPI_Search_NoMatches(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "api", "search", "zzz_nonexistent_xyz_999"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	// Should be valid JSON with null or empty array
	var endpoints []gen.APIGenEndpoint
	err = json.Unmarshal([]byte(output), &endpoints)
	require.NoError(t, err)
	assert.Empty(t, endpoints, "nonsense query should return no matches")
}

func TestAPI_Curl(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "api", "curl", "listSchemas", "--param", "catalog_name=main"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Contains(t, result["curl"], "curl")
	assert.Contains(t, result["curl"], "GET")
}

func TestAPI_Curl_WithTokenAuth(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--output", "json",
		"--token", "my-secret-token",
		"api", "curl", "listSchemas",
		"--param", "catalog_name=main",
	})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Contains(t, result["curl"], "Authorization: Bearer my-secret-token",
		"curl should include Bearer token auth header")
}

func TestAPI_Curl_WithAPIKeyAuth(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--output", "json",
		"--api-key", "my-api-key",
		"api", "curl", "listSchemas",
		"--param", "catalog_name=main",
	})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Contains(t, result["curl"], "X-API-Key: my-api-key",
		"curl should include X-API-Key auth header")
}

func TestAPI_Curl_EmbedsObjectBodyFieldsAsJSON(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--output", "json",
		"--token", "my-secret-token",
		"api", "curl", "createDashboardWidget",
		"--param", "dashboard_id=dash-1",
		"--param", "name=Top Vendors",
		"--param", `layout={"x":0,"y":0,"w":6,"h":4}`,
		"--param", `source={"kind":"sql_query","sql_query":{"sql":"select 1"}}`,
		"--param", `visual_spec={"kind":"chart","chart_type":"bar"}`,
	})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Contains(t, result["curl"], `"layout":{"x":0,"y":0,"w":6,"h":4}`)
	assert.Contains(t, result["curl"], `"source":{"kind":"sql_query"`)
	assert.Contains(t, result["curl"], `"visual_spec":{"kind":"chart","chart_type":"bar"}`)
	assert.NotContains(t, result["curl"], `"layout":"{`)
}

func TestAPI_Curl_ProfileTokenOverriddenByExplicitAPIKey(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	require.NoError(t, SaveUserConfig(&UserConfig{
		CurrentProfile: "default",
		Profiles: map[string]Profile{
			"default": {
				Host:  "http://localhost:8080",
				Token: "stale-profile-token",
			},
		},
	}))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--output", "json",
		"--api-key", "fresh-api-key",
		"api", "curl", "listSchemas",
		"--param", "catalog_name=main",
	})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Contains(t, result["curl"], "X-API-Key: fresh-api-key")
	assert.NotContains(t, result["curl"], "Authorization: Bearer stale-profile-token")
}

func TestAPI_Curl_WithBodyParams(t *testing.T) {
	// createSchema has path param catalog_name and body fields (name, comment, etc.)
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--output", "json",
		"api", "curl", "createSchema",
		"--param", "catalog_name=main",
		"--param", "name=analytics",
		"--param", "comment=test schema",
	})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	curl := result["curl"]
	assert.Contains(t, curl, "POST", "createSchema is a POST endpoint")
	assert.Contains(t, curl, "/main/", "path param should be substituted")
	assert.Contains(t, curl, "Content-Type: application/json", "body should set content type")
	assert.Contains(t, curl, "-d", "should include body data")
	assert.Contains(t, curl, "name", "body should contain name field")
}

func TestAPI_Curl_TypedBodyParams(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--output", "json",
		"api", "curl", "createPipeline",
		"--param", "name=nightly",
		"--param", "concurrency_limit=3",
		"--param", "is_paused=false",
	})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	curl := result["curl"]
	assert.Contains(t, curl, `"concurrency_limit":3`)
	assert.Contains(t, curl, `"is_paused":false`)
	assert.NotContains(t, curl, `"concurrency_limit":"3"`)
	assert.NotContains(t, curl, `"is_paused":"false"`)
}

func TestAPI_Curl_WithQueryParams(t *testing.T) {
	// listQueryHistory has multiple query params
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--output", "json",
		"api", "curl", "listQueryHistory",
		"--param", "principal_name=admin",
		"--param", "status=completed",
	})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var result map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &result))
	curl := result["curl"]
	assert.Contains(t, curl, "principal_name=admin", "curl URL should contain query param")
	assert.Contains(t, curl, "status=completed", "curl URL should contain query param")
	assert.Contains(t, curl, "?", "curl URL should have query string separator")
}

func TestAPI_Curl_NotFound(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"api", "curl", "nonExistentOperation"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestAPI_Describe_TableOutput(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"api", "describe", "createSchema"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	// Verify human-readable output contains key sections
	assert.Contains(t, output, "ENDPOINT:")
	assert.Contains(t, output, "POST")
	assert.Contains(t, output, "createSchema")
	assert.Contains(t, output, "PARAMETERS:")
	assert.Contains(t, output, "catalog_name")
	assert.Contains(t, output, "DESCRIPTION")
	assert.Contains(t, output, "BODY FIELDS:")
	assert.Contains(t, output, "name")
}

func TestAPI_Describe_JSONIncludesDiscoveryMetadata(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "api", "describe", "createSchema"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	assert.Equal(t, "createSchema", payload["operation_id"])
	_, ok := payload["related_docs"]
	assert.True(t, ok)
	_, ok = payload["content_types"]
	assert.True(t, ok)
}

func TestAPI_Spec_EmbeddedYAML(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"api", "spec"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)
	assert.Contains(t, output, "openapi:")
}

func TestAPI_Spec_EmbeddedJSON(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"api", "spec", "--format", "json"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	assert.NotEmpty(t, payload["openapi"])
}

func TestAPI_Spec_Live(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/openapi.json", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"openapi":"3.1.0","info":{"title":"QuackStack","version":"test"}}`))
	}))
	defer srv.Close()

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--host", srv.URL, "api", "spec", "--source", "live"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)
	assert.Contains(t, output, "openapi: 3.1.0")
}
