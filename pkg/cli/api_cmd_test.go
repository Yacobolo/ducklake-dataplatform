package cli

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/pkg/cli/gen"
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

	var endpoints []gen.APIEndpoint
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

	var endpoints []gen.APIEndpoint
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

	var ep gen.APIEndpoint
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
	rootCmd.SetArgs([]string{"--output", "json", "api", "list", "--tag", "Security"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var endpoints []gen.APIEndpoint
	require.NoError(t, json.Unmarshal([]byte(output), &endpoints))
	assert.NotEmpty(t, endpoints, "should find Security-tagged endpoints")
	for _, ep := range endpoints {
		found := false
		for _, tag := range ep.Tags {
			if strings.EqualFold(tag, "Security") {
				found = true
				break
			}
		}
		assert.True(t, found, "endpoint %s should have Security tag", ep.OperationID)
	}
}

func TestAPI_ListByTag_CaseInsensitive(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "api", "list", "--tag", "security"})

	old := captureStdout(t)
	err := rootCmd.Execute()
	output := old()
	require.NoError(t, err)

	var endpoints []gen.APIEndpoint
	require.NoError(t, json.Unmarshal([]byte(output), &endpoints))
	assert.NotEmpty(t, endpoints, "case-insensitive tag filter should match Security")
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
	var endpoints []gen.APIEndpoint
	err = json.Unmarshal([]byte(output), &endpoints)
	require.NoError(t, err)
	assert.Empty(t, endpoints, "nonsense query should return no matches")
}

func TestAPI_Curl(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "api", "curl", "listSchemas", "--param", "catalogName=main"})

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
		"--param", "catalogName=main",
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
		"--param", "catalogName=main",
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

func TestAPI_Curl_WithBodyParams(t *testing.T) {
	// createSchema has path param catalogName and body fields (name, comment, etc.)
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--output", "json",
		"api", "curl", "createSchema",
		"--param", "catalogName=main",
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
	assert.Contains(t, output, "catalogName")
	assert.Contains(t, output, "BODY FIELDS:")
	assert.Contains(t, output, "name")
}
