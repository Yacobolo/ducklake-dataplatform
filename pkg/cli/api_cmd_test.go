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

	var endpoints []gen.ReferenceOperation
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

	var endpoints []gen.ReferenceOperation
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

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	assert.Equal(t, "listSchemas", payload["operation_id"])
	assert.Equal(t, "GET", payload["method"])
	assert.NotEmpty(t, payload["path"])
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

	var endpoints []gen.ReferenceOperation
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

	var endpoints []gen.ReferenceOperation
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
	var endpoints []gen.ReferenceOperation
	err = json.Unmarshal([]byte(output), &endpoints)
	require.NoError(t, err)
	assert.Empty(t, endpoints, "nonsense query should return no matches")
}

func TestAPI_Help_DoesNotExposeCurl(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"api", "--help"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	assert.NotContains(t, output, "\n  curl")
	assert.Contains(t, output, "\n  get")
	assert.Contains(t, output, "\n  post")
}

func TestAPI_RawMethod_NotFound(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"api", "post"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "accepts 1 arg(s)")
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
