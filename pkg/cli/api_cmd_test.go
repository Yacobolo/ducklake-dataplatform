package cli

import (
	"encoding/json"
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
	assert.Greater(t, len(endpoints), 0, "should find schema-related endpoints")
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
