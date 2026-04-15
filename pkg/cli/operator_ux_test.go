package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCLI_RootHelp_ReflectsLeanSurface(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--help"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	assert.Contains(t, output, "Authentication")
	assert.Contains(t, output, "Platform Lifecycle")
	assert.Contains(t, output, "Server/Admin")
	assert.Contains(t, output, "\n  auth")
	assert.Contains(t, output, "\n  adopt")
	assert.Contains(t, output, "\n  project")
	assert.Contains(t, output, "\n  server")
	assert.NotContains(t, output, "\n  config")
	assert.NotContains(t, output, "\n  declarative")
	assert.NotContains(t, output, "\n  init")
	assert.NotContains(t, output, "\n  bootstrap")
}

func TestAPI_RawGet(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"name":"main"}]}`))
	}))
	defer srv.Close()

	rootCmd := newTestRootCmd(t, srv)
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "api", "get", "/catalogs?max_results=1"})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, http.MethodGet, captured.Method)
	assert.Equal(t, "/v1/catalogs", captured.Path)
	assert.Contains(t, captured.Query, "max_results=1")

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	data, ok := payload["data"].([]any)
	require.True(t, ok)
	require.Len(t, data, 1)
}

func TestAPI_RawPost_WithFileJSON(t *testing.T) {
	rec := &requestRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"nb-1","name":"daily"}`))
	}))
	defer srv.Close()

	dir := t.TempDir()
	t.Setenv("HOME", dir)
	bodyPath := filepath.Join(dir, "body.json")
	require.NoError(t, os.WriteFile(bodyPath, []byte(`{"name":"daily","description":"Daily checks"}`), 0o600))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--host", srv.URL, "--output", "json", "api", "post", "/notebooks", "--json", "@" + bodyPath})

	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	captured := rec.last()
	assert.Equal(t, http.MethodPost, captured.Method)
	assert.Equal(t, "/v1/notebooks", captured.Path)
	assert.JSONEq(t, `{"name":"daily","description":"Daily checks"}`, captured.Body)

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	assert.Equal(t, "nb-1", payload["id"])
}

func TestCompletionLifecycle(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("SHELL", "/bin/bash")

	statusCmd := newRootCmd()
	statusCmd.SetArgs([]string{"--output", "json", "completion", "status", "--shell", "bash"})
	restore := captureStdout(t)
	err := statusCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var statusPayload map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &statusPayload))
	assert.Equal(t, "not installed", statusPayload["status"])

	installCmd := newRootCmd()
	installCmd.SetArgs([]string{"--output", "json", "completion", "install", "--shell", "bash"})
	restore = captureStdout(t)
	err = installCmd.Execute()
	output = restore()
	require.NoError(t, err)

	var installPayload map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &installPayload))
	assert.Equal(t, "installed", installPayload["status"])

	rcPath := filepath.Join(home, ".bashrc")
	content, err := os.ReadFile(rcPath)
	require.NoError(t, err)
	assert.Contains(t, string(content), completionMarkerStart)
	assert.Contains(t, string(content), "quack completion bash")

	uninstallCmd := newRootCmd()
	uninstallCmd.SetArgs([]string{"--output", "json", "completion", "uninstall", "--shell", "bash"})
	restore = captureStdout(t)
	err = uninstallCmd.Execute()
	output = restore()
	require.NoError(t, err)

	var uninstallPayload map[string]string
	require.NoError(t, json.Unmarshal([]byte(output), &uninstallPayload))
	assert.Equal(t, "not installed", uninstallPayload["status"])

	content, err = os.ReadFile(rcPath)
	require.NoError(t, err)
	assert.NotContains(t, string(content), completionMarkerStart)
	assert.False(t, strings.Contains(string(content), "quack completion bash"))
}
