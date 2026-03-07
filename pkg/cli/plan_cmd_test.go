package cli

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanCmd_InvalidOutputFormat(t *testing.T) {
	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--host", "http://127.0.0.1:1",
		"plan",
		"--config-dir", t.TempDir(),
		"--output", "yaml",
	})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported output format \"yaml\": use 'text' or 'json'")
}
func TestApplyCmd_AssetActionsFailPreflightBeforeExecution(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	requestCount := 0
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"write endpoints should not be called"}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":[]}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	configDir := t.TempDir()
	assetPath := filepath.Join(configDir, "assets", "daily_kpi.yaml")
	require.NoError(t, os.MkdirAll(filepath.Dir(assetPath), 0o755))
	require.NoError(t, os.WriteFile(assetPath, []byte(`apiVersion: duck/v1
kind: Asset
metadata:
  name: daily_kpi
spec:
  asset_type: table
`), 0o600))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"apply",
		"--config-dir", configDir,
		"--auto-approve",
	})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "preflight capability validation")
	assert.Contains(t, err.Error(), "asset actions are not supported")
	assert.Positive(t, requestCount)
}
