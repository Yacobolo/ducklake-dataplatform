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
func TestApplyCmd_AssetActionsExecute(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	requestCount := 0
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/compute-routing-defaults":
			_, _ = w.Write([]byte(`{"interactive_mode":"LOCAL","scheduled_mode":"LOCAL","notebook_mode":"LOCAL"}`))
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/assets":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"asset_key":"daily_kpi","asset_type":"table"}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":[]}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	configDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(configDir, "cue.mod"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "cue.mod", "module.cue"), []byte(`module: "quackstack.local/test"
language: {
	version: "v0.14.0"
}
`), 0o600))

	assetPath := filepath.Join(configDir, "assets", "daily_kpi.cue")
	require.NoError(t, os.MkdirAll(filepath.Dir(assetPath), 0o755))
	require.NoError(t, os.WriteFile(assetPath, []byte(`package duckconfig

platform: assets: daily_kpi: {
	asset_type: "table"
	owner: "analytics"
}
`), 0o600))

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{
		"--host", srv.URL,
		"apply",
		"--config-dir", configDir,
		"--auto-approve",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)
	assert.Positive(t, requestCount)
}
