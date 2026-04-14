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
		case r.Method == http.MethodGet && r.URL.Path == "/v1/domains":
			_, _ = w.Write([]byte(`{"data":[{"id":"domain-1","name":"revenue"}]}`))
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/domains":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"domain-1","name":"revenue"}`))
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/teams":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"team-1","name":"analytics-engineering"}`))
			return
		case r.Method == http.MethodPost && r.URL.Path == "/v1/data-products":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"product":{"id":"product-1","slug":"daily-kpi-product"}}`))
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

	domainPath := filepath.Join(configDir, "domains", "revenue.cue")
	require.NoError(t, os.MkdirAll(filepath.Dir(domainPath), 0o755))
	require.NoError(t, os.WriteFile(domainPath, []byte(`package duckconfig

platform: domains: revenue: {
	description: "Revenue domain"
}
`), 0o600))

	teamPath := filepath.Join(configDir, "teams", "analytics-engineering.cue")
	require.NoError(t, os.MkdirAll(filepath.Dir(teamPath), 0o755))
	require.NoError(t, os.WriteFile(teamPath, []byte(`package duckconfig

platform: teams: "analytics-engineering": {
	domain_ref: "revenue"
	contact_channel: "#rev-data"
}
`), 0o600))

	productPath := filepath.Join(configDir, "data-products", "daily-kpi-product.cue")
	require.NoError(t, os.MkdirAll(filepath.Dir(productPath), 0o755))
	require.NoError(t, os.WriteFile(productPath, []byte(`package duckconfig

platform: data_products: "daily-kpi-product": {
	name: "Daily KPI Product"
	domain_ref: "revenue"
	owner_team_ref: "analytics-engineering"
	steward_principal: "alice"
	contact_channel: "#rev-data"
	contract: data_grain: "one row per day"
	slo: freshness_slo: "60m"
	outputs: ["daily_kpi"]
}
`), 0o600))

	assetPath := filepath.Join(configDir, "assets", "daily_kpi.cue")
	require.NoError(t, os.MkdirAll(filepath.Dir(assetPath), 0o755))
	require.NoError(t, os.WriteFile(assetPath, []byte(`package duckconfig

platform: assets: daily_kpi: {
	asset_type: "table"
	product_ref: "daily-kpi-product"
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
