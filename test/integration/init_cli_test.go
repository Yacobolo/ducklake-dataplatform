//go:build integration

package integration

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCLI_InitApplyVerifyDestroy_ManagesShowcaseAssets(t *testing.T) {
	initEnv := requireInitS3Env(t)
	env := setupHTTPServer(t, httpTestOpts{WithDuckLake: true, WithAssets: true, WithStorageCredentials: true})

	args := []string{
		"--host", env.Server.URL,
		"--api-key", env.Keys.Admin,
		"init", "apply",
		"--bucket", initEnv.bucket,
		"--endpoint", initEnv.endpoint,
		"--region", initEnv.region,
		"--key-id", initEnv.keyID,
		"--secret", initEnv.secret,
	}
	output := runCLI(t, args...)
	assert.Contains(t, output, "init apply completed")

	verifyOutput := runCLI(t,
		"--host", env.Server.URL,
		"--api-key", env.Keys.Admin,
		"init", "verify",
		"--bucket", initEnv.bucket,
		"--endpoint", initEnv.endpoint,
		"--region", initEnv.region,
		"--key-id", initEnv.keyID,
		"--secret", initEnv.secret,
	)
	assert.Contains(t, verifyOutput, "all opinionated bootstrap resources are present")

	assetList := mustListAssetKeys(t, env)
	assert.Subset(t, assetList, []string{
		"showcase.rides.raw",
		"showcase.rides.bronze",
		"showcase.rides.silver",
		"showcase.rides.gold",
		"showcase.rides.quality",
		"showcase.rides.sandbox",
	})

	graphResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/showcase.rides.gold/graph", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, graphResp.StatusCode)
	var graph map[string]any
	decodeJSON(t, graphResp, &graph)
	assert.Contains(t, graph["upstream_asset_keys"], "showcase.rides.silver")

	checksResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/showcase.rides.gold/checks", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, checksResp.StatusCode)
	var checks map[string]any
	decodeJSON(t, checksResp, &checks)
	checkRows, ok := checks["data"].([]any)
	require.True(t, ok)
	require.Len(t, checkRows, 1)

	destroyOutput := runCLI(t,
		"--host", env.Server.URL,
		"--api-key", env.Keys.Admin,
		"init", "destroy",
		"--yes",
		"--bucket", initEnv.bucket,
		"--endpoint", initEnv.endpoint,
		"--region", initEnv.region,
		"--key-id", initEnv.keyID,
		"--secret", initEnv.secret,
	)
	assert.Contains(t, destroyOutput, "init destroy completed")

	assetList = mustListAssetKeys(t, env)
	assert.NotContains(t, assetList, "showcase.rides.raw")
	assert.NotContains(t, assetList, "showcase.rides.gold")

	tablesResp := doRequest(t, "GET", env.Server.URL+"/v1/catalogs/lake/schemas/gold/tables", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, tablesResp.StatusCode)
	var tables map[string]any
	decodeJSON(t, tablesResp, &tables)
	tableRows, ok := tables["data"].([]any)
	require.True(t, ok)
	assert.False(t, hasTableName(tableRows, "rides_gold_daily_metrics"))
}

func runCLI(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("go", append([]string{"run", "./cmd/cli"}, args...)...)
	cmd.Dir = projectRoot()
	cmd.Env = append(os.Environ(), "TMPDIR="+filepath.Join(projectRoot(), ".tmp"))
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), stderr.String())
	return stdout.String() + stderr.String()
}

func mustListAssetKeys(t *testing.T, env *httpTestEnv) []string {
	t.Helper()
	resp := doRequest(t, "GET", env.Server.URL+"/v1/assets", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, resp.StatusCode)
	var result map[string]any
	decodeJSON(t, resp, &result)
	rows, ok := result["data"].([]any)
	require.True(t, ok)
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		asset, cast := row.(map[string]any)
		require.True(t, cast)
		key, cast := asset["asset_key"].(string)
		require.True(t, cast)
		keys = append(keys, key)
	}
	return keys
}

func hasTableName(rows []any, name string) bool {
	for _, row := range rows {
		table, ok := row.(map[string]any)
		if !ok {
			continue
		}
		if tableName, ok := table["name"].(string); ok && tableName == name {
			return true
		}
	}
	return false
}

type initS3Env struct {
	bucket   string
	endpoint string
	region   string
	keyID    string
	secret   string
}

func requireInitS3Env(t *testing.T) initS3Env {
	t.Helper()
	env := initS3Env{
		bucket:   os.Getenv("S3_BUCKET"),
		endpoint: os.Getenv("S3_ENDPOINT"),
		region:   os.Getenv("S3_REGION"),
		keyID:    os.Getenv("S3_ACCESS_KEY"),
		secret:   os.Getenv("S3_SECRET_KEY"),
	}
	if env.bucket == "" || env.endpoint == "" || env.region == "" || env.keyID == "" || env.secret == "" {
		t.Skip("skipping init CLI integration test: S3_* env vars are required for external location-backed DuckLake schemas")
	}
	return env
}
