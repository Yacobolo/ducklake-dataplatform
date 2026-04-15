package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/declarative"
)

func TestCLI_Summary_WithTarget(t *testing.T) {
	configDir := newDeclarativeTestModule(t)
	writeDeclarativeTestFile(t, filepath.Join(configDir, "catalogs", "main", "catalog.cue"), `
package duckconfig

platform: catalogs: main: {
	metastore_type: "sqlite"
	dsn: "meta.sqlite"
	data_path: "data"
	schemas: analytics: {}
}
`)
	writeDeclarativeTestFile(t, filepath.Join(configDir, "projects", "core", "project.cue"), `
package duckconfig

platform: projects: core: {
	workspace_ref: "personal"
	kind: "shared"
	environments: dev: {
		kind: "development"
		target_catalog: "main"
		target_schema: "analytics"
		variables: {
			owner: "team-${target_name}"
		}
	}
}
`)

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "summary", "--config-dir", configDir, "--target", "dev", "--var", "owner=alice"})
	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	counts, ok := payload["counts"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(1), counts["projects"])
	target, ok := payload["target"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "personal/core/dev", target["target_ref"])
	vars, ok := target["variables"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "alice", vars["owner"])
}

func TestCLI_Schema_JSON(t *testing.T) {
	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--output", "json", "schema"})
	restore := captureStdout(t)
	err := rootCmd.Execute()
	output := restore()
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(output), &payload))
	assert.Equal(t, "cue", payload["format"])
	assert.Equal(t, "duckconfig", payload["package"])
	assert.Contains(t, payload["schema"], "#Platform")
}

func TestCLI_Adopt_CatalogWritesConfig(t *testing.T) {
	configDir := t.TempDir()
	t.Setenv("HOME", t.TempDir())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/catalogs":
			_, _ = w.Write([]byte(`{"data":[{"id":"cat-1","name":"main","metastore_type":"sqlite","dsn":"meta.sqlite","data_path":"data"}]}`))
		case "/v1/catalogs/main/schemas":
			_, _ = w.Write([]byte(`{"data":[{"schema_id":"sch-1","name":"analytics"}]}`))
		default:
			_, _ = w.Write([]byte(`{"data":[]}`))
		}
	}))
	defer srv.Close()

	rootCmd := newRootCmd()
	rootCmd.SetArgs([]string{"--host", srv.URL, "adopt", "catalog", "main", "--config-dir", configDir})
	require.NoError(t, rootCmd.Execute())

	loaded, err := declarative.LoadDirectory(configDir)
	require.NoError(t, err)
	require.Len(t, loaded.Catalogs, 1)
	require.Len(t, loaded.Schemas, 1)
	assert.Equal(t, "main", loaded.Catalogs[0].CatalogName)
	assert.Equal(t, "analytics", loaded.Schemas[0].SchemaName)
}

func newDeclarativeTestModule(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "cue.mod"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cue.mod", "module.cue"), []byte(`module: "quackstack.local/test"
language: {
	version: "v0.14.0"
}
`), 0o600))
	return dir
}

func writeDeclarativeTestFile(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
}
