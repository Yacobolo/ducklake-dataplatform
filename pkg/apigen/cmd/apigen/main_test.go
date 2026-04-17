package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateArtifacts(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	irPath := filepath.Join(dir, "ir.json")

	require.NoError(t, os.WriteFile(irPath, []byte(`{
  "schema_version": "v1",
  "api": {"base_path": "/v1"},
  "info": {"title": "Duck", "version": "0.1.0", "description": "test"},
  "servers": [{"url": "https://localhost:8080", "description": "local"}],
  "schemas": {
    "HealthResponse": {
      "type": "object",
      "properties": {
        "status": {"description": "Health state", "schema": {"type": "string"}}
      },
      "required": ["status"]
    }
  },
  "endpoints": [
    {
      "method": "get",
      "path": "/healthz",
      "operation_id": "getHealth",
      "summary": "Health check",
      "tags": ["system"],
      "responses": [{"status_code": 200, "description": "ok", "schema": {"ref": "HealthResponse"}}]
    }
  ]
}`), 0o644))

	doc, err := loadDocument(irPath)
	require.NoError(t, err)

	openapiPath := filepath.Join(dir, "openapi.yaml")
	serverPath := filepath.Join(dir, "server.apigen.gen.go")
	requestModelsPath := filepath.Join(dir, "request_models.gen.go")
	cliPath := filepath.Join(dir, "cli.gen.go")
	canonicalOpenAPIPath := filepath.Join(dir, "canonical-openapi.yaml")
	require.NoError(t, os.WriteFile(canonicalOpenAPIPath, []byte("openapi: 3.0.0\ninfo:\n  title: Duck\n  version: 0.1.0\npaths: {}\n"), 0o644))

	require.NoError(t, generateOpenAPI(doc, openapiPath))
	require.NoError(t, generateServer(doc, serverPath, "api", requestModelsPath, "api", "", "api", canonicalOpenAPIPath))
	require.NoError(t, generateCLI(doc, cliPath, "gen"))

	_, err = os.Stat(openapiPath)
	require.NoError(t, err)
	_, err = os.Stat(serverPath)
	require.NoError(t, err)
	_, err = os.Stat(requestModelsPath)
	require.NoError(t, err)
	_, err = os.Stat(cliPath)
	require.NoError(t, err)
}

func TestResolveCommandConfig_ManifestTarget(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "apigen.targets.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`targets:
  - name: v1
    cue_dir: api/v1/cue
    ir_out: internal/api/gen/json-ir.json
    openapi_out: internal/api/gen/openapi.yaml
    server_out: internal/api/server.apigen.gen.go
    server_package: api
    request_models_out: internal/api/gen_request_models.gen.go
    request_models_package: api
    compat_types_out: internal/api/types.gen.go
    compat_types_package: api
    cli_out: pkg/cli/gen/apigen_registry.gen.go
    cli_package: gen
    generate_cli: true
`), 0o644))

	config, err := resolveCommandConfig("all", manifestPath, "v1", commandConfig{})
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "api", "v1", "cue"), config.CueDir)
	require.Equal(t, filepath.Join(dir, "internal", "api", "gen", "json-ir.json"), config.IRPath)
	require.Equal(t, filepath.Join(dir, "internal", "api", "gen", "openapi.yaml"), config.CanonicalOpenAPIPath)
	require.Equal(t, filepath.Join(dir, "pkg", "cli", "gen", "apigen_registry.gen.go"), config.CLIOut)
	require.True(t, config.GenerateCLI)
}

func TestResolveCommandConfig_ManifestDisablesCLI(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "apigen.targets.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`targets:
  - name: v2
    cue_dir: api/v2/cue
    ir_out: internal/api/v2/gen/json-ir.json
    openapi_out: internal/api/v2/gen/openapi.yaml
    server_out: internal/api/v2/server.apigen.gen.go
    server_package: apiv2
    request_models_out: internal/api/v2/gen_request_models.gen.go
    request_models_package: apiv2
    generate_cli: false
`), 0o644))

	config, err := resolveCommandConfig("all", manifestPath, "v2", commandConfig{})
	require.NoError(t, err)
	require.False(t, config.GenerateCLI)

	_, err = resolveCommandConfig("cli", manifestPath, "v2", commandConfig{})
	require.Error(t, err)
	require.ErrorContains(t, err, "cli_out")
}
