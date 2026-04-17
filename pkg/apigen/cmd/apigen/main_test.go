package main

import (
	"os"
	"path/filepath"
	"strings"
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

func TestMultiTargetManifest_GeneratesVersionedArtifacts(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	writeMinimalContract(t, filepath.Join(root, "api", "v1", "cue"), "/v1", "Widget API", "1.0.0")
	writeMinimalContract(t, filepath.Join(root, "api", "v2", "cue"), "/v2", "Widget API v2", "2.0.0")

	manifestPath := filepath.Join(root, "apigen.targets.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte(`targets:
  - name: v1
    cue_dir: api/v1/cue
    ir_out: internal/api/v1/gen/json-ir.json
    openapi_out: internal/api/v1/gen/openapi.yaml
    server_out: internal/api/v1/server.apigen.gen.go
    server_package: apiv1
    request_models_out: internal/api/v1/gen_request_models.gen.go
    request_models_package: apiv1
    cli_out: pkg/cli/gen/apigen_v1_registry.gen.go
    cli_package: genv1
    generate_cli: true
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

	v1Config, err := resolveCommandConfig("all", manifestPath, "v1", commandConfig{})
	require.NoError(t, err)
	require.NoError(t, compileCUE(v1Config.CueDir, v1Config.IROut, v1Config.OpenAPIOut))

	v1Doc, err := loadDocument(v1Config.IRPath)
	require.NoError(t, err)
	require.Equal(t, "/v1", v1Doc.API.BasePath)
	require.NoError(t, generateServer(v1Doc, v1Config.ServerOut, v1Config.ServerPackage, v1Config.RequestModelsOut, v1Config.RequestModelsPackage, v1Config.CompatTypesOut, v1Config.CompatTypesPackage, v1Config.CanonicalOpenAPIPath))
	require.NoError(t, generateCLI(v1Doc, v1Config.CLIOut, v1Config.CLIPackage))

	v1OpenAPI := mustReadString(t, v1Config.OpenAPIOut)
	require.Contains(t, v1OpenAPI, "/v1/widgets:")
	v1Server := mustReadString(t, v1Config.ServerOut)
	require.Contains(t, v1Server, `Path: "/v1/widgets"`)
	v1CLI := mustReadString(t, v1Config.CLIOut)
	require.Contains(t, v1CLI, `Path: "/v1/widgets"`)

	v2Config, err := resolveCommandConfig("all", manifestPath, "v2", commandConfig{})
	require.NoError(t, err)
	require.False(t, v2Config.GenerateCLI)
	require.NoError(t, compileCUE(v2Config.CueDir, v2Config.IROut, v2Config.OpenAPIOut))

	v2Doc, err := loadDocument(v2Config.IRPath)
	require.NoError(t, err)
	require.Equal(t, "/v2", v2Doc.API.BasePath)
	require.NoError(t, generateServer(v2Doc, v2Config.ServerOut, v2Config.ServerPackage, v2Config.RequestModelsOut, v2Config.RequestModelsPackage, v2Config.CompatTypesOut, v2Config.CompatTypesPackage, v2Config.CanonicalOpenAPIPath))

	v2OpenAPI := mustReadString(t, v2Config.OpenAPIOut)
	require.Contains(t, v2OpenAPI, "/v2/widgets:")
	v2Server := mustReadString(t, v2Config.ServerOut)
	require.Contains(t, v2Server, `Path: "/v2/widgets"`)
	_, err = os.Stat(v2Config.CLIOut)
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func writeMinimalContract(t *testing.T, cueDir string, basePath string, title string, version string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(cueDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(cueDir, "schema.cue"), []byte(`package api

#Source: {
	schema_version: string
	api: {
		base_path: string
	}
	info: {
		title: string
		version: string
	}
	openapi?: _
	schemas: [string]: _
	endpoints: [..._]
}
`), 0o644))

	apiCUE := `package api

schema_version: "v1"

api: {
	base_path: "` + basePath + `"
}

info: {
	title:   "` + title + `"
	version: "` + version + `"
}

openapi: {
	version: "3.0.0"
}

schemas: {
	"Widget": {
		type: "object"
		properties: {
			"id": {
				schema: {
					type: "string"
				}
			}
			"name": {
				schema: {
					type: "string"
				}
			}
		}
		required: ["id", "name"]
	}
}

endpoints: [
	{
		method:       "get"
		path:         "/widgets"
		operation_id: "listWidgets"
		summary:      "List widgets"
		cli: {
			command: ["widgets", "list"]
		}
		responses: [{
			status_code: 200
			description: "ok"
			schema: {
				ref: "Widget"
			}
		}]
	},
]
`
	require.NoError(t, os.WriteFile(filepath.Join(cueDir, "api.cue"), []byte(apiCUE), 0o644))
}

func mustReadString(t *testing.T, path string) string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	return strings.TrimSpace(string(content))
}
