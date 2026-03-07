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
	typesPath := filepath.Join(dir, "types.gen.go")
	legacyServerPath := filepath.Join(dir, "server.gen.go")
	require.NoError(t, os.WriteFile(typesPath, []byte("package api\n\ntype HealthResponse struct{}\n"), 0o644))
	require.NoError(t, os.WriteFile(legacyServerPath, []byte("package api\n\ntype GetHealth200JSONResponse HealthResponse\n"), 0o644))

	require.NoError(t, generateOpenAPI(doc, openapiPath))
	require.NoError(t, generateServer(doc, serverPath, requestModelsPath))
	require.NoError(t, generateCLI(doc, cliPath))

	_, err = os.Stat(openapiPath)
	require.NoError(t, err)
	_, err = os.Stat(serverPath)
	require.NoError(t, err)
	_, err = os.Stat(requestModelsPath)
	require.NoError(t, err)
	_, err = os.Stat(cliPath)
	require.NoError(t, err)
}
