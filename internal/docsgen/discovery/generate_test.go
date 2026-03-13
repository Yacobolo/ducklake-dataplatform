package discovery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerate_EmitsDocAndOperationMetadata(t *testing.T) {
	root := t.TempDir()
	docsDir := filepath.Join(root, "docs")
	require.NoError(t, os.MkdirAll(filepath.Join(docsDir, "govern"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(docsDir, "reference", "generated"), 0o750))

	docBody := `---
title: Access the Platform
description: Use tokens or API keys.
keywords: [auth, login]
operation_ids: [login]
---

# Access the Platform

Use Duck securely.

## Workflow

1. Authenticate.
` + "```bash\n" + `duck auth login
` + "```" + `
`
	require.NoError(t, os.WriteFile(filepath.Join(docsDir, "govern", "authentication-and-identities.md"), []byte(docBody), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(docsDir, "reference", "generated", "ignored.md"), []byte("# Ignore me"), 0o600))

	specPath := filepath.Join(root, "openapi.yaml")
	spec := `openapi: 3.1.0
info:
  title: Duck
  version: test
paths:
  /auth/login:
    post:
      operationId: login
      summary: Login
      tags: [Auth]
      x-cli-command: auth login
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [token]
              properties:
                token:
                  type: string
      responses:
        "200":
          description: ok
`
	require.NoError(t, os.WriteFile(specPath, []byte(spec), 0o600))

	outPath := filepath.Join(root, "discovery_index.gen.go")
	require.NoError(t, Generate(docsDir, specPath, outPath))

	output, err := os.ReadFile(outPath)
	require.NoError(t, err)
	assert.Contains(t, string(output), `ID: "govern/authentication-and-identities"`)
	assert.Contains(t, string(output), `OperationID: "login"`)
	assert.Contains(t, string(output), `TargetID: "auth login"`)
	assert.NotContains(t, string(output), `ignored.md`)
}
