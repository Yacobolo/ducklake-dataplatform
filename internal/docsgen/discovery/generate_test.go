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
	require.NoError(t, os.MkdirAll(filepath.Join(docsDir, "how-to"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(docsDir, "reference", "generated"), 0o750))

	docBody := `---
title: Access the Platform
description: Use tokens or API keys.
doc_kind: task
audiences: [ai-agents, admins]
product_areas: [auth, identity]
surfaces: [api, cli]
tasks: [authenticate, issue api keys]
prerequisites: [deployment URL]
permissions: [approved auth path]
cli_commands: [auth login]
command_groups: [auth]
operation_ids: [login]
api_tags: [Auth]
declarative_kinds: [api-key-list]
related_docs: [reference/index]
keywords: [auth, login]
last_verified: 2026-03-12
source_of_truth: [docs, api/gen/openapi.yaml]
---

# Access the Platform

Use Duck securely.

## Workflow

1. Authenticate.
` + "```bash\n" + `duck auth login
` + "```" + `
`
	require.NoError(t, os.WriteFile(filepath.Join(docsDir, "how-to", "authentication.md"), []byte(docBody), 0o600))
	referenceIndex := `---
title: Reference
description: Reference landing.
doc_kind: reference
audiences: [ai-agents]
product_areas: [reference]
surfaces: [docs]
tasks: [route to reference]
prerequisites: [documentation access]
permissions: [documentation access]
cli_commands: [docs list]
command_groups: [docs]
operation_ids: []
api_tags: []
declarative_kinds: ["*"]
related_docs: [how-to/authentication]
keywords: [reference]
last_verified: 2026-03-12
source_of_truth: [docs]
---

# Reference
`
	require.NoError(t, os.MkdirAll(filepath.Join(docsDir, "reference"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(docsDir, "reference", "index.md"), []byte(referenceIndex), 0o600))
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

	declIndexPath := filepath.Join(root, "declarative-index.json")
	declIndex := `{"files":{"kinds/api-key-list.schema.json":"sha"}}`
	require.NoError(t, os.WriteFile(declIndexPath, []byte(declIndex), 0o600))

	outPath := filepath.Join(root, "discovery_index.gen.go")
	require.NoError(t, Generate(docsDir, specPath, declIndexPath, outPath))

	output, err := os.ReadFile(outPath)
	require.NoError(t, err)
	assert.Contains(t, string(output), `ID: "how-to/authentication"`)
	assert.Contains(t, string(output), `OperationID: "login"`)
	assert.Contains(t, string(output), `TargetID: "auth login"`)
	assert.NotContains(t, string(output), `ignored.md`)

	llmsIndex, err := os.ReadFile(filepath.Join(docsDir, "public", "llms.txt"))
	require.NoError(t, err)
	assert.Contains(t, string(llmsIndex), "/agent-index.json")

	agentIndex, err := os.ReadFile(filepath.Join(docsDir, "public", "agent-index.json"))
	require.NoError(t, err)
	assert.Contains(t, string(agentIndex), `"missing_api_tags": []`)
}
