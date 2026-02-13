package apilint

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTempSpec(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "openapi.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func mustLint(t *testing.T, content string) []Violation {
	t.Helper()
	path := writeTempSpec(t, content)
	l, err := New(path)
	require.NoError(t, err)
	return l.Run()
}

func mustLintWithConfig(t *testing.T, content string, cfg *Config) []Violation {
	t.Helper()
	path := writeTempSpec(t, content)
	l, err := New(path)
	require.NoError(t, err)
	return l.RunWithConfig(cfg)
}

func findRule(vs []Violation, ruleID string) []Violation {
	var out []Violation
	for _, v := range vs {
		if v.RuleID == ruleID {
			out = append(out, v)
		}
	}
	return out
}

// Minimal valid spec helper — includes all fields needed for vacuum to parse.
const specHeader = `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
security:
  - BearerAuth: []
servers:
  - url: https://api.example.com
components:
  securitySchemes:
    BearerAuth:
      type: http
      scheme: bearer
  parameters:
    MaxResults:
      name: max_results
      in: query
      schema:
        type: integer
    PageToken:
      name: page_token
      in: query
      schema:
        type: string
  schemas:
    Error:
      type: object
      properties:
        message:
          type: string
    PaginatedItems:
      type: object
      properties:
        data:
          type: array
          items:
            $ref: '#/components/schemas/Error'
        next_page_token:
          type: string
`

// ============================================================
// Custom rule: check-schema-ref (OAL004)
// ============================================================

func TestCheckSchemaRef_InlineResponse(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                type: object
                properties:
                  name:
                    type: string
`
	vs := findRule(mustLint(t, spec), "check-schema-ref")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "inline schema")
}

func TestCheckSchemaRef_RefResponse(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
`
	vs := findRule(mustLint(t, spec), "check-schema-ref")
	assert.Empty(t, vs)
}

// ============================================================
// Custom rule: check-pagination-params (OAL005)
// ============================================================

func TestCheckPaginationParams_Missing(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PaginatedItems'
`
	vs := findRule(mustLint(t, spec), "check-pagination-params")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "MaxResults")
	assert.Contains(t, vs[0].Message, "PageToken")
}

func TestCheckPaginationParams_Present(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      parameters:
        - $ref: '#/components/parameters/MaxResults'
        - $ref: '#/components/parameters/PageToken'
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PaginatedItems'
`
	vs := findRule(mustLint(t, spec), "check-pagination-params")
	assert.Empty(t, vs)
}

// ============================================================
// Custom rule: check-collection-ordering (OAL006)
// ============================================================

func TestCheckCollectionOrdering_PostBeforeGet(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
      description: Create a new item in the system.
      responses:
        '201':
          description: created
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "check-collection-ordering")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "POST")
	assert.Contains(t, vs[0].Message, "before GET")
}

func TestCheckCollectionOrdering_GetBeforePost(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
      description: Create a new item in the system.
      responses:
        '201':
          description: created
`
	vs := findRule(mustLint(t, spec), "check-collection-ordering")
	assert.Empty(t, vs)
}

// ============================================================
// Custom rule: check-secured-endpoint-401 (OAL009)
// ============================================================

func TestCheckSecuredEndpoint401_Missing(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "check-secured-endpoint-401")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "401")
}

func TestCheckSecuredEndpoint401_Present(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
        '401':
          description: unauthorized
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
`
	vs := findRule(mustLint(t, spec), "check-secured-endpoint-401")
	assert.Empty(t, vs)
}

func TestCheckSecuredEndpoint401_NoGlobalSecurity(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
servers:
  - url: https://api.example.com
components:
  schemas:
    Error:
      type: object
      properties:
        message:
          type: string
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "check-secured-endpoint-401")
	assert.Empty(t, vs)
}

// ============================================================
// Custom rule: check-paginated-schema (OAL010)
// ============================================================

func TestCheckPaginatedSchema(t *testing.T) {
	t.Run("missing_data", func(t *testing.T) {
		spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
servers:
  - url: https://api.example.com
components:
  schemas:
    PaginatedBad:
      type: object
      properties:
        next_page_token:
          type: string
paths: {}
`
		vs := findRule(mustLint(t, spec), "check-paginated-schema")
		require.Len(t, vs, 1)
		assert.Contains(t, vs[0].Message, "missing 'data'")
	})

	t.Run("valid", func(t *testing.T) {
		spec := specHeader + `
paths: {}
`
		vs := findRule(mustLint(t, spec), "check-paginated-schema")
		assert.Empty(t, vs)
	})
}

// ============================================================
// Custom rule: check-post-create-status (OAL012)
// ============================================================

func TestCheckPostCreateStatus_Returns200(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
      description: Create a new item in the system.
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "check-post-create-status")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "200 instead of 201")
}

func TestCheckPostCreateStatus_Returns201(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
      description: Create a new item in the system.
      responses:
        '201':
          description: created
`
	vs := findRule(mustLint(t, spec), "check-post-create-status")
	assert.Empty(t, vs)
}

func TestCheckPostCreateStatus_ActionVerbExcluded(t *testing.T) {
	spec := specHeader + `
paths:
  /query:
    post:
      operationId: executeQuery
      tags: [Query]
      summary: Execute query
      description: Execute a SQL query against the data platform.
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "check-post-create-status")
	assert.Empty(t, vs)
}

// ============================================================
// Custom rule: check-mutating-ops-403 (OAL014)
// ============================================================

func TestCheckMutatingOps403_Missing(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
      description: Create a new item in the system.
      responses:
        '201':
          description: created
`
	vs := findRule(mustLint(t, spec), "check-mutating-ops-403")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "403")
}

func TestCheckMutatingOps403_Present(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
      description: Create a new item in the system.
      responses:
        '201':
          description: created
        '403':
          description: forbidden
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
`
	vs := findRule(mustLint(t, spec), "check-mutating-ops-403")
	assert.Empty(t, vs)
}

func TestCheckMutatingOps403_GetSkipped(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "check-mutating-ops-403")
	assert.Empty(t, vs)
}

func TestCheckMutatingOps403_SecurityOverrideEmpty(t *testing.T) {
	spec := specHeader + `
paths:
  /health:
    post:
      operationId: checkHealth
      tags: [Health]
      summary: Health check
      description: Check if the service is healthy.
      security: []
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "check-mutating-ops-403")
	assert.Empty(t, vs)
}

// ============================================================
// Custom rule: check-get-resource-404 (OAL015)
// ============================================================

func TestCheckGetResource404_Missing(t *testing.T) {
	spec := specHeader + `
paths:
  /items/{itemId}:
    parameters:
      - name: itemId
        in: path
        required: true
        schema:
          type: integer
    get:
      operationId: getItem
      tags: [Items]
      summary: Get item
      description: Get a single item by its identifier.
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "check-get-resource-404")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "404")
}

func TestCheckGetResource404_Present(t *testing.T) {
	spec := specHeader + `
paths:
  /items/{itemId}:
    parameters:
      - name: itemId
        in: path
        required: true
        schema:
          type: integer
    get:
      operationId: getItem
      tags: [Items]
      summary: Get item
      description: Get a single item by its identifier.
      responses:
        '200':
          description: ok
        '404':
          description: not found
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
`
	vs := findRule(mustLint(t, spec), "check-get-resource-404")
	assert.Empty(t, vs)
}

func TestCheckGetResource404_CollectionPathSkipped(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "check-get-resource-404")
	assert.Empty(t, vs)
}

// ============================================================
// Custom rule: check-create-request-required (OAL017)
// ============================================================

func TestCheckCreateRequestRequired_NoRequired(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
servers:
  - url: https://api.example.com
components:
  schemas:
    CreateItemRequest:
      type: object
      properties:
        name:
          type: string
paths: {}
`
	vs := findRule(mustLint(t, spec), "check-create-request-required")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "CreateItemRequest")
}

func TestCheckCreateRequestRequired_WithRequired(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
servers:
  - url: https://api.example.com
components:
  schemas:
    CreateItemRequest:
      type: object
      required: [name]
      properties:
        name:
          type: string
paths: {}
`
	vs := findRule(mustLint(t, spec), "check-create-request-required")
	assert.Empty(t, vs)
}

func TestCheckCreateRequestRequired_UpdateSkipped(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
servers:
  - url: https://api.example.com
components:
  schemas:
    UpdateItemRequest:
      type: object
      properties:
        name:
          type: string
paths: {}
`
	vs := findRule(mustLint(t, spec), "check-create-request-required")
	assert.Empty(t, vs)
}

// ============================================================
// Custom rule: check-error-schema-ref (OAL021)
// ============================================================

func TestCheckErrorSchemaRef_NonErrorRef(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PaginatedItems'
        '400':
          description: bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PaginatedItems'
`
	vs := findRule(mustLint(t, spec), "check-error-schema-ref")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "400")
}

func TestCheckErrorSchemaRef_ErrorRef(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
        '400':
          description: bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
`
	vs := findRule(mustLint(t, spec), "check-error-schema-ref")
	assert.Empty(t, vs)
}

func TestCheckErrorSchemaRef_NoContent(t *testing.T) {
	spec := specHeader + `
paths:
  /items/{itemId}:
    parameters:
      - name: itemId
        in: path
        required: true
        schema:
          type: integer
    delete:
      operationId: deleteItem
      tags: [Items]
      summary: Delete item
      description: Delete an item by its identifier.
      responses:
        '204':
          description: no content
        '404':
          description: not found
`
	vs := findRule(mustLint(t, spec), "check-error-schema-ref")
	assert.Empty(t, vs)
}

// ============================================================
// Custom rule: check-enum-min-values (OAL022)
// ============================================================

func TestCheckEnumMinValues_SingleValue(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
servers:
  - url: https://api.example.com
components:
  schemas:
    StorageType:
      type: object
      properties:
        storage_type:
          type: string
          enum: [s3]
paths: {}
`
	vs := findRule(mustLint(t, spec), "check-enum-min-values")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "1 value")
}

func TestCheckEnumMinValues_MultiValue(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
servers:
  - url: https://api.example.com
components:
  schemas:
    StorageType:
      type: object
      properties:
        storage_type:
          type: string
          enum: [s3, gcs]
paths: {}
`
	vs := findRule(mustLint(t, spec), "check-enum-min-values")
	assert.Empty(t, vs)
}

// ============================================================
// Custom rule: check-delete-returns-204 (OAL024)
// ============================================================

func TestCheckDeleteReturns204_Missing(t *testing.T) {
	spec := specHeader + `
paths:
  /items/{itemId}:
    parameters:
      - name: itemId
        in: path
        required: true
        schema:
          type: integer
    delete:
      operationId: deleteItem
      tags: [Items]
      summary: Delete item
      description: Delete an item by its identifier.
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "check-delete-returns-204")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "204")
}

func TestCheckDeleteReturns204_Present(t *testing.T) {
	spec := specHeader + `
paths:
  /items/{itemId}:
    parameters:
      - name: itemId
        in: path
        required: true
        schema:
          type: integer
    delete:
      operationId: deleteItem
      tags: [Items]
      summary: Delete item
      description: Delete an item by its identifier.
      responses:
        '204':
          description: no content
`
	vs := findRule(mustLint(t, spec), "check-delete-returns-204")
	assert.Empty(t, vs)
}

// ============================================================
// Custom rule: check-pagination-schema-match (OAL025)
// ============================================================

func TestCheckPaginationSchemaMatch_NoPaginated(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      parameters:
        - $ref: '#/components/parameters/MaxResults'
        - $ref: '#/components/parameters/PageToken'
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
`
	vs := findRule(mustLint(t, spec), "check-pagination-schema-match")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "pagination params")
}

func TestCheckPaginationSchemaMatch_WithPaginated(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      parameters:
        - $ref: '#/components/parameters/MaxResults'
        - $ref: '#/components/parameters/PageToken'
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PaginatedItems'
`
	vs := findRule(mustLint(t, spec), "check-pagination-schema-match")
	assert.Empty(t, vs)
}

// ============================================================
// Engine tests: Config, utility functions
// ============================================================

func TestConfig_SeverityOverride(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
`
	// check-secured-endpoint-401 normally fires as warning. Override to error.
	cfg := &Config{Rules: map[string]string{"check-secured-endpoint-401": "error"}}
	vs := findRule(mustLintWithConfig(t, spec, cfg), "check-secured-endpoint-401")
	require.Len(t, vs, 1)
	assert.Equal(t, SeverityError, vs[0].Severity)
}

func TestConfig_RuleOff(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: List all items in the system.
      responses:
        '200':
          description: ok
`
	// Turn off check-secured-endpoint-401.
	cfg := &Config{Rules: map[string]string{"check-secured-endpoint-401": "off"}}
	vs := findRule(mustLintWithConfig(t, spec, cfg), "check-secured-endpoint-401")
	assert.Empty(t, vs)
}

func TestRegisteredRules_NotEmpty(t *testing.T) {
	rules := RegisteredRules()
	assert.Greater(t, len(rules), 10, "expected at least 10 registered rules")

	// Verify IDs are unique.
	ids := map[string]bool{}
	for _, r := range rules {
		assert.False(t, ids[r.ID], "duplicate rule ID: %s", r.ID)
		ids[r.ID] = true
	}
}

// ============================================================
// Utility function tests
// ============================================================

func TestFilter_BySeverity(t *testing.T) {
	vs := []Violation{
		{Severity: SeverityError, RuleID: "E1"},
		{Severity: SeverityWarning, RuleID: "W1"},
		{Severity: SeverityInfo, RuleID: "I1"},
	}

	t.Run("error_only", func(t *testing.T) {
		filtered := Filter(vs, SeverityError)
		require.Len(t, filtered, 1)
		assert.Equal(t, "E1", filtered[0].RuleID)
	})
	t.Run("warning_and_above", func(t *testing.T) {
		filtered := Filter(vs, SeverityWarning)
		require.Len(t, filtered, 2)
	})
	t.Run("all", func(t *testing.T) {
		filtered := Filter(vs, SeverityInfo)
		require.Len(t, filtered, 3)
	})
}

func TestHasErrors(t *testing.T) {
	t.Run("with_errors", func(t *testing.T) {
		assert.True(t, HasErrors([]Violation{{Severity: SeverityError}}))
	})
	t.Run("only_warnings", func(t *testing.T) {
		assert.False(t, HasErrors([]Violation{{Severity: SeverityWarning}}))
	})
	t.Run("empty", func(t *testing.T) {
		assert.False(t, HasErrors(nil))
	})
}

func TestViolation_String(t *testing.T) {
	v := Violation{
		File:     "openapi.yaml",
		Line:     42,
		RuleID:   "check-schema-ref",
		Severity: SeverityWarning,
		Message:  "test message",
	}
	assert.Equal(t, "openapi.yaml:42: check-schema-ref warning: test message", v.String())
}

func TestLintActualSpec(t *testing.T) {
	// Lint the bundled project spec and ensure 0 error-level violations.
	// The spec is split into multiple files and must be bundled first via
	// "npx @redocly/cli bundle" (run by "task bundle-api").
	bundledPath := "../../internal/api/openapi.bundled.yaml"
	sourcePath := "../../internal/api/openapi.yaml"

	// If the bundled file doesn't exist, try to generate it.
	if _, err := os.Stat(bundledPath); os.IsNotExist(err) {
		if _, err := os.Stat(sourcePath); os.IsNotExist(err) {
			t.Skip("openapi.yaml not found at expected path")
		}
		cmd := exec.CommandContext(context.Background(), "npx", "--yes", "@redocly/cli", "bundle", sourcePath, "-o", bundledPath)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Skipf("failed to bundle spec (install @redocly/cli): %s: %v", out, err)
		}
	}

	l, err := New(bundledPath)
	require.NoError(t, err)

	vs := l.Run()
	errors := Filter(vs, SeverityError)
	for _, v := range errors {
		t.Errorf("%s", v)
	}
	assert.Empty(t, errors, "expected 0 error-level violations in openapi.bundled.yaml")
}
