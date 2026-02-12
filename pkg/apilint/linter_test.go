package apilint

import (
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

// Minimal valid spec helper.
const specHeader = `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
security:
  - BearerAuth: []
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
// OAL001
// ============================================================

func TestOAL001_MissingTags(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      summary: List items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL001")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "listItems")
	assert.Equal(t, SeverityError, vs[0].Severity)
}

func TestOAL001_WithTags(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL001")
	assert.Empty(t, vs)
}

// ============================================================
// OAL002
// ============================================================

func TestOAL002_DuplicateOperationID(t *testing.T) {
	spec := specHeader + `
paths:
  /a:
    get:
      operationId: doThing
      tags: [A]
      summary: Do thing
      responses:
        '200':
          description: ok
  /b:
    get:
      operationId: doThing
      tags: [B]
      summary: Do thing
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL002")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "duplicate")
}

func TestOAL002_MissingOperationID(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL002")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "missing")
}

// ============================================================
// OAL003
// ============================================================

func TestOAL003_DeleteWithBody(t *testing.T) {
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
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Error'
      responses:
        '204':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL003")
	require.Len(t, vs, 1)
	assert.Equal(t, SeverityWarning, vs[0].Severity)
}

func TestOAL003_DeleteWithoutBody(t *testing.T) {
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
      responses:
        '204':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL003")
	assert.Empty(t, vs)
}

// ============================================================
// OAL004
// ============================================================

func TestOAL004_InlineResponseSchema(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
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
	vs := findRule(mustLint(t, spec), "OAL004")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "inline schema")
}

func TestOAL004_RefResponseSchema(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
`
	vs := findRule(mustLint(t, spec), "OAL004")
	assert.Empty(t, vs)
}

// ============================================================
// OAL005
// ============================================================

func TestOAL005_PaginatedMissingParams(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/PaginatedItems'
`
	vs := findRule(mustLint(t, spec), "OAL005")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "MaxResults")
	assert.Contains(t, vs[0].Message, "PageToken")
}

func TestOAL005_PaginatedWithParams(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
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
	vs := findRule(mustLint(t, spec), "OAL005")
	assert.Empty(t, vs)
}

// ============================================================
// OAL006
// ============================================================

func TestOAL006_PostBeforeGet(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
      responses:
        '201':
          description: created
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL006")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "POST")
	assert.Contains(t, vs[0].Message, "before GET")
}

func TestOAL006_GetBeforePost(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
      responses:
        '201':
          description: created
`
	vs := findRule(mustLint(t, spec), "OAL006")
	assert.Empty(t, vs)
}

// ============================================================
// OAL007
// ============================================================

func TestOAL007_PathParamNotCamelCase(t *testing.T) {
	spec := specHeader + `
paths:
  /items/{item_id}:
    parameters:
      - name: item_id
        in: path
        required: true
        schema:
          type: integer
    get:
      operationId: getItem
      tags: [Items]
      summary: Get item
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL007")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "item_id")
}

func TestOAL007_PathParamCamelCase(t *testing.T) {
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
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL007")
	assert.Empty(t, vs)
}

// ============================================================
// OAL008
// ============================================================

func TestOAL008_QueryParamNotSnakeCase(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      parameters:
        - name: pageSize
          in: query
          schema:
            type: integer
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL008")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "pageSize")
}

func TestOAL008_QueryParamSnakeCase(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      parameters:
        - name: page_size
          in: query
          schema:
            type: integer
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL008")
	assert.Empty(t, vs)
}

// ============================================================
// OAL009
// ============================================================

func TestOAL009_Missing401(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL009")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "401")
}

func TestOAL009_With401(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
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
	vs := findRule(mustLint(t, spec), "OAL009")
	assert.Empty(t, vs)
}

func TestOAL009_NoGlobalSecurity(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
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
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL009")
	assert.Empty(t, vs)
}

// ============================================================
// OAL010
// ============================================================

func TestOAL010_PaginatedSchemaShape(t *testing.T) {
	t.Run("missing_data", func(t *testing.T) {
		spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
components:
  schemas:
    PaginatedBad:
      type: object
      properties:
        next_page_token:
          type: string
paths: {}
`
		vs := findRule(mustLint(t, spec), "OAL010")
		require.Len(t, vs, 1)
		assert.Contains(t, vs[0].Message, "missing 'data'")
	})

	t.Run("valid", func(t *testing.T) {
		spec := specHeader + `
paths: {}
`
		vs := findRule(mustLint(t, spec), "OAL010")
		assert.Empty(t, vs)
	})
}

// ============================================================
// OAL011
// ============================================================

func TestOAL011_UnresolvedRef(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/DoesNotExist'
`
	vs := findRule(mustLint(t, spec), "OAL011")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "DoesNotExist")
}

func TestOAL011_ValidRef(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
`
	vs := findRule(mustLint(t, spec), "OAL011")
	assert.Empty(t, vs)
}

// ============================================================
// OAL012
// ============================================================

func TestOAL012_PostReturns200InsteadOf201(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL012")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "200 instead of 201")
}

func TestOAL012_PostReturns201_NoViolation(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
      responses:
        '201':
          description: created
`
	vs := findRule(mustLint(t, spec), "OAL012")
	assert.Empty(t, vs)
}

func TestOAL012_ActionVerbExcluded(t *testing.T) {
	spec := specHeader + `
paths:
  /query:
    post:
      operationId: executeQuery
      tags: [Query]
      summary: Execute query
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL012")
	assert.Empty(t, vs)
}

// ============================================================
// OAL013
// ============================================================

func TestOAL013_MissingSummary(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL013")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "summary")
}

func TestOAL013_WithSummary(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List all items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL013")
	assert.Empty(t, vs)
}

// ============================================================
// OAL014
// ============================================================

func TestOAL014_MutatingMissing403(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
      responses:
        '201':
          description: created
`
	vs := findRule(mustLint(t, spec), "OAL014")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "403")
}

func TestOAL014_MutatingWith403(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    post:
      operationId: createItem
      tags: [Items]
      summary: Create item
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
	vs := findRule(mustLint(t, spec), "OAL014")
	assert.Empty(t, vs)
}

func TestOAL014_GetSkipped(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL014")
	assert.Empty(t, vs)
}

func TestOAL014_SecurityOverrideEmpty(t *testing.T) {
	spec := specHeader + `
paths:
  /health:
    post:
      operationId: checkHealth
      tags: [Health]
      summary: Health check
      security: []
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL014")
	assert.Empty(t, vs)
}

// ============================================================
// OAL015
// ============================================================

func TestOAL015_GetResourceMissing404(t *testing.T) {
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
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL015")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "404")
}

func TestOAL015_GetResourceWith404(t *testing.T) {
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
	vs := findRule(mustLint(t, spec), "OAL015")
	assert.Empty(t, vs)
}

func TestOAL015_CollectionPathSkipped(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL015")
	assert.Empty(t, vs)
}

// ============================================================
// OAL016
// ============================================================

func TestOAL016_CamelCaseProperty(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
components:
  schemas:
    BadSchema:
      type: object
      properties:
        firstName:
          type: string
paths: {}
`
	vs := findRule(mustLint(t, spec), "OAL016")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "firstName")
}

func TestOAL016_SnakeCaseProperty(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
components:
  schemas:
    GoodSchema:
      type: object
      properties:
        first_name:
          type: string
paths: {}
`
	vs := findRule(mustLint(t, spec), "OAL016")
	assert.Empty(t, vs)
}

// ============================================================
// OAL017
// ============================================================

func TestOAL017_CreateRequestNoRequired(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
components:
  schemas:
    CreateItemRequest:
      type: object
      properties:
        name:
          type: string
paths: {}
`
	vs := findRule(mustLint(t, spec), "OAL017")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "CreateItemRequest")
}

func TestOAL017_CreateRequestWithRequired(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
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
	vs := findRule(mustLint(t, spec), "OAL017")
	assert.Empty(t, vs)
}

func TestOAL017_UpdateRequestSkipped(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
components:
  schemas:
    UpdateItemRequest:
      type: object
      properties:
        name:
          type: string
paths: {}
`
	vs := findRule(mustLint(t, spec), "OAL017")
	assert.Empty(t, vs)
}

// ============================================================
// OAL018
// ============================================================

func TestOAL018_UnusedSchema(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
components:
  schemas:
    Orphan:
      type: object
      properties:
        name:
          type: string
    Used:
      type: object
      properties:
        name:
          type: string
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Used'
`
	vs := findRule(mustLint(t, spec), "OAL018")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "Orphan")
}

func TestOAL018_AllUsed(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
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
        '400':
          description: bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
`
	vs := findRule(mustLint(t, spec), "OAL018")
	assert.Empty(t, vs)
}

// ============================================================
// OAL019
// ============================================================

func TestOAL019_NonCamelCaseOpId(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: List_Items
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL019")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "List_Items")
}

func TestOAL019_CamelCaseOpId(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL019")
	assert.Empty(t, vs)
}

// ============================================================
// OAL020
// ============================================================

func TestOAL020_SnakeCasePath(t *testing.T) {
	spec := specHeader + `
paths:
  /my_items:
    get:
      operationId: listMyItems
      tags: [Items]
      summary: List my items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL020")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "my_items")
}

func TestOAL020_KebabCasePath(t *testing.T) {
	spec := specHeader + `
paths:
  /my-items:
    get:
      operationId: listMyItems
      tags: [Items]
      summary: List my items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL020")
	assert.Empty(t, vs)
}

func TestOAL020_ParamSegmentSkipped(t *testing.T) {
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
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL020")
	assert.Empty(t, vs)
}

// ============================================================
// OAL021
// ============================================================

func TestOAL021_NonErrorRef(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
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
	vs := findRule(mustLint(t, spec), "OAL021")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "400")
}

func TestOAL021_ErrorRef(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
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
	vs := findRule(mustLint(t, spec), "OAL021")
	assert.Empty(t, vs)
}

func TestOAL021_NoContent(t *testing.T) {
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
      responses:
        '204':
          description: no content
        '404':
          description: not found
`
	vs := findRule(mustLint(t, spec), "OAL021")
	assert.Empty(t, vs)
}

// ============================================================
// OAL022
// ============================================================

func TestOAL022_SingleValueEnum(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
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
	vs := findRule(mustLint(t, spec), "OAL022")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "1 value")
}

func TestOAL022_MultiValueEnum(t *testing.T) {
	spec := `openapi: "3.0.3"
info:
  title: Test
  version: "1.0"
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
	vs := findRule(mustLint(t, spec), "OAL022")
	assert.Empty(t, vs)
}

// ============================================================
// OAL023
// ============================================================

func TestOAL023_MissingDescription(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL023")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "description")
}

func TestOAL023_WithDescription(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      description: Returns a paginated list of items.
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL023")
	assert.Empty(t, vs)
}

// ============================================================
// OAL024
// ============================================================

func TestOAL024_DeleteMissing204(t *testing.T) {
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
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL024")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "204")
}

func TestOAL024_DeleteWith204(t *testing.T) {
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
      responses:
        '204':
          description: no content
`
	vs := findRule(mustLint(t, spec), "OAL024")
	assert.Empty(t, vs)
}

// ============================================================
// OAL025
// ============================================================

func TestOAL025_PaginationParamsNoPaginatedResponse(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
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
	vs := findRule(mustLint(t, spec), "OAL025")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "pagination params")
}

func TestOAL025_PaginationParamsWithPaginatedResponse(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
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
	vs := findRule(mustLint(t, spec), "OAL025")
	assert.Empty(t, vs)
}

// ============================================================
// Engine tests: Config, Suppression, Registry
// ============================================================

func TestConfig_SeverityOverride(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
      summary: List items
      responses:
        '200':
          description: ok
`
	// OAL009 normally fires as warning. Override to error.
	cfg := &Config{Rules: map[string]string{"OAL009": "error"}}
	vs := findRule(mustLintWithConfig(t, spec, cfg), "OAL009")
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
      responses:
        '200':
          description: ok
`
	// Turn off OAL009.
	cfg := &Config{Rules: map[string]string{"OAL009": "off"}}
	vs := findRule(mustLintWithConfig(t, spec, cfg), "OAL009")
	assert.Empty(t, vs)
}

func TestRegistry_AllRulesRegistered(t *testing.T) {
	rules := RegisteredRules()
	assert.Len(t, rules, 25, "expected 25 registered rules")

	// Verify IDs are unique and sequential.
	ids := map[string]bool{}
	for _, r := range rules {
		assert.False(t, ids[r.ID()], "duplicate rule ID: %s", r.ID())
		ids[r.ID()] = true
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
		RuleID:   "OAL001",
		Severity: SeverityError,
		Message:  "test message",
	}
	assert.Equal(t, "openapi.yaml:42: OAL001 error: test message", v.String())
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
		cmd := exec.Command("npx", "--yes", "@redocly/cli", "bundle", sourcePath, "-o", bundledPath)
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
