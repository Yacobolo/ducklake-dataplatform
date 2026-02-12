package apilint

import (
	"os"
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

func TestOAL001_MissingTags(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
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
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL001")
	assert.Empty(t, vs)
}

func TestOAL002_DuplicateOperationID(t *testing.T) {
	spec := specHeader + `
paths:
  /a:
    get:
      operationId: doThing
      tags: [A]
      responses:
        '200':
          description: ok
  /b:
    get:
      operationId: doThing
      tags: [B]
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
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL002")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "missing")
}

func TestOAL003_DeleteWithBody(t *testing.T) {
	spec := specHeader + `
paths:
  /items/{id}:
    parameters:
      - name: id
        in: path
        required: true
        schema:
          type: integer
    delete:
      operationId: deleteItem
      tags: [Items]
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
  /items/{id}:
    parameters:
      - name: id
        in: path
        required: true
        schema:
          type: integer
    delete:
      operationId: deleteItem
      tags: [Items]
      responses:
        '204':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL003")
	assert.Empty(t, vs)
}

func TestOAL005_PaginatedMissingParams(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
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
      responses:
        '200':
          description: ok
`
	vs := findRule(mustLint(t, spec), "OAL007")
	require.Len(t, vs, 1)
	assert.Contains(t, vs[0].Message, "item_id")
}

func TestOAL008_QueryParamNotSnakeCase(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
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

func TestOAL011_UnresolvedRef(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    get:
      operationId: listItems
      tags: [Items]
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

func TestOAL012_PostReturns200InsteadOf201(t *testing.T) {
	spec := specHeader + `
paths:
  /items:
    post:
      operationId: createItem
      tags: [Items]
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
      responses:
        '201':
          description: created
`
	vs := findRule(mustLint(t, spec), "OAL012")
	assert.Empty(t, vs)
}

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

func TestLintActualSpec(t *testing.T) {
	// Lint the actual project spec and ensure 0 error-level violations.
	specPath := "../../internal/api/openapi.yaml"
	if _, err := os.Stat(specPath); os.IsNotExist(err) {
		t.Skip("openapi.yaml not found at expected path")
	}
	l, err := New(specPath)
	require.NoError(t, err)

	vs := l.Run()
	errors := Filter(vs, SeverityError)
	for _, v := range errors {
		t.Errorf("%s", v)
	}
	assert.Empty(t, errors, "expected 0 error-level violations in openapi.yaml")
}
