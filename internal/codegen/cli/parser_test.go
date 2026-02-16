package cli

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToKebabCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"schemaName", "schema-name"},
		{"principalId", "principal-id"},
		{"table_name", "table-name"},
		{"already-kebab", "already-kebab"},
		{"simple", "simple"},
		{"API", "api"},
		{"URLPath", "urlpath"},
		{"max_results", "max-results"},
		{"page_token", "page-token"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, toKebabCase(tt.input))
		})
	}
}

func TestToPascalCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"schema_name", "SchemaName"},
		{"principal_id", "PrincipalID"},
		{"table-name", "TableName"},
		{"simple", "Simple"},
		{"api_key", "APIKey"},
		{"max_results", "MaxResults"},
		{"url", "URL"},
		{"sql", "SQL"},
		{"http", "HTTP"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, toPascalCase(tt.input))
		})
	}
}

func TestToCamelCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"schema_name", "schemaName"},
		{"table-name", "tableName"},
		{"simple", "simple"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, toCamelCase(tt.input))
		})
	}
}

func TestResponsePattern_String(t *testing.T) {
	tests := []struct {
		pattern  ResponsePattern
		expected string
	}{
		{PaginatedList, "PaginatedList"},
		{SingleResource, "SingleResource"},
		{NoContent, "NoContent"},
		{CustomResult, "CustomResult"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.pattern.String())
		})
	}
}

func TestParse_CoverageCheck(t *testing.T) {
	// Build a minimal spec with one operation
	spec := buildMinimalSpec("listSchemas", "GET", "/catalog/schemas")

	t.Run("missing operation covered by convention", func(t *testing.T) {
		// Convention-based parser auto-discovers operations; just need skip_operations
		cfg := &Config{
			CommandOverrides: map[string]CommandOverride{},
		}
		groups, err := Parse(spec, cfg)
		require.NoError(t, err)
		// The operation should be auto-discovered and placed in a group
		assert.NotEmpty(t, groups)
	})

	t.Run("operation in skip_operations", func(t *testing.T) {
		cfg := &Config{
			SkipOperations: []string{"listSchemas"},
		}
		groups, err := Parse(spec, cfg)
		require.NoError(t, err)
		assert.Empty(t, groups)
	})

	t.Run("override references nonexistent operation", func(t *testing.T) {
		cfg := &Config{
			CommandOverrides: map[string]CommandOverride{
				"doesNotExist": {
					Group: "test",
				},
			},
		}
		_, err := Parse(spec, cfg)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DRIFT ERROR")
		assert.Contains(t, err.Error(), "doesNotExist")
	})
}

func TestParse_FullSpec(t *testing.T) {
	// The spec is split into multiple files; use the bundled output.
	bundledPath := "../../../internal/api/openapi.bundled.yaml"
	sourcePath := "../../../internal/api/openapi.yaml"

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

	// Load the real OpenAPI spec
	loader := openapi3.NewLoader()
	spec, err := loader.LoadFromFile(bundledPath)
	require.NoError(t, err)
	require.NoError(t, spec.Validate(context.Background()))

	// Load the real CLI config
	cfg, err := LoadConfig("../../../cli-config.yaml")
	require.NoError(t, err)

	// Parse should succeed with full coverage
	groups, err := Parse(spec, cfg)
	require.NoError(t, err)

	// Verify we got all expected groups
	groupNames := make(map[string]bool)
	for _, g := range groups {
		groupNames[g.Name] = true
	}
	for _, expected := range []string{"catalog", "security", "query", "ingestion", "lineage", "governance", "observability", "storage", "manifest", "compute", "notebooks", "pipelines"} {
		assert.True(t, groupNames[expected], "missing group: %s", expected)
	}

	// Verify some specific commands
	for _, g := range groups {
		switch g.Name {
		case "catalog":
			// Should have multiple commands
			assert.Greater(t, len(g.Commands), 10, "catalog should have many commands")
			// Find listSchemas
			found := false
			for _, cmd := range g.Commands {
				if cmd.OperationID == "listSchemas" {
					found = true
					assert.Equal(t, "GET", cmd.Method)
					assert.Equal(t, PaginatedList, cmd.Response.Pattern)
					assert.NotEmpty(t, cmd.Response.TableColumns)
					break
				}
			}
			assert.True(t, found, "listSchemas command not found")

		case "security":
			assert.Greater(t, len(g.Commands), 15, "security should have many commands")

		case "query":
			assert.Len(t, g.Commands, 1)
			assert.Equal(t, "executeQuery", g.Commands[0].OperationID)

		case "notebooks":
			assert.Greater(t, len(g.Commands), 15, "notebooks should have many commands")

		case "pipelines":
			assert.Greater(t, len(g.Commands), 10, "pipelines should have many commands")
		}
	}
}

func TestComputeUseString(t *testing.T) {
	tests := []struct {
		name     string
		cfg      CommandConfig
		expected string
	}{
		{
			name:     "simple verb",
			cfg:      CommandConfig{Verb: "list"},
			expected: "list",
		},
		{
			name:     "verb with positional args",
			cfg:      CommandConfig{Verb: "get", PositionalArgs: []string{"schemaName"}},
			expected: "get <schema-name>",
		},
		{
			name:     "verb with multiple positional args",
			cfg:      CommandConfig{Verb: "get", PositionalArgs: []string{"schemaName", "tableName"}},
			expected: "get <schema-name> <table-name>",
		},
		{
			name:     "empty verb",
			cfg:      CommandConfig{Verb: ""},
			expected: "execute",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, computeUseString(tt.cfg))
		})
	}
}

func TestFieldToFlag_ObjectSchemaParsesJSON(t *testing.T) {
	objType := openapi3.Types{"object"}
	strType := openapi3.Types{"string"}
	schema := &openapi3.Schema{
		Type: &objType,
		Properties: openapi3.Schemas{
			"unique_key": {Value: &openapi3.Schema{Type: &strType}},
		},
	}

	fm := fieldToFlag("config", schema, false, nil)
	assert.Equal(t, "String", fm.CobraType)
	assert.True(t, fm.ParseJSON)
	assert.Contains(t, fm.Usage, "JSON object")
}

func TestFieldToFlag_MapSchemaDoesNotParseJSON(t *testing.T) {
	objType := openapi3.Types{"object"}
	strType := openapi3.Types{"string"}
	schema := &openapi3.Schema{
		Type: &objType,
		AdditionalProperties: openapi3.AdditionalProperties{
			Schema: &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &strType}},
		},
	}

	fm := fieldToFlag("properties", schema, false, nil)
	assert.Equal(t, "StringSlice", fm.CobraType)
	assert.False(t, fm.ParseJSON)
}

// === Drift Validation Tests ===

func TestParse_DriftValidation_StaleOverrideKey(t *testing.T) {
	spec := buildMinimalSpec("listItems", "GET", "/items")
	cfg := &Config{
		CommandOverrides: map[string]CommandOverride{
			"listItems":    {},
			"doesNotExist": {Group: "test"}, // stale override
		},
	}
	_, err := Parse(spec, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DRIFT ERROR")
	assert.Contains(t, err.Error(), "doesNotExist")
}

func TestParse_DriftValidation_StaleTableColumn(t *testing.T) {
	// Build a spec with a paginated response
	op := makeOperationWithResponses("listItems", map[string]*openapi3.ResponseRef{
		"200": makePaginatedResponseWithItemProps("PaginatedItems", "Item", map[string]string{
			"id":   "string",
			"name": "string",
		}),
	})
	op.Tags = []string{"Test"}
	spec := buildSpecFromOp("listItems", "GET", "/items", op)

	staleColumns := []string{"id", "nonexistent_field"}
	cfg := &Config{
		CommandOverrides: map[string]CommandOverride{
			"listItems": {
				TableColumns: &staleColumns,
			},
		},
	}
	_, err := Parse(spec, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table_columns")
	assert.Contains(t, err.Error(), "nonexistent_field")
}

func TestParse_DriftValidation_StalePositionalArg(t *testing.T) {
	// Build a spec with a path param
	op := &openapi3.Operation{
		OperationID: "getItem",
		Summary:     "Get item",
		Tags:        []string{"Test"},
		Responses:   &openapi3.Responses{},
	}
	op.Responses.Set("200", makeResponse("OK"))

	spec := buildSpecFromOp("getItem", "GET", "/items/{itemId}", op)
	// Add path param to the path item
	spec.Paths.Find("/items/{itemId}").Parameters = []*openapi3.ParameterRef{
		makeParam("itemId", "path", "string", true),
	}

	staleArgs := []string{"nonexistentParam"}
	cfg := &Config{
		CommandOverrides: map[string]CommandOverride{
			"getItem": {
				PositionalArgs: &staleArgs,
			},
		},
	}
	_, err := Parse(spec, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "positional_arg")
	assert.Contains(t, err.Error(), "nonexistentParam")
}

// === Test Helpers ===

// buildMinimalSpec creates a minimal OpenAPI spec with one operation.
func buildMinimalSpec(opID, method, path string) *openapi3.T {
	op := &openapi3.Operation{
		OperationID: opID,
		Summary:     "Test operation",
		Responses: &openapi3.Responses{
			Extensions: map[string]interface{}{},
		},
	}
	// Add a 200 response
	resp := &openapi3.Response{
		Description: stringPtr("Success"),
	}
	op.Responses.Set("200", &openapi3.ResponseRef{Value: resp})

	return buildSpecFromOp(opID, method, path, op)
}

// buildSpecFromOp creates a spec with a single operation.
func buildSpecFromOp(opID, method, path string, op *openapi3.Operation) *openapi3.T {
	pathItem := &openapi3.PathItem{}
	switch method {
	case "GET":
		pathItem.Get = op
	case "POST":
		pathItem.Post = op
	case "PUT":
		pathItem.Put = op
	case "DELETE":
		pathItem.Delete = op
	}

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info: &openapi3.Info{
			Title:   "Test",
			Version: "1.0.0",
		},
		Paths: &openapi3.Paths{},
	}
	spec.Paths.Set(path, pathItem)

	return spec
}

// makePaginatedResponseWithItemProps builds a paginated response with known item properties.
func makePaginatedResponseWithItemProps(refTypeName, itemRefName string, fields map[string]string) *openapi3.ResponseRef {
	desc := "OK"
	arrayTypes := openapi3.Types{"array"}
	objectTypes := openapi3.Types{"object"}

	// Build item schema with actual properties
	itemProps := openapi3.Schemas{}
	for name, typeName := range fields {
		t := openapi3.Types{typeName}
		itemProps[name] = &openapi3.SchemaRef{
			Value: &openapi3.Schema{Type: &t},
		}
	}
	itemSchema := &openapi3.Schema{
		Type:       &objectTypes,
		Properties: itemProps,
	}

	stringTypes := openapi3.Types{"string"}
	props := openapi3.Schemas{
		"data": &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Type: &arrayTypes,
				Items: &openapi3.SchemaRef{
					Ref:   "#/components/schemas/" + itemRefName,
					Value: itemSchema,
				},
			},
		},
		"next_page_token": &openapi3.SchemaRef{
			Value: &openapi3.Schema{Type: &stringTypes},
		},
	}

	return &openapi3.ResponseRef{
		Value: &openapi3.Response{
			Description: &desc,
			Content: openapi3.Content{
				"application/json": &openapi3.MediaType{
					Schema: &openapi3.SchemaRef{
						Ref: "#/components/schemas/" + refTypeName,
						Value: &openapi3.Schema{
							Type:       &objectTypes,
							Properties: props,
						},
					},
				},
			},
		},
	}
}

func stringPtr(s string) *string {
	return &s
}
