package cli

import (
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
)

// --- Test helpers ---

// makeOpInfo builds an opInfo with optional parameters and no request body.
func makeOpInfo(method, path string, params []*openapi3.ParameterRef) *opInfo {
	op := &openapi3.Operation{
		OperationID: "testOp",
		Summary:     "Test operation",
		Responses:   &openapi3.Responses{},
	}
	resp := &openapi3.Response{Description: stringPtr("OK")}
	op.Responses.Set("200", &openapi3.ResponseRef{Value: resp})
	return &opInfo{method: method, urlPath: path, op: op, params: params}
}

// makeOpInfoWithBodyExample builds an opInfo with a request body that has an example.
func makeOpInfoWithBodyExample(method, path string, example map[string]interface{}, params []*openapi3.ParameterRef) *opInfo {
	info := makeOpInfo(method, path, params)
	info.op.RequestBody = &openapi3.RequestBodyRef{
		Value: &openapi3.RequestBody{
			Content: openapi3.Content{
				"application/json": &openapi3.MediaType{
					Example: example,
					Schema: &openapi3.SchemaRef{
						Value: &openapi3.Schema{},
					},
				},
			},
		},
	}
	return info
}

// makeParamWithExample builds a path parameter with an example value.
func makeParamWithExample(name string, example interface{}) *openapi3.ParameterRef {
	return &openapi3.ParameterRef{
		Value: &openapi3.Parameter{
			Name:     name,
			In:       "path",
			Required: true,
			Example:  example,
		},
	}
}

// --- Tests ---

func TestBuildCommandPrefix(t *testing.T) {
	tests := []struct {
		name     string
		group    string
		cmdCfg   CommandConfig
		expected string
	}{
		{
			name:     "group with subcommand and verb",
			group:    "catalog",
			cmdCfg:   CommandConfig{CommandPath: []string{"schemas"}, Verb: "create"},
			expected: "duck catalog schemas create",
		},
		{
			name:     "group with verb only",
			group:    "lineage",
			cmdCfg:   CommandConfig{Verb: "purge"},
			expected: "duck lineage purge",
		},
		{
			name:     "group with empty verb",
			group:    "query",
			cmdCfg:   CommandConfig{},
			expected: "duck query",
		},
		{
			name:     "group with empty command path and verb",
			group:    "manifest",
			cmdCfg:   CommandConfig{CommandPath: []string{}, Verb: "create"},
			expected: "duck manifest create",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, buildCommandPrefix(tt.group, tt.cmdCfg))
		})
	}
}

func TestFormatFlag(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		value    interface{}
		expected []string
	}{
		{
			name:     "simple string no spaces",
			field:    "type",
			value:    "user",
			expected: []string{"--type", "user"},
		},
		{
			name:     "string with spaces is quoted",
			field:    "comment",
			value:    "hello world",
			expected: []string{"--comment", `"hello world"`},
		},
		{
			name:     "underscore name becomes kebab",
			field:    "metastore_type",
			value:    "sqlite",
			expected: []string{"--metastore-type", "sqlite"},
		},
		{
			name:     "bool true emits flag without value",
			field:    "is_admin",
			value:    true,
			expected: []string{"--is-admin"},
		},
		{
			name:     "bool false is omitted",
			field:    "is_admin",
			value:    false,
			expected: nil,
		},
		{
			name:     "array values comma-separated",
			field:    "s3_keys",
			value:    []interface{}{"a.parquet", "b.parquet"},
			expected: []string{"--s3-keys", "a.parquet,b.parquet"},
		},
		{
			name:     "empty array is omitted",
			field:    "tags",
			value:    []interface{}{},
			expected: nil,
		},
		{
			name:     "map renders key=value pairs",
			field:    "properties",
			value:    map[string]interface{}{"team": "eng"},
			expected: []string{"--properties", "team=eng"},
		},
		{
			name:     "map multiple keys sorted",
			field:    "properties",
			value:    map[string]interface{}{"team": "eng", "env": "prod"},
			expected: []string{"--properties", "env=prod", "--properties", "team=eng"},
		},
		{
			name:     "integer value",
			field:    "max_memory_gb",
			value:    64,
			expected: []string{"--max-memory-gb", "64"},
		},
		{
			name:     "float value",
			field:    "threshold",
			value:    0.95,
			expected: []string{"--threshold", "0.95"},
		},
		{
			name:     "nil value is omitted",
			field:    "comment",
			value:    nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, formatFlag(tt.field, tt.value))
		})
	}
}

func TestFormatCompoundFlag(t *testing.T) {
	t.Run("array of objects", func(t *testing.T) {
		cfg := CompoundFlagConfig{Fields: []string{"name", "type"}, Separator: ":"}
		value := []interface{}{
			map[string]interface{}{"name": "id", "type": "BIGINT"},
			map[string]interface{}{"name": "email", "type": "VARCHAR"},
		}
		result := formatCompoundFlag("columns", cfg, value)
		assert.Equal(t, []string{"--columns", "id:BIGINT", "--columns", "email:VARCHAR"}, result)
	})

	t.Run("non-array value returns nil", func(t *testing.T) {
		cfg := CompoundFlagConfig{Fields: []string{"name", "type"}, Separator: ":"}
		result := formatCompoundFlag("columns", cfg, "not-an-array")
		assert.Nil(t, result)
	})

	t.Run("empty array", func(t *testing.T) {
		cfg := CompoundFlagConfig{Fields: []string{"name", "type"}, Separator: ":"}
		result := formatCompoundFlag("columns", cfg, []interface{}{})
		assert.Nil(t, result)
	})
}

func TestFormatValue(t *testing.T) {
	assert.Equal(t, "hello", formatValue("hello"))
	assert.Equal(t, "42", formatValue(42))
	assert.Equal(t, "true", formatValue(true))
	assert.Equal(t, "", formatValue(nil))
	assert.Equal(t, "3.14", formatValue(3.14))
}

func TestParamExampleValues(t *testing.T) {
	t.Run("parameter-level example", func(t *testing.T) {
		info := makeOpInfo("GET", "/items/{id}", []*openapi3.ParameterRef{
			makeParamWithExample("id", "abc-123"),
		})
		m := paramExampleValues(info)
		assert.Equal(t, "abc-123", m["id"])
	})

	t.Run("schema-level example", func(t *testing.T) {
		pRef := makeParam("name", "path", "string", true)
		pRef.Value.Schema.Value.Example = "main"
		info := makeOpInfo("GET", "/schemas/{name}", []*openapi3.ParameterRef{pRef})
		m := paramExampleValues(info)
		assert.Equal(t, "main", m["name"])
	})

	t.Run("no example returns empty map", func(t *testing.T) {
		info := makeOpInfo("GET", "/items/{id}", []*openapi3.ParameterRef{
			makeParam("id", "path", "string", true),
		})
		m := paramExampleValues(info)
		assert.Empty(t, m)
	})
}

func TestGenerateExamples(t *testing.T) {
	tests := []struct {
		name     string
		group    string
		cmdCfg   CommandConfig
		info     *opInfo
		expected []string
	}{
		{
			name:  "create with positional and flags",
			group: "catalog",
			cmdCfg: CommandConfig{
				OperationID:    "createSchema",
				CommandPath:    []string{"schemas"},
				Verb:           "create",
				PositionalArgs: []string{"name"},
			},
			info: makeOpInfoWithBodyExample("POST", "/catalogs/{catalogName}/schemas", map[string]interface{}{
				"name":    "analytics",
				"comment": "Analytics data schema",
			}, nil),
			expected: []string{
				`duck catalog schemas create analytics --comment "Analytics data schema"`,
			},
		},
		{
			name:  "list with no body and no positional returns nil",
			group: "catalog",
			cmdCfg: CommandConfig{
				OperationID: "listSchemas",
				CommandPath: []string{"schemas"},
				Verb:        "list",
			},
			info:     makeOpInfo("GET", "/catalogs/{catalogName}/schemas", nil),
			expected: nil,
		},
		{
			name:  "get with positional path param example",
			group: "catalog",
			cmdCfg: CommandConfig{
				OperationID:    "getSchema",
				CommandPath:    []string{"schemas"},
				Verb:           "get",
				PositionalArgs: []string{"schemaName"},
			},
			info: makeOpInfo("GET", "/catalogs/{catalogName}/schemas/{schemaName}", []*openapi3.ParameterRef{
				makeParamWithExample("schemaName", "main"),
			}),
			expected: []string{
				"duck catalog schemas get main",
			},
		},
		{
			name:  "get with positional path param no example uses placeholder",
			group: "catalog",
			cmdCfg: CommandConfig{
				OperationID:    "getSchema",
				CommandPath:    []string{"schemas"},
				Verb:           "get",
				PositionalArgs: []string{"schemaName"},
			},
			info: makeOpInfo("GET", "/catalogs/{catalogName}/schemas/{schemaName}", []*openapi3.ParameterRef{
				makeParam("schemaName", "path", "string", true),
			}),
			expected: []string{
				"duck catalog schemas get <schema-name>",
			},
		},
		{
			name:  "delete with positional",
			group: "catalog",
			cmdCfg: CommandConfig{
				OperationID:    "deleteSchema",
				CommandPath:    []string{"schemas"},
				Verb:           "delete",
				PositionalArgs: []string{"schemaName"},
				Confirm:        true,
			},
			info: makeOpInfo("DELETE", "/catalogs/{catalogName}/schemas/{schemaName}", []*openapi3.ParameterRef{
				makeParamWithExample("schemaName", "main"),
			}),
			expected: []string{
				"duck catalog schemas delete main",
			},
		},
		{
			name:  "create with compound flags",
			group: "catalog",
			cmdCfg: CommandConfig{
				OperationID:    "createTable",
				CommandPath:    []string{"tables"},
				Verb:           "create",
				PositionalArgs: []string{"schemaName"},
				CompoundFlags: map[string]CompoundFlagConfig{
					"columns": {Fields: []string{"name", "type"}, Separator: ":"},
				},
			},
			info: makeOpInfoWithBodyExample("POST", "/catalogs/{catalogName}/schemas/{schemaName}/tables", map[string]interface{}{
				"name": "users",
				"columns": []interface{}{
					map[string]interface{}{"name": "id", "type": "BIGINT"},
					map[string]interface{}{"name": "email", "type": "VARCHAR"},
				},
				"comment": "User accounts table",
			}, []*openapi3.ParameterRef{
				makeParamWithExample("schemaName", "main"),
			}),
			expected: []string{
				`duck catalog tables create main --columns id:BIGINT --columns email:VARCHAR --comment "User accounts table" --name users`,
			},
		},
		{
			name:  "boolean true shown and false omitted",
			group: "security",
			cmdCfg: CommandConfig{
				OperationID:    "createPrincipal",
				CommandPath:    []string{"principals"},
				Verb:           "create",
				PositionalArgs: []string{"name"},
			},
			info: makeOpInfoWithBodyExample("POST", "/principals", map[string]interface{}{
				"name":     "alice",
				"type":     "user",
				"is_admin": false,
			}, nil),
			expected: []string{
				"duck security principals create alice --type user",
			},
		},
		{
			name:  "boolean true is shown",
			group: "security",
			cmdCfg: CommandConfig{
				OperationID: "updatePrincipalAdmin",
				CommandPath: []string{"principals"},
				Verb:        "update-admin",
			},
			info: makeOpInfoWithBodyExample("PUT", "/principals/{principalId}/admin", map[string]interface{}{
				"is_admin": true,
			}, nil),
			expected: []string{
				"duck security principals update-admin --is-admin",
			},
		},
		{
			name:  "map fields rendered as key=value",
			group: "catalog",
			cmdCfg: CommandConfig{
				OperationID:    "updateSchema",
				CommandPath:    []string{"schemas"},
				Verb:           "update",
				PositionalArgs: []string{"schemaName"},
			},
			info: makeOpInfoWithBodyExample("PATCH", "/catalogs/{catalogName}/schemas/{schemaName}", map[string]interface{}{
				"comment":    "Updated schema description",
				"properties": map[string]interface{}{"team": "data-eng"},
			}, []*openapi3.ParameterRef{
				makeParamWithExample("schemaName", "main"),
			}),
			expected: []string{
				`duck catalog schemas update main --comment "Updated schema description" --properties team=data-eng`,
			},
		},
		{
			name:  "empty body example produces prefix only",
			group: "catalog",
			cmdCfg: CommandConfig{
				OperationID: "setDefaultCatalog",
				CommandPath: []string{},
				Verb:        "set-default",
			},
			info: makeOpInfoWithBodyExample("POST", "/catalogs/{catalogName}/set-default", map[string]interface{}{}, nil),
			expected: []string{
				"duck catalog set-default",
			},
		},
		{
			name:  "flattened nested fields",
			group: "ingestion",
			cmdCfg: CommandConfig{
				OperationID:   "commitTableIngestion",
				CommandPath:   []string{},
				Verb:          "commit",
				FlattenFields: []string{"options"},
			},
			info: makeOpInfoWithBodyExample("POST", "/ingestion/commit", map[string]interface{}{
				"s3_keys": []interface{}{
					"uploads/2025/01/events-part1.parquet",
					"uploads/2025/01/events-part2.parquet",
				},
				"options": map[string]interface{}{
					"allow_missing_columns": false,
					"ignore_extra_columns":  true,
				},
			}, nil),
			expected: []string{
				"duck ingestion commit --ignore-extra-columns --s3-keys uploads/2025/01/events-part1.parquet,uploads/2025/01/events-part2.parquet",
			},
		},
		{
			name:  "all flags no positional",
			group: "security",
			cmdCfg: CommandConfig{
				OperationID: "createGrant",
				CommandPath: []string{"grants"},
				Verb:        "create",
			},
			info: makeOpInfoWithBodyExample("POST", "/grants", map[string]interface{}{
				"principal_id":   "550e8400-e29b-41d4-a716-446655440002",
				"principal_type": "user",
				"securable_type": "schema",
				"securable_id":   "550e8400-e29b-41d4-a716-446655440001",
				"privilege":      "USE_SCHEMA",
			}, nil),
			expected: []string{
				"duck security grants create --principal-id 550e8400-e29b-41d4-a716-446655440002 --principal-type user --privilege USE_SCHEMA --securable-id 550e8400-e29b-41d4-a716-446655440001 --securable-type schema",
			},
		},
		{
			name:  "query command with empty verb",
			group: "query",
			cmdCfg: CommandConfig{
				OperationID: "executeQuery",
				CommandPath: []string{},
				Verb:        "",
			},
			info: makeOpInfoWithBodyExample("POST", "/query", map[string]interface{}{
				"sql": "SELECT id, name FROM main.users LIMIT 10",
			}, nil),
			expected: []string{
				`duck query --sql "SELECT id, name FROM main.users LIMIT 10"`,
			},
		},
		{
			name:  "body with path param positional not in body",
			group: "catalog",
			cmdCfg: CommandConfig{
				OperationID:    "createView",
				CommandPath:    []string{"views"},
				Verb:           "create",
				PositionalArgs: []string{"schemaName"},
			},
			info: makeOpInfoWithBodyExample("POST", "/catalogs/{catalogName}/schemas/{schemaName}/views", map[string]interface{}{
				"name":            "active_users",
				"view_definition": "SELECT * FROM main.users WHERE active = true",
				"comment":         "Active users view",
			}, []*openapi3.ParameterRef{
				makeParamWithExample("schemaName", "main"),
			}),
			expected: []string{
				`duck catalog views create main --comment "Active users view" --name active_users --view-definition "SELECT * FROM main.users WHERE active = true"`,
			},
		},
		{
			name:  "no request body and no positional args returns nil",
			group: "observability",
			cmdCfg: CommandConfig{
				OperationID: "listAuditLogs",
				CommandPath: []string{},
				Verb:        "list",
			},
			info:     makeOpInfo("GET", "/audit-logs", nil),
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateExamples(tt.group, tt.cmdCfg, tt.info)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerateExamples_ConfigPrecedence(t *testing.T) {
	// When config provides examples, they should be used by buildCommandModel
	// (the caller checks len(cm.Examples) == 0 before calling generateExamples).
	// This test verifies the contract: generateExamples always produces output,
	// but the caller skips it when config examples exist.
	cmdCfg := CommandConfig{
		OperationID:    "createSchema",
		CommandPath:    []string{"schemas"},
		Verb:           "create",
		PositionalArgs: []string{"name"},
		Examples:       []string{"duck catalog schemas create my-custom-example"},
	}
	info := makeOpInfoWithBodyExample("POST", "/catalogs/{catalogName}/schemas", map[string]interface{}{
		"name":    "analytics",
		"comment": "Analytics data schema",
	}, nil)

	// generateExamples would produce auto-generated output...
	autoGenerated := generateExamples("catalog", cmdCfg, info)
	assert.NotEmpty(t, autoGenerated, "auto-generation should produce output")

	// ...but the caller preserves config examples (simulating parser.go logic)
	examples := cmdCfg.Examples
	if len(examples) == 0 {
		examples = autoGenerated
	}
	assert.Equal(t, []string{"duck catalog schemas create my-custom-example"}, examples)
}
