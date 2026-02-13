package cli

import (
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeParam builds an openapi3.ParameterRef for testing.
func makeParam(name, in, typeName string, required bool) *openapi3.ParameterRef {
	p := &openapi3.Parameter{
		Name:     name,
		In:       in,
		Required: required,
	}
	if typeName != "" {
		types := openapi3.Types{typeName}
		p.Schema = &openapi3.SchemaRef{
			Value: &openapi3.Schema{Type: &types},
		}
	}
	return &openapi3.ParameterRef{Value: p}
}

// makeParamWithDefault builds a parameter with a default value.
func makeParamWithDefault(name, in, typeName string, required bool, defaultVal interface{}) *openapi3.ParameterRef {
	pRef := makeParam(name, in, typeName, required)
	pRef.Value.Schema.Value.Default = defaultVal
	return pRef
}

// makeParamWithDescription builds a parameter with a description.
func makeParamWithDescription(name, in, typeName string, required bool, desc string) *openapi3.ParameterRef {
	pRef := makeParam(name, in, typeName, required)
	pRef.Value.Description = desc
	return pRef
}

// makeResponse builds a simple openapi3.Response with no JSON content.
func makeResponse(desc string) *openapi3.ResponseRef {
	return &openapi3.ResponseRef{
		Value: &openapi3.Response{
			Description: &desc,
		},
	}
}

// makeSingleResourceResponse builds a 200 response with a $ref schema.
func makeSingleResourceResponse(refName string) *openapi3.ResponseRef {
	desc := "OK"
	return &openapi3.ResponseRef{
		Value: &openapi3.Response{
			Description: &desc,
			Content: openapi3.Content{
				"application/json": &openapi3.MediaType{
					Schema: &openapi3.SchemaRef{
						Ref:   "#/components/schemas/" + refName,
						Value: &openapi3.Schema{},
					},
				},
			},
		},
	}
}

// makePaginatedResponse builds a paginated response with data array + next_page_token.
func makePaginatedResponse(refTypeName, itemRefName string) *openapi3.ResponseRef {
	desc := "OK"
	arrayTypes := openapi3.Types{"array"}
	stringTypes := openapi3.Types{"string"}
	objectTypes := openapi3.Types{"object"}

	props := openapi3.Schemas{
		"data": &openapi3.SchemaRef{
			Value: &openapi3.Schema{
				Type: &arrayTypes,
				Items: &openapi3.SchemaRef{
					Ref:   "#/components/schemas/" + itemRefName,
					Value: &openapi3.Schema{},
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

// makeOperationWithResponses builds an Operation with the given responses map.
func makeOperationWithResponses(opID string, responseCodes map[string]*openapi3.ResponseRef) *openapi3.Operation {
	op := &openapi3.Operation{
		OperationID: opID,
		Summary:     "Test operation",
		Responses:   &openapi3.Responses{},
	}
	for code, ref := range responseCodes {
		op.Responses.Set(code, ref)
	}
	return op
}

func TestClassifyResponse(t *testing.T) {
	tests := []struct {
		name            string
		responses       map[string]*openapi3.ResponseRef
		tableColumns    []string
		expectedPattern ResponsePattern
		expectedCode    int
		expectedType    string
		expectedItem    string
	}{
		{
			name: "204 only is NoContent",
			responses: map[string]*openapi3.ResponseRef{
				"204": makeResponse("No Content"),
			},
			expectedPattern: NoContent,
			expectedCode:    204,
		},
		{
			name: "204 with 200 is SingleResource",
			responses: map[string]*openapi3.ResponseRef{
				"204": makeResponse("No Content"),
				"200": makeSingleResourceResponse("ItemDetail"),
			},
			expectedPattern: SingleResource,
			expectedCode:    200,
			expectedType:    "ItemDetail",
		},
		{
			name: "200 paginated",
			responses: map[string]*openapi3.ResponseRef{
				"200": makePaginatedResponse("PaginatedItems", "Item"),
			},
			tableColumns:    []string{"id", "name"},
			expectedPattern: PaginatedList,
			expectedCode:    200,
			expectedType:    "PaginatedItems",
			expectedItem:    "Item",
		},
		{
			name: "200 single resource",
			responses: map[string]*openapi3.ResponseRef{
				"200": makeSingleResourceResponse("SchemaDetail"),
			},
			expectedPattern: SingleResource,
			expectedCode:    200,
			expectedType:    "SchemaDetail",
		},
		{
			name: "201 single resource",
			responses: map[string]*openapi3.ResponseRef{
				"201": makeSingleResourceResponse("Task"),
			},
			expectedPattern: SingleResource,
			expectedCode:    201,
			expectedType:    "Task",
		},
		{
			name: "no success responses is CustomResult",
			responses: map[string]*openapi3.ResponseRef{
				"400": makeResponse("Bad Request"),
			},
			expectedPattern: CustomResult,
			expectedCode:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := makeOperationWithResponses("testOp", tt.responses)
			rm := classifyResponse(op, tt.tableColumns)

			assert.Equal(t, tt.expectedPattern, rm.Pattern)
			assert.Equal(t, tt.expectedCode, rm.SuccessCode)
			assert.Equal(t, tt.expectedType, rm.GoTypeName)
			assert.Equal(t, tt.expectedItem, rm.ItemTypeName)
			if tt.tableColumns != nil {
				assert.Equal(t, tt.tableColumns, rm.TableColumns)
			}
		})
	}
}

func TestParamToModel(t *testing.T) {
	tests := []struct {
		name     string
		param    *openapi3.Parameter
		expected ParamModel
	}{
		{
			name: "string path param",
			param: func() *openapi3.Parameter {
				p := makeParam("schemaName", "path", "string", true)
				return p.Value
			}(),
			expected: ParamModel{
				Name: "schemaName", GoName: "SchemaName",
				Type: "string", GoType: "string",
				Required: true, In: "path",
			},
		},
		{
			name: "integer query param with default",
			param: func() *openapi3.Parameter {
				p := makeParamWithDefault("max_results", "query", "integer", false, 100)
				return p.Value
			}(),
			expected: ParamModel{
				Name: "max_results", GoName: "MaxResults",
				Type: "integer", GoType: "int64",
				Required: false, In: "query", Default: "100",
			},
		},
		{
			name: "no schema defaults to string",
			param: &openapi3.Parameter{
				Name: "token", In: "query", Required: false,
			},
			expected: ParamModel{
				Name: "token", GoName: "Token",
				Type: "string", GoType: "string",
				Required: false, In: "query",
			},
		},
		{
			name: "boolean param",
			param: func() *openapi3.Parameter {
				p := makeParam("force", "query", "boolean", false)
				return p.Value
			}(),
			expected: ParamModel{
				Name: "force", GoName: "Force",
				Type: "boolean", GoType: "bool",
				Required: false, In: "query",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := paramToModel(tt.param)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParamToFlag(t *testing.T) {
	tests := []struct {
		name     string
		param    *openapi3.Parameter
		aliases  map[string]FlagAliasConfig
		expected FlagModel
	}{
		{
			name: "string query param",
			param: func() *openapi3.Parameter {
				p := makeParamWithDescription("page_token", "query", "string", false, "Pagination token")
				return p.Value
			}(),
			expected: FlagModel{
				Name: "page-token", GoName: "PageToken",
				GoType: "string", CobraType: "String",
				FieldName: "page_token", Usage: "Pagination token",
			},
		},
		{
			name: "integer param with default",
			param: func() *openapi3.Parameter {
				p := makeParamWithDefault("max_results", "query", "integer", false, 100)
				return p.Value
			}(),
			expected: FlagModel{
				Name: "max-results", GoName: "MaxResults",
				GoType: "int64", CobraType: "Int64",
				FieldName: "max_results", Default: "100",
			},
		},
		{
			name: "boolean param",
			param: func() *openapi3.Parameter {
				p := makeParam("force", "query", "boolean", false)
				return p.Value
			}(),
			expected: FlagModel{
				Name: "force", GoName: "Force",
				GoType: "bool", CobraType: "Bool",
				FieldName: "force",
			},
		},
		{
			name: "with alias",
			param: func() *openapi3.Parameter {
				p := makeParam("sql", "query", "string", false)
				return p.Value
			}(),
			aliases: map[string]FlagAliasConfig{"sql": {Short: "s"}},
			expected: FlagModel{
				Name: "sql", GoName: "SQL",
				GoType: "string", CobraType: "String",
				FieldName: "sql", Short: "s",
			},
		},
		{
			name: "no schema defaults to string",
			param: &openapi3.Parameter{
				Name: "x", In: "query", Required: false,
			},
			expected: FlagModel{
				Name: "x", GoName: "X",
				GoType: "string", CobraType: "String",
				FieldName: "x",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := paramToFlag(tt.param, tt.aliases)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFieldToFlag(t *testing.T) {
	tests := []struct {
		name     string
		field    string
		schema   *openapi3.Schema
		required bool
		aliases  map[string]FlagAliasConfig
		expected FlagModel
	}{
		{
			name:     "string field",
			field:    "description",
			schema:   makeSchema("string"),
			required: false,
			expected: FlagModel{
				Name: "description", GoName: "Description",
				GoType: "string", CobraType: "String",
				FieldName: "description", IsBody: true,
			},
		},
		{
			name:     "array field",
			field:    "tags",
			schema:   makeSchema("array"),
			required: false,
			expected: FlagModel{
				Name: "tags", GoName: "Tags",
				GoType: "[]string", CobraType: "StringSlice",
				FieldName: "tags", IsBody: true,
			},
		},
		{
			name:     "map field with additionalProperties",
			field:    "properties",
			schema:   makeSchemaWithAP(),
			required: true,
			expected: FlagModel{
				Name: "properties", GoName: "Properties",
				GoType: "[]string", CobraType: "StringSlice",
				FieldName: "properties", IsBody: true, Required: true,
				Usage: "properties (key=value pairs)",
			},
		},
		{
			name:  "boolean field with default",
			field: "compact",
			schema: func() *openapi3.Schema {
				s := makeSchema("boolean")
				s.Default = true
				return s
			}(),
			required: false,
			expected: FlagModel{
				Name: "compact", GoName: "Compact",
				GoType: "bool", CobraType: "Bool",
				FieldName: "compact", IsBody: true,
				Default: "true",
			},
		},
		{
			name:     "with alias",
			field:    "name",
			schema:   makeSchema("string"),
			required: true,
			aliases:  map[string]FlagAliasConfig{"name": {Short: "n"}},
			expected: FlagModel{
				Name: "name", GoName: "Name",
				GoType: "string", CobraType: "String",
				FieldName: "name", IsBody: true, Required: true,
				Short: "n",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fieldToFlag(tt.field, tt.schema, tt.required, tt.aliases)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// makeRequestBody builds a JSON request body with the given properties.
func makeRequestBody(props map[string]*openapi3.Schema, required []string) *openapi3.RequestBody {
	objectTypes := openapi3.Types{"object"}
	schemas := openapi3.Schemas{}
	for name, schema := range props {
		schemas[name] = &openapi3.SchemaRef{Value: schema}
	}
	return &openapi3.RequestBody{
		Content: openapi3.Content{
			"application/json": &openapi3.MediaType{
				Schema: &openapi3.SchemaRef{
					Value: &openapi3.Schema{
						Type:       &objectTypes,
						Properties: schemas,
						Required:   required,
					},
				},
			},
		},
	}
}

func TestBuildCommandModel(t *testing.T) {
	t.Run("GET with query params", func(t *testing.T) {
		op := &openapi3.Operation{
			OperationID: "listItems",
			Summary:     "List items",
			Responses:   &openapi3.Responses{},
		}
		op.Responses.Set("200", makeResponse("OK"))

		info := &opInfo{
			method:  "GET",
			urlPath: "/items",
			op:      op,
			params: []*openapi3.ParameterRef{
				makeParamWithDefault("max_results", "query", "integer", false, 100),
				makeParam("page_token", "query", "string", false),
			},
		}
		cmdCfg := CommandConfig{
			OperationID:  "listItems",
			CommandPath:  []string{"items"},
			Verb:         "list",
			TableColumns: []string{"id", "name"},
		}

		cm, err := buildCommandModel("test", "list-items", cmdCfg, info, nil)
		require.NoError(t, err)

		assert.Equal(t, "listItems", cm.OperationID)
		assert.Equal(t, "GET", cm.Method)
		assert.Equal(t, "/items", cm.URLPath)
		assert.Equal(t, "list", cm.Use)
		assert.False(t, cm.HasBody)
		assert.Len(t, cm.QueryParams, 2)
		assert.Len(t, cm.Flags, 2)
		// Flags should be sorted alphabetically
		assert.Equal(t, "max-results", cm.Flags[0].Name)
		assert.Equal(t, "page-token", cm.Flags[1].Name)
	})

	t.Run("POST with body fields", func(t *testing.T) {
		op := &openapi3.Operation{
			OperationID: "createTask",
			Summary:     "Create a task",
			Responses:   &openapi3.Responses{},
			RequestBody: &openapi3.RequestBodyRef{
				Value: makeRequestBody(
					map[string]*openapi3.Schema{
						"name":        makeSchema("string"),
						"description": makeSchema("string"),
						"priority":    makeSchema("integer"),
					},
					[]string{"name"},
				),
			},
		}
		op.Responses.Set("201", makeResponse("Created"))

		info := &opInfo{
			method:  "POST",
			urlPath: "/tasks",
			op:      op,
		}
		cmdCfg := CommandConfig{
			OperationID: "createTask",
			CommandPath: []string{"tasks"},
			Verb:        "create",
		}

		cm, err := buildCommandModel("test", "create-task", cmdCfg, info, nil)
		require.NoError(t, err)

		assert.True(t, cm.HasBody)
		assert.Len(t, cm.BodyFields, 3)
		// Should have body field flags + --json flag
		jsonFlagFound := false
		for _, f := range cm.Flags {
			if f.Name == "json" {
				jsonFlagFound = true
				assert.Equal(t, "__json__", f.FieldName)
			}
		}
		assert.True(t, jsonFlagFound, "--json flag should be present for body commands")

		// Check required flag
		for _, f := range cm.Flags {
			if f.FieldName == "name" {
				assert.True(t, f.Required)
			}
		}
	})

	t.Run("DELETE with positional path param", func(t *testing.T) {
		op := &openapi3.Operation{
			OperationID: "deleteItem",
			Summary:     "Delete an item",
			Responses:   &openapi3.Responses{},
		}
		op.Responses.Set("204", makeResponse("No Content"))

		info := &opInfo{
			method:  "DELETE",
			urlPath: "/items/{itemId}",
			op:      op,
			params: []*openapi3.ParameterRef{
				makeParam("itemId", "path", "string", true),
			},
		}
		cmdCfg := CommandConfig{
			OperationID:    "deleteItem",
			CommandPath:    []string{"items"},
			Verb:           "delete",
			PositionalArgs: []string{"itemId"},
			Confirm:        true,
		}

		cm, err := buildCommandModel("test", "delete-item", cmdCfg, info, nil)
		require.NoError(t, err)

		assert.Equal(t, "delete <item-id>", cm.Use)
		assert.True(t, cm.Confirm)
		assert.Len(t, cm.PathParams, 1)
		assert.Equal(t, "itemId", cm.PathParams[0].Name)
		// Positional params should NOT appear as flags
		for _, f := range cm.Flags {
			assert.NotEqual(t, "itemId", f.FieldName, "positional arg should not be in flags")
		}
		assert.Equal(t, NoContent, cm.Response.Pattern)
	})

	t.Run("body with flatten fields", func(t *testing.T) {
		objectTypes := openapi3.Types{"object"}
		boolTypes := openapi3.Types{"boolean"}
		optionsSchema := &openapi3.Schema{
			Type: &objectTypes,
			Properties: openapi3.Schemas{
				"compact": &openapi3.SchemaRef{
					Value: &openapi3.Schema{Type: &boolTypes},
				},
			},
			Required: []string{"compact"},
		}

		op := &openapi3.Operation{
			OperationID: "commitIngestion",
			Summary:     "Commit ingestion",
			Responses:   &openapi3.Responses{},
			RequestBody: &openapi3.RequestBodyRef{
				Value: makeRequestBody(
					map[string]*openapi3.Schema{
						"options": optionsSchema,
					},
					nil,
				),
			},
		}
		op.Responses.Set("200", makeResponse("OK"))

		info := &opInfo{
			method:  "POST",
			urlPath: "/ingestion/commit",
			op:      op,
		}
		cmdCfg := CommandConfig{
			OperationID:   "commitIngestion",
			CommandPath:   []string{},
			Verb:          "commit",
			FlattenFields: []string{"options"},
		}

		cm, err := buildCommandModel("test", "commit", cmdCfg, info, nil)
		require.NoError(t, err)

		assert.True(t, cm.HasBody)
		// Flattened field should appear as options.compact
		found := false
		for _, f := range cm.Flags {
			if f.FieldName == "options.compact" {
				found = true
				assert.True(t, f.IsBody)
				assert.Equal(t, "bool", f.GoType)
				assert.True(t, f.Required)
			}
		}
		assert.True(t, found, "flattened field options.compact should be in flags")
	})

	t.Run("body with compound flags", func(t *testing.T) {
		op := &openapi3.Operation{
			OperationID: "createTable",
			Summary:     "Create a table",
			Responses:   &openapi3.Responses{},
			RequestBody: &openapi3.RequestBodyRef{
				Value: makeRequestBody(
					map[string]*openapi3.Schema{
						"columns": makeSchema("array"),
					},
					[]string{"columns"},
				),
			},
		}
		op.Responses.Set("201", makeResponse("Created"))

		info := &opInfo{
			method:  "POST",
			urlPath: "/tables",
			op:      op,
		}
		cmdCfg := CommandConfig{
			OperationID: "createTable",
			CommandPath: []string{"tables"},
			Verb:        "create",
			CompoundFlags: map[string]CompoundFlagConfig{
				"columns": {Fields: []string{"name", "type"}, Separator: ":"},
			},
		}

		cm, err := buildCommandModel("test", "create-table", cmdCfg, info, nil)
		require.NoError(t, err)

		found := false
		for _, f := range cm.Flags {
			if f.FieldName == "columns" {
				found = true
				assert.True(t, f.IsCompound)
				assert.Equal(t, []string{"name", "type"}, f.CompoundFields)
				assert.Equal(t, ":", f.CompoundSep)
				assert.Equal(t, "[]string", f.GoType)
				assert.True(t, f.Required)
			}
		}
		assert.True(t, found, "compound flag columns should be in flags")
	})

	t.Run("positional arg not required in spec errors", func(t *testing.T) {
		op := &openapi3.Operation{
			OperationID: "getItem",
			Summary:     "Get item",
			Responses:   &openapi3.Responses{},
		}
		op.Responses.Set("200", makeResponse("OK"))

		info := &opInfo{
			method:  "GET",
			urlPath: "/items/{itemId}",
			op:      op,
			params: []*openapi3.ParameterRef{
				makeParam("itemId", "path", "string", false), // Required=false should error
			},
		}
		cmdCfg := CommandConfig{
			OperationID:    "getItem",
			CommandPath:    []string{"items"},
			Verb:           "get",
			PositionalArgs: []string{"itemId"},
		}

		_, err := buildCommandModel("test", "get-item", cmdCfg, info, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must be required")
	})
}
