package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildGoldenSpec builds a small OpenAPI spec with 5 operations covering all 4 response patterns.
//
// Group "items" (3 operations):
//   - listItems     GET  /items          → PaginatedList
//   - getItem       GET  /items/{itemId} → SingleResource
//   - deleteItem    DELETE /items/{itemId} → NoContent
//
// Group "tasks" (2 operations):
//   - createTask    POST /tasks           → SingleResource (201)
//   - runTask       POST /tasks/{taskId}/run → CustomResult
func buildGoldenSpec() *openapi3.T {
	stringTypes := openapi3.Types{"string"}
	intTypes := openapi3.Types{"integer"}
	objectTypes := openapi3.Types{"object"}
	arrayTypes := openapi3.Types{"array"}

	spec := &openapi3.T{
		OpenAPI: "3.0.3",
		Info: &openapi3.Info{
			Title:   "Test API",
			Version: "1.0.0",
		},
		Paths: &openapi3.Paths{},
	}

	// --- /items (GET listItems) ---
	listItemsOp := &openapi3.Operation{
		OperationID: "listItems",
		Summary:     "List all items",
		Description: "Returns a paginated list of items.",
		Parameters: openapi3.Parameters{
			{Value: &openapi3.Parameter{
				Name: "max_results", In: "query", Required: false,
				Schema: &openapi3.SchemaRef{Value: &openapi3.Schema{
					Type:    &intTypes,
					Default: 100,
				}},
				Description: "Maximum number of results",
			}},
			{Value: &openapi3.Parameter{
				Name: "page_token", In: "query", Required: false,
				Schema:      &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &stringTypes}},
				Description: "Pagination token",
			}},
		},
		Responses: &openapi3.Responses{},
	}
	listItemsOp.Responses.Set("200", &openapi3.ResponseRef{
		Value: &openapi3.Response{
			Description: strPtr("OK"),
			Content: openapi3.Content{
				"application/json": &openapi3.MediaType{
					Schema: &openapi3.SchemaRef{
						Ref: "#/components/schemas/PaginatedItems",
						Value: &openapi3.Schema{
							Type: &objectTypes,
							Properties: openapi3.Schemas{
								"data": &openapi3.SchemaRef{
									Value: &openapi3.Schema{
										Type: &arrayTypes,
										Items: &openapi3.SchemaRef{
											Ref:   "#/components/schemas/Item",
											Value: &openapi3.Schema{},
										},
									},
								},
								"next_page_token": &openapi3.SchemaRef{
									Value: &openapi3.Schema{Type: &stringTypes},
								},
							},
						},
					},
				},
			},
		},
	})
	itemsPath := &openapi3.PathItem{Get: listItemsOp}

	// --- /items/{itemId} (GET getItem, DELETE deleteItem) ---
	itemIDParam := &openapi3.ParameterRef{
		Value: &openapi3.Parameter{
			Name: "itemId", In: "path", Required: true,
			Schema:      &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &stringTypes}},
			Description: "The item ID",
		},
	}

	getItemOp := &openapi3.Operation{
		OperationID: "getItem",
		Summary:     "Get an item",
		Description: "Returns a single item by ID.",
		Responses:   &openapi3.Responses{},
	}
	getItemOp.Responses.Set("200", &openapi3.ResponseRef{
		Value: &openapi3.Response{
			Description: strPtr("OK"),
			Content: openapi3.Content{
				"application/json": &openapi3.MediaType{
					Schema: &openapi3.SchemaRef{
						Ref:   "#/components/schemas/ItemDetail",
						Value: &openapi3.Schema{},
					},
				},
			},
		},
	})

	deleteItemOp := &openapi3.Operation{
		OperationID: "deleteItem",
		Summary:     "Delete an item",
		Description: "Deletes an item by ID.",
		Responses:   &openapi3.Responses{},
	}
	deleteItemOp.Responses.Set("204", &openapi3.ResponseRef{
		Value: &openapi3.Response{Description: strPtr("No Content")},
	})

	itemByIDPath := &openapi3.PathItem{
		Get:        getItemOp,
		Delete:     deleteItemOp,
		Parameters: openapi3.Parameters{itemIDParam},
	}

	// --- /tasks (POST createTask) ---
	createTaskOp := &openapi3.Operation{
		OperationID: "createTask",
		Summary:     "Create a task",
		Description: "Creates a new task.",
		Responses:   &openapi3.Responses{},
		RequestBody: &openapi3.RequestBodyRef{
			Value: &openapi3.RequestBody{
				Content: openapi3.Content{
					"application/json": &openapi3.MediaType{
						Schema: &openapi3.SchemaRef{
							Value: &openapi3.Schema{
								Type:     &objectTypes,
								Required: []string{"name"},
								Properties: openapi3.Schemas{
									"name": &openapi3.SchemaRef{
										Value: &openapi3.Schema{
											Type:        &stringTypes,
											Description: "Task name",
										},
									},
									"description": &openapi3.SchemaRef{
										Value: &openapi3.Schema{
											Type:        &stringTypes,
											Description: "Task description",
										},
									},
									"priority": &openapi3.SchemaRef{
										Value: &openapi3.Schema{
											Type:        &intTypes,
											Description: "Task priority",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	createTaskOp.Responses.Set("201", &openapi3.ResponseRef{
		Value: &openapi3.Response{
			Description: strPtr("Created"),
			Content: openapi3.Content{
				"application/json": &openapi3.MediaType{
					Schema: &openapi3.SchemaRef{
						Ref:   "#/components/schemas/Task",
						Value: &openapi3.Schema{},
					},
				},
			},
		},
	})
	tasksPath := &openapi3.PathItem{Post: createTaskOp}

	// --- /tasks/{taskId}/run (POST runTask) ---
	taskIDParam := &openapi3.ParameterRef{
		Value: &openapi3.Parameter{
			Name: "taskId", In: "path", Required: true,
			Schema:      &openapi3.SchemaRef{Value: &openapi3.Schema{Type: &stringTypes}},
			Description: "The task ID",
		},
	}

	runTaskOp := &openapi3.Operation{
		OperationID: "runTask",
		Summary:     "Run a task",
		Description: "Triggers execution of a task.",
		Responses:   &openapi3.Responses{},
		RequestBody: &openapi3.RequestBodyRef{
			Value: &openapi3.RequestBody{
				Content: openapi3.Content{
					"application/json": &openapi3.MediaType{
						Schema: &openapi3.SchemaRef{
							Value: &openapi3.Schema{
								Type: &objectTypes,
								Properties: openapi3.Schemas{
									"parameters": &openapi3.SchemaRef{
										Value: &openapi3.Schema{
											Type:        &objectTypes,
											Description: "Key-value parameters for the task run",
											AdditionalProperties: openapi3.AdditionalProperties{
												Schema: &openapi3.SchemaRef{
													Value: &openapi3.Schema{Type: &stringTypes},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	// Custom result: 200 with inline schema (no $ref)
	runTaskOp.Responses.Set("200", &openapi3.ResponseRef{
		Value: &openapi3.Response{
			Description: strPtr("OK"),
			Content: openapi3.Content{
				"application/json": &openapi3.MediaType{
					Schema: &openapi3.SchemaRef{
						// No Ref — inline schema → CustomResult pattern
						Value: &openapi3.Schema{
							Type: &objectTypes,
							Properties: openapi3.Schemas{
								"run_id": &openapi3.SchemaRef{
									Value: &openapi3.Schema{Type: &stringTypes},
								},
								"status": &openapi3.SchemaRef{
									Value: &openapi3.Schema{Type: &stringTypes},
								},
							},
						},
					},
				},
			},
		},
	})
	taskRunPath := &openapi3.PathItem{
		Post:       runTaskOp,
		Parameters: openapi3.Parameters{taskIDParam},
	}

	spec.Paths.Set("/items", itemsPath)
	spec.Paths.Set("/items/{itemId}", itemByIDPath)
	spec.Paths.Set("/tasks", tasksPath)
	spec.Paths.Set("/tasks/{taskId}/run", taskRunPath)

	return spec
}

func strPtr(s string) *string {
	return &s
}

// buildGoldenConfig builds a Config matching the golden spec.
func buildGoldenConfig() *Config {
	return &Config{
		Global: GlobalConfig{
			DefaultOutput:      "table",
			ConfirmDestructive: true,
		},
		Groups: map[string]GroupConfig{
			"items": {
				Short: "Item management commands",
				Commands: map[string]CommandConfig{
					"list-items": {
						OperationID:  "listItems",
						CommandPath:  []string{"items"},
						Verb:         "list",
						TableColumns: []string{"id", "name", "status"},
					},
					"get-item": {
						OperationID:    "getItem",
						CommandPath:    []string{"items"},
						Verb:           "get",
						PositionalArgs: []string{"itemId"},
					},
					"delete-item": {
						OperationID:    "deleteItem",
						CommandPath:    []string{"items"},
						Verb:           "delete",
						PositionalArgs: []string{"itemId"},
						Confirm:        true,
					},
				},
			},
			"tasks": {
				Short: "Task management commands",
				Commands: map[string]CommandConfig{
					"create-task": {
						OperationID: "createTask",
						CommandPath: []string{},
						Verb:        "create",
						FlagAliases: map[string]FlagAliasConfig{
							"name": {Short: "n"},
						},
					},
					"run-task": {
						OperationID:    "runTask",
						CommandPath:    []string{},
						Verb:           "run",
						PositionalArgs: []string{"taskId"},
					},
				},
			},
		},
	}
}

func TestRender_Golden(t *testing.T) {
	spec := buildGoldenSpec()
	cfg := buildGoldenConfig()

	groups, err := Parse(spec, cfg)
	require.NoError(t, err)

	outDir := t.TempDir()
	err = Render(groups, cfg, outDir)
	require.NoError(t, err)

	goldenDir := filepath.Join("testdata", "golden")
	update := os.Getenv("UPDATE_GOLDEN") == "1"

	expectedFiles := []string{
		"root.gen.go",
		"client.gen.go",
		"output.gen.go",
		"pagination.gen.go",
		"overrides.gen.go",
		"items.gen.go",
		"tasks.gen.go",
	}

	for _, fname := range expectedFiles {
		t.Run(fname, func(t *testing.T) {
			actual, err := os.ReadFile(filepath.Join(outDir, fname))
			require.NoError(t, err)

			goldenPath := filepath.Join(goldenDir, fname)
			if update {
				require.NoError(t, os.MkdirAll(goldenDir, 0o750))
				require.NoError(t, os.WriteFile(goldenPath, actual, 0o644))
				t.Skipf("updated golden file: %s", goldenPath)
				return
			}

			expected, err := os.ReadFile(goldenPath)
			require.NoError(t, err, "golden file %q missing — run with UPDATE_GOLDEN=1 to create", fname)
			assert.Equal(t, string(expected), string(actual))
		})
	}
}

func TestRender_OutputDir(t *testing.T) {
	t.Run("creates output dir", func(t *testing.T) {
		outDir := filepath.Join(t.TempDir(), "nested", "dir")
		cfg := &Config{Global: GlobalConfig{DefaultOutput: "table"}}
		err := Render(nil, cfg, outDir)
		require.NoError(t, err)

		_, err = os.Stat(outDir)
		require.NoError(t, err, "output dir should be created")

		// Static files should exist
		for _, fname := range []string{"client.gen.go", "output.gen.go", "pagination.gen.go", "overrides.gen.go", "root.gen.go"} {
			_, err := os.Stat(filepath.Join(outDir, fname))
			assert.NoError(t, err, "expected file %s to exist", fname)
		}
	})

	t.Run("empty groups produces only static files", func(t *testing.T) {
		outDir := t.TempDir()
		cfg := &Config{Global: GlobalConfig{DefaultOutput: "json"}}
		err := Render([]GroupModel{}, cfg, outDir)
		require.NoError(t, err)

		entries, err := os.ReadDir(outDir)
		require.NoError(t, err)

		fileNames := make([]string, 0, len(entries))
		for _, e := range entries {
			fileNames = append(fileNames, e.Name())
		}
		// Should have exactly 5 static files, no group files
		assert.Len(t, fileNames, 5)
		assert.Contains(t, fileNames, "root.gen.go")
		assert.Contains(t, fileNames, "client.gen.go")
		assert.Contains(t, fileNames, "output.gen.go")
		assert.Contains(t, fileNames, "pagination.gen.go")
		assert.Contains(t, fileNames, "overrides.gen.go")
	})
}

func TestRender_FuncMap_DefaultValue(t *testing.T) {
	tests := []struct {
		name     string
		flag     FlagModel
		expected string
	}{
		{"Bool default false", FlagModel{CobraType: "Bool"}, "false"},
		{"Bool default true", FlagModel{CobraType: "Bool", Default: "true"}, "true"},
		{"Int64 with default", FlagModel{CobraType: "Int64", Default: "100"}, "100"},
		{"Int64 no default", FlagModel{CobraType: "Int64"}, "0"},
		{"Float64 no default", FlagModel{CobraType: "Float64"}, "0"},
		{"String with default", FlagModel{CobraType: "String", Default: "hello"}, "hello"},
		{"String no default", FlagModel{CobraType: "String"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, defaultValueForFlag(tt.flag))
		})
	}
}

func TestRender_FuncMap_UniqueSubcommands(t *testing.T) {
	tests := []struct {
		name     string
		commands []CommandModel
		expected []string
	}{
		{
			name: "distinct paths sorted",
			commands: []CommandModel{
				{CommandPath: []string{"tables"}},
				{CommandPath: []string{"schemas"}},
			},
			expected: []string{"schemas", "tables"},
		},
		{
			name: "duplicates deduplicated",
			commands: []CommandModel{
				{CommandPath: []string{"schemas"}},
				{CommandPath: []string{"schemas"}},
				{CommandPath: []string{"schemas"}},
			},
			expected: []string{"schemas"},
		},
		{
			name: "empty command paths excluded",
			commands: []CommandModel{
				{CommandPath: []string{}},
				{CommandPath: nil},
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := uniqueSubcommandNames(tt.commands)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRender_FuncMap_GroupCommands(t *testing.T) {
	t.Run("groups by first path element", func(t *testing.T) {
		commands := []CommandModel{
			{OperationID: "a", CommandPath: []string{"schemas"}},
			{OperationID: "b", CommandPath: []string{"schemas"}},
			{OperationID: "c", CommandPath: []string{"tables"}},
		}
		result := groupCommandsByPath(commands)
		assert.Len(t, result["schemas"], 2)
		assert.Len(t, result["tables"], 1)
	})

	t.Run("empty path uses empty key", func(t *testing.T) {
		commands := []CommandModel{
			{OperationID: "x", CommandPath: []string{}},
			{OperationID: "y", CommandPath: nil},
		}
		result := groupCommandsByPath(commands)
		assert.Len(t, result[""], 2)
	})
}
