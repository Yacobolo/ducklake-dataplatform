package cli

import (
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
)

func TestInferGroup(t *testing.T) {
	tests := []struct {
		name         string
		tags         []string
		tagOverrides map[string]GroupOverride
		expected     string
	}{
		{
			name:     "no tags returns default",
			tags:     nil,
			expected: "default",
		},
		{
			name:     "empty tags returns default",
			tags:     []string{},
			expected: "default",
		},
		{
			name:     "lowercase tag",
			tags:     []string{"security"},
			expected: "security",
		},
		{
			name:     "capitalized tag lowercased",
			tags:     []string{"Security"},
			expected: "security",
		},
		{
			name:     "built-in tag mapping Catalogs to catalog",
			tags:     []string{"Catalogs"},
			expected: "catalog",
		},
		{
			name: "tag override takes precedence",
			tags: []string{"Catalogs"},
			tagOverrides: map[string]GroupOverride{
				"Catalogs": {Name: "my-catalog"},
			},
			expected: "my-catalog",
		},
		{
			name: "tag override with empty name falls through to built-in",
			tags: []string{"Catalogs"},
			tagOverrides: map[string]GroupOverride{
				"Catalogs": {Short: "description only"},
			},
			expected: "catalog",
		},
		{
			name:     "uses first tag only",
			tags:     []string{"Alpha", "Beta"},
			expected: "alpha",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferGroup(tt.tags, tt.tagOverrides)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGroupDescription(t *testing.T) {
	tests := []struct {
		name         string
		groupName    string
		tagOverrides map[string]GroupOverride
		expected     string
	}{
		{
			name:      "known group returns built-in description",
			groupName: "catalog",
			expected:  "Manage the data catalog",
		},
		{
			name:      "unknown group returns generic",
			groupName: "widgets",
			expected:  "Manage widgets",
		},
		{
			name:      "override with explicit Name match",
			groupName: "catalog",
			tagOverrides: map[string]GroupOverride{
				"Catalogs": {Name: "catalog", Short: "Custom catalog description"},
			},
			expected: "Custom catalog description",
		},
		{
			name:      "override matched by tag inference",
			groupName: "items",
			tagOverrides: map[string]GroupOverride{
				"Items": {Short: "Item management commands"},
			},
			expected: "Item management commands",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := groupDescription(tt.groupName, tt.tagOverrides)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInferVerb(t *testing.T) {
	tests := []struct {
		name        string
		operationID string
		httpMethod  string
		wantVerb    string
		wantOK      bool
	}{
		{"list prefix", "listSchemas", "GET", "list", true},
		{"create prefix", "createSchema", "POST", "create", true},
		{"get prefix", "getSchema", "GET", "get", true},
		{"update prefix", "updateSchema", "PATCH", "update", true},
		{"delete prefix", "deleteSchema", "DELETE", "delete", true},
		{"non-CRUD verb", "runTask", "POST", "", false},
		{"no resource suffix", "list", "GET", "", false},
		{"lowercase suffix not detected", "listschemas", "GET", "", false},
		{"execute prefix", "executeQuery", "POST", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verb, ok := inferVerb(tt.operationID, tt.httpMethod)
			assert.Equal(t, tt.wantVerb, verb)
			assert.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestInferCommandPath(t *testing.T) {
	tests := []struct {
		name     string
		urlPath  string
		expected []string
	}{
		{"simple resource", "/items", []string{"items"}},
		{"resource with param", "/items/{itemId}", []string{"items"}},
		{"nested resource", "/schemas/{schemaName}/tables", []string{"tables"}},
		{"deeply nested with param", "/schemas/{schemaName}/tables/{tableName}", []string{"tables"}},
		{"action endpoint", "/tasks/{taskId}/run", []string{"run"}},
		{"root path", "/", nil},
		{"only params", "/{catalogName}", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferCommandPath(tt.urlPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInferPositionalArgs(t *testing.T) {
	tests := []struct {
		name           string
		params         []*openapi3.ParameterRef
		implicitParams map[string]bool
		expected       []string
	}{
		{
			name:   "no params",
			params: nil,
		},
		{
			name: "single path param",
			params: []*openapi3.ParameterRef{
				makeParam("itemId", "path", "string", true),
			},
			expected: []string{"itemId"},
		},
		{
			name: "skips implicit params",
			params: []*openapi3.ParameterRef{
				makeParam("catalogName", "path", "string", true),
				makeParam("schemaName", "path", "string", true),
			},
			implicitParams: map[string]bool{"catalogName": true},
			expected:       []string{"schemaName"},
		},
		{
			name: "skips query params",
			params: []*openapi3.ParameterRef{
				makeParam("itemId", "path", "string", true),
				makeParam("max_results", "query", "integer", false),
			},
			expected: []string{"itemId"},
		},
		{
			name: "multiple path params",
			params: []*openapi3.ParameterRef{
				makeParam("schemaName", "path", "string", true),
				makeParam("tableName", "path", "string", true),
			},
			expected: []string{"schemaName", "tableName"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferPositionalArgs(tt.params, tt.implicitParams)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInferConfirm(t *testing.T) {
	tests := []struct {
		method   string
		expected bool
	}{
		{"DELETE", true},
		{"delete", true},
		{"GET", false},
		{"POST", false},
		{"PUT", false},
		{"PATCH", false},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			assert.Equal(t, tt.expected, inferConfirm(tt.method))
		})
	}
}

func TestInferGroupFromTag(t *testing.T) {
	tests := []struct {
		tag      string
		expected string
	}{
		{"Catalogs", "catalog"},
		{"Security", "security"},
		{"Items", "items"},
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			assert.Equal(t, tt.expected, inferGroupFromTag(tt.tag))
		})
	}
}

func TestPrioritizeColumns(t *testing.T) {
	tests := []struct {
		name     string
		cols     []string
		expected []string
	}{
		{
			name:     "id and name first",
			cols:     []string{"status", "name", "id"},
			expected: []string{"id", "name", "status"},
		},
		{
			name:     "timestamps last",
			cols:     []string{"created_at", "name", "type"},
			expected: []string{"name", "type", "created_at"},
		},
		{
			name:     "suffix _id fields ranked high",
			cols:     []string{"zz_field", "principal_id", "name"},
			expected: []string{"principal_id", "name", "zz_field"},
		},
		{
			name:     "empty input",
			cols:     nil,
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := prioritizeColumns(tt.cols)
			assert.Equal(t, tt.expected, result)
		})
	}
}
