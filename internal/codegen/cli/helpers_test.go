package cli

import (
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
)

// makeSchema creates an openapi3.Schema with the given type name.
// Pass "" for a nil-type schema.
func makeSchema(typeName string) *openapi3.Schema {
	if typeName == "" {
		return &openapi3.Schema{}
	}
	types := openapi3.Types{typeName}
	return &openapi3.Schema{Type: &types}
}

// makeSchemaWithAP creates an object schema with additionalProperties.
func makeSchemaWithAP() *openapi3.Schema {
	s := makeSchema("object")
	valTypes := openapi3.Types{"string"}
	s.AdditionalProperties = openapi3.AdditionalProperties{
		Schema: &openapi3.SchemaRef{
			Value: &openapi3.Schema{Type: &valTypes},
		},
	}
	return s
}

func TestSchemaType(t *testing.T) {
	tests := []struct {
		name     string
		schema   *openapi3.Schema
		expected string
	}{
		{"nil type", &openapi3.Schema{Type: nil}, "string"},
		{"empty type slice", makeSchema(""), "string"},
		{"string", makeSchema("string"), "string"},
		{"integer", makeSchema("integer"), "integer"},
		{"boolean", makeSchema("boolean"), "boolean"},
		{"number", makeSchema("number"), "number"},
		{"array", makeSchema("array"), "array"},
		{"object", makeSchema("object"), "object"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, schemaType(tt.schema))
		})
	}
}

func TestSchemaGoType(t *testing.T) {
	tests := []struct {
		name     string
		schema   *openapi3.Schema
		expected string
	}{
		{"nil type", &openapi3.Schema{Type: nil}, "string"},
		{"empty type slice", makeSchema(""), "string"},
		{"string", makeSchema("string"), "string"},
		{"integer", makeSchema("integer"), "int64"},
		{"boolean", makeSchema("boolean"), "bool"},
		{"number", makeSchema("number"), "float64"},
		{"array", makeSchema("array"), "[]string"},
		{"object with additionalProperties", makeSchemaWithAP(), "map[string]string"},
		{"object without additionalProperties", makeSchema("object"), "string"},
		{"unknown type", makeSchema("custom"), "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, schemaGoType(tt.schema))
		})
	}
}

func TestIsArrayType(t *testing.T) {
	tests := []struct {
		name     string
		schema   *openapi3.Schema
		expected bool
	}{
		{"nil type", &openapi3.Schema{Type: nil}, false},
		{"empty type slice", makeSchema(""), false},
		{"array", makeSchema("array"), true},
		{"string", makeSchema("string"), false},
		{"object", makeSchema("object"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isArrayType(tt.schema))
		})
	}
}

func TestIsMapType(t *testing.T) {
	tests := []struct {
		name     string
		schema   *openapi3.Schema
		expected bool
	}{
		{"nil type", &openapi3.Schema{Type: nil}, false},
		{"empty type slice", makeSchema(""), false},
		{"object with additionalProperties", makeSchemaWithAP(), true},
		{"object without additionalProperties", makeSchema("object"), false},
		{"array", makeSchema("array"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isMapType(tt.schema))
		})
	}
}

func TestGoTypeToCobraType(t *testing.T) {
	tests := []struct {
		goType   string
		expected string
	}{
		{"int64", "Int64"},
		{"bool", "Bool"},
		{"float64", "Float64"},
		{"[]string", "StringSlice"},
		{"map[string]string", "StringToString"},
		{"string", "String"},
		{"complex128", "String"},
	}

	for _, tt := range tests {
		t.Run(tt.goType, func(t *testing.T) {
			assert.Equal(t, tt.expected, goTypeToCobraType(tt.goType))
		})
	}
}

func TestRefToTypeName(t *testing.T) {
	tests := []struct {
		name     string
		ref      string
		expected string
	}{
		{"full ref", "#/components/schemas/SchemaDetail", "SchemaDetail"},
		{"short ref", "SchemaDetail", "SchemaDetail"},
		{"nested ref", "#/a/b/c/Foo", "Foo"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, refToTypeName(tt.ref))
		})
	}
}
