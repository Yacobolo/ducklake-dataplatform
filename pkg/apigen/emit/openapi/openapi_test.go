package openapi

import (
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v4"

	"duck-demo/pkg/apigen/ir"
)

func TestEmitYAML(t *testing.T) {
	t.Helper()

	docIR := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "test", Version: "1.0.0"},
		Schemas: map[string]ir.Schema{
			"Item": {
				Type: "object",
				Example: map[string]any{
					"id": "item_123",
				},
				Properties: map[string]ir.SchemaProperty{
					"id": {Schema: ir.SchemaRef{Type: "string"}, Example: "item_123"},
				},
			},
			"Envelope": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"item": {Schema: ir.SchemaRef{Ref: "Item"}},
				},
			},
		},
		Endpoints: []ir.Endpoint{
			{
				Method:      "get",
				Path:        "/items/{id}",
				OperationID: "getItem",
				Parameters: []ir.Parameter{
					{Name: "id", In: "path", Required: true, Schema: ir.SchemaRef{Type: "string"}, Example: "item_123"},
				},
				Responses: []ir.Response{{
					StatusCode:  200,
					Description: "ok",
					Example: map[string]any{
						"id": "item_123",
					},
					Headers: []ir.Header{{
						Name:        "X-RateLimit-Remaining",
						Description: "Requests left in the current window.",
						Schema:      ir.SchemaRef{Type: "integer", Format: "int32"},
					}},
					Schema: &ir.SchemaRef{Ref: "Item"},
				}},
			},
		},
	}

	b, err := EmitYAML(docIR, Options{})
	require.NoError(t, err)

	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(b)
	require.NoError(t, err)
	require.Equal(t, "3.0.0", doc.OpenAPI)
	require.Equal(t, "getItem", doc.Paths.Value("/items/{id}").Get.OperationID)
	require.Equal(t, "item_123", doc.Paths.Value("/items/{id}").Get.Parameters[0].Value.Example)
	require.Equal(t, "item_123", doc.Components.Schemas["Item"].Value.Example.(map[string]any)["id"])
	headers := doc.Paths.Value("/items/{id}").Get.Responses.Value("200").Value.Headers
	require.Contains(t, headers, "X-RateLimit-Remaining")
	require.Equal(t, openapi3.Types{"integer"}, *headers["X-RateLimit-Remaining"].Value.Schema.Value.Type)
	require.Equal(t, "item_123", doc.Paths.Value("/items/{id}").Get.Responses.Value("200").Value.Content.Get("application/json").Example.(map[string]any)["id"])

	var root yaml.Node
	require.NoError(t, yaml.Unmarshal(b, &root))
	itemProperty := lookupYAMLMappingNode(&root, "components", "schemas", "Envelope", "properties", "item")
	require.NotNil(t, itemProperty)
	require.False(t, mappingNodeHasKey(itemProperty, "example"))
	require.True(t, strings.Contains(string(b), "example:"))
}

func lookupYAMLMappingNode(root *yaml.Node, path ...string) *yaml.Node {
	current := root
	if current.Kind == yaml.DocumentNode && len(current.Content) > 0 {
		current = current.Content[0]
	}
	for _, key := range path {
		if current == nil || current.Kind != yaml.MappingNode {
			return nil
		}
		var next *yaml.Node
		for i := 0; i < len(current.Content); i += 2 {
			if current.Content[i].Value == key {
				next = current.Content[i+1]
				break
			}
		}
		current = next
	}
	return current
}

func mappingNodeHasKey(node *yaml.Node, key string) bool {
	if node == nil || node.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return true
		}
	}
	return false
}
