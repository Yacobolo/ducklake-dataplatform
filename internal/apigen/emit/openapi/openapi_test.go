package openapi

import (
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/apigen/ir"
)

func TestEmitYAML(t *testing.T) {
	t.Helper()

	docIR := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "test", Version: "1.0.0"},
		Schemas: map[string]ir.Schema{
			"Item": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"id": {Schema: ir.SchemaRef{Type: "string"}},
				},
			},
		},
		Endpoints: []ir.Endpoint{
			{
				Method:      "get",
				Path:        "/items/{id}",
				OperationID: "getItem",
				Parameters: []ir.Parameter{
					{Name: "id", In: "path", Required: true, Schema: ir.SchemaRef{Type: "string"}},
				},
				Responses: []ir.Response{{
					StatusCode:  200,
					Description: "ok",
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

	b, err := EmitYAML(docIR)
	require.NoError(t, err)

	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(b)
	require.NoError(t, err)
	require.Equal(t, "3.2.0", doc.OpenAPI)
	require.Equal(t, "getItem", doc.Paths.Value("/items/{id}").Get.OperationID)
	headers := doc.Paths.Value("/items/{id}").Get.Responses.Value("200").Value.Headers
	require.Contains(t, headers, "X-RateLimit-Remaining")
	require.Equal(t, openapi3.Types{"integer"}, *headers["X-RateLimit-Remaining"].Value.Schema.Value.Type)
}
