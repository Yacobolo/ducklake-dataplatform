package cligo

import (
	"testing"

	"github.com/stretchr/testify/require"

	"duck-demo/pkg/apigen/ir"
)

func TestEmit(t *testing.T) {
	t.Helper()
	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Schemas: map[string]ir.Schema{
			"CreateQueryRequest": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"sql": {Description: "SQL text to execute", Schema: ir.SchemaRef{Type: "string"}},
				},
				Required: []string{"sql"},
			},
		},
		Endpoints: []ir.Endpoint{
			{
				Method:      "post",
				Path:        "/v1/query",
				OperationID: "executeQuery",
				Summary:     "Execute a query",
				Description: "Runs SQL against the default catalog",
				Tags:        []string{"query"},
				Parameters: []ir.Parameter{
					{Name: "catalogName", In: "path", Required: true, Description: "Catalog to query", Schema: ir.SchemaRef{Type: "string"}},
				},
				RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "CreateQueryRequest"}},
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok"}},
				Extensions: map[string]any{
					"x-cli-command": "query execute",
				},
			},
		},
	}

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	require.Contains(t, string(b), "APIGeneratedEndpoints")
	require.Contains(t, string(b), "executeQuery")
	require.Contains(t, string(b), "Summary: \"Execute a query\"")
	require.Contains(t, string(b), "Description: \"Runs SQL against the default catalog\"")
	require.Contains(t, string(b), "Parameters: []apigencobra.Param{{Name: \"catalogName\", In: \"path\", Type: \"string\", Description: \"Catalog to query\"")
	require.Contains(t, string(b), "BodyFields: []apigencobra.Field{{Name: \"sql\", Type: \"string\", Description: \"SQL text to execute\"")
	require.Contains(t, string(b), "CLICommand: \"query execute\"")
}
