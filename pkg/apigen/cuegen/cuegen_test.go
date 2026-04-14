package cuegen

import (
	"os"
	"path/filepath"
	"testing"

	openapiemit "github.com/Yacobolo/quackstack/pkg/apigen/emit/openapi"
	"github.com/Yacobolo/quackstack/pkg/apigen/ir"
	"github.com/stretchr/testify/require"
)

func TestCompileDir(t *testing.T) {
	t.Helper()

	bundle, err := CompileDir(filepath.Join("testdata", "minimal_api"))
	require.NoError(t, err)

	require.Equal(t, "v1", bundle.Document.SchemaVersion)
	require.Equal(t, "Widget API", bundle.Document.Info.Title)
	require.Len(t, bundle.Document.Endpoints, 1)
	require.Equal(t, "listWidgets", bundle.Document.Endpoints[0].OperationID)
	require.Contains(t, string(bundle.CanonicalOpenAPI), "x-cli-command: widgets list")
	require.Contains(t, string(bundle.CanonicalOpenAPI), "x-apigen-manual: true")
}

func TestBootstrapRoundTrip(t *testing.T) {
	t.Helper()

	original, err := CompileDir(filepath.Join("testdata", "minimal_api"))
	require.NoError(t, err)

	outDir := t.TempDir()
	require.NoError(t, Bootstrap(original.Document, outDir))

	roundTrip, err := CompileDir(outDir)
	require.NoError(t, err)
	require.Equal(t, original.Document, roundTrip.Document)
	expectedCanonicalOpenAPI, err := openapiemit.EmitYAML(original.Document, openapiemit.Options{})
	require.NoError(t, err)
	require.Equal(t, string(expectedCanonicalOpenAPI), string(roundTrip.CanonicalOpenAPI))
}

func TestCompileDir_LineageCompactAuthoringParity(t *testing.T) {
	t.Helper()

	bundle, err := CompileDir(filepath.Join("..", "..", "..", "internal", "api", "contract", "cue"))
	require.NoError(t, err)

	getTableLineage := requireEndpoint(t, bundle.Document, "getTableLineage")
	require.Equal(t, []string{"schema_name", "table_name", "max_results", "page_token"}, parameterNames(getTableLineage.Parameters))
	require.NotNil(t, getTableLineage.Parameters[2].Explode)
	require.False(t, *getTableLineage.Parameters[2].Explode)
	require.NotNil(t, getTableLineage.Parameters[3].Explode)
	require.False(t, *getTableLineage.Parameters[3].Explode)

	purgeLineage := requireEndpoint(t, bundle.Document, "purgeLineage")
	require.NotNil(t, purgeLineage.RequestBody)
	require.Equal(t, "PurgeLineageRequest", purgeLineage.RequestBody.Schema.Ref)

	lineageNode := bundle.Document.Schemas["LineageNode"]
	require.Equal(t, []string{"table_name", "upstream", "downstream"}, lineageNode.PropertyOrder)

	paginatedLineageEdges := bundle.Document.Schemas["PaginatedLineageEdges"]
	require.Equal(t, []string{"data", "next_page_token"}, paginatedLineageEdges.PropertyOrder)

	apiKeyInfo := bundle.Document.Schemas["APIKeyInfo"]
	require.Equal(t, []string{"id", "principal_id", "name", "key_prefix", "expires_at", "created_at"}, apiKeyInfo.PropertyOrder)
	require.Equal(t, "date-time", apiKeyInfo.Properties["created_at"].Schema.Format)
	require.Equal(t, "date-time", apiKeyInfo.Properties["expires_at"].Schema.Format)
}

func TestCompileDir_PreservesExamples(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "api.cue"), []byte(`package api

schema_version: "v1"

info: {
	title:   "Example API"
	version: "1.0.0"
}

schemas: {
	Thing: {
		type: "object"
		example: {
			id:   "thing_123"
			name: "Primary thing"
		}
		properties: {
			id: {
				example: "thing_123"
				schema: {
					type: "string"
				}
			}
			name: {
				schema: {
					type: "string"
				}
			}
		}
		required: ["id", "name"]
	}
}

endpoints: [
	{
		method:       "post"
		path:         "/things"
		operation_id: "createThing"
		parameters: [
			{
				name:    "dry_run"
				in:      "query"
				example: true
				schema: {
					type: "boolean"
				}
			},
		]
		request_body: {
			example: {
				name: "Primary thing"
			}
			schema: {
				ref: "Thing"
			}
		}
		responses: [
			{
				status_code: 201
				description: "created"
				example: {
					id:   "thing_123"
					name: "Primary thing"
				}
				schema: {
					ref: "Thing"
				}
			},
		]
	},
]`), 0o600))

	bundle, err := CompileDir(dir)
	require.NoError(t, err)

	thing := bundle.Document.Schemas["Thing"]
	require.Equal(t, map[string]any{"id": "thing_123", "name": "Primary thing"}, thing.Example)
	require.Equal(t, "thing_123", thing.Properties["id"].Example)

	endpoint := requireEndpoint(t, bundle.Document, "createThing")
	require.Len(t, endpoint.Parameters, 1)
	require.Equal(t, true, endpoint.Parameters[0].Example)
	require.NotNil(t, endpoint.RequestBody)
	require.Equal(t, map[string]any{"name": "Primary thing"}, endpoint.RequestBody.Example)
	require.Len(t, endpoint.Responses, 1)
	require.Equal(t, map[string]any{"id": "thing_123", "name": "Primary thing"}, endpoint.Responses[0].Example)
}

func requireEndpoint(t *testing.T, doc ir.Document, operationID string) ir.Endpoint {
	t.Helper()

	for _, endpoint := range doc.Endpoints {
		if endpoint.OperationID == operationID {
			return endpoint
		}
	}
	t.Fatalf("endpoint %q not found", operationID)
	return ir.Endpoint{}
}

func parameterNames(parameters []ir.Parameter) []string {
	names := make([]string, len(parameters))
	for i, parameter := range parameters {
		names[i] = parameter.Name
	}
	return names
}
