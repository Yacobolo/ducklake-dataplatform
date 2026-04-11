package cuegen

import (
	"path/filepath"
	"testing"

	openapiemit "duck-demo/pkg/apigen/emit/openapi"
	"duck-demo/pkg/apigen/ir"
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

	bundle, err := CompileDir(filepath.Join("..", "..", "..", "api", "cue"))
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
