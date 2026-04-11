package cuegen

import (
	"path/filepath"
	"testing"

	openapiemit "duck-demo/pkg/apigen/emit/openapi"
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
