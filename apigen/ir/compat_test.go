package ir_test

import (
	"path/filepath"
	"strings"
	"testing"

	cligoemit "duck-demo/apigen/emit/cligo"
	openapiemit "duck-demo/apigen/emit/openapi"
	requestmodelgoemit "duck-demo/apigen/emit/requestmodelgo"
	servergoemit "duck-demo/apigen/emit/servergo"
	"duck-demo/apigen/ir"
	"github.com/stretchr/testify/require"
)

func TestV1FixtureLoadsAndEmits(t *testing.T) {
	t.Helper()

	path := filepath.Join("testdata", "document_v1.json")
	doc, err := ir.Load(path)
	require.NoError(t, err)
	require.Equal(t, ir.CurrentSchemaVersion, doc.SchemaVersion)
	require.Len(t, doc.Endpoints, 3)

	openapiYAML, err := openapiemit.EmitYAML(doc, openapiemit.Options{})
	require.NoError(t, err)
	require.Contains(t, string(openapiYAML), "x-authz:")
	require.Contains(t, string(openapiYAML), "x-cli-command: widgets list")

	requestModels, err := requestmodelgoemit.EmitWithResponseRoots(doc, requestmodelgoemit.Options{})
	require.NoError(t, err)
	require.Contains(t, string(requestModels), "type GenSchemaCreateWidgetRequest = CreateWidgetRequest")

	serverCode, err := servergoemit.EmitWithLegacyResponses(doc, servergoemit.Options{})
	require.NoError(t, err)
	require.Contains(t, string(serverCode), `apigenchi "duck-demo/apigen/runtime/chi"`)
	require.Contains(t, string(serverCode), "func RegisterAPIGenRoutes(router apigenchi.Router, server GenServerInterface)")
	require.Contains(t, string(serverCode), "type GenCreateWidget201JSONResponse struct {")

	cliCode, err := cligoemit.Emit(doc, cligoemit.Options{})
	require.NoError(t, err)
	require.Contains(t, string(cliCode), `import apigencobra "duck-demo/apigen/runtime/cobra"`)
	require.Contains(t, string(cliCode), `CLICommand: "widgets list"`)
	require.Contains(t, string(cliCode), `CLICommand: "widgets create"`)
	require.False(t, strings.Contains(string(cliCode), "deleteWidget"))
}
