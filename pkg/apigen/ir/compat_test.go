package ir_test

import (
	"path/filepath"
	"testing"

	cligoemit "github.com/Yacobolo/quackstack/pkg/apigen/emit/cligo"
	openapiemit "github.com/Yacobolo/quackstack/pkg/apigen/emit/openapi"
	requestmodelgoemit "github.com/Yacobolo/quackstack/pkg/apigen/emit/requestmodelgo"
	servergoemit "github.com/Yacobolo/quackstack/pkg/apigen/emit/servergo"
	"github.com/Yacobolo/quackstack/pkg/apigen/ir"
	"github.com/stretchr/testify/require"
)

func TestV1FixtureLoadsAndEmits(t *testing.T) {
	t.Helper()

	path := filepath.Join("testdata", "document_v1.json")
	doc, err := ir.Load(path)
	require.NoError(t, err)
	require.Equal(t, ir.CurrentSchemaVersion, doc.SchemaVersion)
	require.Equal(t, "/v1", doc.API.BasePath)
	require.Len(t, doc.Endpoints, 3)

	openapiYAML, err := openapiemit.EmitYAML(doc, openapiemit.Options{})
	require.NoError(t, err)
	require.Contains(t, string(openapiYAML), "x-authz:")
	require.NotContains(t, string(openapiYAML), "x-cli-command:")

	requestModels, err := requestmodelgoemit.EmitWithResponseRoots(doc, requestmodelgoemit.Options{})
	require.NoError(t, err)
	require.Contains(t, string(requestModels), "type GenSchemaCreateWidgetRequest = CreateWidgetRequest")

	serverCode, err := servergoemit.EmitWithLegacyResponses(doc, servergoemit.Options{})
	require.NoError(t, err)
	require.Contains(t, string(serverCode), `apigenchi "github.com/Yacobolo/quackstack/pkg/apigen/runtime/chi"`)
	require.Contains(t, string(serverCode), "func RegisterAPIGenRoutes(router apigenchi.Router, server GenServerInterface)")
	require.Contains(t, string(serverCode), "type GenCreateWidget201JSONResponse struct {")

	cliCode, err := cligoemit.Emit(doc, cligoemit.Options{})
	require.NoError(t, err)
	require.Contains(t, string(cliCode), `import apigencobra "github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra"`)
	require.Contains(t, string(cliCode), `Path: "/v1/widgets"`)
	require.Contains(t, string(cliCode), `Command: []string{"widgets", "list"}`)
	require.Contains(t, string(cliCode), `Command: []string{"widgets", "create"}`)
	require.NotContains(t, string(cliCode), "deleteWidget")
}
