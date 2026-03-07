package requestmodelgo

import (
	"os"
	"path/filepath"
	"testing"

	"duck-demo/internal/apigen/ir"

	"github.com/stretchr/testify/require"
)

func TestEmit_ClonesRequestRootsAndNestedStructs(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "types.gen.go")
	require.NoError(t, os.WriteFile(legacyPath, []byte("package api\n\nimport \"time\"\n\ntype CreateWidgetRequest struct {\n\tName string `json:\"name\"`\n\tConfig *WidgetConfig `json:\"config,omitempty\"`\n\tMode *WidgetMode `json:\"mode,omitempty\"`\n\tCreatedAt *time.Time `json:\"created_at,omitempty\"`\n}\n\ntype WidgetConfig struct {\n\tEnabled *bool `json:\"enabled,omitempty\"`\n}\n\ntype WidgetMode string\n"), 0o600))

	doc := ir.Document{Endpoints: []ir.Endpoint{{OperationID: "createWidget", RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "CreateWidgetRequest"}}}}}

	b, err := Emit(doc, legacyPath)
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "import \"time\"")
	require.Contains(t, content, "type GenSchemaCreateWidgetRequest struct {")
	require.Contains(t, content, "Config *GenSchemaWidgetConfig `json:\"config,omitempty\"`")
	require.Contains(t, content, "Mode *WidgetMode `json:\"mode,omitempty\"`")
	require.Contains(t, content, "CreatedAt *time.Time `json:\"created_at,omitempty\"`")
	require.Contains(t, content, "type GenSchemaWidgetConfig struct {")
}

func TestEmit_AliasesNonStructRequestRoots(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "types.gen.go")
	require.NoError(t, os.WriteFile(legacyPath, []byte("package api\n\ntype SetDefaultCatalogRequest = map[string]interface{}\n"), 0o600))

	doc := ir.Document{Endpoints: []ir.Endpoint{{OperationID: "setDefaultCatalog", RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "SetDefaultCatalogRequest"}}}}}

	b, err := Emit(doc, legacyPath)
	require.NoError(t, err)
	require.Contains(t, string(b), "type GenSchemaSetDefaultCatalogRequest = SetDefaultCatalogRequest")
}

func TestEmitWithResponseRoots_ClonesSafeDirectResponseSchemas(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	legacyTypesPath := filepath.Join(dir, "types.gen.go")

	require.NoError(t, os.WriteFile(legacyTypesPath, []byte("package api\n\ntype SemanticModel struct {\n\tName string `json:\"name\"`\n\tConfig *SemanticConfig `json:\"config,omitempty\"`\n}\n\ntype SemanticConfig struct {\n\tEnabled *bool `json:\"enabled,omitempty\"`\n}\n\ntype Model struct {\n\tID string `json:\"id\"`\n}\n"), 0o600))

	doc := ir.Document{
		Schemas: map[string]ir.Schema{
			"SemanticModel": {},
		},
		Endpoints: []ir.Endpoint{
			{OperationID: "createSemanticModel", Responses: []ir.Response{{StatusCode: 201, Schema: &ir.SchemaRef{Ref: "SemanticModel"}}}},
			{OperationID: "createModel", Responses: []ir.Response{{StatusCode: 201, Schema: &ir.SchemaRef{Type: "string"}, Extensions: map[string]any{ir.ResponseShapeExtensionKey: map[string]any{"kind": "wrapped_json", "body_type": "Model"}}}}},
		},
	}

	b, err := EmitWithResponseRoots(doc, legacyTypesPath)
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenSchemaSemanticModel struct {")
	require.Contains(t, content, "Config *GenSchemaSemanticConfig `json:\"config,omitempty\"`")
	require.Contains(t, content, "type GenSchemaModel struct {")
}
