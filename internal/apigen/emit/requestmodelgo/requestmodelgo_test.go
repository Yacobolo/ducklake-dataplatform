package requestmodelgo

import (
	"testing"

	"duck-demo/internal/apigen/ir"

	"github.com/stretchr/testify/require"
)

func TestEmit_AliasesRequestRoots(t *testing.T) {
	t.Helper()

	doc := ir.Document{Endpoints: []ir.Endpoint{{OperationID: "createWidget", RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "CreateWidgetRequest"}}}}}

	b, err := Emit(doc, "")
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenSchemaCreateWidgetRequest = CreateWidgetRequest")
}

func TestEmit_AliasesNonStructRequestRoots(t *testing.T) {
	t.Helper()

	doc := ir.Document{Endpoints: []ir.Endpoint{{OperationID: "setDefaultCatalog", RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "SetDefaultCatalogRequest"}}}}}

	b, err := Emit(doc, "")
	require.NoError(t, err)
	require.Contains(t, string(b), "type GenSchemaSetDefaultCatalogRequest = SetDefaultCatalogRequest")
}

func TestEmitWithResponseRoots_AliasesSafeDirectResponseSchemas(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		Schemas: map[string]ir.Schema{
			"SemanticModel": {},
		},
		Endpoints: []ir.Endpoint{
			{OperationID: "createSemanticModel", Responses: []ir.Response{{StatusCode: 201, Schema: &ir.SchemaRef{Ref: "SemanticModel"}}}},
			{OperationID: "createModel", Responses: []ir.Response{{StatusCode: 201, Schema: &ir.SchemaRef{Type: "string"}, Extensions: map[string]any{ir.ResponseShapeExtensionKey: map[string]any{"kind": "wrapped_json", "body_type": "Model"}}}}},
		},
	}

	b, err := EmitWithResponseRoots(doc, "")
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenSchemaSemanticModel = SemanticModel")
	require.Contains(t, content, "type GenSchemaModel = Model")
}

func TestEmitWithResponseRoots_EmitsAPIGenOwnedGenericResponse(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		Schemas: map[string]ir.Schema{
			"GenericResponse": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"data": {Schema: ir.SchemaRef{Ref: "Record"}},
				},
			},
		},
		Endpoints: []ir.Endpoint{
			{OperationID: "listWidgets", Responses: []ir.Response{{StatusCode: 200, Schema: &ir.SchemaRef{Ref: "GenericResponse"}}}},
		},
	}

	b, err := EmitWithResponseRoots(doc, "")
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenSchemaRecord map[string]any")
	require.Contains(t, content, "type GenSchemaGenericResponse struct")
	require.Contains(t, content, "Data *GenSchemaRecord `json:\"data,omitempty\"`")
}

func TestEmitWithResponseRoots_PreservesSchemaRootWhenResponseShapeMetadataExists(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		Schemas: map[string]ir.Schema{
			"GenericResponse": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"data": {Schema: ir.SchemaRef{Ref: "Record"}},
				},
			},
			"PaginatedTags": {},
		},
		Endpoints: []ir.Endpoint{
			{OperationID: "listTags", Responses: []ir.Response{{
				StatusCode: 200,
				Schema:     &ir.SchemaRef{Ref: "GenericResponse"},
				Extensions: map[string]any{ir.ResponseShapeExtensionKey: map[string]any{"kind": "wrapped_json", "body_type": "PaginatedTags"}},
			}}},
		},
	}

	b, err := EmitWithResponseRoots(doc, "")
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenSchemaPaginatedTags = PaginatedTags")
	require.Contains(t, content, "type GenSchemaGenericResponse struct")
}

func TestEmit_ApigenOwnedSchemaNames(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		Schemas: map[string]ir.Schema{
			"HealthResponse": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"status": {Schema: ir.SchemaRef{Type: "string"}},
				},
				Required: []string{"status"},
			},
			"QueryResponse": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"columns": {Schema: ir.SchemaRef{Type: "array"}},
				},
				Required: []string{"columns"},
			},
		},
		Endpoints: []ir.Endpoint{
			{OperationID: "getHealth", Responses: []ir.Response{{StatusCode: 200, Schema: &ir.SchemaRef{Ref: "HealthResponse"}}}},
			{OperationID: "executeQuery", Responses: []ir.Response{{StatusCode: 200, Schema: &ir.SchemaRef{Ref: "QueryResponse"}}}},
		},
	}

	b, err := EmitWithResponseRoots(doc, "")
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenSchemaHealthResponse struct")
	require.Contains(t, content, "Status string `json:\"status\"`")
	require.Contains(t, content, "type GenSchemaQueryResponse struct")
	require.Contains(t, content, "Columns []any `json:\"columns\"`")
}

func TestEmitStandaloneCompatibilityTypes_EmitsConcreteCanonicalTypes(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		Schemas: map[string]ir.Schema{
			"CreateWidgetRequest": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"name": {Schema: ir.SchemaRef{Type: "string"}},
				},
				Required: []string{"name"},
			},
			"PaginatedWidgets": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"data": {Schema: ir.SchemaRef{Ref: "WidgetList"}},
				},
			},
			"WidgetList": {
				Type:  "array",
				Items: &ir.SchemaRef{Ref: "Widget"},
			},
			"Widget": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"id": {Schema: ir.SchemaRef{Type: "string"}},
				},
				Required: []string{"id"},
			},
		},
		Endpoints: []ir.Endpoint{
			{OperationID: "createWidget", RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "CreateWidgetRequest"}}},
			{OperationID: "listWidgets", Responses: []ir.Response{{StatusCode: 200, Schema: &ir.SchemaRef{Ref: "PaginatedWidgets"}}}},
		},
	}

	b, err := EmitStandaloneCompatibilityTypes(doc, "")
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type CreateWidgetRequest struct")
	require.Contains(t, content, "Name string `json:\"name\"`")
	require.Contains(t, content, "type Widget struct")
	require.Contains(t, content, "type WidgetList []Widget")
	require.Contains(t, content, "type PaginatedWidgets struct")
	require.Contains(t, content, "type GenSchemaCreateWidgetRequest = CreateWidgetRequest")
	require.Contains(t, content, "type GenSchemaPaginatedWidgets = PaginatedWidgets")
}
