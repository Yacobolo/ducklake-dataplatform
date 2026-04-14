package requestmodelgo

import (
	"testing"

	"duck-demo/pkg/apigen/ir"

	"github.com/stretchr/testify/require"
)

func TestEmit_AliasesRequestRoots(t *testing.T) {
	t.Helper()

	doc := ir.Document{Endpoints: []ir.Endpoint{{OperationID: "createWidget", RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "CreateWidgetRequest"}}}}}

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenSchemaCreateWidgetRequest = CreateWidgetRequest")
}

func TestEmit_AliasesNonStructRequestRoots(t *testing.T) {
	t.Helper()

	doc := ir.Document{Endpoints: []ir.Endpoint{{OperationID: "setDefaultCatalog", RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "SetDefaultCatalogRequest"}}}}}

	b, err := Emit(doc, Options{})
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

	b, err := EmitWithResponseRoots(doc, Options{})
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

	b, err := EmitWithResponseRoots(doc, Options{})
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

	b, err := EmitWithResponseRoots(doc, Options{})
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

	b, err := EmitWithResponseRoots(doc, Options{})
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
			"WidgetState": {
				Type: "string",
				Enum: []string{"draft", "published"},
			},
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
			"AuditWidget": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"state": {Schema: ir.SchemaRef{Ref: "WidgetState"}},
				},
			},
		},
		Endpoints: []ir.Endpoint{
			{
				OperationID: "createWidget",
				RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "CreateWidgetRequest"}},
			},
			{
				OperationID: "listWidgets",
				Parameters: []ir.Parameter{
					{Name: "max_results", In: "query", Schema: ir.SchemaRef{Type: "integer", Format: "int32"}},
				},
				Responses: []ir.Response{{StatusCode: 200, Schema: &ir.SchemaRef{Ref: "PaginatedWidgets"}}},
			},
		},
	}

	b, err := EmitStandaloneCompatibilityTypes(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type CreateWidgetRequest struct")
	require.Contains(t, content, "Name string `json:\"name\"`")
	require.Contains(t, content, "type Widget struct")
	require.Contains(t, content, "type WidgetList []Widget")
	require.Contains(t, content, "type PaginatedWidgets struct")
	require.Contains(t, content, "type WidgetState string")
	require.Contains(t, content, "const (")
	require.Contains(t, content, "WidgetStateDraft WidgetState = \"draft\"")
	require.Contains(t, content, "WidgetStatePublished WidgetState = \"published\"")
	require.Contains(t, content, "type AuditWidget struct")
	require.Contains(t, content, "type ListWidgetsParams = GenListWidgetsParams")
	require.Contains(t, content, "type CreateWidgetJSONRequestBody = GenCreateWidgetJSONBody")
}

func TestEmitStandaloneCompatibilityTypes_EmitsLegacyBareEnumConstantsWhenUnique(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		Schemas: map[string]ir.Schema{
			"ComputeEndpointType": {
				Type: "string",
				Enum: []string{"LOCAL", "REMOTE"},
			},
			"PipelineRunStatus": {
				Type: "string",
				Enum: []string{"RUNNING", "FAILED"},
			},
			"NotebookSessionState": {
				Type: "string",
				Enum: []string{"RUNNING", "IDLE"},
			},
		},
	}

	b, err := EmitStandaloneCompatibilityTypes(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "ComputeEndpointTypeLOCAL ComputeEndpointType = \"LOCAL\"")
	require.Contains(t, content, "LOCAL ComputeEndpointType = \"LOCAL\"")
	require.Contains(t, content, "ComputeEndpointTypeREMOTE ComputeEndpointType = \"REMOTE\"")
	require.Contains(t, content, "REMOTE ComputeEndpointType = \"REMOTE\"")
	require.Contains(t, content, "PipelineRunStatusRUNNING PipelineRunStatus = \"RUNNING\"")
	require.Contains(t, content, "NotebookSessionStateRUNNING NotebookSessionState = \"RUNNING\"")
	require.NotContains(t, content, "\n\tRUNNING PipelineRunStatus = \"RUNNING\"\n")
}

func TestEmitStandaloneCompatibilityTypes_EmitsTypePrefixedEnumConstantsForMixedCaseValues(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		Schemas: map[string]ir.Schema{
			"ComputeAssignmentPrincipalType": {
				Type: "string",
				Enum: []string{"user", "group"},
			},
		},
	}

	b, err := EmitStandaloneCompatibilityTypes(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "ComputeAssignmentPrincipalTypeUser ComputeAssignmentPrincipalType = \"user\"")
	require.Contains(t, content, "ComputeAssignmentPrincipalTypeGroup ComputeAssignmentPrincipalType = \"group\"")
	require.NotContains(t, content, "\n\tUser ComputeAssignmentPrincipalType = \"user\"\n")
}

func TestEmitStandaloneCompatibilityTypes_EmitsManualRequestBodyAliasesWhenSchemasExist(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		Schemas: map[string]ir.Schema{
			"LocalLoginRequest": {
				Type: "object",
				Properties: map[string]ir.SchemaProperty{
					"email": {Schema: ir.SchemaRef{Type: "string"}},
				},
			},
		},
	}

	b, err := EmitStandaloneCompatibilityTypes(doc, Options{})
	require.NoError(t, err)
	require.Contains(t, string(b), "type LocalLoginJSONRequestBody = LocalLoginRequest")
}

func TestEmitStandaloneCompatibilityTypes_EmitsLegacyGenericPlaceholders(t *testing.T) {
	t.Helper()

	b, err := EmitStandaloneCompatibilityTypes(ir.Document{}, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenericRequest struct")
	require.Contains(t, content, "Payload *map[string]string `json:\"payload,omitempty\"`")
	require.Contains(t, content, "type GenericResponse struct")
	require.Contains(t, content, "Data *map[string]string `json:\"data,omitempty\"`")
}
