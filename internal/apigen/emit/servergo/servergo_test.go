package servergo

import (
	"testing"

	"github.com/stretchr/testify/require"

	"duck-demo/internal/apigen/ir"
)

func TestEmit(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{Method: "get", Path: "/healthz", OperationID: "getHealth", Responses: []ir.Response{{StatusCode: 200, Description: "ok"}}},
		},
	}

	b, err := Emit(doc)
	require.NoError(t, err)
	content := string(b)
	require.Contains(t, content, "type GenServerInterface interface")
	require.Contains(t, content, "RegisterAPIGenRoutes")
	require.Contains(t, content, "HandleAPIGen")
	require.Contains(t, content, "type GenOperationDispatcher interface")
	require.Contains(t, content, "DispatchAPIGenOperation")
	require.NotContains(t, content, "*ServerInterfaceWrapper")
	require.Contains(t, content, "router.MethodFunc(\"GET\", \"/healthz\"")
	require.Contains(t, content, "func RegisterAPIGenRoutes(router chi.Router, server GenServerInterface)")
	require.Contains(t, content, "func DispatchAPIGenOperation(operationID string, dispatcher GenOperationDispatcher")
	require.Contains(t, content, "\"github.com/oapi-codegen/runtime\"")
	require.Contains(t, content, "type genStrictBridge struct")
	require.Contains(t, content, "type GenStrictServerInterface interface")
	require.Contains(t, content, "func DispatchAPIGenStrictOperation(operationID string, handler GenStrictServerInterface")
}

func TestEmit_UsesIRPathAsIs(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{Method: "post", Path: "/query", OperationID: "executeQuery", Responses: []ir.Response{{StatusCode: 200, Description: "ok"}}},
		},
	}

	b, err := Emit(doc)
	require.NoError(t, err)
	content := string(b)
	require.Contains(t, content, "router.MethodFunc(\"POST\", \"/query\"")
	require.NotContains(t, content, "router.MethodFunc(\"POST\", \"/v1/query\"")
}

func TestValidateOperationIDs(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{Method: "get", Path: "/a", OperationID: "create-user", Responses: []ir.Response{{StatusCode: 200, Description: "ok"}}},
			{Method: "post", Path: "/b", OperationID: "create_user", Responses: []ir.Response{{StatusCode: 200, Description: "ok"}}},
		},
	}

	err := ValidateOperationIDs(doc)
	require.Error(t, err)
}

func TestEmit_DispatchParityAndHealthHandling(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{Method: "get", Path: "/healthz", OperationID: "getHealth", Responses: []ir.Response{{StatusCode: 200, Description: "ok"}}},
			{Method: "post", Path: "/query", OperationID: "executeQuery", Responses: []ir.Response{{StatusCode: 200, Description: "ok"}}},
			{Method: "get", Path: "/groups", OperationID: "listGroups", Responses: []ir.Response{{StatusCode: 200, Description: "ok"}}},
		},
	}

	b, err := Emit(doc)
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "router.MethodFunc(\"GET\", \"/healthz\"")
	require.Contains(t, content, "server.HandleAPIGen(\"getHealth\", w, r)")
	require.Contains(t, content, "router.MethodFunc(\"POST\", \"/query\"")
	require.Contains(t, content, "server.HandleAPIGen(\"executeQuery\", w, r)")
	require.Contains(t, content, "router.MethodFunc(\"GET\", \"/groups\"")
	require.Contains(t, content, "server.HandleAPIGen(\"listGroups\", w, r)")

	require.Contains(t, content, "ExecuteQuery(w http.ResponseWriter, r *http.Request)")
	require.Contains(t, content, "ListGroups(w http.ResponseWriter, r *http.Request)")
	require.NotContains(t, content, "GetHealth(w http.ResponseWriter, r *http.Request)")

	require.Contains(t, content, "case \"executeQuery\":")
	require.Contains(t, content, "dispatcher.ExecuteQuery(w, r)")
	require.Contains(t, content, "case \"listGroups\":")
	require.Contains(t, content, "dispatcher.ListGroups(w, r)")
	require.Contains(t, content, "case \"getHealth\":")
	require.Contains(t, content, "w.Header().Set(\"Content-Type\", \"application/json\")")
	require.Contains(t, content, "_ = json.NewEncoder(w).Encode(map[string]string{\"status\": \"ok\"})")
	require.NotContains(t, content, "dispatcher.GetHealth(w, r)")
}

func TestEmit_GeneratesPathAndQueryBinding(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{
				Method:      "get",
				Path:        "/groups/{groupId}/members",
				OperationID: "listGroupMembers",
				Parameters: []ir.Parameter{
					{Name: "groupId", In: "path", Required: true, Schema: ir.SchemaRef{Type: "string"}},
					{Name: "max_results", In: "query", Required: false, Schema: ir.SchemaRef{Type: "integer", Format: "int32"}},
				},
				Responses: []ir.Response{{StatusCode: 200, Description: "ok"}},
			},
		},
	}

	b, err := Emit(doc)
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "ListGroupMembers(w http.ResponseWriter, r *http.Request, groupId string, params ListGroupMembersParams)")
	require.Contains(t, content, "runtime.BindStyledParameterWithOptions(\"simple\", \"groupId\", chi.URLParam(r, \"groupId\")")
	require.Contains(t, content, "runtime.BindQueryParameter(\"form\", true, false, \"max_results\", r.URL.Query(), &params.MaxResults)")
	require.Contains(t, content, "dispatcher.ListGroupMembers(w, r, groupId, params)")
	require.Contains(t, content, "var request GenListGroupMembersRequest")
	require.Contains(t, content, "response, err := b.handler.ListGroupMembers(r.Context(), request)")
	require.Contains(t, content, "if err := response.VisitListGroupMembersResponse(w); err != nil")
	require.Contains(t, content, "type GenListGroupMembersRequest struct {")
	require.Contains(t, content, "\tGroupId string")
	require.Contains(t, content, "\tParams ListGroupMembersParams")
	require.Contains(t, content, "type GenListGroupMembersResponse interface {")
	require.Contains(t, content, "\tVisitListGroupMembersResponse(w http.ResponseWriter) error")
	require.Contains(t, content, "type GenListGroupMembers200Response = ListGroupMembers200Response")
	require.Contains(t, content, "ListGroupMembers(ctx context.Context, request GenListGroupMembersRequest) (GenListGroupMembersResponse, error)")
}

func TestEmit_GeneratesConcreteResponseAliasesFromIR(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{
				Method:      "post",
				Path:        "/query",
				OperationID: "executeQuery",
				RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "#/schemas/QueryRequest"}},
				Responses: []ir.Response{
					{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/QueryResult"}},
					{StatusCode: 204, Description: "no content"},
				},
			},
		},
	}

	b, err := Emit(doc)
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenExecuteQueryRequest struct {")
	require.Contains(t, content, "\tBody *GenExecuteQueryJSONBody")
	require.Contains(t, content, "type GenExecuteQueryResponse interface {")
	require.Contains(t, content, "\tVisitExecuteQueryResponse(w http.ResponseWriter) error")
	require.Contains(t, content, "type GenExecuteQueryJSONBody = ExecuteQueryJSONRequestBody")
	require.Contains(t, content, "type GenExecuteQuery200JSONResponse = ExecuteQuery200JSONResponse")
	require.NotContains(t, content, "type GenExecuteQuery200Response = ExecuteQuery200Response")
	require.Contains(t, content, "type GenExecuteQuery204Response = ExecuteQuery204Response")
	require.NotContains(t, content, "type GenExecuteQuery204JSONResponse = ExecuteQuery204JSONResponse")
}

func TestPathParamTypeName(t *testing.T) {
	t.Helper()

	tests := []struct {
		name     string
		param    ir.Parameter
		expected string
	}{
		{
			name:     "default string",
			param:    ir.Parameter{Schema: ir.SchemaRef{Type: "string"}},
			expected: "string",
		},
		{
			name:     "int32",
			param:    ir.Parameter{Schema: ir.SchemaRef{Type: "integer", Format: "int32"}},
			expected: "int32",
		},
		{
			name:     "int64",
			param:    ir.Parameter{Schema: ir.SchemaRef{Type: "integer", Format: "int64"}},
			expected: "int64",
		},
		{
			name:     "integer default",
			param:    ir.Parameter{Schema: ir.SchemaRef{Type: "integer"}},
			expected: "int",
		},
		{
			name:     "float",
			param:    ir.Parameter{Schema: ir.SchemaRef{Type: "number", Format: "float"}},
			expected: "float32",
		},
		{
			name:     "double",
			param:    ir.Parameter{Schema: ir.SchemaRef{Type: "number", Format: "double"}},
			expected: "float64",
		},
		{
			name:     "number default",
			param:    ir.Parameter{Schema: ir.SchemaRef{Type: "number"}},
			expected: "float64",
		},
		{
			name:     "boolean",
			param:    ir.Parameter{Schema: ir.SchemaRef{Type: "boolean"}},
			expected: "bool",
		},
		{
			name:     "unknown type fallback",
			param:    ir.Parameter{Schema: ir.SchemaRef{Type: "object"}},
			expected: "string",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()
			require.Equal(t, tc.expected, pathParamTypeName(tc.param))
		})
	}
}
