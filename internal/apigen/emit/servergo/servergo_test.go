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

func TestEmit_GeneratesNativeConcreteResponsesFromIR(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{
				Method:      "post",
				Path:        "/query",
				OperationID: "executeQuery",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/QueryResult"}}},
			},
			{
				Method:      "post",
				Path:        "/queries",
				OperationID: "submitQuery",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/SubmitQueryResponse"}}},
			},
			{
				Method:      "get",
				Path:        "/queries/{queryId}",
				OperationID: "getQuery",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/QueryJob"}}},
			},
			{
				Method:      "get",
				Path:        "/queries/{queryId}/results",
				OperationID: "getQueryResults",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/QueryResult"}}},
			},
			{
				Method:      "post",
				Path:        "/queries/{queryId}/cancel",
				OperationID: "cancelQuery",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/CancelQueryResponse"}}},
			},
			{
				Method:      "delete",
				Path:        "/queries/{queryId}",
				OperationID: "deleteQuery",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "get",
				Path:        "/groups",
				OperationID: "listGroups",
				Responses: []ir.Response{
					{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedGroups"}},
					{StatusCode: 403, Description: "forbidden", Schema: &ir.SchemaRef{Ref: "#/schemas/Error"}},
				},
			},
			{
				Method:      "post",
				Path:        "/groups",
				OperationID: "createGroup",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/Group"}}},
			},
			{
				Method:      "get",
				Path:        "/groups/{groupId}",
				OperationID: "getGroup",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/Group"}}},
			},
			{
				Method:      "delete",
				Path:        "/groups/{groupId}",
				OperationID: "deleteGroup",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "get",
				Path:        "/principals",
				OperationID: "listPrincipals",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedPrincipals"}}},
			},
			{
				Method:      "post",
				Path:        "/principals",
				OperationID: "createPrincipal",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/Principal"}}},
			},
			{
				Method:      "get",
				Path:        "/principals/{principalId}",
				OperationID: "getPrincipal",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/Principal"}}},
			},
			{
				Method:      "delete",
				Path:        "/principals/{principalId}",
				OperationID: "deletePrincipal",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "get",
				Path:        "/api-keys",
				OperationID: "listAPIKeys",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedAPIKeys"}}},
			},
			{
				Method:      "post",
				Path:        "/api-keys",
				OperationID: "createAPIKey",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/CreateAPIKeyResponse"}}},
			},
			{
				Method:      "post",
				Path:        "/api-keys/cleanup",
				OperationID: "cleanupExpiredAPIKeys",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/CleanupAPIKeysResponse"}}},
			},
			{
				Method:      "delete",
				Path:        "/api-keys/{apiKeyId}",
				OperationID: "deleteAPIKey",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "get",
				Path:        "/tables",
				OperationID: "listTables",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedTables"}}},
			},
		},
	}

	b, err := Emit(doc)
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenExecuteQueryRequest struct {")
	require.Contains(t, content, "type GenExecuteQueryResponse interface {")
	require.Contains(t, content, "\tVisitExecuteQueryResponse(w http.ResponseWriter) error")
	require.Contains(t, content, "type GenExecuteQuery201JSONResponse ExecuteQuery201JSONResponse")
	require.Contains(t, content, "func (response GenExecuteQuery201JSONResponse) VisitExecuteQueryResponse(w http.ResponseWriter) error {")
	require.Contains(t, content, "return ExecuteQuery201JSONResponse(response).VisitExecuteQueryResponse(w)")
	require.NotContains(t, content, "type GenExecuteQuery201JSONResponse = ExecuteQuery201JSONResponse")

	require.Contains(t, content, "type GenSubmitQuery201JSONResponse SubmitQuery201JSONResponse")
	require.Contains(t, content, "return SubmitQuery201JSONResponse(response).VisitSubmitQueryResponse(w)")
	require.Contains(t, content, "type GenGetQuery200JSONResponse GetQuery200JSONResponse")
	require.Contains(t, content, "return GetQuery200JSONResponse(response).VisitGetQueryResponse(w)")
	require.Contains(t, content, "type GenGetQueryResults200JSONResponse GetQueryResults200JSONResponse")
	require.Contains(t, content, "return GetQueryResults200JSONResponse(response).VisitGetQueryResultsResponse(w)")
	require.Contains(t, content, "type GenCancelQuery201JSONResponse CancelQuery201JSONResponse")
	require.Contains(t, content, "return CancelQuery201JSONResponse(response).VisitCancelQueryResponse(w)")
	require.Contains(t, content, "type GenDeleteQuery204Response DeleteQuery204Response")
	require.Contains(t, content, "return DeleteQuery204Response(response).VisitDeleteQueryResponse(w)")
	require.NotContains(t, content, "type GenDeleteQuery204Response = DeleteQuery204Response")

	require.Contains(t, content, "type GenListGroups200JSONResponse ListGroups200JSONResponse")
	require.Contains(t, content, "return ListGroups200JSONResponse(response).VisitListGroupsResponse(w)")
	require.Contains(t, content, "type GenListGroups403JSONResponse ListGroups403JSONResponse")
	require.Contains(t, content, "return ListGroups403JSONResponse(response).VisitListGroupsResponse(w)")
	require.Contains(t, content, "type GenCreateGroup201JSONResponse CreateGroup201JSONResponse")
	require.Contains(t, content, "return CreateGroup201JSONResponse(response).VisitCreateGroupResponse(w)")
	require.Contains(t, content, "type GenGetGroup200JSONResponse GetGroup200JSONResponse")
	require.Contains(t, content, "return GetGroup200JSONResponse(response).VisitGetGroupResponse(w)")
	require.Contains(t, content, "type GenDeleteGroup204Response DeleteGroup204Response")
	require.Contains(t, content, "return DeleteGroup204Response(response).VisitDeleteGroupResponse(w)")
	require.Contains(t, content, "type GenListPrincipals200JSONResponse ListPrincipals200JSONResponse")
	require.Contains(t, content, "return ListPrincipals200JSONResponse(response).VisitListPrincipalsResponse(w)")
	require.Contains(t, content, "type GenCreatePrincipal201JSONResponse CreatePrincipal201JSONResponse")
	require.Contains(t, content, "return CreatePrincipal201JSONResponse(response).VisitCreatePrincipalResponse(w)")
	require.Contains(t, content, "type GenGetPrincipal200JSONResponse GetPrincipal200JSONResponse")
	require.Contains(t, content, "return GetPrincipal200JSONResponse(response).VisitGetPrincipalResponse(w)")
	require.Contains(t, content, "type GenDeletePrincipal204Response DeletePrincipal204Response")
	require.Contains(t, content, "return DeletePrincipal204Response(response).VisitDeletePrincipalResponse(w)")
	require.Contains(t, content, "type GenListAPIKeys200JSONResponse ListAPIKeys200JSONResponse")
	require.Contains(t, content, "return ListAPIKeys200JSONResponse(response).VisitListAPIKeysResponse(w)")
	require.Contains(t, content, "type GenCreateAPIKey201JSONResponse CreateAPIKey201JSONResponse")
	require.Contains(t, content, "return CreateAPIKey201JSONResponse(response).VisitCreateAPIKeyResponse(w)")
	require.Contains(t, content, "type GenCleanupExpiredAPIKeys200JSONResponse CleanupExpiredAPIKeys200JSONResponse")
	require.Contains(t, content, "return CleanupExpiredAPIKeys200JSONResponse(response).VisitCleanupExpiredAPIKeysResponse(w)")
	require.Contains(t, content, "type GenDeleteAPIKey204Response DeleteAPIKey204Response")
	require.Contains(t, content, "return DeleteAPIKey204Response(response).VisitDeleteAPIKeyResponse(w)")

	require.NotContains(t, content, "type GenListGroups200JSONResponse = ListGroups200JSONResponse")
	require.NotContains(t, content, "type GenCreateGroup201JSONResponse = CreateGroup201JSONResponse")
	require.NotContains(t, content, "type GenGetGroup200JSONResponse = GetGroup200JSONResponse")
	require.NotContains(t, content, "type GenDeleteGroup204Response = DeleteGroup204Response")
	require.NotContains(t, content, "type GenListPrincipals200JSONResponse = ListPrincipals200JSONResponse")
	require.NotContains(t, content, "type GenCreatePrincipal201JSONResponse = CreatePrincipal201JSONResponse")
	require.NotContains(t, content, "type GenGetPrincipal200JSONResponse = GetPrincipal200JSONResponse")
	require.NotContains(t, content, "type GenDeletePrincipal204Response = DeletePrincipal204Response")
	require.NotContains(t, content, "type GenListAPIKeys200JSONResponse = ListAPIKeys200JSONResponse")
	require.NotContains(t, content, "type GenCreateAPIKey201JSONResponse = CreateAPIKey201JSONResponse")
	require.NotContains(t, content, "type GenCleanupExpiredAPIKeys200JSONResponse = CleanupExpiredAPIKeys200JSONResponse")
	require.NotContains(t, content, "type GenDeleteAPIKey204Response = DeleteAPIKey204Response")

	require.Contains(t, content, "type GenListTables200JSONResponse = ListTables200JSONResponse")
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
