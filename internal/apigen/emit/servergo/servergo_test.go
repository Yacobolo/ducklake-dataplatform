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
				Path:        "/storage/credentials",
				OperationID: "listStorageCredentials",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedStorageCredentials"}}},
			},
			{
				Method:      "post",
				Path:        "/storage/credentials",
				OperationID: "createStorageCredential",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/StorageCredential"}}},
			},
			{
				Method:      "get",
				Path:        "/storage/credentials/{credentialName}",
				OperationID: "getStorageCredential",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/StorageCredential"}}},
			},
			{
				Method:      "patch",
				Path:        "/storage/credentials/{credentialName}",
				OperationID: "updateStorageCredential",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/StorageCredential"}}},
			},
			{
				Method:      "delete",
				Path:        "/storage/credentials/{credentialName}",
				OperationID: "deleteStorageCredential",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "get",
				Path:        "/external-locations",
				OperationID: "listExternalLocations",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedExternalLocations"}}},
			},
			{
				Method:      "post",
				Path:        "/external-locations",
				OperationID: "createExternalLocation",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/ExternalLocation"}}},
			},
			{
				Method:      "get",
				Path:        "/external-locations/{locationName}",
				OperationID: "getExternalLocation",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/ExternalLocation"}}},
			},
			{
				Method:      "patch",
				Path:        "/external-locations/{locationName}",
				OperationID: "updateExternalLocation",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/ExternalLocation"}}},
			},
			{
				Method:      "delete",
				Path:        "/external-locations/{locationName}",
				OperationID: "deleteExternalLocation",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "get",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/volumes",
				OperationID: "listVolumes",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedVolumes"}}},
			},
			{
				Method:      "post",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/volumes",
				OperationID: "createVolume",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/VolumeDetail"}}},
			},
			{
				Method:      "get",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}",
				OperationID: "getVolume",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/VolumeDetail"}}},
			},
			{
				Method:      "patch",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}",
				OperationID: "updateVolume",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/VolumeDetail"}}},
			},
			{
				Method:      "delete",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/volumes/{volumeName}",
				OperationID: "deleteVolume",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "get",
				Path:        "/tables",
				OperationID: "listTables",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedTables"}}},
			},
			{
				Method:      "get",
				Path:        "/catalogs/{catalogName}",
				OperationID: "getCatalog",
				Responses: []ir.Response{
					{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/CatalogInfo"}},
					{StatusCode: 404, Description: "not found", Schema: &ir.SchemaRef{Ref: "#/schemas/Error"}},
				},
			},
			{
				Method:      "get",
				Path:        "/catalogs/{catalogName}/schemas",
				OperationID: "listSchemas",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedSchemaDetails"}}},
			},
			{
				Method:      "post",
				Path:        "/catalogs/{catalogName}/schemas",
				OperationID: "createSchema",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/SchemaDetail"}}},
			},
			{
				Method:      "get",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}",
				OperationID: "getSchema",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/SchemaDetail"}}},
			},
			{
				Method:      "patch",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}",
				OperationID: "updateSchema",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/SchemaDetail"}}},
			},
			{
				Method:      "delete",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}",
				OperationID: "deleteSchema",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "post",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/tables",
				OperationID: "createTable",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/TableDetail"}}},
			},
			{
				Method:      "get",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}",
				OperationID: "getTable",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/TableDetail"}}},
			},
			{
				Method:      "patch",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}",
				OperationID: "updateTable",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/TableDetail"}}},
			},
			{
				Method:      "delete",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}",
				OperationID: "deleteTable",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "get",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/columns",
				OperationID: "listTableColumns",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedColumnDetails"}}},
			},
			{
				Method:      "patch",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/columns/{columnName}",
				OperationID: "updateColumn",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/ColumnDetail"}}},
			},
			{
				Method:      "post",
				Path:        "/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/profile",
				OperationID: "profileTable",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/TableStatistics"}}},
			},
			{
				Method:      "get",
				Path:        "/catalogs/{catalogName}/summary",
				OperationID: "getMetastoreSummary",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/MetastoreSummary"}}},
			},
			{
				Method:      "get",
				Path:        "/pipelines",
				OperationID: "listPipelines",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedPipelines"}}},
			},
			{
				Method:      "post",
				Path:        "/pipelines",
				OperationID: "createPipeline",
				Responses: []ir.Response{
					{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/Pipeline"}},
					{StatusCode: 400, Description: "bad request", Schema: &ir.SchemaRef{Ref: "#/schemas/Error"}},
				},
			},
			{
				Method:      "get",
				Path:        "/pipelines/{pipelineName}",
				OperationID: "getPipeline",
				Responses: []ir.Response{
					{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/Pipeline"}},
					{StatusCode: 404, Description: "not found", Schema: &ir.SchemaRef{Ref: "#/schemas/Error"}},
				},
			},
			{
				Method:      "patch",
				Path:        "/pipelines/{pipelineName}",
				OperationID: "updatePipeline",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/Pipeline"}}},
			},
			{
				Method:      "delete",
				Path:        "/pipelines/{pipelineName}",
				OperationID: "deletePipeline",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "get",
				Path:        "/pipelines/{pipelineName}/jobs",
				OperationID: "listPipelineJobs",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PipelineJobList"}}},
			},
			{
				Method:      "post",
				Path:        "/pipelines/{pipelineName}/jobs",
				OperationID: "createPipelineJob",
				Responses: []ir.Response{
					{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/PipelineJob"}},
					{StatusCode: 409, Description: "conflict", Schema: &ir.SchemaRef{Ref: "#/schemas/Error"}},
				},
			},
			{
				Method:      "delete",
				Path:        "/pipelines/{pipelineName}/jobs/{jobId}",
				OperationID: "deletePipelineJob",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "post",
				Path:        "/pipelines/{pipelineName}/runs",
				OperationID: "triggerPipelineRun",
				Responses: []ir.Response{
					{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/PipelineRun"}},
					{StatusCode: 404, Description: "not found", Schema: &ir.SchemaRef{Ref: "#/schemas/Error"}},
				},
			},
			{
				Method:      "get",
				Path:        "/pipelines/{pipelineName}/runs",
				OperationID: "listPipelineRuns",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedPipelineRuns"}}},
			},
			{
				Method:      "get",
				Path:        "/pipelines/runs/{runId}",
				OperationID: "getPipelineRun",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PipelineRun"}}},
			},
			{
				Method:      "post",
				Path:        "/pipelines/runs/{runId}/cancel",
				OperationID: "cancelPipelineRun",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PipelineRun"}}},
			},
			{
				Method:      "get",
				Path:        "/pipelines/runs/{runId}/jobs",
				OperationID: "listPipelineJobRuns",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PipelineJobRunList"}}},
			},
			{
				Method:      "get",
				Path:        "/catalogs",
				OperationID: "listCatalogs",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/CatalogRegistrationList"}}},
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
	require.Contains(t, content, "type GenListStorageCredentials200JSONResponse ListStorageCredentials200JSONResponse")
	require.Contains(t, content, "return ListStorageCredentials200JSONResponse(response).VisitListStorageCredentialsResponse(w)")
	require.Contains(t, content, "type GenCreateStorageCredential201JSONResponse CreateStorageCredential201JSONResponse")
	require.Contains(t, content, "return CreateStorageCredential201JSONResponse(response).VisitCreateStorageCredentialResponse(w)")
	require.Contains(t, content, "type GenGetStorageCredential200JSONResponse GetStorageCredential200JSONResponse")
	require.Contains(t, content, "return GetStorageCredential200JSONResponse(response).VisitGetStorageCredentialResponse(w)")
	require.Contains(t, content, "type GenUpdateStorageCredential200JSONResponse UpdateStorageCredential200JSONResponse")
	require.Contains(t, content, "return UpdateStorageCredential200JSONResponse(response).VisitUpdateStorageCredentialResponse(w)")
	require.Contains(t, content, "type GenDeleteStorageCredential204Response DeleteStorageCredential204Response")
	require.Contains(t, content, "return DeleteStorageCredential204Response(response).VisitDeleteStorageCredentialResponse(w)")
	require.Contains(t, content, "type GenListExternalLocations200JSONResponse ListExternalLocations200JSONResponse")
	require.Contains(t, content, "return ListExternalLocations200JSONResponse(response).VisitListExternalLocationsResponse(w)")
	require.Contains(t, content, "type GenCreateExternalLocation201JSONResponse CreateExternalLocation201JSONResponse")
	require.Contains(t, content, "return CreateExternalLocation201JSONResponse(response).VisitCreateExternalLocationResponse(w)")
	require.Contains(t, content, "type GenGetExternalLocation200JSONResponse GetExternalLocation200JSONResponse")
	require.Contains(t, content, "return GetExternalLocation200JSONResponse(response).VisitGetExternalLocationResponse(w)")
	require.Contains(t, content, "type GenUpdateExternalLocation200JSONResponse UpdateExternalLocation200JSONResponse")
	require.Contains(t, content, "return UpdateExternalLocation200JSONResponse(response).VisitUpdateExternalLocationResponse(w)")
	require.Contains(t, content, "type GenDeleteExternalLocation204Response DeleteExternalLocation204Response")
	require.Contains(t, content, "return DeleteExternalLocation204Response(response).VisitDeleteExternalLocationResponse(w)")
	require.Contains(t, content, "type GenListVolumes200JSONResponse ListVolumes200JSONResponse")
	require.Contains(t, content, "return ListVolumes200JSONResponse(response).VisitListVolumesResponse(w)")
	require.Contains(t, content, "type GenCreateVolume201JSONResponse CreateVolume201JSONResponse")
	require.Contains(t, content, "return CreateVolume201JSONResponse(response).VisitCreateVolumeResponse(w)")
	require.Contains(t, content, "type GenGetVolume200JSONResponse GetVolume200JSONResponse")
	require.Contains(t, content, "return GetVolume200JSONResponse(response).VisitGetVolumeResponse(w)")
	require.Contains(t, content, "type GenUpdateVolume200JSONResponse UpdateVolume200JSONResponse")
	require.Contains(t, content, "return UpdateVolume200JSONResponse(response).VisitUpdateVolumeResponse(w)")
	require.Contains(t, content, "type GenDeleteVolume204Response DeleteVolume204Response")
	require.Contains(t, content, "return DeleteVolume204Response(response).VisitDeleteVolumeResponse(w)")

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
	require.NotContains(t, content, "type GenListStorageCredentials200JSONResponse = ListStorageCredentials200JSONResponse")
	require.NotContains(t, content, "type GenCreateStorageCredential201JSONResponse = CreateStorageCredential201JSONResponse")
	require.NotContains(t, content, "type GenGetStorageCredential200JSONResponse = GetStorageCredential200JSONResponse")
	require.NotContains(t, content, "type GenUpdateStorageCredential200JSONResponse = UpdateStorageCredential200JSONResponse")
	require.NotContains(t, content, "type GenDeleteStorageCredential204Response = DeleteStorageCredential204Response")
	require.NotContains(t, content, "type GenListExternalLocations200JSONResponse = ListExternalLocations200JSONResponse")
	require.NotContains(t, content, "type GenCreateExternalLocation201JSONResponse = CreateExternalLocation201JSONResponse")
	require.NotContains(t, content, "type GenGetExternalLocation200JSONResponse = GetExternalLocation200JSONResponse")
	require.NotContains(t, content, "type GenUpdateExternalLocation200JSONResponse = UpdateExternalLocation200JSONResponse")
	require.NotContains(t, content, "type GenDeleteExternalLocation204Response = DeleteExternalLocation204Response")
	require.NotContains(t, content, "type GenListVolumes200JSONResponse = ListVolumes200JSONResponse")
	require.NotContains(t, content, "type GenCreateVolume201JSONResponse = CreateVolume201JSONResponse")
	require.NotContains(t, content, "type GenGetVolume200JSONResponse = GetVolume200JSONResponse")
	require.NotContains(t, content, "type GenUpdateVolume200JSONResponse = UpdateVolume200JSONResponse")
	require.NotContains(t, content, "type GenDeleteVolume204Response = DeleteVolume204Response")

	require.Contains(t, content, "type GenGetCatalog200JSONResponse GetCatalog200JSONResponse")
	require.Contains(t, content, "return GetCatalog200JSONResponse(response).VisitGetCatalogResponse(w)")
	require.Contains(t, content, "type GenGetCatalog404JSONResponse GetCatalog404JSONResponse")
	require.Contains(t, content, "return GetCatalog404JSONResponse(response).VisitGetCatalogResponse(w)")
	require.Contains(t, content, "type GenListSchemas200JSONResponse ListSchemas200JSONResponse")
	require.Contains(t, content, "return ListSchemas200JSONResponse(response).VisitListSchemasResponse(w)")
	require.Contains(t, content, "type GenCreateSchema201JSONResponse CreateSchema201JSONResponse")
	require.Contains(t, content, "return CreateSchema201JSONResponse(response).VisitCreateSchemaResponse(w)")
	require.Contains(t, content, "type GenGetSchema200JSONResponse GetSchema200JSONResponse")
	require.Contains(t, content, "return GetSchema200JSONResponse(response).VisitGetSchemaResponse(w)")
	require.Contains(t, content, "type GenUpdateSchema200JSONResponse UpdateSchema200JSONResponse")
	require.Contains(t, content, "return UpdateSchema200JSONResponse(response).VisitUpdateSchemaResponse(w)")
	require.Contains(t, content, "type GenDeleteSchema204Response DeleteSchema204Response")
	require.Contains(t, content, "return DeleteSchema204Response(response).VisitDeleteSchemaResponse(w)")
	require.Contains(t, content, "type GenListTables200JSONResponse ListTables200JSONResponse")
	require.Contains(t, content, "return ListTables200JSONResponse(response).VisitListTablesResponse(w)")
	require.Contains(t, content, "type GenCreateTable201JSONResponse CreateTable201JSONResponse")
	require.Contains(t, content, "return CreateTable201JSONResponse(response).VisitCreateTableResponse(w)")
	require.Contains(t, content, "type GenGetTable200JSONResponse GetTable200JSONResponse")
	require.Contains(t, content, "return GetTable200JSONResponse(response).VisitGetTableResponse(w)")
	require.Contains(t, content, "type GenUpdateTable200JSONResponse UpdateTable200JSONResponse")
	require.Contains(t, content, "return UpdateTable200JSONResponse(response).VisitUpdateTableResponse(w)")
	require.Contains(t, content, "type GenDeleteTable204Response DeleteTable204Response")
	require.Contains(t, content, "return DeleteTable204Response(response).VisitDeleteTableResponse(w)")
	require.Contains(t, content, "type GenListTableColumns200JSONResponse ListTableColumns200JSONResponse")
	require.Contains(t, content, "return ListTableColumns200JSONResponse(response).VisitListTableColumnsResponse(w)")
	require.Contains(t, content, "type GenUpdateColumn200JSONResponse UpdateColumn200JSONResponse")
	require.Contains(t, content, "return UpdateColumn200JSONResponse(response).VisitUpdateColumnResponse(w)")
	require.Contains(t, content, "type GenProfileTable200JSONResponse ProfileTable200JSONResponse")
	require.Contains(t, content, "return ProfileTable200JSONResponse(response).VisitProfileTableResponse(w)")
	require.Contains(t, content, "type GenGetMetastoreSummary200JSONResponse GetMetastoreSummary200JSONResponse")
	require.Contains(t, content, "return GetMetastoreSummary200JSONResponse(response).VisitGetMetastoreSummaryResponse(w)")

	require.NotContains(t, content, "type GenGetCatalog200JSONResponse = GetCatalog200JSONResponse")
	require.NotContains(t, content, "type GenListSchemas200JSONResponse = ListSchemas200JSONResponse")
	require.NotContains(t, content, "type GenCreateSchema201JSONResponse = CreateSchema201JSONResponse")
	require.NotContains(t, content, "type GenGetSchema200JSONResponse = GetSchema200JSONResponse")
	require.NotContains(t, content, "type GenUpdateSchema200JSONResponse = UpdateSchema200JSONResponse")
	require.NotContains(t, content, "type GenDeleteSchema204Response = DeleteSchema204Response")
	require.NotContains(t, content, "type GenListTables200JSONResponse = ListTables200JSONResponse")
	require.NotContains(t, content, "type GenCreateTable201JSONResponse = CreateTable201JSONResponse")
	require.NotContains(t, content, "type GenGetTable200JSONResponse = GetTable200JSONResponse")
	require.NotContains(t, content, "type GenUpdateTable200JSONResponse = UpdateTable200JSONResponse")
	require.NotContains(t, content, "type GenDeleteTable204Response = DeleteTable204Response")
	require.NotContains(t, content, "type GenListTableColumns200JSONResponse = ListTableColumns200JSONResponse")
	require.NotContains(t, content, "type GenUpdateColumn200JSONResponse = UpdateColumn200JSONResponse")
	require.NotContains(t, content, "type GenProfileTable200JSONResponse = ProfileTable200JSONResponse")
	require.NotContains(t, content, "type GenGetMetastoreSummary200JSONResponse = GetMetastoreSummary200JSONResponse")
	require.Contains(t, content, "type GenListPipelines200JSONResponse ListPipelines200JSONResponse")
	require.Contains(t, content, "return ListPipelines200JSONResponse(response).VisitListPipelinesResponse(w)")
	require.Contains(t, content, "type GenCreatePipeline201JSONResponse CreatePipeline201JSONResponse")
	require.Contains(t, content, "return CreatePipeline201JSONResponse(response).VisitCreatePipelineResponse(w)")
	require.Contains(t, content, "type GenCreatePipeline400JSONResponse CreatePipeline400JSONResponse")
	require.Contains(t, content, "return CreatePipeline400JSONResponse(response).VisitCreatePipelineResponse(w)")
	require.Contains(t, content, "type GenGetPipeline200JSONResponse GetPipeline200JSONResponse")
	require.Contains(t, content, "return GetPipeline200JSONResponse(response).VisitGetPipelineResponse(w)")
	require.Contains(t, content, "type GenGetPipeline404JSONResponse GetPipeline404JSONResponse")
	require.Contains(t, content, "return GetPipeline404JSONResponse(response).VisitGetPipelineResponse(w)")
	require.Contains(t, content, "type GenUpdatePipeline200JSONResponse UpdatePipeline200JSONResponse")
	require.Contains(t, content, "return UpdatePipeline200JSONResponse(response).VisitUpdatePipelineResponse(w)")
	require.Contains(t, content, "type GenDeletePipeline204Response DeletePipeline204Response")
	require.Contains(t, content, "return DeletePipeline204Response(response).VisitDeletePipelineResponse(w)")
	require.Contains(t, content, "type GenListPipelineJobs200JSONResponse ListPipelineJobs200JSONResponse")
	require.Contains(t, content, "return ListPipelineJobs200JSONResponse(response).VisitListPipelineJobsResponse(w)")
	require.Contains(t, content, "type GenCreatePipelineJob201JSONResponse CreatePipelineJob201JSONResponse")
	require.Contains(t, content, "return CreatePipelineJob201JSONResponse(response).VisitCreatePipelineJobResponse(w)")
	require.Contains(t, content, "type GenCreatePipelineJob409JSONResponse CreatePipelineJob409JSONResponse")
	require.Contains(t, content, "return CreatePipelineJob409JSONResponse(response).VisitCreatePipelineJobResponse(w)")
	require.Contains(t, content, "type GenDeletePipelineJob204Response DeletePipelineJob204Response")
	require.Contains(t, content, "return DeletePipelineJob204Response(response).VisitDeletePipelineJobResponse(w)")
	require.Contains(t, content, "type GenTriggerPipelineRun201JSONResponse TriggerPipelineRun201JSONResponse")
	require.Contains(t, content, "return TriggerPipelineRun201JSONResponse(response).VisitTriggerPipelineRunResponse(w)")
	require.Contains(t, content, "type GenTriggerPipelineRun404JSONResponse TriggerPipelineRun404JSONResponse")
	require.Contains(t, content, "return TriggerPipelineRun404JSONResponse(response).VisitTriggerPipelineRunResponse(w)")
	require.Contains(t, content, "type GenListPipelineRuns200JSONResponse ListPipelineRuns200JSONResponse")
	require.Contains(t, content, "return ListPipelineRuns200JSONResponse(response).VisitListPipelineRunsResponse(w)")
	require.Contains(t, content, "type GenGetPipelineRun200JSONResponse GetPipelineRun200JSONResponse")
	require.Contains(t, content, "return GetPipelineRun200JSONResponse(response).VisitGetPipelineRunResponse(w)")
	require.Contains(t, content, "type GenCancelPipelineRun200JSONResponse CancelPipelineRun200JSONResponse")
	require.Contains(t, content, "return CancelPipelineRun200JSONResponse(response).VisitCancelPipelineRunResponse(w)")
	require.Contains(t, content, "type GenListPipelineJobRuns200JSONResponse ListPipelineJobRuns200JSONResponse")
	require.Contains(t, content, "return ListPipelineJobRuns200JSONResponse(response).VisitListPipelineJobRunsResponse(w)")

	require.NotContains(t, content, "type GenListPipelines200JSONResponse = ListPipelines200JSONResponse")
	require.NotContains(t, content, "type GenCreatePipeline201JSONResponse = CreatePipeline201JSONResponse")
	require.NotContains(t, content, "type GenCreatePipeline400JSONResponse = CreatePipeline400JSONResponse")
	require.NotContains(t, content, "type GenGetPipeline200JSONResponse = GetPipeline200JSONResponse")
	require.NotContains(t, content, "type GenGetPipeline404JSONResponse = GetPipeline404JSONResponse")
	require.NotContains(t, content, "type GenUpdatePipeline200JSONResponse = UpdatePipeline200JSONResponse")
	require.NotContains(t, content, "type GenDeletePipeline204Response = DeletePipeline204Response")
	require.NotContains(t, content, "type GenListPipelineJobs200JSONResponse = ListPipelineJobs200JSONResponse")
	require.NotContains(t, content, "type GenCreatePipelineJob201JSONResponse = CreatePipelineJob201JSONResponse")
	require.NotContains(t, content, "type GenCreatePipelineJob409JSONResponse = CreatePipelineJob409JSONResponse")
	require.NotContains(t, content, "type GenDeletePipelineJob204Response = DeletePipelineJob204Response")
	require.NotContains(t, content, "type GenTriggerPipelineRun201JSONResponse = TriggerPipelineRun201JSONResponse")
	require.NotContains(t, content, "type GenTriggerPipelineRun404JSONResponse = TriggerPipelineRun404JSONResponse")
	require.NotContains(t, content, "type GenListPipelineRuns200JSONResponse = ListPipelineRuns200JSONResponse")
	require.NotContains(t, content, "type GenGetPipelineRun200JSONResponse = GetPipelineRun200JSONResponse")
	require.NotContains(t, content, "type GenCancelPipelineRun200JSONResponse = CancelPipelineRun200JSONResponse")
	require.NotContains(t, content, "type GenListPipelineJobRuns200JSONResponse = ListPipelineJobRuns200JSONResponse")

	require.Contains(t, content, "type GenListCatalogs200JSONResponse = ListCatalogs200JSONResponse")
}

func TestEmit_GeneratesNativeConcreteResponsesForModelsAndSemantic(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{
				Method:      "get",
				Path:        "/models",
				OperationID: "listModels",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedModels"}}},
			},
			{
				Method:      "get",
				Path:        "/semantic/models",
				OperationID: "listSemanticModels",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedSemanticModels"}}},
			},
			{
				Method:      "get",
				Path:        "/catalogs",
				OperationID: "listCatalogs",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/CatalogRegistrationList"}}},
			},
		},
	}

	b, err := Emit(doc)
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenListModels200JSONResponse ListModels200JSONResponse")
	require.Contains(t, content, "return ListModels200JSONResponse(response).VisitListModelsResponse(w)")
	require.NotContains(t, content, "type GenListModels200JSONResponse = ListModels200JSONResponse")

	require.Contains(t, content, "type GenListSemanticModels200JSONResponse ListSemanticModels200JSONResponse")
	require.Contains(t, content, "return ListSemanticModels200JSONResponse(response).VisitListSemanticModelsResponse(w)")
	require.NotContains(t, content, "type GenListSemanticModels200JSONResponse = ListSemanticModels200JSONResponse")

	require.Contains(t, content, "type GenListCatalogs200JSONResponse = ListCatalogs200JSONResponse")
}

func TestEmit_GeneratesNativeConcreteResponsesForNotebookDomainOps(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{
				Method:      "get",
				Path:        "/notebooks",
				OperationID: "listNotebooks",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedNotebooks"}}},
			},
			{
				Method:      "delete",
				Path:        "/notebooks/{notebookId}",
				OperationID: "deleteNotebook",
				Responses:   []ir.Response{{StatusCode: 204, Description: "no content"}},
			},
			{
				Method:      "post",
				Path:        "/notebooks/{notebookId}/sessions/{sessionId}/execute/{cellId}",
				OperationID: "executeCell",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/CellExecutionResult"}}},
			},
			{
				Method:      "post",
				Path:        "/notebooks/{notebookId}/sessions/{sessionId}/run-all-async",
				OperationID: "runAllCellsAsync",
				Responses:   []ir.Response{{StatusCode: 202, Description: "accepted", Schema: &ir.SchemaRef{Ref: "#/schemas/NotebookJob"}}},
			},
			{
				Method:      "get",
				Path:        "/git-repos",
				OperationID: "listGitRepos",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedGitRepos"}}},
			},
			{
				Method:      "post",
				Path:        "/git-repos/{gitRepoId}/sync",
				OperationID: "syncGitRepo",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/GitSyncResult"}}},
			},
			{
				Method:      "get",
				Path:        "/catalogs",
				OperationID: "listCatalogs",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/CatalogRegistrationList"}}},
			},
		},
	}

	b, err := Emit(doc)
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenListNotebooks200JSONResponse ListNotebooks200JSONResponse")
	require.Contains(t, content, "return ListNotebooks200JSONResponse(response).VisitListNotebooksResponse(w)")
	require.NotContains(t, content, "type GenListNotebooks200JSONResponse = ListNotebooks200JSONResponse")

	require.Contains(t, content, "type GenDeleteNotebook204Response DeleteNotebook204Response")
	require.Contains(t, content, "return DeleteNotebook204Response(response).VisitDeleteNotebookResponse(w)")
	require.NotContains(t, content, "type GenDeleteNotebook204Response = DeleteNotebook204Response")

	require.Contains(t, content, "type GenExecuteCell201JSONResponse ExecuteCell201JSONResponse")
	require.Contains(t, content, "return ExecuteCell201JSONResponse(response).VisitExecuteCellResponse(w)")
	require.Contains(t, content, "type GenRunAllCellsAsync202JSONResponse RunAllCellsAsync202JSONResponse")
	require.Contains(t, content, "return RunAllCellsAsync202JSONResponse(response).VisitRunAllCellsAsyncResponse(w)")

	require.Contains(t, content, "type GenListGitRepos200JSONResponse ListGitRepos200JSONResponse")
	require.Contains(t, content, "return ListGitRepos200JSONResponse(response).VisitListGitReposResponse(w)")
	require.Contains(t, content, "type GenSyncGitRepo201JSONResponse SyncGitRepo201JSONResponse")
	require.Contains(t, content, "return SyncGitRepo201JSONResponse(response).VisitSyncGitRepoResponse(w)")

	require.Contains(t, content, "type GenListCatalogs200JSONResponse = ListCatalogs200JSONResponse")
}

func TestEmit_GeneratesNativeConcreteResponsesForRemainingDomains(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{
				Method:      "post",
				Path:        "/governance/grants",
				OperationID: "createGrant",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/Grant"}}},
			},
			{
				Method:      "get",
				Path:        "/lineage/columns/{columnId}",
				OperationID: "getColumnLineage",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/ColumnLineage"}}},
			},
			{
				Method:      "post",
				Path:        "/macros",
				OperationID: "createMacro",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/Macro"}}},
			},
			{
				Method:      "post",
				Path:        "/catalogs/register",
				OperationID: "registerCatalog",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/CatalogRegistration"}}},
			},
			{
				Method:      "get",
				Path:        "/tags",
				OperationID: "listTags",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/TagList"}}},
			},
			{
				Method:      "get",
				Path:        "/row-filters",
				OperationID: "listRowFilters",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/RowFilterList"}}},
			},
			{
				Method:      "get",
				Path:        "/catalogs",
				OperationID: "listCatalogs",
				Responses:   []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/CatalogRegistrationList"}}},
			},
		},
	}

	b, err := Emit(doc)
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenCreateGrant201JSONResponse CreateGrant201JSONResponse")
	require.Contains(t, content, "return CreateGrant201JSONResponse(response).VisitCreateGrantResponse(w)")
	require.NotContains(t, content, "type GenCreateGrant201JSONResponse = CreateGrant201JSONResponse")

	require.Contains(t, content, "type GenGetColumnLineage200JSONResponse GetColumnLineage200JSONResponse")
	require.Contains(t, content, "return GetColumnLineage200JSONResponse(response).VisitGetColumnLineageResponse(w)")
	require.NotContains(t, content, "type GenGetColumnLineage200JSONResponse = GetColumnLineage200JSONResponse")

	require.Contains(t, content, "type GenCreateMacro201JSONResponse CreateMacro201JSONResponse")
	require.Contains(t, content, "return CreateMacro201JSONResponse(response).VisitCreateMacroResponse(w)")
	require.NotContains(t, content, "type GenCreateMacro201JSONResponse = CreateMacro201JSONResponse")

	require.Contains(t, content, "type GenRegisterCatalog201JSONResponse RegisterCatalog201JSONResponse")
	require.Contains(t, content, "return RegisterCatalog201JSONResponse(response).VisitRegisterCatalogResponse(w)")
	require.NotContains(t, content, "type GenRegisterCatalog201JSONResponse = RegisterCatalog201JSONResponse")

	require.Contains(t, content, "type GenListTags200JSONResponse ListTags200JSONResponse")
	require.Contains(t, content, "return ListTags200JSONResponse(response).VisitListTagsResponse(w)")
	require.NotContains(t, content, "type GenListTags200JSONResponse = ListTags200JSONResponse")

	require.Contains(t, content, "type GenListRowFilters200JSONResponse ListRowFilters200JSONResponse")
	require.Contains(t, content, "return ListRowFilters200JSONResponse(response).VisitListRowFiltersResponse(w)")
	require.NotContains(t, content, "type GenListRowFilters200JSONResponse = ListRowFilters200JSONResponse")

	require.Contains(t, content, "type GenListCatalogs200JSONResponse = ListCatalogs200JSONResponse")
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
