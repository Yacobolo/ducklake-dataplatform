package servergo

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/pkg/apigen/ir"
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

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)
	require.Contains(t, content, "type GenServerInterface interface")
	require.Contains(t, content, "RegisterAPIGenRoutes")
	require.Contains(t, content, "HandleAPIGen")
	require.Contains(t, content, "type GenOperationDispatcher interface")
	require.Contains(t, content, "DispatchAPIGenOperation")
	require.NotContains(t, content, "*ServerInterfaceWrapper")
	require.Contains(t, content, "apigenchi.RegisterRoutes(router, []apigenchi.Route{")
	require.Contains(t, content, "{Method: \"GET\", Path: \"/healthz\", OperationID: \"getHealth\"}")
	require.Contains(t, content, "func RegisterAPIGenRoutes(router apigenchi.Router, server GenServerInterface)")
	require.Contains(t, content, "func RegisterAPIGenStrictRoutes(router apigenchi.Router, handler GenStrictServerInterface)")
	require.Contains(t, content, "func DispatchAPIGenOperation(operationID string, dispatcher GenOperationDispatcher")
	require.NotContains(t, content, "\"github.com/oapi-codegen/runtime\"")
	require.Contains(t, content, "type genStrictAdapter struct")
	require.Contains(t, content, "func (a genStrictAdapter) HandleAPIGen(operationID string, w http.ResponseWriter, r *http.Request)")
	require.Contains(t, content, "type genStrictBridge struct")
	require.Contains(t, content, "type GenStrictServerInterface interface")
	require.Contains(t, content, "func DispatchAPIGenStrictOperation(operationID string, handler GenStrictServerInterface")
	require.Contains(t, content, "type GenOperationContract struct")
	require.Contains(t, content, "var genOperationContracts = map[string]GenOperationContract{")
	require.Contains(t, content, "func GetAPIGenOperationContracts() map[string]GenOperationContract")
	require.Contains(t, content, "func GetAPIGenOperationContract(operationID string) (GenOperationContract, bool)")
	require.Contains(t, content, "func APIGenOperationAllowsStatus(operationID string, statusCode int) bool")
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

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)
	require.Contains(t, content, "{Method: \"POST\", Path: \"/query\", OperationID: \"executeQuery\"}")
	require.NotContains(t, content, "{Method: \"POST\", Path: \"/v1/query\", OperationID: \"executeQuery\"}")
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

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "{Method: \"GET\", Path: \"/healthz\", OperationID: \"getHealth\"}")
	require.Contains(t, content, "{Method: \"POST\", Path: \"/query\", OperationID: \"executeQuery\"}")
	require.Contains(t, content, "{Method: \"GET\", Path: \"/groups\", OperationID: \"listGroups\"}")
	require.Contains(t, content, "}, server.HandleAPIGen)")

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

func TestEmit_OperationContractsIncludeManualAndBodyMetadata(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{
				Method:      "post",
				Path:        "/auth/local-login",
				OperationID: "localLogin",
				Tags:        []string{"Auth"},
				RequestBody: &ir.RequestBody{Required: true, Schema: ir.SchemaRef{Ref: "LoginRequest"}},
				Responses: []ir.Response{
					{StatusCode: 200, Description: "ok"},
					{StatusCode: 401, Description: "unauthorized"},
				},
				Extensions: map[string]any{
					"x-apigen-manual": true,
					"x-authz":         map[string]any{"mode": "none"},
				},
			},
		},
		Schemas: map[string]ir.Schema{
			"LoginRequest": {Type: "object"},
		},
	}

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)
	require.Contains(t, content, `"localLogin": {OperationID: "localLogin", Method: "POST", Path: "/auth/local-login"`)
	require.Contains(t, content, `DocumentedStatusCodes: []int{200, 400, 401, 403, 404, 409, 429, 500, 502}`)
	require.Contains(t, content, `RequestBodyRequired: true`)
	require.Contains(t, content, `AuthzMode: "none"`)
	require.Contains(t, content, `Protected: true`)
	require.Contains(t, content, `Manual: true`)
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

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "ListGroupMembers(w http.ResponseWriter, r *http.Request, groupId string, params GenListGroupMembersParams)")
	require.Contains(t, content, "apigenchi.BindPathParameter(\"groupId\", apigenchi.URLParam(r, \"groupId\"), true, &groupId)")
	require.Contains(t, content, "apigenchi.BindQueryParameter(r.URL.Query(), \"max_results\", false, &params.MaxResults)")
	require.Contains(t, content, "writeAPIGenError(w, http.StatusBadRequest, err.Error())")
	require.Contains(t, content, "dispatcher.ListGroupMembers(w, r, groupId, params)")
	require.Contains(t, content, "type GenListGroupMembersParams struct {")
	require.Contains(t, content, "\tMaxResults *int32")
	require.Contains(t, content, "func apigenErrorMessage(statusCode int, message string) string {")
	require.Contains(t, content, "if statusCode >= http.StatusInternalServerError {")
	require.Contains(t, content, "if statusText := strings.ToLower(http.StatusText(statusCode)); statusText != \"\" {")
	require.Contains(t, content, "func writeAPIGenError(w http.ResponseWriter, statusCode int, message string) {")
	require.Contains(t, content, "_ = json.NewEncoder(w).Encode(Error{Code: apigenchi.SafeIntToInt32(statusCode), Message: apigenErrorMessage(statusCode, message)})")
	require.Contains(t, content, "var request GenListGroupMembersRequest")
	require.Contains(t, content, "\"fmt\"")
	require.Contains(t, content, "\"reflect\"")
	require.Contains(t, content, "response, err := b.handler.ListGroupMembers(r.Context(), request)")
	require.Contains(t, content, "if err := response.VisitListGroupMembersResponse(w); err != nil")
	require.Contains(t, content, "type GenListGroupMembersRequest struct {")
	require.Contains(t, content, "\tGroupId string")
	require.Contains(t, content, "\tParams GenListGroupMembersParams")
	require.Contains(t, content, "type GenListGroupMembersResponse interface {")
	require.Contains(t, content, "\tVisitListGroupMembersResponse(w http.ResponseWriter) error")
	require.Contains(t, content, "type GenListGroupMembers200ResponseHeaders struct {")
	require.Contains(t, content, "type GenListGroupMembers200Response struct {")
	require.Contains(t, content, "\tHeaders GenListGroupMembers200ResponseHeaders")
	require.Contains(t, content, "func (response GenListGroupMembers200Response) VisitListGroupMembersResponse(w http.ResponseWriter) error {")
	require.Contains(t, content, "w.Header().Set(\"X-RateLimit-Limit\", fmt.Sprint(response.Headers.XRateLimitLimit))")
	require.Contains(t, content, "w.WriteHeader(200)")
	require.Contains(t, content, "return nil")
	require.Contains(t, content, "type ListGroupMembers200Response = GenListGroupMembers200Response")
	require.Contains(t, content, "ListGroupMembers(ctx context.Context, request GenListGroupMembersRequest) (GenListGroupMembersResponse, error)")
}

func TestEmit_GeneratesStrictJSONBodyDecoding(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Schemas: map[string]ir.Schema{
			"CreatePipelineRequest": {Type: "object"},
		},
		Endpoints: []ir.Endpoint{
			{
				Method:      "post",
				Path:        "/pipelines",
				OperationID: "createPipeline",
				RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "CreatePipelineRequest"}},
				Responses:   []ir.Response{{StatusCode: 201, Description: "created"}},
			},
		},
	}

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "\"io\"")
	require.Contains(t, content, "func decodeAPIGenJSONBody(body io.Reader, dest any, requiredFields ...string) error {")
	require.Contains(t, content, "decoder.DisallowUnknownFields()")
	require.Contains(t, content, "return fmt.Errorf(\"request body must not be empty\")")
	require.Contains(t, content, "return fmt.Errorf(\"request body must contain a single JSON value\")")
	require.Contains(t, content, "decoder := json.NewDecoder(strings.NewReader(string(raw)))")
	require.Contains(t, content, "if err := decodeAPIGenJSONBody(r.Body, &body); err != nil {")
	require.Contains(t, content, "writeAPIGenError(w, http.StatusBadRequest, err.Error())")
	require.Contains(t, content, "writeAPIGenError(w, http.StatusInternalServerError, err.Error())")
	require.Contains(t, content, "Message: apigenErrorMessage(statusCode, message)")
}

func TestEmit_GeneratesNativeBodyAliasesWhenIRHasConcreteSchemaRefs(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Schemas: map[string]ir.Schema{
			"CreateAPIKeyRequest":   {Type: "object"},
			"CreatePipelineRequest": {Type: "object"},
			"MetricQueryRequest":    {Type: "object"},
		},
		Endpoints: []ir.Endpoint{
			{
				Method:      "post",
				Path:        "/api-keys",
				OperationID: "createAPIKey",
				RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "CreateAPIKeyRequest"}},
				Responses:   []ir.Response{{StatusCode: 201, Description: "created"}},
			},
			{
				Method:      "post",
				Path:        "/pipelines",
				OperationID: "createPipeline",
				RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "GenericRequest"}},
				Responses:   []ir.Response{{StatusCode: 201, Description: "created"}},
			},
			{
				Method:      "post",
				Path:        "/metric-queries:run",
				OperationID: "runMetricQuery",
				RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "GenericRequest"}},
				Responses:   []ir.Response{{StatusCode: 201, Description: "created"}},
			},
		},
	}

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenCreateAPIKeyJSONBody = GenSchemaCreateAPIKeyRequest")
	require.Contains(t, content, "type GenCreatePipelineJSONBody = GenSchemaCreatePipelineRequest")
	require.Contains(t, content, "type GenRunMetricQueryJSONBody = GenSchemaMetricQueryRequest")
}

func TestEmit_FallsBackToLegacyBodyAliasWhenGenericRequestHasNoConcreteSchema(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Schemas: map[string]ir.Schema{
			"GenericRequest": {Type: "object"},
		},
		Endpoints: []ir.Endpoint{
			{
				Method:      "post",
				Path:        "/widgets",
				OperationID: "createWidget",
				RequestBody: &ir.RequestBody{Schema: ir.SchemaRef{Ref: "GenericRequest"}},
				Responses:   []ir.Response{{StatusCode: 201, Description: "created"}},
			},
		},
	}

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenCreateWidgetJSONBody = CreateWidgetJSONRequestBody")
}

func TestEmit_ImportsTimeForDateTimeParameters(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{
				Method:      "get",
				Path:        "/audit-logs",
				OperationID: "listAuditLogs",
				Parameters: []ir.Parameter{
					{Name: "from", In: "query", Schema: ir.SchemaRef{Type: "string", Format: "date-time"}},
				},
				Responses: []ir.Response{{StatusCode: 200, Description: "ok"}},
			},
		},
	}

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "\"time\"")
	require.Contains(t, content, "type GenListAuditLogsParams struct {")
	require.Contains(t, content, "\tFrom *time.Time")
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

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenExecuteQueryRequest struct {")
	require.Contains(t, content, "type GenExecuteQueryResponse interface {")
	require.Contains(t, content, "\tVisitExecuteQueryResponse(w http.ResponseWriter) error")
	require.Contains(t, content, "type GenExecuteQuery201JSONResponse ")
	require.Contains(t, content, "func (response GenExecuteQuery201JSONResponse) VisitExecuteQueryResponse(w http.ResponseWriter) error {")
	require.Contains(t, content, "w.Header().Set(\"Content-Type\", \"application/json\")")
	require.Contains(t, content, "w.WriteHeader(201)")
	require.Contains(t, content, "return json.NewEncoder(w).Encode(")
	require.Contains(t, content, "w.WriteHeader(204)")
	require.Contains(t, content, "return nil")
	require.Contains(t, content, "type ExecuteQuery200JSONResponse GenExecuteQuery201JSONResponse")
	require.Contains(t, content, "type GenExecuteQuery200JSONResponse = ExecuteQuery200JSONResponse")
	require.Contains(t, content, "func (response ExecuteQuery200JSONResponse) VisitExecuteQueryResponse(w http.ResponseWriter) error {")

	require.Contains(t, content, "type SubmitQuery202JSONResponse GenSubmitQuery201JSONResponse")
	require.Contains(t, content, "type GenSubmitQuery202JSONResponse = SubmitQuery202JSONResponse")
	require.Contains(t, content, "type GetQuery200JSONResponse = GenGetQuery200JSONResponse")
	require.Contains(t, content, "type GetQueryResults200JSONResponse = GenGetQueryResults200JSONResponse")
	require.Contains(t, content, "type CancelQuery200JSONResponse GenCancelQuery201JSONResponse")
	require.Contains(t, content, "type GenCancelQuery200JSONResponse = CancelQuery200JSONResponse")
	require.Contains(t, content, "type DeleteQuery204Response = GenDeleteQuery204Response")

	require.Contains(t, content, "type ListGroups200JSONResponse = GenListGroups200JSONResponse")
	require.Contains(t, content, "type GenForbiddenJSONResponse struct {")
	require.Contains(t, content, "type GenListGroups403JSONResponse struct{ GenForbiddenJSONResponse }")
	require.Contains(t, content, "w.WriteHeader(403)")
	require.Contains(t, content, "type CreateGroup201JSONResponse = GenCreateGroup201JSONResponse")
	require.Contains(t, content, "type GetGroup200JSONResponse = GenGetGroup200JSONResponse")
	require.Contains(t, content, "type DeleteGroup204Response = GenDeleteGroup204Response")
	require.Contains(t, content, "type ListPrincipals200JSONResponse = GenListPrincipals200JSONResponse")
	require.Contains(t, content, "type CreatePrincipal201JSONResponse = GenCreatePrincipal201JSONResponse")
	require.Contains(t, content, "type GetPrincipal200JSONResponse = GenGetPrincipal200JSONResponse")
	require.Contains(t, content, "type DeletePrincipal204Response = GenDeletePrincipal204Response")
	require.Contains(t, content, "type ListAPIKeys200JSONResponse = GenListAPIKeys200JSONResponse")
	require.Contains(t, content, "type CreateAPIKey201JSONResponse = GenCreateAPIKey201JSONResponse")
	require.Contains(t, content, "type CleanupExpiredAPIKeys200JSONResponse = GenCleanupExpiredAPIKeys200JSONResponse")
	require.Contains(t, content, "type DeleteAPIKey204Response = GenDeleteAPIKey204Response")
	require.Contains(t, content, "type ListStorageCredentials200JSONResponse = GenListStorageCredentials200JSONResponse")
	require.Contains(t, content, "type CreateStorageCredential201JSONResponse = GenCreateStorageCredential201JSONResponse")
	require.Contains(t, content, "type GetStorageCredential200JSONResponse = GenGetStorageCredential200JSONResponse")
	require.Contains(t, content, "type UpdateStorageCredential200JSONResponse = GenUpdateStorageCredential200JSONResponse")
	require.Contains(t, content, "type DeleteStorageCredential204Response = GenDeleteStorageCredential204Response")
	require.Contains(t, content, "type ListExternalLocations200JSONResponse = GenListExternalLocations200JSONResponse")
	require.Contains(t, content, "type CreateExternalLocation201JSONResponse = GenCreateExternalLocation201JSONResponse")
	require.Contains(t, content, "type GetExternalLocation200JSONResponse = GenGetExternalLocation200JSONResponse")
	require.Contains(t, content, "type UpdateExternalLocation200JSONResponse = GenUpdateExternalLocation200JSONResponse")
	require.Contains(t, content, "type DeleteExternalLocation204Response = GenDeleteExternalLocation204Response")
	require.Contains(t, content, "type ListVolumes200JSONResponse = GenListVolumes200JSONResponse")
	require.Contains(t, content, "type CreateVolume201JSONResponse = GenCreateVolume201JSONResponse")
	require.Contains(t, content, "type GetVolume200JSONResponse = GenGetVolume200JSONResponse")
	require.Contains(t, content, "type UpdateVolume200JSONResponse = GenUpdateVolume200JSONResponse")
	require.Contains(t, content, "type DeleteVolume204Response = GenDeleteVolume204Response")

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

	require.Contains(t, content, "type GetCatalog200JSONResponse = GenGetCatalog200JSONResponse")
	require.Contains(t, content, "type GenGetCatalog404JSONResponse struct{ GenNotFoundJSONResponse }")
	require.Contains(t, content, "w.WriteHeader(404)")
	require.Contains(t, content, "type ListSchemas200JSONResponse = GenListSchemas200JSONResponse")
	require.Contains(t, content, "type CreateSchema201JSONResponse = GenCreateSchema201JSONResponse")
	require.Contains(t, content, "type GetSchema200JSONResponse = GenGetSchema200JSONResponse")
	require.Contains(t, content, "type UpdateSchema200JSONResponse = GenUpdateSchema200JSONResponse")
	require.Contains(t, content, "type DeleteSchema204Response = GenDeleteSchema204Response")
	require.Contains(t, content, "type ListTables200JSONResponse = GenListTables200JSONResponse")
	require.Contains(t, content, "type CreateTable201JSONResponse = GenCreateTable201JSONResponse")
	require.Contains(t, content, "type GetTable200JSONResponse = GenGetTable200JSONResponse")
	require.Contains(t, content, "type UpdateTable200JSONResponse = GenUpdateTable200JSONResponse")
	require.Contains(t, content, "type DeleteTable204Response = GenDeleteTable204Response")
	require.Contains(t, content, "type ListTableColumns200JSONResponse = GenListTableColumns200JSONResponse")
	require.Contains(t, content, "type UpdateColumn200JSONResponse = GenUpdateColumn200JSONResponse")
	require.Contains(t, content, "type ProfileTable200JSONResponse = GenProfileTable200JSONResponse")
	require.Contains(t, content, "type GetMetastoreSummary200JSONResponse = GenGetMetastoreSummary200JSONResponse")

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
	require.Contains(t, content, "type ListPipelines200JSONResponse = GenListPipelines200JSONResponse")
	require.Contains(t, content, "type CreatePipeline201JSONResponse = GenCreatePipeline201JSONResponse")
	require.Contains(t, content, "type GenCreatePipeline400JSONResponse struct{ GenBadRequestJSONResponse }")
	require.Contains(t, content, "w.WriteHeader(400)")
	require.Contains(t, content, "type GetPipeline200JSONResponse = GenGetPipeline200JSONResponse")
	require.Contains(t, content, "type GenGetPipeline404JSONResponse struct{ GenNotFoundJSONResponse }")
	require.Contains(t, content, "type UpdatePipeline200JSONResponse = GenUpdatePipeline200JSONResponse")
	require.Contains(t, content, "type DeletePipeline204Response = GenDeletePipeline204Response")
	require.Contains(t, content, "type ListPipelineJobs200JSONResponse = GenListPipelineJobs200JSONResponse")
	require.Contains(t, content, "type CreatePipelineJob201JSONResponse = GenCreatePipelineJob201JSONResponse")
	require.Contains(t, content, "type GenCreatePipelineJob409JSONResponse struct{ GenConflictJSONResponse }")
	require.Contains(t, content, "w.WriteHeader(409)")
	require.Contains(t, content, "type DeletePipelineJob204Response = GenDeletePipelineJob204Response")
	require.Contains(t, content, "type TriggerPipelineRun201JSONResponse = GenTriggerPipelineRun201JSONResponse")
	require.Contains(t, content, "type GenTriggerPipelineRun404JSONResponse struct{ GenNotFoundJSONResponse }")
	require.Contains(t, content, "type ListPipelineRuns200JSONResponse = GenListPipelineRuns200JSONResponse")
	require.Contains(t, content, "type GetPipelineRun200JSONResponse = GenGetPipelineRun200JSONResponse")
	require.Contains(t, content, "type CancelPipelineRun200JSONResponse = GenCancelPipelineRun200JSONResponse")
	require.Contains(t, content, "type ListPipelineJobRuns200JSONResponse = GenListPipelineJobRuns200JSONResponse")

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

	require.Contains(t, content, "type ListCatalogs200JSONResponse = GenListCatalogs200JSONResponse")
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

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type ListModels200JSONResponse = GenListModels200JSONResponse")
	require.Contains(t, content, "func (response GenListModels200JSONResponse) VisitListModelsResponse(w http.ResponseWriter) error {")
	require.Contains(t, content, "w.WriteHeader(200)")
	require.Contains(t, content, "return json.NewEncoder(w).Encode(")

	require.Contains(t, content, "type ListSemanticModels200JSONResponse = GenListSemanticModels200JSONResponse")
	require.Contains(t, content, "func (response GenListSemanticModels200JSONResponse) VisitListSemanticModelsResponse(w http.ResponseWriter) error {")

	require.Contains(t, content, "type ListCatalogs200JSONResponse = GenListCatalogs200JSONResponse")
	require.Contains(t, content, "func (response GenListCatalogs200JSONResponse) VisitListCatalogsResponse(w http.ResponseWriter) error {")
}

func TestEmitWithLegacyResponses_OwnsDirectSchemaResponses(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Schemas: map[string]ir.Schema{
			"PaginatedSemanticModels": {},
			"SemanticModel":           {},
			"Model":                   {},
		},
		Endpoints: []ir.Endpoint{
			{Method: "get", Path: "/semantic/models", OperationID: "listSemanticModels", Responses: []ir.Response{{StatusCode: 200, Schema: &ir.SchemaRef{Ref: "PaginatedSemanticModels"}}}},
			{Method: "post", Path: "/semantic/models", OperationID: "createSemanticModel", Responses: []ir.Response{{StatusCode: 201, Schema: &ir.SchemaRef{Ref: "SemanticModel"}}}},
			{Method: "post", Path: "/models", OperationID: "createModel", Responses: []ir.Response{{StatusCode: 201, Schema: &ir.SchemaRef{Ref: "Model"}}}},
		},
	}

	b, err := EmitWithLegacyResponses(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenListSemanticModels200JSONResponse struct {")
	require.Contains(t, content, "\tBody GenSchemaPaginatedSemanticModels")
	require.Contains(t, content, "type GenCreateSemanticModel201JSONResponse struct {")
	require.Contains(t, content, "\tBody GenSchemaSemanticModel")
	require.Contains(t, content, "return json.NewEncoder(w).Encode(response.Body)")
	require.Contains(t, content, "type GenCreateModel201JSONResponse struct {")
	require.Contains(t, content, "\tBody GenSchemaModel")
}

func TestEmitWithLegacyResponses_OwnsWrappedResponseStructs(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{Method: "post", Path: "/widgets", OperationID: "createWidget", Responses: []ir.Response{{StatusCode: 201, Headers: []ir.Header{{Name: "X-RateLimit-Limit", Schema: ir.SchemaRef{Type: "integer", Format: "int32"}}, {Name: "X-RateLimit-Remaining", Schema: ir.SchemaRef{Type: "integer", Format: "int32"}}, {Name: "X-RateLimit-Reset", Schema: ir.SchemaRef{Type: "integer", Format: "int64"}}}, Schema: &ir.SchemaRef{Type: "string"}, Extensions: map[string]any{ir.ResponseShapeExtensionKey: map[string]any{"kind": "wrapped_json", "body_type": "CreateWidgetResponse"}}}}},
		},
	}

	b, err := EmitWithLegacyResponses(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenCreateWidget201ResponseHeaders struct {")
	require.Contains(t, content, "\tXRateLimitLimit int32")
	require.Contains(t, content, "\tXRateLimitRemaining int32")
	require.Contains(t, content, "\tXRateLimitReset int64")
	require.Contains(t, content, "type GenCreateWidget201JSONResponse struct {")
	require.Contains(t, content, "\tBody CreateWidgetResponse")
	require.Contains(t, content, "\tHeaders GenCreateWidget201ResponseHeaders")
	require.Contains(t, content, "w.Header().Set(\"X-RateLimit-Limit\", fmt.Sprint(response.Headers.XRateLimitLimit))")
	require.Contains(t, content, "return json.NewEncoder(w).Encode(response.Body)")
	require.NotContains(t, content, "type GenCreateWidget201JSONResponse CreateWidget201JSONResponse")
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

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type ListNotebooks200JSONResponse = GenListNotebooks200JSONResponse")
	require.Contains(t, content, "func (response GenListNotebooks200JSONResponse) VisitListNotebooksResponse(w http.ResponseWriter) error {")

	require.Contains(t, content, "type DeleteNotebook204Response = GenDeleteNotebook204Response")
	require.Contains(t, content, "func (response GenDeleteNotebook204Response) VisitDeleteNotebookResponse(w http.ResponseWriter) error {")
	require.Contains(t, content, "w.WriteHeader(204)")
	require.Contains(t, content, "return nil")

	require.Contains(t, content, "type ExecuteCell200JSONResponse GenExecuteCell201JSONResponse")
	require.Contains(t, content, "type GenExecuteCell200JSONResponse = ExecuteCell200JSONResponse")
	require.Contains(t, content, "func (response ExecuteCell200JSONResponse) VisitExecuteCellResponse(w http.ResponseWriter) error {")
	require.Contains(t, content, "type RunAllCellsAsync202JSONResponse = GenRunAllCellsAsync202JSONResponse")
	require.Contains(t, content, "w.WriteHeader(202)")

	require.Contains(t, content, "type ListGitRepos200JSONResponse = GenListGitRepos200JSONResponse")
	require.Contains(t, content, "type SyncGitRepo200JSONResponse GenSyncGitRepo201JSONResponse")
	require.Contains(t, content, "type GenSyncGitRepo200JSONResponse = SyncGitRepo200JSONResponse")
	require.Contains(t, content, "func (response SyncGitRepo200JSONResponse) VisitSyncGitRepoResponse(w http.ResponseWriter) error {")

	require.Contains(t, content, "type ListCatalogs200JSONResponse = GenListCatalogs200JSONResponse")
}

func TestEmit_UsesLegacyConcreteResponseTypesForKnownStatusDrift(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{Method: "post", Path: "/queries", OperationID: "submitQuery", Responses: []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/SubmitQueryResponse"}}}},
			{Method: "post", Path: "/queries/{queryId}/cancel", OperationID: "cancelQuery", Responses: []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/CancelQueryResponse"}}}},
			{Method: "post", Path: "/security/column-masks/{maskName}/bindings", OperationID: "bindColumnMask", Responses: []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Type: "string"}}}},
		},
	}

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type SubmitQuery202JSONResponse GenSubmitQuery201JSONResponse")
	require.Contains(t, content, "type GenSubmitQuery202JSONResponse = SubmitQuery202JSONResponse")
	require.Contains(t, content, "func (response SubmitQuery202JSONResponse) VisitSubmitQueryResponse(w http.ResponseWriter) error {")
	require.Contains(t, content, "type CancelQuery200JSONResponse GenCancelQuery201JSONResponse")
	require.Contains(t, content, "type GenCancelQuery200JSONResponse = CancelQuery200JSONResponse")
	require.NotContains(t, content, "type CancelQuery200JSONResponse struct {\n\tBody any")
	require.Contains(t, content, "type BindColumnMask204Response struct {")
	require.Contains(t, content, "\tHeaders BindColumnMask204ResponseHeaders")
	require.Contains(t, content, "type GenBindColumnMask204Response = BindColumnMask204Response")
	require.Contains(t, content, "func (response BindColumnMask204Response) VisitBindColumnMaskResponse(w http.ResponseWriter) error {")
	require.Contains(t, content, "w.WriteHeader(204)")
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

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type CreateGrant201JSONResponse = GenCreateGrant201JSONResponse")
	require.Contains(t, content, "func (response GenCreateGrant201JSONResponse) VisitCreateGrantResponse(w http.ResponseWriter) error {")
	require.Contains(t, content, "w.WriteHeader(201)")
	require.Contains(t, content, "return json.NewEncoder(w).Encode(")
	require.NotContains(t, content, "type GenCreateGrant201JSONResponse = CreateGrant201JSONResponse")

	require.Contains(t, content, "type GetColumnLineage200JSONResponse = GenGetColumnLineage200JSONResponse")
	require.Contains(t, content, "func (response GenGetColumnLineage200JSONResponse) VisitGetColumnLineageResponse(w http.ResponseWriter) error {")
	require.NotContains(t, content, "type GenGetColumnLineage200JSONResponse = GetColumnLineage200JSONResponse")

	require.Contains(t, content, "type CreateMacro201JSONResponse = GenCreateMacro201JSONResponse")
	require.NotContains(t, content, "type GenCreateMacro201JSONResponse = CreateMacro201JSONResponse")

	require.Contains(t, content, "type RegisterCatalog201JSONResponse = GenRegisterCatalog201JSONResponse")
	require.NotContains(t, content, "type GenRegisterCatalog201JSONResponse = RegisterCatalog201JSONResponse")

	require.Contains(t, content, "type ListTags200JSONResponse = GenListTags200JSONResponse")
	require.NotContains(t, content, "type GenListTags200JSONResponse = ListTags200JSONResponse")

	require.Contains(t, content, "type ListRowFilters200JSONResponse = GenListRowFilters200JSONResponse")
	require.NotContains(t, content, "type GenListRowFilters200JSONResponse = ListRowFilters200JSONResponse")

	require.Contains(t, content, "type ListCatalogs200JSONResponse = GenListCatalogs200JSONResponse")
	require.NotContains(t, content, "type GenListCatalogs200JSONResponse = ListCatalogs200JSONResponse")
}

func TestEmit_GeneratesNativeConcreteResponsesForSelectorGapOps(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{Method: "get", Path: "/catalogs", OperationID: "listCatalogs", Responses: []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/CatalogRegistrationList"}}}},
			{Method: "get", Path: "/compute/endpoints", OperationID: "listComputeEndpoints", Responses: []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedComputeEndpoints"}}}},
			{Method: "post", Path: "/compute/endpoints", OperationID: "createComputeEndpoint", Responses: []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/ComputeEndpoint"}}}},
			{Method: "get", Path: "/compute/endpoints/{endpointId}", OperationID: "getComputeEndpoint", Responses: []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/ComputeEndpoint"}}}},
			{Method: "patch", Path: "/compute/endpoints/{endpointId}", OperationID: "updateComputeEndpoint", Responses: []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/ComputeEndpoint"}}}},
			{Method: "delete", Path: "/compute/endpoints/{endpointId}", OperationID: "deleteComputeEndpoint", Responses: []ir.Response{{StatusCode: 204, Description: "no content"}}},
			{Method: "get", Path: "/compute/assignments", OperationID: "listComputeAssignments", Responses: []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedComputeAssignments"}}}},
			{Method: "post", Path: "/compute/assignments", OperationID: "createComputeAssignment", Responses: []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/ComputeAssignment"}}}},
			{Method: "delete", Path: "/compute/assignments/{assignmentId}", OperationID: "deleteComputeAssignment", Responses: []ir.Response{{StatusCode: 204, Description: "no content"}}},
			{Method: "get", Path: "/compute/endpoints/{endpointId}/health", OperationID: "getComputeEndpointHealth", Responses: []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/ComputeEndpointHealth"}}}},
			{Method: "get", Path: "/groups/{groupId}/members", OperationID: "listGroupMembers", Responses: []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/PaginatedGroupMembers"}}}},
			{Method: "post", Path: "/groups/{groupId}/members", OperationID: "createGroupMember", Responses: []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/GroupMember"}}}},
			{Method: "delete", Path: "/groups/{groupId}/members/{principalId}", OperationID: "deleteGroupMember", Responses: []ir.Response{{StatusCode: 204, Description: "no content"}}},
			{Method: "post", Path: "/manifests", OperationID: "createManifest", Responses: []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/Manifest"}}}},
			{Method: "patch", Path: "/principals/{principalId}", OperationID: "updatePrincipal", Responses: []ir.Response{{StatusCode: 200, Description: "ok", Schema: &ir.SchemaRef{Ref: "#/schemas/Principal"}}}},
		},
	}

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type ListCatalogs200JSONResponse = GenListCatalogs200JSONResponse")
	require.Contains(t, content, "func (response GenListCatalogs200JSONResponse) VisitListCatalogsResponse(w http.ResponseWriter) error {")
	require.NotContains(t, content, "type GenListCatalogs200JSONResponse = ListCatalogs200JSONResponse")

	require.Contains(t, content, "type ListComputeEndpoints200JSONResponse = GenListComputeEndpoints200JSONResponse")
	require.Contains(t, content, "func (response GenListComputeEndpoints200JSONResponse) VisitListComputeEndpointsResponse(w http.ResponseWriter) error {")
	require.NotContains(t, content, "type GenListComputeEndpoints200JSONResponse = ListComputeEndpoints200JSONResponse")

	require.Contains(t, content, "type CreateComputeEndpoint201JSONResponse = GenCreateComputeEndpoint201JSONResponse")
	require.Contains(t, content, "func (response GenCreateComputeEndpoint201JSONResponse) VisitCreateComputeEndpointResponse(w http.ResponseWriter) error {")
	require.NotContains(t, content, "type GenCreateComputeEndpoint201JSONResponse = CreateComputeEndpoint201JSONResponse")

	require.Contains(t, content, "type GetComputeEndpoint200JSONResponse = GenGetComputeEndpoint200JSONResponse")
	require.NotContains(t, content, "type GenGetComputeEndpoint200JSONResponse = GetComputeEndpoint200JSONResponse")

	require.Contains(t, content, "type UpdateComputeEndpoint200JSONResponse = GenUpdateComputeEndpoint200JSONResponse")
	require.NotContains(t, content, "type GenUpdateComputeEndpoint200JSONResponse = UpdateComputeEndpoint200JSONResponse")

	require.Contains(t, content, "type DeleteComputeEndpoint204Response = GenDeleteComputeEndpoint204Response")
	require.Contains(t, content, "w.WriteHeader(204)")
	require.Contains(t, content, "return nil")
	require.NotContains(t, content, "type GenDeleteComputeEndpoint204Response = DeleteComputeEndpoint204Response")

	require.Contains(t, content, "type ListComputeAssignments200JSONResponse = GenListComputeAssignments200JSONResponse")
	require.NotContains(t, content, "type GenListComputeAssignments200JSONResponse = ListComputeAssignments200JSONResponse")

	require.Contains(t, content, "type CreateComputeAssignment201JSONResponse = GenCreateComputeAssignment201JSONResponse")
	require.NotContains(t, content, "type GenCreateComputeAssignment201JSONResponse = CreateComputeAssignment201JSONResponse")

	require.Contains(t, content, "type DeleteComputeAssignment204Response = GenDeleteComputeAssignment204Response")
	require.NotContains(t, content, "type GenDeleteComputeAssignment204Response = DeleteComputeAssignment204Response")

	require.Contains(t, content, "type GetComputeEndpointHealth200JSONResponse = GenGetComputeEndpointHealth200JSONResponse")
	require.NotContains(t, content, "type GenGetComputeEndpointHealth200JSONResponse = GetComputeEndpointHealth200JSONResponse")

	require.Contains(t, content, "type ListGroupMembers200JSONResponse = GenListGroupMembers200JSONResponse")
	require.NotContains(t, content, "type GenListGroupMembers200JSONResponse = ListGroupMembers200JSONResponse")

	require.Contains(t, content, "type GenCreateGroupMember201JSONResponse struct {")
	require.Contains(t, content, "type GenCreateGroupMember204Response = CreateGroupMember204Response")

	require.Contains(t, content, "type DeleteGroupMember204Response = GenDeleteGroupMember204Response")
	require.NotContains(t, content, "type GenDeleteGroupMember204Response = DeleteGroupMember204Response")

	require.Contains(t, content, "type CreateManifest200JSONResponse GenCreateManifest201JSONResponse")
	require.Contains(t, content, "type GenCreateManifest200JSONResponse = CreateManifest200JSONResponse")
	require.NotContains(t, content, "type GenCreateManifest201JSONResponse = CreateManifest201JSONResponse")

	require.Contains(t, content, "type UpdatePrincipal200JSONResponse = GenUpdatePrincipal200JSONResponse")
	require.Contains(t, content, "func (response GenUpdatePrincipal200JSONResponse) VisitUpdatePrincipalResponse(w http.ResponseWriter) error {")
	require.Contains(t, content, "return json.NewEncoder(w).Encode(")
	require.NotContains(t, content, "type GenUpdatePrincipal200JSONResponse = UpdatePrincipal200JSONResponse")
}

func TestEmit_WritesNativeHeadersWithoutLegacyVisitFallback(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{
			{
				Method:      "get",
				Path:        "/api-keys",
				OperationID: "listAPIKeys",
				Responses: []ir.Response{{
					StatusCode:  429,
					Description: "rate limited",
					Headers:     []ir.Header{{Name: "Retry-After", Schema: ir.SchemaRef{Type: "integer", Format: "int32"}}, {Name: "X-RateLimit-Limit", Schema: ir.SchemaRef{Type: "integer", Format: "int32"}}, {Name: "X-RateLimit-Remaining", Schema: ir.SchemaRef{Type: "integer", Format: "int32"}}, {Name: "X-RateLimit-Reset", Schema: ir.SchemaRef{Type: "integer", Format: "int64"}}},
					Schema:      &ir.SchemaRef{Ref: "#/schemas/Error"},
				}},
			},
			{
				Method:      "post",
				Path:        "/query",
				OperationID: "executeQuery",
				Responses:   []ir.Response{{StatusCode: 201, Description: "created", Schema: &ir.SchemaRef{Ref: "#/schemas/QueryResult"}}},
			},
		},
	}

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenRateLimitExceededResponseHeaders struct {")
	require.Contains(t, content, "type GenListAPIKeys429JSONResponse struct{ GenRateLimitExceededJSONResponse }")
	require.Contains(t, content, "headers := rv.FieldByName(\"Headers\")")
	require.Contains(t, content, "type ExecuteQuery200JSONResponse GenExecuteQuery201JSONResponse")
	require.Contains(t, content, "func (response ExecuteQuery200JSONResponse) VisitExecuteQueryResponse(w http.ResponseWriter) error {")
	require.Contains(t, content, "w.WriteHeader(200)")
	require.NotContains(t, content, "return ExecuteQuery201JSONResponse(response).VisitExecuteQueryResponse(w)")
	require.NotContains(t, content, "return ListAPIKeys429JSONResponse(response).VisitListAPIKeysResponse(w)")
}

func TestEmit_UsesIRResponseHeadersForVisitMethods(t *testing.T) {
	t.Helper()

	doc := ir.Document{
		SchemaVersion: "v1",
		Info:          ir.Info{Title: "t", Version: "1"},
		Endpoints: []ir.Endpoint{{
			Method:      "get",
			Path:        "/widgets/{id}",
			OperationID: "getWidget",
			Responses: []ir.Response{{
				StatusCode:  200,
				Description: "ok",
				Headers: []ir.Header{{
					Name:   "X-Trace-Id",
					Schema: ir.SchemaRef{Type: "string"},
				}},
				Schema: &ir.SchemaRef{Type: "string"},
			}},
		}},
	}

	b, err := Emit(doc, Options{})
	require.NoError(t, err)
	content := string(b)

	require.Contains(t, content, "type GenGetWidget200ResponseHeaders struct {")
	require.Contains(t, content, "\tXTraceId string")
	require.Contains(t, content, "w.Header().Set(\"X-Trace-Id\", fmt.Sprint(response.Headers.XTraceId))")
	require.Contains(t, content, "return json.NewEncoder(w).Encode(response.Body)")
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
