// Package servergo emits Go server scaffolding from JSON IR.
package servergo

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	openapiemit "duck-demo/internal/apigen/emit/openapi"
	"duck-demo/internal/apigen/ir"
	"go.yaml.in/yaml/v4"
)

// Emit renders Go server scaffolding from IR.
func Emit(doc ir.Document) ([]byte, error) {
	specJSON, err := emitSpecJSON(doc)
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	usesTime := docUsesTimeTypes(doc)
	hasStrictOperations := false
	for _, endpoint := range doc.Endpoints {
		if endpoint.OperationID != "getHealth" {
			hasStrictOperations = true
			break
		}
	}
	b.WriteString("package api\n\n")
	b.WriteString("import (\n")
	if hasStrictOperations {
		b.WriteString("\t\"context\"\n")
		b.WriteString("\t\"fmt\"\n")
		b.WriteString("\t\"reflect\"\n")
	}
	b.WriteString("\t\"encoding/json\"\n")
	b.WriteString("\t\"net/http\"\n\n")
	if usesTime {
		b.WriteString("\t\"time\"\n\n")
	}
	b.WriteString("\t\"github.com/go-chi/chi/v5\"\n")
	b.WriteString(")\n\n")
	b.WriteString("const apigenOpenAPISpecJSON = `")
	b.WriteString(specJSON)
	b.WriteString("`\n\n")
	b.WriteString("// GetAPIGenSwagger returns the generated OpenAPI document as generic JSON map.\n")
	b.WriteString("func GetAPIGenSwagger() (map[string]any, error) {\n")
	b.WriteString("\tvar doc map[string]any\n")
	b.WriteString("\tif err := json.Unmarshal([]byte(apigenOpenAPISpecJSON), &doc); err != nil {\n")
	b.WriteString("\t\treturn nil, err\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn doc, nil\n")
	b.WriteString("}\n\n")
	b.WriteString("// GenServerInterface dispatches generated operations.\n")
	b.WriteString("type GenServerInterface interface {\n")
	b.WriteString("\tHandleAPIGen(operationID string, w http.ResponseWriter, r *http.Request)\n")
	b.WriteString("}\n\n")
	b.WriteString("// RegisterAPIGenRoutes mounts generated routes on Chi router.\n")
	b.WriteString("func RegisterAPIGenRoutes(router chi.Router, server GenServerInterface) {\n")
	for _, endpoint := range doc.Endpoints {
		method := strings.ToUpper(endpoint.Method)
		b.WriteString("\trouter.MethodFunc(\"" + method + "\", \"" + endpoint.Path + "\", func(w http.ResponseWriter, r *http.Request) {\n")
		b.WriteString("\t\tserver.HandleAPIGen(\"" + endpoint.OperationID + "\", w, r)\n")
		b.WriteString("\t})\n")
	}
	b.WriteString("}\n")
	b.WriteString("\n")
	b.WriteString("// GenOperationDispatcher is the dispatch target for generated operations.\n")
	b.WriteString("type GenOperationDispatcher interface {\n")
	for _, endpoint := range doc.Endpoints {
		if endpoint.OperationID == "getHealth" {
			continue
		}
		name := exportedName(endpoint.OperationID)
		signature := "\t" + name + "(w http.ResponseWriter, r *http.Request"
		for _, p := range endpointPathParams(endpoint) {
			signature += ", " + lowerCamelName(p.Name) + " " + pathParamTypeName(p)
		}
		queryParams := endpointQueryParams(endpoint)
		if len(queryParams) > 0 {
			signature += ", params Gen" + name + "Params"
		}
		signature += ")\n"
		b.WriteString(signature)
	}
	b.WriteString("}\n\n")
	b.WriteString("// DispatchAPIGenOperation dispatches operation IDs to generated wrapper methods.\n")
	b.WriteString("func DispatchAPIGenOperation(operationID string, dispatcher GenOperationDispatcher, w http.ResponseWriter, r *http.Request) bool {\n")
	b.WriteString("\tswitch operationID {\n")
	for _, endpoint := range doc.Endpoints {
		name := exportedName(endpoint.OperationID)
		b.WriteString("\tcase \"" + endpoint.OperationID + "\":\n")
		if endpoint.OperationID == "getHealth" {
			b.WriteString("\t\tw.Header().Set(\"Content-Type\", \"application/json\")\n")
			b.WriteString("\t\tw.WriteHeader(http.StatusOK)\n")
			b.WriteString("\t\t_ = json.NewEncoder(w).Encode(map[string]string{\"status\": \"ok\"})\n")
			b.WriteString("\t\treturn true\n")
			continue
		}

		pathParams := endpointPathParams(endpoint)
		queryParams := endpointQueryParams(endpoint)
		if len(pathParams) > 0 || len(queryParams) > 0 {
			b.WriteString("\t\tvar err error\n")
		}

		for _, p := range pathParams {
			varName := lowerCamelName(p.Name)
			typeName := pathParamTypeName(p)
			required := "false"
			if p.Required {
				required = "true"
			}
			b.WriteString("\t\tvar " + varName + " " + typeName + "\n")
			b.WriteString("\t\terr = bindPathParameter(\"" + p.Name + "\", chi.URLParam(r, \"" + p.Name + "\"), " + required + ", &" + varName + ")\n")
			b.WriteString("\t\tif err != nil {\n")
			b.WriteString("\t\t\thttp.Error(w, err.Error(), http.StatusBadRequest)\n")
			b.WriteString("\t\t\treturn true\n")
			b.WriteString("\t\t}\n")
		}

		if len(queryParams) > 0 {
			b.WriteString("\t\tvar params Gen" + name + "Params\n")
			for _, p := range queryParams {
				fieldName := exportedName(p.Name)
				required := "false"
				if p.Required {
					required = "true"
				}
				b.WriteString("\t\terr = bindQueryParameter(r.URL.Query(), \"" + p.Name + "\", " + required + ", &params." + fieldName + ")\n")
				b.WriteString("\t\tif err != nil {\n")
				b.WriteString("\t\t\thttp.Error(w, err.Error(), http.StatusBadRequest)\n")
				b.WriteString("\t\t\treturn true\n")
				b.WriteString("\t\t}\n")
			}
		}

		call := "\t\tdispatcher." + name + "(w, r"
		for _, p := range pathParams {
			call += ", " + lowerCamelName(p.Name)
		}
		if len(queryParams) > 0 {
			call += ", params"
		}
		call += ")\n"
		b.WriteString(call)
		b.WriteString("\t\treturn true\n")
	}
	b.WriteString("\tdefault:\n")
	b.WriteString("\t\treturn false\n")
	b.WriteString("\t}\n")
	b.WriteString("}\n")
	b.WriteString("\n")
	for _, endpoint := range doc.Endpoints {
		if endpoint.OperationID == "getHealth" {
			continue
		}
		name := exportedName(endpoint.OperationID)
		pathParams := endpointPathParams(endpoint)
		queryParams := endpointQueryParams(endpoint)
		if len(queryParams) > 0 {
			b.WriteString("// Gen" + name + "Params represents the APIGen strict query parameter contract for " + name + ".\n")
			b.WriteString("type Gen" + name + "Params struct {\n")
			for _, p := range queryParams {
				fieldType := parameterTypeName(endpoint, p)
				if !p.Required {
					fieldType = "*" + fieldType
				}
				b.WriteString("\t" + exportedName(p.Name) + " " + fieldType + "\n")
			}
			b.WriteString("}\n\n")
		}
		b.WriteString("// Gen" + name + "Request represents the APIGen strict request contract for " + name + ".\n")
		b.WriteString("type Gen" + name + "Request struct {\n")
		for _, p := range pathParams {
			b.WriteString("\t" + exportedName(p.Name) + " " + pathParamTypeName(p) + "\n")
		}
		if len(queryParams) > 0 {
			b.WriteString("\tParams Gen" + name + "Params\n")
		}
		if endpoint.RequestBody != nil {
			b.WriteString("\tBody *Gen" + name + "JSONBody\n")
		}
		b.WriteString("}\n\n")
		b.WriteString("// Gen" + name + "Response represents the APIGen strict response contract for " + name + ".\n")
		b.WriteString("type Gen" + name + "Response interface {\n")
		b.WriteString("\tVisit" + name + "Response(w http.ResponseWriter) error\n")
		b.WriteString("}\n\n")
		for _, response := range endpoint.Responses {
			statusCode := fmt.Sprintf("%d", response.StatusCode)
			responseTypeName := responseTypeName(endpoint.OperationID, response)
			legacyNoBodyJSONResponse := isLegacyNoBodyJSONResponse(endpoint.OperationID, response)
			if response.Schema != nil {
				b.WriteString("// Gen" + name + statusCode + "JSONResponse is the APIGen concrete JSON response for " + name + " " + statusCode + ".\n")
				b.WriteString("type Gen" + name + statusCode + "JSONResponse " + responseTypeName + "\n\n")
				b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCode + " responses to the client.\n")
				b.WriteString("func (response Gen" + name + statusCode + "JSONResponse) Visit" + name + "Response(w http.ResponseWriter) error {\n")
				b.WriteString("\trv := reflect.ValueOf(response)\n")
				b.WriteString("\theaders := rv.FieldByName(\"Headers\")\n")
				b.WriteString("\tif headers.IsValid() {\n")
				b.WriteString("\t\tif v := headers.FieldByName(\"RetryAfter\"); v.IsValid() {\n")
				b.WriteString("\t\t\tw.Header().Set(\"Retry-After\", fmt.Sprint(v.Interface()))\n")
				b.WriteString("\t\t}\n")
				b.WriteString("\t\tif v := headers.FieldByName(\"XRateLimitLimit\"); v.IsValid() {\n")
				b.WriteString("\t\t\tw.Header().Set(\"X-RateLimit-Limit\", fmt.Sprint(v.Interface()))\n")
				b.WriteString("\t\t}\n")
				b.WriteString("\t\tif v := headers.FieldByName(\"XRateLimitRemaining\"); v.IsValid() {\n")
				b.WriteString("\t\t\tw.Header().Set(\"X-RateLimit-Remaining\", fmt.Sprint(v.Interface()))\n")
				b.WriteString("\t\t}\n")
				b.WriteString("\t\tif v := headers.FieldByName(\"XRateLimitReset\"); v.IsValid() {\n")
				b.WriteString("\t\t\tw.Header().Set(\"X-RateLimit-Reset\", fmt.Sprint(v.Interface()))\n")
				b.WriteString("\t\t}\n")
				b.WriteString("\t}\n")
				b.WriteString("\tbody := rv.FieldByName(\"Body\")\n")
				b.WriteString("\tif !body.IsValid() {\n")
				if legacyNoBodyJSONResponse {
					b.WriteString("\t\tw.WriteHeader(" + statusCode + ")\n")
					b.WriteString("\t\treturn nil\n")
				} else {
					b.WriteString("\t\treturn fmt.Errorf(\"apigen: " + name + " " + statusCode + " response body missing\")\n")
				}
				b.WriteString("\t}\n")
				b.WriteString("\tw.Header().Set(\"Content-Type\", \"application/json\")\n")
				b.WriteString("\tw.WriteHeader(" + statusCode + ")\n")
				b.WriteString("\treturn json.NewEncoder(w).Encode(body.Interface())\n")
				b.WriteString("}\n\n")
				continue
			}

			b.WriteString("// Gen" + name + statusCode + "Response is the APIGen concrete response for " + name + " " + statusCode + ".\n")
			b.WriteString("type Gen" + name + statusCode + "Response " + responseTypeName + "\n\n")
			b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCode + " responses to the client.\n")
			b.WriteString("func (response Gen" + name + statusCode + "Response) Visit" + name + "Response(w http.ResponseWriter) error {\n")
			b.WriteString("\trv := reflect.ValueOf(response)\n")
			b.WriteString("\theaders := rv.FieldByName(\"Headers\")\n")
			b.WriteString("\tif headers.IsValid() {\n")
			b.WriteString("\t\tif v := headers.FieldByName(\"RetryAfter\"); v.IsValid() {\n")
			b.WriteString("\t\t\tw.Header().Set(\"Retry-After\", fmt.Sprint(v.Interface()))\n")
			b.WriteString("\t\t}\n")
			b.WriteString("\t\tif v := headers.FieldByName(\"XRateLimitLimit\"); v.IsValid() {\n")
			b.WriteString("\t\t\tw.Header().Set(\"X-RateLimit-Limit\", fmt.Sprint(v.Interface()))\n")
			b.WriteString("\t\t}\n")
			b.WriteString("\t\tif v := headers.FieldByName(\"XRateLimitRemaining\"); v.IsValid() {\n")
			b.WriteString("\t\t\tw.Header().Set(\"X-RateLimit-Remaining\", fmt.Sprint(v.Interface()))\n")
			b.WriteString("\t\t}\n")
			b.WriteString("\t\tif v := headers.FieldByName(\"XRateLimitReset\"); v.IsValid() {\n")
			b.WriteString("\t\t\tw.Header().Set(\"X-RateLimit-Reset\", fmt.Sprint(v.Interface()))\n")
			b.WriteString("\t\t}\n")
			b.WriteString("\t}\n")
			b.WriteString("\tw.WriteHeader(" + statusCode + ")\n")
			b.WriteString("\treturn nil\n")
			b.WriteString("}\n\n")
		}
		if endpoint.RequestBody != nil {
			bodyTypeName, usesLegacyAlias := requestBodyTypeName(doc, endpoint)
			if usesLegacyAlias {
				b.WriteString("// Gen" + name + "JSONBody aliases the APIGen strict JSON request body contract for " + name + ".\n")
			} else {
				b.WriteString("// Gen" + name + "JSONBody aliases the APIGen strict JSON request body schema for " + name + ".\n")
			}
			b.WriteString("type Gen" + name + "JSONBody = " + bodyTypeName + "\n\n")
		}
	}

	b.WriteString("// GenStrictServerInterface represents strict handlers for APIGen transport dispatch.\n")
	b.WriteString("type GenStrictServerInterface interface {\n")
	for _, endpoint := range doc.Endpoints {
		if endpoint.OperationID == "getHealth" {
			continue
		}
		name := exportedName(endpoint.OperationID)
		b.WriteString("\t" + name + "(ctx context.Context, request Gen" + name + "Request) (Gen" + name + "Response, error)\n")
	}
	b.WriteString("}\n\n")

	b.WriteString("type genStrictBridge struct {\n")
	b.WriteString("\thandler GenStrictServerInterface\n")
	b.WriteString("}\n\n")

	for _, endpoint := range doc.Endpoints {
		if endpoint.OperationID == "getHealth" {
			continue
		}
		name := exportedName(endpoint.OperationID)
		pathParams := endpointPathParams(endpoint)
		queryParams := endpointQueryParams(endpoint)

		sig := "func (b genStrictBridge) " + name + "(w http.ResponseWriter, r *http.Request"
		for _, p := range pathParams {
			sig += ", " + lowerCamelName(p.Name) + " " + pathParamTypeName(p)
		}
		if len(queryParams) > 0 {
			sig += ", params Gen" + name + "Params"
		}
		sig += ") {\n"
		b.WriteString(sig)
		b.WriteString("\tvar request Gen" + name + "Request\n")

		for _, p := range pathParams {
			fieldName := exportedName(p.Name)
			paramName := lowerCamelName(p.Name)
			b.WriteString("\trequest." + fieldName + " = " + paramName + "\n")
		}
		if len(queryParams) > 0 {
			b.WriteString("\trequest.Params = params\n")
		}

		if endpoint.RequestBody != nil {
			b.WriteString("\tvar body Gen" + name + "JSONBody\n")
			b.WriteString("\tif err := json.NewDecoder(r.Body).Decode(&body); err != nil {\n")
			b.WriteString("\t\thttp.Error(w, err.Error(), http.StatusBadRequest)\n")
			b.WriteString("\t\treturn\n")
			b.WriteString("\t}\n")
			b.WriteString("\trequest.Body = &body\n")
		}

		b.WriteString("\tresponse, err := b.handler." + name + "(r.Context(), request)\n")
		b.WriteString("\tif err != nil {\n")
		b.WriteString("\t\thttp.Error(w, err.Error(), http.StatusInternalServerError)\n")
		b.WriteString("\t\treturn\n")
		b.WriteString("\t}\n")
		b.WriteString("\tif err := response.Visit" + name + "Response(w); err != nil {\n")
		b.WriteString("\t\thttp.Error(w, err.Error(), http.StatusInternalServerError)\n")
		b.WriteString("\t}\n")
		b.WriteString("}\n\n")
	}

	b.WriteString("// DispatchAPIGenStrictOperation dispatches to strict handlers without oapi strict wrappers.\n")
	b.WriteString("func DispatchAPIGenStrictOperation(operationID string, handler GenStrictServerInterface, w http.ResponseWriter, r *http.Request) bool {\n")
	b.WriteString("\treturn DispatchAPIGenOperation(operationID, genStrictBridge{handler: handler}, w, r)\n")
	b.WriteString("}\n")

	return []byte(b.String()), nil
}

func emitSpecJSON(docIR ir.Document) (string, error) {
	yamlBytes, err := openapiemit.EmitYAML(docIR)
	if err != nil {
		return "", fmt.Errorf("emit embedded openapi yaml: %w", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(yamlBytes, &doc); err != nil {
		return "", fmt.Errorf("decode emitted openapi yaml: %w", err)
	}
	jsonBytes, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("marshal embedded openapi json: %w", err)
	}
	return string(jsonBytes), nil
}

func exportedName(operationID string) string {
	parts := splitIdentifier(operationID)
	if len(parts) == 0 {
		return "Operation"
	}
	for i := range parts {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}

func splitIdentifier(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	replacer := strings.NewReplacer("-", " ", "_", " ", ".", " ", "/", " ")
	value = replacer.Replace(value)
	chunks := strings.Fields(value)
	if len(chunks) > 0 {
		return chunks
	}
	return []string{value}
}

func lowerCamelName(value string) string {
	parts := splitIdentifier(value)
	if len(parts) == 0 {
		return "value"
	}
	parts[0] = strings.ToLower(parts[0][:1]) + parts[0][1:]
	for i := 1; i < len(parts); i++ {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}

func endpointPathParams(endpoint ir.Endpoint) []ir.Parameter {
	var out []ir.Parameter
	for _, p := range endpoint.Parameters {
		if strings.EqualFold(p.In, "path") {
			out = append(out, p)
		}
	}
	return out
}

func endpointQueryParams(endpoint ir.Endpoint) []ir.Parameter {
	var out []ir.Parameter
	for _, p := range endpoint.Parameters {
		if strings.EqualFold(p.In, "query") {
			out = append(out, p)
		}
	}
	return out
}

func pathParamTypeName(param ir.Parameter) string {
	return schemaTypeName(param.Schema)
}

func parameterTypeName(endpoint ir.Endpoint, param ir.Parameter) string {
	if typeName, ok := legacyParameterTypeName(endpoint.OperationID, param.Name); ok {
		return typeName
	}
	return schemaTypeName(param.Schema)
}

func schemaTypeName(schema ir.SchemaRef) string {
	if schema.Ref != "" {
		return exportedName(schema.Ref)
	}

	schemaType := strings.ToLower(strings.TrimSpace(schema.Type))
	schemaFormat := strings.ToLower(strings.TrimSpace(schema.Format))

	switch schemaType {
	case "integer":
		switch schemaFormat {
		case "int32":
			return "int32"
		case "int64":
			return "int64"
		}
		return "int"
	case "number":
		switch schemaFormat {
		case "float", "float32":
			return "float32"
		case "double", "float64":
			return "float64"
		}
		return "float64"
	case "boolean", "bool":
		return "bool"
	case "string":
		switch schemaFormat {
		case "date-time":
			return "time.Time"
		}
		return "string"
	default:
		return "string"
	}
}

func docUsesTimeTypes(doc ir.Document) bool {
	for _, endpoint := range doc.Endpoints {
		for _, param := range endpoint.Parameters {
			if parameterTypeName(endpoint, param) == "time.Time" {
				return true
			}
		}
	}
	return false
}

func legacyParameterTypeName(operationID, paramName string) (string, bool) {
	key := operationID + ":" + paramName
	switch key {
	case "listQueryHistory:from", "listQueryHistory:to":
		return "time.Time", true
	default:
		return "", false
	}
}

func requestBodyTypeName(doc ir.Document, endpoint ir.Endpoint) (string, bool) {
	if endpoint.RequestBody == nil {
		return "", false
	}

	schema := endpoint.RequestBody.Schema
	if schema.Ref != "" && schema.Ref != "GenericRequest" {
		if _, ok := doc.Schemas[schema.Ref]; ok {
			return exportedName(schema.Ref), false
		}
	}
	if schema.Ref == "GenericRequest" {
		if schemaName, ok := resolveGenericRequestBodySchemaName(doc, endpoint.OperationID); ok {
			return schemaName, false
		}
	}
	if schema.Type != "" {
		return schemaTypeName(schema), false
	}

	name := exportedName(endpoint.OperationID)
	return name + "JSONRequestBody", true
}

func resolveGenericRequestBodySchemaName(doc ir.Document, operationID string) (string, bool) {
	if schemaName, ok := genericRequestBodySchemaOverrides[operationID]; ok {
		return schemaName, true
	}
	for _, candidate := range genericRequestBodySchemaCandidates(operationID) {
		if _, ok := doc.Schemas[candidate]; ok {
			return candidate, true
		}
	}
	return "", false
}

func genericRequestBodySchemaCandidates(operationID string) []string {
	return []string{exportedName(operationID) + "Request"}
}

var genericRequestBodySchemaOverrides = map[string]string{
	"bindColumnMask":               "ColumnMaskBindingRequest",
	"bindRowFilter":                "RowFilterBindingRequest",
	"commitTableIngestion":         "CommitIngestionRequest",
	"createCell":                   "CreateCellRequest",
	"createComputeAssignment":      "CreateComputeAssignmentRequest",
	"createComputeEndpoint":        "CreateComputeEndpointRequest",
	"createGitRepo":                "CreateGitRepoRequest",
	"createMacro":                  "CreateMacroRequest",
	"createManifest":               "ManifestRequest",
	"createModelTest":              "CreateModelTestRequest",
	"createNotebook":               "CreateNotebookRequest",
	"createPipeline":               "CreatePipelineRequest",
	"createPipelineJob":            "CreatePipelineJobRequest",
	"createSemanticMetric":         "CreateSemanticMetricRequest",
	"createSemanticModel":          "CreateSemanticModelRequest",
	"createSemanticPreAggregation": "CreateSemanticPreAggregationRequest",
	"createSemanticRelationship":   "CreateSemanticRelationshipRequest",
	"createTag":                    "CreateTagRequest",
	"createTagAssignment":          "CreateTagAssignmentRequest",
	"createUploadUrl":              "UploadUrlRequest",
	"executeQuery":                 "QueryRequest",
	"explainMetricQuery":           "MetricQueryRequest",
	"loadTableExternalFiles":       "LoadExternalRequest",
	"promoteNotebookToModel":       "PromoteNotebookRequest",
	"purgeLineage":                 "PurgeLineageRequest",
	"reorderCells":                 "ReorderCellsRequest",
	"runMetricQuery":               "MetricQueryRequest",
	"triggerModelRun":              "TriggerModelRunRequest",
	"triggerPipelineRun":           "TriggerPipelineRunRequest",
	"updateCell":                   "UpdateCellRequest",
	"updateComputeEndpoint":        "UpdateComputeEndpointRequest",
	"updateMacro":                  "UpdateMacroRequest",
	"updateModel":                  "UpdateModelRequest",
	"updateNotebook":               "UpdateNotebookRequest",
	"updatePipeline":               "UpdatePipelineRequest",
	"updateSemanticMetric":         "UpdateSemanticMetricRequest",
	"updateSemanticModel":          "UpdateSemanticModelRequest",
	"updateSemanticPreAggregation": "UpdateSemanticPreAggregationRequest",
	"updateSemanticRelationship":   "UpdateSemanticRelationshipRequest",
}

func responseTypeName(operationID string, response ir.Response) string {
	if typeName, ok := legacyResponseTypeName(operationID, response.StatusCode, response.Schema != nil); ok {
		return typeName
	}

	statusCode := fmt.Sprintf("%d", response.StatusCode)
	if response.Schema != nil {
		return exportedName(operationID) + statusCode + "JSONResponse"
	}
	return exportedName(operationID) + statusCode + "Response"
}

func legacyResponseTypeName(operationID string, statusCode int, hasSchema bool) (string, bool) {
	key := fmt.Sprintf("%s:%d:%t", operationID, statusCode, hasSchema)
	switch key {
	case "bindColumnMask:201:true":
		return "BindColumnMask204Response", true
	case "bindRowFilter:201:true":
		return "BindRowFilter204Response", true
	case "cancelModelRun:201:true":
		return "CancelModelRun200JSONResponse", true
	case "cancelPipelineRun:201:true":
		return "CancelPipelineRun200JSONResponse", true
	case "cancelQuery:201:true":
		return "CancelQuery200JSONResponse", true
	case "cleanupExpiredAPIKeys:201:true":
		return "CleanupExpiredAPIKeys200JSONResponse", true
	case "commitTableIngestion:201:true":
		return "CommitTableIngestion200JSONResponse", true
	case "createManifest:201:true":
		return "CreateManifest200JSONResponse", true
	case "createUploadUrl:201:true":
		return "CreateUploadUrl200JSONResponse", true
	case "executeCell:201:true":
		return "ExecuteCell200JSONResponse", true
	case "executeQuery:201:true":
		return "ExecuteQuery200JSONResponse", true
	case "explainMetricQuery:201:true":
		return "ExplainMetricQuery200JSONResponse", true
	case "loadTableExternalFiles:201:true":
		return "LoadTableExternalFiles200JSONResponse", true
	case "profileTable:201:true":
		return "ProfileTable200JSONResponse", true
	case "purgeLineage:201:true":
		return "PurgeLineage200JSONResponse", true
	case "reorderCells:201:true":
		return "ReorderCells200JSONResponse", true
	case "runAllCells:201:true":
		return "RunAllCells200JSONResponse", true
	case "runAllCellsAsync:201:true":
		return "RunAllCellsAsync202JSONResponse", true
	case "runMetricQuery:201:true":
		return "RunMetricQuery200JSONResponse", true
	case "setDefaultCatalog:201:true":
		return "SetDefaultCatalog200JSONResponse", true
	case "submitQuery:201:true":
		return "SubmitQuery202JSONResponse", true
	case "syncGitRepo:201:true":
		return "SyncGitRepo200JSONResponse", true
	default:
		return "", false
	}
}

func isLegacyNoBodyJSONResponse(operationID string, response ir.Response) bool {
	if response.Schema == nil {
		return false
	}

	key := fmt.Sprintf("%s:%d", operationID, response.StatusCode)
	switch key {
	case "bindColumnMask:201", "bindRowFilter:201":
		return true
	default:
		return false
	}
}

// ValidateOperationIDs checks for exported handler name collisions.
func ValidateOperationIDs(doc ir.Document) error {
	seen := make(map[string]string, len(doc.Endpoints))
	for _, endpoint := range doc.Endpoints {
		exported := exportedName(endpoint.OperationID)
		if prev, exists := seen[exported]; exists {
			return fmt.Errorf("operation name collision %q for %q and %q", exported, prev, endpoint.OperationID)
		}
		seen[exported] = endpoint.OperationID
	}
	return nil
}

// SortedOperationIDs returns operation IDs in deterministic order.
func SortedOperationIDs(doc ir.Document) []string {
	ids := make([]string, 0, len(doc.Endpoints))
	for _, endpoint := range doc.Endpoints {
		ids = append(ids, endpoint.OperationID)
	}
	sort.Strings(ids)
	return ids
}
