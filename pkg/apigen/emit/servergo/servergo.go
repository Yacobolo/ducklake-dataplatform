// Package servergo emits Go server scaffolding from JSON IR.
package servergo

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	openapiemit "github.com/Yacobolo/quackstack/pkg/apigen/emit/openapi"
	"github.com/Yacobolo/quackstack/pkg/apigen/ir"
	"go.yaml.in/yaml/v4"
)

// Options configures Go server emission.
type Options struct {
	PackageName             string
	EmbeddedOpenAPISpecJSON string
}

// Emit renders Go server scaffolding from IR.
func Emit(doc ir.Document, opts Options) ([]byte, error) {
	return emit(doc, opts)
}

// EmitWithLegacyResponses renders Go server scaffolding using APIGen-owned
// response metadata while preserving legacy concrete response aliases.
func EmitWithLegacyResponses(doc ir.Document, opts Options) ([]byte, error) {
	return EmitWithLegacyResponsesAndSpec(doc, opts)
}

// EmitWithLegacyResponsesAndSpec renders Go server scaffolding using APIGen-owned
// response metadata while embedding the provided OpenAPI JSON when non-empty.
// embedding the provided OpenAPI JSON when non-empty.
func EmitWithLegacyResponsesAndSpec(doc ir.Document, opts Options) ([]byte, error) {
	return emit(doc, opts)
}

func emit(doc ir.Document, opts Options) ([]byte, error) {
	specJSON := opts.EmbeddedOpenAPISpecJSON
	if specJSON == "" {
		var err error
		specJSON, err = emitSpecJSON(doc)
		if err != nil {
			return nil, err
		}
	}

	var b strings.Builder
	packageName := packageName(opts)
	usesTime := docUsesTimeTypes(doc)
	hasStrictOperations := false
	hasRequestBodies := false
	for _, endpoint := range doc.Endpoints {
		if endpoint.OperationID != "getHealth" {
			hasStrictOperations = true
		}
		if endpoint.RequestBody != nil {
			hasRequestBodies = true
		}
	}
	b.WriteString("package ")
	b.WriteString(packageName)
	b.WriteString("\n\n")
	b.WriteString("import (\n")
	if hasStrictOperations {
		b.WriteString("\t\"context\"\n")
		b.WriteString("\t\"fmt\"\n")
		if hasRequestBodies {
			b.WriteString("\t\"io\"\n")
		}
		b.WriteString("\t\"reflect\"\n")
		b.WriteString("\t\"strings\"\n")
	}
	b.WriteString("\t\"encoding/json\"\n")
	b.WriteString("\t\"net/http\"\n\n")
	if usesTime {
		b.WriteString("\t\"time\"\n\n")
	}
	b.WriteString("\tapigenchi \"github.com/Yacobolo/quackstack/pkg/apigen/runtime/chi\"\n")
	b.WriteString(")\n\n")
	b.WriteString("const embeddedOpenAPISpecJSON = `")
	b.WriteString(specJSON)
	b.WriteString("`\n\n")
	b.WriteString("// GetEmbeddedOpenAPISpec returns the canonical OpenAPI document as generic JSON map.\n")
	b.WriteString("func GetEmbeddedOpenAPISpec() (map[string]any, error) {\n")
	b.WriteString("\tvar doc map[string]any\n")
	b.WriteString("\tif err := json.Unmarshal([]byte(embeddedOpenAPISpecJSON), &doc); err != nil {\n")
	b.WriteString("\t\treturn nil, err\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn doc, nil\n")
	b.WriteString("}\n\n")
	b.WriteString("// GenOperationContract captures APIGen-owned contract metadata for one operation.\n")
	b.WriteString("type GenOperationContract struct {\n")
	b.WriteString("\tOperationID string\n")
	b.WriteString("\tMethod string\n")
	b.WriteString("\tPath string\n")
	b.WriteString("\tTags []string\n")
	b.WriteString("\tDocumentedStatusCodes []int\n")
	b.WriteString("\tRequestBodyRequired bool\n")
	b.WriteString("\tAuthzMode string\n")
	b.WriteString("\tProtected bool\n")
	b.WriteString("\tManual bool\n")
	b.WriteString("}\n\n")
	b.WriteString("var genOperationContracts = map[string]GenOperationContract{\n")
	for _, endpoint := range doc.Endpoints {
		fmt.Fprintf(&b, "\t%q: {OperationID: %q, Method: %q, Path: %q, Tags: %s, DocumentedStatusCodes: %s, RequestBodyRequired: %t, AuthzMode: %q, Protected: %t, Manual: %t},\n",
			endpoint.OperationID,
			endpoint.OperationID,
			strings.ToUpper(endpoint.Method),
			ir.JoinAPIPath(doc.API.BasePath, endpoint.Path),
			renderGoStringSlice(endpoint.Tags),
			renderGoIntSlice(documentedStatusCodes(endpoint)),
			endpoint.RequestBody != nil && endpoint.RequestBody.Required,
			endpointAuthzMode(endpoint),
			endpointProtected(endpoint),
			endpointManual(endpoint),
		)
	}
	b.WriteString("}\n\n")
	b.WriteString("// GetAPIGenOperationContracts returns a defensive copy of the generated contract registry.\n")
	b.WriteString("func GetAPIGenOperationContracts() map[string]GenOperationContract {\n")
	b.WriteString("\tout := make(map[string]GenOperationContract, len(genOperationContracts))\n")
	b.WriteString("\tfor operationID, contract := range genOperationContracts {\n")
	b.WriteString("\t\tout[operationID] = cloneAPIGenOperationContract(contract)\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn out\n")
	b.WriteString("}\n\n")
	b.WriteString("// GetAPIGenOperationContract returns generated contract metadata for a single operation.\n")
	b.WriteString("func GetAPIGenOperationContract(operationID string) (GenOperationContract, bool) {\n")
	b.WriteString("\tcontract, ok := genOperationContracts[operationID]\n")
	b.WriteString("\tif !ok {\n")
	b.WriteString("\t\treturn GenOperationContract{}, false\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn cloneAPIGenOperationContract(contract), true\n")
	b.WriteString("}\n\n")
	b.WriteString("// APIGenOperationAllowsStatus reports whether a status code is documented for an operation.\n")
	b.WriteString("//nolint:revive // exported generated helper name matches the APIGen contract registry namespace.\n")
	b.WriteString("func APIGenOperationAllowsStatus(operationID string, statusCode int) bool {\n")
	b.WriteString("\tcontract, ok := genOperationContracts[operationID]\n")
	b.WriteString("\tif !ok {\n")
	b.WriteString("\t\treturn false\n")
	b.WriteString("\t}\n")
	b.WriteString("\tfor _, documented := range contract.DocumentedStatusCodes {\n")
	b.WriteString("\t\tif documented == statusCode {\n")
	b.WriteString("\t\t\treturn true\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn false\n")
	b.WriteString("}\n\n")
	b.WriteString("func cloneAPIGenOperationContract(contract GenOperationContract) GenOperationContract {\n")
	b.WriteString("\tcontract.Tags = append([]string(nil), contract.Tags...)\n")
	b.WriteString("\tcontract.DocumentedStatusCodes = append([]int(nil), contract.DocumentedStatusCodes...)\n")
	b.WriteString("\treturn contract\n")
	b.WriteString("}\n\n")
	b.WriteString("// GenServerInterface dispatches generated operations.\n")
	b.WriteString("type GenServerInterface interface {\n")
	b.WriteString("\tHandleAPIGen(operationID string, w http.ResponseWriter, r *http.Request)\n")
	b.WriteString("}\n\n")
	b.WriteString("// RegisterAPIGenRoutes mounts generated routes on the supported Chi runtime boundary.\n")
	b.WriteString("func RegisterAPIGenRoutes(router apigenchi.Router, server GenServerInterface) {\n")
	b.WriteString("\tapigenchi.RegisterRoutes(router, []apigenchi.Route{\n")
	for _, endpoint := range doc.Endpoints {
		method := strings.ToUpper(endpoint.Method)
		fmt.Fprintf(&b, "\t\t{Method: %q, Path: %q, OperationID: %q},\n", method, ir.JoinAPIPath(doc.API.BasePath, endpoint.Path), endpoint.OperationID)
	}
	b.WriteString("\t}, server.HandleAPIGen)\n")
	b.WriteString("}\n")
	b.WriteString("\n")
	b.WriteString("// RegisterAPIGenStrictRoutes mounts generated routes backed by strict handlers.\n")
	b.WriteString("func RegisterAPIGenStrictRoutes(router apigenchi.Router, handler GenStrictServerInterface) {\n")
	b.WriteString("\tRegisterAPIGenRoutes(router, genStrictAdapter{handler: handler})\n")
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
			b.WriteString("\t\terr = apigenchi.BindPathParameter(\"" + p.Name + "\", apigenchi.URLParam(r, \"" + p.Name + "\"), " + required + ", &" + varName + ")\n")
			b.WriteString("\t\tif err != nil {\n")
			b.WriteString("\t\t\twriteAPIGenError(w, http.StatusBadRequest, err.Error())\n")
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
				b.WriteString("\t\terr = apigenchi.BindQueryParameter(r.URL.Query(), \"" + p.Name + "\", " + required + ", &params." + fieldName + ")\n")
				b.WriteString("\t\tif err != nil {\n")
				b.WriteString("\t\t\twriteAPIGenError(w, http.StatusBadRequest, err.Error())\n")
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
	if hasStrictOperations {
		b.WriteString("func apigenErrorMessage(statusCode int, message string) string {\n")
		b.WriteString("\tif statusCode >= http.StatusInternalServerError {\n")
		b.WriteString("\t\tif statusText := strings.ToLower(http.StatusText(statusCode)); statusText != \"\" {\n")
		b.WriteString("\t\t\treturn statusText\n")
		b.WriteString("\t\t}\n")
		b.WriteString("\t}\n")
		b.WriteString("\treturn message\n")
		b.WriteString("}\n\n")
		b.WriteString("func writeAPIGenError(w http.ResponseWriter, statusCode int, message string) {\n")
		b.WriteString("\tw.Header().Set(\"Content-Type\", \"application/json\")\n")
		b.WriteString("\tw.WriteHeader(statusCode)\n")
		b.WriteString("\t_ = json.NewEncoder(w).Encode(Error{Code: apigenchi.SafeIntToInt32(statusCode), Message: apigenErrorMessage(statusCode, message)})\n")
		b.WriteString("}\n\n")
	}
	if hasRequestBodies {
		b.WriteString("func decodeAPIGenJSONBody(body io.Reader, dest any, requiredFields ...string) error {\n")
		b.WriteString("\traw, err := io.ReadAll(body)\n")
		b.WriteString("\tif err != nil {\n")
		b.WriteString("\t\treturn fmt.Errorf(\"read request body: %w\", err)\n")
		b.WriteString("\t}\n")
		b.WriteString("\tif len(strings.TrimSpace(string(raw))) == 0 {\n")
		b.WriteString("\t\treturn fmt.Errorf(\"request body must not be empty\")\n")
		b.WriteString("\t}\n")
		b.WriteString("\tif len(requiredFields) > 0 {\n")
		b.WriteString("\t\tvar envelope map[string]json.RawMessage\n")
		b.WriteString("\t\tif err := json.Unmarshal(raw, &envelope); err == nil {\n")
		b.WriteString("\t\t\tfor _, field := range requiredFields {\n")
		b.WriteString("\t\t\t\tif _, ok := envelope[field]; !ok {\n")
		b.WriteString("\t\t\t\t\treturn fmt.Errorf(\"%s is required\", field)\n")
		b.WriteString("\t\t\t\t}\n")
		b.WriteString("\t\t\t}\n")
		b.WriteString("\t\t}\n")
		b.WriteString("\t}\n")
		b.WriteString("\tdecoder := json.NewDecoder(strings.NewReader(string(raw)))\n")
		b.WriteString("\tdecoder.DisallowUnknownFields()\n")
		b.WriteString("\tif err := decoder.Decode(dest); err != nil {\n")
		b.WriteString("\t\treturn fmt.Errorf(\"invalid JSON body: %w\", err)\n")
		b.WriteString("\t}\n")
		b.WriteString("\tvar extra json.RawMessage\n")
		b.WriteString("\tif err := decoder.Decode(&extra); err != io.EOF {\n")
		b.WriteString("\t\tif err == nil {\n")
		b.WriteString("\t\t\treturn fmt.Errorf(\"request body must contain a single JSON value\")\n")
		b.WriteString("\t\t}\n")
		b.WriteString("\t\treturn fmt.Errorf(\"invalid JSON body: %w\", err)\n")
		b.WriteString("\t}\n")
		b.WriteString("\treturn nil\n")
		b.WriteString("}\n\n")
	}
	emitSharedErrorResponseTypes(&b, doc)
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
			legacyTypeName := compatibilityResponseTypeName(endpoint.OperationID, response)
			legacyNoBodyJSONResponse := isLegacyNoBodyJSONResponse(endpoint.OperationID, response)
			if shared, ok := sharedErrorResponseType(response); ok {
				b.WriteString("// Gen" + name + statusCode + "ResponseHeaders aliases the APIGen shared response headers for " + name + " " + statusCode + " errors.\n")
				b.WriteString("type Gen" + name + statusCode + "ResponseHeaders = Gen" + shared + "ResponseHeaders\n\n")
				b.WriteString("// Gen" + name + statusCode + "JSONResponse is the APIGen concrete JSON response for " + name + " " + statusCode + ".\n")
				b.WriteString("type Gen" + name + statusCode + "JSONResponse struct{ Gen" + shared + "JSONResponse }\n\n")
				b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCode + " responses to the client.\n")
				b.WriteString("func (response Gen" + name + statusCode + "JSONResponse) Visit" + name + "Response(w http.ResponseWriter) error {\n")
				emitVisitResponseBody(&b, response, statusCode, !legacyNoBodyJSONResponse, "apigen: "+name+" "+statusCode+" response body missing")
				b.WriteString("}\n\n")
				emitLegacyResponseCompatibilityAlias(&b, name, legacyTypeName, "Gen"+name+statusCode+"JSONResponse", statusCode, responseHeaderFields(response), legacyNoBodyJSONResponse)
				continue
			}
			if shape, ok, err := ir.ResponseShapeMetadata(response); err == nil && ok && shape.Kind == "wrapped_json" {
				headersTypeName := "Gen" + name + statusCode + "ResponseHeaders"
				headersFields := responseHeaderFieldsWithDefaults(response)
				emitOwnedResponseHeaders(&b, headersTypeName, headersFields)
				b.WriteString("// Gen" + name + statusCode + "JSONResponse is the APIGen concrete JSON response for " + name + " " + statusCode + ".\n")
				b.WriteString("type Gen" + name + statusCode + "JSONResponse struct {\n")
				b.WriteString("\tBody " + shape.BodyType + "\n")
				b.WriteString("\tHeaders " + headersTypeName + "\n")
				b.WriteString("}\n\n")
				b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCode + " responses to the client.\n")
				b.WriteString("func (response Gen" + name + statusCode + "JSONResponse) Visit" + name + "Response(w http.ResponseWriter) error {\n")
				emitDirectHeaderWrites(&b, headersFields)
				b.WriteString("\tw.Header().Set(\"Content-Type\", \"application/json\")\n")
				b.WriteString("\tw.WriteHeader(" + statusCode + ")\n")
				b.WriteString("\treturn json.NewEncoder(w).Encode(response.Body)\n")
				b.WriteString("}\n\n")
				emitLegacyResponseCompatibilityAlias(&b, name, legacyTypeName, "Gen"+name+statusCode+"JSONResponse", statusCode, headersFields, legacyNoBodyJSONResponse)
				continue
			}
			if len(response.Headers) == 0 && response.Schema != nil && usesDirectOwnedResponseSchema(response, doc) && len(responseHeaderFieldsWithDefaults(response)) == 0 {
				schemaName, _ := responseSchemaTypeName(doc, *response.Schema)
				b.WriteString("// Gen" + name + statusCode + "JSONResponse is the APIGen concrete JSON response for " + name + " " + statusCode + ".\n")
				b.WriteString("type Gen" + name + statusCode + "JSONResponse GenSchema" + schemaName + "\n\n")
				b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCode + " responses to the client.\n")
				b.WriteString("func (response Gen" + name + statusCode + "JSONResponse) Visit" + name + "Response(w http.ResponseWriter) error {\n")
				b.WriteString("\tw.Header().Set(\"Content-Type\", \"application/json\")\n")
				b.WriteString("\tw.WriteHeader(" + statusCode + ")\n")
				b.WriteString("\treturn json.NewEncoder(w).Encode(response)\n")
				b.WriteString("}\n\n")
				emitLegacyResponseCompatibilityAlias(&b, name, legacyTypeName, "Gen"+name+statusCode+"JSONResponse", statusCode, nil, legacyNoBodyJSONResponse)
				continue
			}
			if response.Schema != nil {
				bodyTypeName := responseBodyTypeName(doc, *response.Schema)
				headersFields := responseHeaderFieldsWithDefaults(response)
				if len(headersFields) > 0 {
					headersTypeName := "Gen" + name + statusCode + "ResponseHeaders"
					emitOwnedResponseHeaders(&b, headersTypeName, headersFields)
					b.WriteString("// Gen" + name + statusCode + "JSONResponse is the APIGen concrete JSON response for " + name + " " + statusCode + ".\n")
					b.WriteString("type Gen" + name + statusCode + "JSONResponse struct {\n")
					b.WriteString("\tBody " + bodyTypeName + "\n")
					b.WriteString("\tHeaders " + headersTypeName + "\n")
					b.WriteString("}\n\n")
					b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCode + " responses to the client.\n")
					b.WriteString("func (response Gen" + name + statusCode + "JSONResponse) Visit" + name + "Response(w http.ResponseWriter) error {\n")
					emitDirectHeaderWrites(&b, headersFields)
					b.WriteString("\tw.Header().Set(\"Content-Type\", \"application/json\")\n")
					b.WriteString("\tw.WriteHeader(" + statusCode + ")\n")
					b.WriteString("\treturn json.NewEncoder(w).Encode(response.Body)\n")
					b.WriteString("}\n\n")
					emitLegacyResponseCompatibilityAlias(&b, name, legacyTypeName, "Gen"+name+statusCode+"JSONResponse", statusCode, headersFields, legacyNoBodyJSONResponse)
					continue
				}
				b.WriteString("// Gen" + name + statusCode + "JSONResponse is the APIGen concrete JSON response for " + name + " " + statusCode + ".\n")
				b.WriteString("type Gen" + name + statusCode + "JSONResponse " + bodyTypeName + "\n\n")
				b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCode + " responses to the client.\n")
				b.WriteString("func (response Gen" + name + statusCode + "JSONResponse) Visit" + name + "Response(w http.ResponseWriter) error {\n")
				b.WriteString("\tw.Header().Set(\"Content-Type\", \"application/json\")\n")
				b.WriteString("\tw.WriteHeader(" + statusCode + ")\n")
				b.WriteString("\treturn json.NewEncoder(w).Encode(response)\n")
				b.WriteString("}\n\n")
				emitLegacyResponseCompatibilityAlias(&b, name, legacyTypeName, "Gen"+name+statusCode+"JSONResponse", statusCode, nil, legacyNoBodyJSONResponse)
				continue
			}
			headersFields := responseHeaderFieldsWithDefaults(response)
			if len(headersFields) > 0 {
				headersTypeName := "Gen" + name + statusCode + "ResponseHeaders"
				emitOwnedResponseHeaders(&b, headersTypeName, headersFields)
				b.WriteString("// Gen" + name + statusCode + "Response is the APIGen concrete response for " + name + " " + statusCode + ".\n")
				b.WriteString("type Gen" + name + statusCode + "Response struct {\n")
				b.WriteString("\tHeaders " + headersTypeName + "\n")
				b.WriteString("}\n\n")
				b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCode + " responses to the client.\n")
				b.WriteString("func (response Gen" + name + statusCode + "Response) Visit" + name + "Response(w http.ResponseWriter) error {\n")
				emitDirectHeaderWrites(&b, headersFields)
				b.WriteString("\tw.WriteHeader(" + statusCode + ")\n")
				b.WriteString("\treturn nil\n")
				b.WriteString("}\n\n")
				emitLegacyResponseCompatibilityAlias(&b, name, legacyTypeName, "Gen"+name+statusCode+"Response", statusCode, headersFields, isLegacyNoBodyResponse(endpoint.OperationID, response))
				continue
			}

			b.WriteString("// Gen" + name + statusCode + "Response is the APIGen concrete response for " + name + " " + statusCode + ".\n")
			b.WriteString("type Gen" + name + statusCode + "Response struct{}\n\n")
			b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCode + " responses to the client.\n")
			b.WriteString("func (response Gen" + name + statusCode + "Response) Visit" + name + "Response(w http.ResponseWriter) error {\n")
			b.WriteString("\tw.WriteHeader(" + statusCode + ")\n")
			b.WriteString("\treturn nil\n")
			b.WriteString("}\n\n")
			emitLegacyResponseCompatibilityAlias(&b, name, legacyTypeName, "Gen"+name+statusCode+"Response", statusCode, nil, isLegacyNoBodyResponse(endpoint.OperationID, response))
		}
		emitMissingSharedErrorResponses(&b, endpoint)
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

	b.WriteString("type genStrictAdapter struct {\n")
	b.WriteString("\thandler GenStrictServerInterface\n")
	b.WriteString("}\n\n")

	b.WriteString("func (a genStrictAdapter) HandleAPIGen(operationID string, w http.ResponseWriter, r *http.Request) {\n")
	b.WriteString("\tif ok := DispatchAPIGenStrictOperation(operationID, a.handler, w, r); !ok {\n")
	b.WriteString("\t\thttp.NotFound(w, r)\n")
	b.WriteString("\t}\n")
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
			requiredFields := requestBodyRequiredFields(doc, endpoint)
			if len(requiredFields) > 0 {
				b.WriteString("\tif err := decodeAPIGenJSONBody(r.Body, &body, " + renderGoStringSlice(requiredFields) + "...); err != nil {\n")
			} else {
				b.WriteString("\tif err := decodeAPIGenJSONBody(r.Body, &body); err != nil {\n")
			}
			b.WriteString("\t\twriteAPIGenError(w, http.StatusBadRequest, err.Error())\n")
			b.WriteString("\t\treturn\n")
			b.WriteString("\t}\n")
			b.WriteString("\trequest.Body = &body\n")
		}

		b.WriteString("\tresponse, err := b.handler." + name + "(r.Context(), request)\n")
		b.WriteString("\tif err != nil {\n")
		b.WriteString("\t\twriteAPIGenError(w, http.StatusInternalServerError, err.Error())\n")
		b.WriteString("\t\treturn\n")
		b.WriteString("\t}\n")
		b.WriteString("\tif err := response.Visit" + name + "Response(w); err != nil {\n")
		b.WriteString("\t\twriteAPIGenError(w, http.StatusInternalServerError, err.Error())\n")
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
	yamlBytes, err := openapiemit.EmitYAML(docIR, openapiemit.Options{})
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

func renderGoStringSlice(values []string) string {
	if len(values) == 0 {
		return "nil"
	}
	var b strings.Builder
	b.WriteString("[]string{")
	for i, value := range values {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%q", value)
	}
	b.WriteString("}")
	return b.String()
}

func renderGoIntSlice(values []int) string {
	if len(values) == 0 {
		return "nil"
	}
	var b strings.Builder
	b.WriteString("[]int{")
	for i, value := range values {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%d", value)
	}
	b.WriteString("}")
	return b.String()
}

func documentedStatusCodes(endpoint ir.Endpoint) []int {
	seen := make(map[int]struct{}, len(endpoint.Responses)+8)
	codes := make([]int, 0, len(endpoint.Responses)+8)
	for _, response := range endpoint.Responses {
		if _, ok := seen[response.StatusCode]; ok {
			continue
		}
		seen[response.StatusCode] = struct{}{}
		codes = append(codes, response.StatusCode)
	}
	for _, statusCode := range []int{400, 401, 403, 404, 409, 429, 500, 502} {
		if _, ok := seen[statusCode]; ok {
			continue
		}
		seen[statusCode] = struct{}{}
		codes = append(codes, statusCode)
	}
	sort.Ints(codes)
	return codes
}

func endpointProtected(endpoint ir.Endpoint) bool {
	for _, response := range endpoint.Responses {
		if response.StatusCode == 401 || response.StatusCode == 403 {
			return true
		}
	}
	return false
}

func endpointManual(endpoint ir.Endpoint) bool {
	if len(endpoint.Extensions) == 0 {
		return false
	}
	raw, ok := endpoint.Extensions["x-apigen-manual"]
	if !ok {
		return false
	}
	manual, ok := raw.(bool)
	return ok && manual
}

func endpointAuthzMode(endpoint ir.Endpoint) string {
	if len(endpoint.Extensions) == 0 {
		return ""
	}
	raw, ok := endpoint.Extensions["x-authz"]
	if !ok {
		return ""
	}
	extension, ok := raw.(map[string]any)
	if !ok {
		return ""
	}
	mode, _ := extension["mode"].(string)
	return mode
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
	case "array":
		if schema.Items != nil {
			return "[]" + schemaTypeName(*schema.Items)
		}
		return "[]any"
	case "string":
		if schemaFormat == "date-time" {
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
		if schemaName, ok := ir.NormalizedSchemaRefName(schema); ok {
			if _, exists := doc.Schemas[schemaName]; exists {
				return "GenSchema" + exportedName(schemaName), false
			}
		}
	}
	if schema.Ref == "GenericRequest" {
		if schemaName, ok := ir.ResolveGenericRequestBodySchemaName(doc, endpoint.OperationID); ok {
			return "GenSchema" + schemaName, false
		}
	}
	if schema.Type != "" {
		return schemaTypeName(schema), false
	}

	name := exportedName(endpoint.OperationID)
	return name + "JSONRequestBody", true
}

func requestBodyRequiredFields(doc ir.Document, endpoint ir.Endpoint) []string {
	if endpoint.RequestBody == nil {
		return nil
	}
	schema, ok := resolveSchema(doc, endpoint.RequestBody.Schema)
	if !ok {
		return nil
	}
	if schema.Type != "object" || len(schema.Required) == 0 {
		return nil
	}
	fields := append([]string(nil), schema.Required...)
	sort.Strings(fields)
	return fields
}

func resolveSchema(doc ir.Document, schemaRef ir.SchemaRef) (ir.Schema, bool) {
	if schemaRef.Ref != "" {
		return ir.ResolveSchema(doc, schemaRef)
	}
	if schemaRef.Type == "" {
		return ir.Schema{}, false
	}
	return ir.Schema{Type: schemaRef.Type}, true
}

func responseTypeName(operationID string, response ir.Response) string {
	if _, ok := sharedErrorResponseType(response); ok {
		return ""
	}
	if typeName, ok := legacyResponseTypeName(operationID, response.StatusCode, response.Schema != nil); ok {
		return typeName
	}

	statusCode := fmt.Sprintf("%d", response.StatusCode)
	if response.Schema != nil {
		return exportedName(operationID) + statusCode + "JSONResponse"
	}
	return exportedName(operationID) + statusCode + "Response"
}

func compatibilityResponseTypeName(operationID string, response ir.Response) string {
	if typeName := responseTypeName(operationID, response); typeName != "" {
		return typeName
	}
	statusCode := fmt.Sprintf("%d", response.StatusCode)
	if response.Schema != nil {
		return exportedName(operationID) + statusCode + "JSONResponse"
	}
	return exportedName(operationID) + statusCode + "Response"
}

func emitMissingSharedErrorResponses(b *strings.Builder, endpoint ir.Endpoint) {
	present := make(map[int]struct{}, len(endpoint.Responses))
	for _, response := range endpoint.Responses {
		present[response.StatusCode] = struct{}{}
	}
	sharedStatuses := []int{400, 401, 403, 404, 409, 429, 500, 502}
	name := exportedName(endpoint.OperationID)
	for _, statusCode := range sharedStatuses {
		if _, ok := present[statusCode]; ok {
			continue
		}
		response := ir.Response{
			StatusCode: statusCode,
			Headers:    defaultSharedErrorHeaders(statusCode),
			Schema:     &ir.SchemaRef{Ref: "Error"},
		}
		shared, ok := sharedErrorResponseType(response)
		if !ok {
			continue
		}
		statusCodeText := fmt.Sprintf("%d", statusCode)
		b.WriteString("// Gen" + name + statusCodeText + "ResponseHeaders aliases the APIGen shared response headers for " + name + " " + statusCodeText + " errors.\n")
		b.WriteString("type Gen" + name + statusCodeText + "ResponseHeaders = Gen" + shared + "ResponseHeaders\n\n")
		b.WriteString("// Gen" + name + statusCodeText + "JSONResponse is the APIGen legacy-compatible shared JSON response for " + name + " " + statusCodeText + ".\n")
		b.WriteString("type Gen" + name + statusCodeText + "JSONResponse struct{ Gen" + shared + "JSONResponse }\n\n")
		b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCodeText + " responses to the client.\n")
		b.WriteString("func (response Gen" + name + statusCodeText + "JSONResponse) Visit" + name + "Response(w http.ResponseWriter) error {\n")
		emitVisitResponseBody(b, response, statusCodeText, true, "apigen: "+name+" "+statusCodeText+" response body missing")
		b.WriteString("}\n\n")
		emitLegacyResponseCompatibilityAlias(b, name, compatibilityResponseTypeName(endpoint.OperationID, response), "Gen"+name+statusCodeText+"JSONResponse", statusCodeText, responseHeaderFields(response), false)
	}
}

func legacyResponseTypeName(operationID string, statusCode int, hasSchema bool) (string, bool) {
	key := fmt.Sprintf("%s:%d:%t", operationID, statusCode, hasSchema)
	switch key {
	case "bindColumnMask:201:false":
		return "BindColumnMask204Response", true
	case "bindRowFilter:201:false":
		return "BindRowFilter204Response", true
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
	case "createGroupMember:201:false":
		return "CreateGroupMember204Response", true
	case "createGroupMember:201:true":
		return "CreateGroupMember204Response", true
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

func emitSharedErrorResponseTypes(b *strings.Builder, doc ir.Document) {
	sharedTypes := []struct {
		name       string
		statusCode int
	}{
		{name: "BadRequest", statusCode: 400},
		{name: "Conflict", statusCode: 409},
		{name: "Forbidden", statusCode: 403},
		{name: "InternalError", statusCode: 500},
		{name: "NotFound", statusCode: 404},
		{name: "RateLimitExceeded", statusCode: 429},
		{name: "Unauthorized", statusCode: 401},
	}

	for _, shared := range sharedTypes {
		b.WriteString("// Gen" + shared.name + "ResponseHeaders represents the APIGen shared response headers for " + shared.name + " JSON errors.\n")
		b.WriteString("type Gen" + shared.name + "ResponseHeaders struct {\n")
		for _, header := range sharedErrorHeaders(doc, shared.statusCode) {
			b.WriteString("\t" + headerFieldName(header.Name) + " " + schemaTypeName(header.Schema) + "\n")
		}
		b.WriteString("}\n\n")
		b.WriteString("// Gen" + shared.name + "JSONResponse represents the APIGen shared JSON error body for " + shared.name + " responses.\n")
		b.WriteString("type Gen" + shared.name + "JSONResponse struct {\n")
		b.WriteString("\tBody Error\n\n")
		b.WriteString("\tHeaders Gen" + shared.name + "ResponseHeaders\n")
		b.WriteString("}\n\n")
		b.WriteString("// " + shared.name + "ResponseHeaders aliases the APIGen shared response headers for legacy handlers.\n")
		b.WriteString("type " + shared.name + "ResponseHeaders = Gen" + shared.name + "ResponseHeaders\n\n")
		b.WriteString("// " + shared.name + "JSONResponse aliases the APIGen shared JSON error type for legacy handlers.\n")
		b.WriteString("type " + shared.name + "JSONResponse = Gen" + shared.name + "JSONResponse\n\n")
	}
}

func emitLegacyResponseCompatibilityAlias(b *strings.Builder, operationName string, legacyTypeName string, genTypeName string, genStatusCode string, headers []ownedHeaderField, legacyNoBody bool) {
	if legacyTypeName == "" {
		return
	}
	legacyStatusCode, ok := compatibilityResponseStatusCode(legacyTypeName)
	if !ok || legacyStatusCode == genStatusCode {
		if len(headers) > 0 {
			legacyHeadersTypeName := responseHeadersTypeName(legacyTypeName)
			genHeadersTypeName := responseHeadersTypeName(genTypeName)
			if legacyHeadersTypeName != "" && genHeadersTypeName != "" {
				b.WriteString("// " + legacyHeadersTypeName + " aliases the APIGen-owned response headers for legacy handlers.\n")
				b.WriteString("type " + legacyHeadersTypeName + " = " + genHeadersTypeName + "\n\n")
			}
		}
		b.WriteString("// " + legacyTypeName + " aliases the APIGen concrete response for legacy handlers.\n")
		b.WriteString("type " + legacyTypeName + " = " + genTypeName + "\n\n")
		return
	}
	genLegacyTypeName := "Gen" + legacyTypeName
	legacyHeadersTypeName := responseHeadersTypeName(legacyTypeName)
	if len(headers) > 0 {
		genHeadersTypeName := responseHeadersTypeName(genTypeName)
		if legacyHeadersTypeName != "" && genHeadersTypeName != "" {
			b.WriteString("// " + legacyHeadersTypeName + " aliases the APIGen-owned response headers for legacy handlers.\n")
			b.WriteString("type " + legacyHeadersTypeName + " = " + genHeadersTypeName + "\n\n")
			genLegacyHeadersTypeName := responseHeadersTypeName(genLegacyTypeName)
			if genLegacyHeadersTypeName != "" {
				b.WriteString("// " + genLegacyHeadersTypeName + " aliases the APIGen-owned legacy response headers.\n")
				b.WriteString("type " + genLegacyHeadersTypeName + " = " + legacyHeadersTypeName + "\n\n")
			}
		}
	} else if legacyHeadersTypeName != "" {
		b.WriteString("// " + legacyHeadersTypeName + " preserves the legacy response headers type for handlers.\n")
		b.WriteString("type " + legacyHeadersTypeName + " struct{}\n\n")
		genLegacyHeadersTypeName := responseHeadersTypeName(genLegacyTypeName)
		if genLegacyHeadersTypeName != "" {
			b.WriteString("// " + genLegacyHeadersTypeName + " aliases the APIGen-owned legacy response headers.\n")
			b.WriteString("type " + genLegacyHeadersTypeName + " = " + legacyHeadersTypeName + "\n\n")
		}
	}
	if legacyNoBody {
		b.WriteString("// " + legacyTypeName + " preserves the legacy concrete response status for handlers.\n")
		b.WriteString("type " + legacyTypeName + " struct {")
		if legacyHeadersTypeName != "" {
			b.WriteString("\n\tHeaders " + legacyHeadersTypeName + "\n")
		}
		b.WriteString("}\n\n")
		b.WriteString("// Visit" + operationName + "Response writes legacy-compatible " + operationName + " responses to the client.\n")
		b.WriteString("func (response " + legacyTypeName + ") Visit" + operationName + "Response(w http.ResponseWriter) error {\n")
		if len(headers) > 0 {
			for _, header := range headers {
				b.WriteString("\tw.Header().Set(\"" + header.HeaderName + "\", fmt.Sprint(response.Headers." + header.Name + "))\n")
			}
		}
		b.WriteString("\tw.WriteHeader(" + legacyStatusCode + ")\n")
		b.WriteString("\treturn nil\n")
		b.WriteString("}\n\n")
		b.WriteString("// " + genLegacyTypeName + " aliases the APIGen legacy-compatible response type.\n")
		b.WriteString("type " + genLegacyTypeName + " = " + legacyTypeName + "\n\n")
		return
	}
	if strings.HasSuffix(legacyTypeName, "JSONResponse") {
		b.WriteString("// " + legacyTypeName + " preserves the legacy concrete response status for handlers.\n")
		b.WriteString("type " + legacyTypeName + " " + genTypeName + "\n\n")
		b.WriteString("// Visit" + operationName + "Response writes legacy-compatible " + operationName + " responses to the client.\n")
		b.WriteString("func (response " + legacyTypeName + ") Visit" + operationName + "Response(w http.ResponseWriter) error {\n")
		if len(headers) > 0 {
			for _, header := range headers {
				b.WriteString("\tw.Header().Set(\"" + header.HeaderName + "\", fmt.Sprint(response.Headers." + header.Name + "))\n")
			}
		}
		b.WriteString("\tw.Header().Set(\"Content-Type\", \"application/json\")\n")
		b.WriteString("\tw.WriteHeader(" + legacyStatusCode + ")\n")
		b.WriteString("\treturn json.NewEncoder(w).Encode(response.Body)\n")
		b.WriteString("}\n\n")
		b.WriteString("// " + genLegacyTypeName + " aliases the APIGen legacy-compatible response type.\n")
		b.WriteString("type " + genLegacyTypeName + " = " + legacyTypeName + "\n\n")
		return
	}
	b.WriteString("// " + legacyTypeName + " preserves the legacy concrete response status for handlers.\n")
	b.WriteString("type " + legacyTypeName + " " + genTypeName + "\n\n")
	b.WriteString("// Visit" + operationName + "Response writes legacy-compatible " + operationName + " responses to the client.\n")
	b.WriteString("func (response " + legacyTypeName + ") Visit" + operationName + "Response(w http.ResponseWriter) error {\n")
	b.WriteString("\trv := reflect.ValueOf(response)\n")
	if len(headers) > 0 {
		b.WriteString("\theaders := rv.FieldByName(\"Headers\")\n")
		b.WriteString("\tif headers.IsValid() {\n")
		for _, header := range headers {
			b.WriteString("\t\tif v := headers.FieldByName(\"" + header.Name + "\"); v.IsValid() {\n")
			b.WriteString("\t\t\tw.Header().Set(\"" + header.HeaderName + "\", fmt.Sprint(v.Interface()))\n")
			b.WriteString("\t\t}\n")
		}
		b.WriteString("\t}\n")
	}
	b.WriteString("\tw.Header().Set(\"Content-Type\", \"application/json\")\n")
	b.WriteString("\tw.WriteHeader(" + legacyStatusCode + ")\n")
	b.WriteString("\tbody := rv.FieldByName(\"Body\")\n")
	b.WriteString("\tif body.IsValid() {\n")
	b.WriteString("\t\treturn json.NewEncoder(w).Encode(body.Interface())\n")
	b.WriteString("\t}\n")
	b.WriteString("\treturn json.NewEncoder(w).Encode(response)\n")
	b.WriteString("}\n\n")
	b.WriteString("// " + genLegacyTypeName + " aliases the APIGen legacy-compatible response type.\n")
	b.WriteString("type " + genLegacyTypeName + " = " + legacyTypeName + "\n\n")
}

func compatibilityResponseStatusCode(responseTypeName string) (string, bool) {
	base := strings.TrimSuffix(strings.TrimSuffix(responseTypeName, "JSONResponse"), "Response")
	for i := len(base) - 1; i >= 0; i-- {
		if base[i] < '0' || base[i] > '9' {
			status := base[i+1:]
			if len(status) == 3 {
				return status, true
			}
			return "", false
		}
	}
	if len(base) == 3 {
		return base, true
	}
	return "", false
}

func responseHeadersTypeName(responseTypeName string) string {
	switch {
	case strings.HasSuffix(responseTypeName, "JSONResponse"):
		return strings.TrimSuffix(responseTypeName, "JSONResponse") + "ResponseHeaders"
	case strings.HasSuffix(responseTypeName, "Response"):
		return responseTypeName + "Headers"
	default:
		return ""
	}
}

func responseBodyTypeName(doc ir.Document, schema ir.SchemaRef) string {
	if ref, ok := normalizedSchemaRefName(schema); ok {
		name := exportedName(ref)
		if _, ok := doc.Schemas[ref]; ok {
			return "GenSchema" + name
		}
		return name
	}
	return schemaTypeName(schema)
}

func emitOwnedResponseHeaders(b *strings.Builder, typeName string, fields []ownedHeaderField) {
	b.WriteString("// " + typeName + " represents the APIGen-owned response headers for generated concrete responses.\n")
	b.WriteString("type " + typeName + " struct {\n")
	for _, field := range fields {
		b.WriteString("\t" + field.Name + " " + field.Type + "\n")
	}
	b.WriteString("}\n\n")
}

func emitDirectHeaderWrites(b *strings.Builder, fields []ownedHeaderField) {
	for _, field := range fields {
		if field.HeaderName == "" {
			continue
		}
		b.WriteString("\tw.Header().Set(\"" + field.HeaderName + "\", fmt.Sprint(response.Headers." + field.Name + "))\n")
	}
}

func responseHeaderFields(response ir.Response) []ownedHeaderField {
	fields := make([]ownedHeaderField, 0, len(response.Headers))
	for _, header := range response.Headers {
		fields = append(fields, ownedHeaderField{
			Name:       headerFieldName(header.Name),
			HeaderName: header.Name,
			Type:       schemaTypeName(header.Schema),
		})
	}
	return fields
}

func responseHeaderFieldsWithDefaults(response ir.Response) []ownedHeaderField {
	if len(response.Headers) > 0 {
		return responseHeaderFields(response)
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return responseHeaderFields(ir.Response{Headers: defaultSharedErrorHeaders(response.StatusCode)})
	}
	return nil
}

func emitVisitResponseHeaders(b *strings.Builder, response ir.Response) {
	if len(response.Headers) > 0 {
		b.WriteString("\theaders := rv.FieldByName(\"Headers\")\n")
		b.WriteString("\tif headers.IsValid() {\n")
		for _, header := range response.Headers {
			b.WriteString("\t\tif v := headers.FieldByName(\"" + headerFieldName(header.Name) + "\"); v.IsValid() {\n")
			b.WriteString("\t\t\tw.Header().Set(\"" + header.Name + "\", fmt.Sprint(v.Interface()))\n")
			b.WriteString("\t\t}\n")
		}
		b.WriteString("\t}\n")
	}
}

func emitVisitResponseBody(b *strings.Builder, response ir.Response, statusCode string, requireBody bool, missingBodyError string) {
	b.WriteString("\trv := reflect.ValueOf(response)\n")
	emitVisitResponseHeaders(b, response)
	b.WriteString("\tbody := rv.FieldByName(\"Body\")\n")
	b.WriteString("\tif !body.IsValid() {\n")
	if requireBody {
		b.WriteString("\t\treturn fmt.Errorf(\"" + missingBodyError + "\")\n")
	} else {
		b.WriteString("\t\tw.WriteHeader(" + statusCode + ")\n")
		b.WriteString("\t\treturn nil\n")
	}
	b.WriteString("\t}\n")
	b.WriteString("\tw.Header().Set(\"Content-Type\", \"application/json\")\n")
	b.WriteString("\tw.WriteHeader(" + statusCode + ")\n")
	b.WriteString("\treturn json.NewEncoder(w).Encode(body.Interface())\n")
}

func sharedErrorResponseType(response ir.Response) (string, bool) {
	if response.Schema == nil || !isErrorSchema(*response.Schema) {
		return "", false
	}

	switch response.StatusCode {
	case 400:
		return "BadRequest", true
	case 401:
		return "Unauthorized", true
	case 403:
		return "Forbidden", true
	case 404:
		return "NotFound", true
	case 409:
		return "Conflict", true
	case 429:
		return "RateLimitExceeded", true
	case 500:
		return "InternalError", true
	case 502:
		return "InternalError", true
	default:
		return "", false
	}
}

func sharedErrorHeaders(doc ir.Document, statusCode int) []ir.Header {
	for _, endpoint := range doc.Endpoints {
		for _, response := range endpoint.Responses {
			if response.StatusCode != statusCode {
				continue
			}
			if _, ok := sharedErrorResponseType(response); !ok {
				continue
			}
			if len(response.Headers) > 0 {
				return response.Headers
			}
		}
	}
	return defaultSharedErrorHeaders(statusCode)
}

func defaultSharedErrorHeaders(statusCode int) []ir.Header {
	headers := []ir.Header{
		{Name: "X-RateLimit-Limit", Schema: ir.SchemaRef{Type: "integer", Format: "int32"}},
		{Name: "X-RateLimit-Remaining", Schema: ir.SchemaRef{Type: "integer", Format: "int32"}},
		{Name: "X-RateLimit-Reset", Schema: ir.SchemaRef{Type: "integer", Format: "int64"}},
	}
	if statusCode == 429 {
		headers = append([]ir.Header{{Name: "Retry-After", Schema: ir.SchemaRef{Type: "integer", Format: "int32"}}}, headers...)
	}
	return headers
}

func headerFieldName(name string) string {
	return exportedName(strings.NewReplacer("-", " ", "_", " ").Replace(name))
}

func isErrorSchema(schema ir.SchemaRef) bool {
	if schema.Ref == "" {
		return false
	}
	ref := strings.TrimSpace(schema.Ref)
	ref = strings.TrimPrefix(ref, "#/components/schemas/")
	ref = strings.TrimPrefix(ref, "#/schemas/")
	if idx := strings.LastIndex(ref, "/"); idx >= 0 {
		ref = ref[idx+1:]
	}
	return exportedName(ref) == "Error"
}

func isLegacyNoBodyJSONResponse(operationID string, response ir.Response) bool {
	if response.Schema == nil {
		return false
	}
	return isLegacyNoBodyResponse(operationID, response)
}

func isLegacyNoBodyResponse(operationID string, response ir.Response) bool {
	key := fmt.Sprintf("%s:%d", operationID, response.StatusCode)
	switch key {
	case "bindColumnMask:201", "bindRowFilter:201":
		return true
	default:
		return false
	}
}

type ownedHeaderField struct {
	Name       string
	HeaderName string
	Type       string
}

func usesDirectOwnedResponseSchema(response ir.Response, doc ir.Document) bool {
	if response.Schema == nil {
		return false
	}
	if shape, ok, err := ir.ResponseShapeMetadata(response); err == nil && ok && shape.Kind == "wrapped_json" {
		return false
	}
	if isErrorSchema(*response.Schema) {
		return false
	}
	_, ok := responseSchemaTypeName(doc, *response.Schema)
	return ok
}

func responseSchemaTypeName(doc ir.Document, schema ir.SchemaRef) (string, bool) {
	ref, ok := normalizedSchemaRefName(schema)
	if !ok {
		return "", false
	}
	if _, ok := doc.Schemas[ref]; !ok {
		return "", false
	}
	return exportedName(ref), true
}

func normalizedSchemaRefName(schema ir.SchemaRef) (string, bool) {
	if schema.Ref == "" {
		return "", false
	}
	ref := strings.TrimSpace(schema.Ref)
	ref = strings.TrimPrefix(ref, "#/components/schemas/")
	ref = strings.TrimPrefix(ref, "#/schemas/")
	if idx := strings.LastIndex(ref, "/"); idx >= 0 {
		ref = ref[idx+1:]
	}
	if ref == "" {
		return "", false
	}
	return ref, true
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

func packageName(opts Options) string {
	if strings.TrimSpace(opts.PackageName) == "" {
		return "api"
	}
	return opts.PackageName
}
