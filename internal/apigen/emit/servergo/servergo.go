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
	}
	b.WriteString("\t\"encoding/json\"\n")
	b.WriteString("\t\"net/http\"\n\n")
	b.WriteString("\t\"github.com/go-chi/chi/v5\"\n")
	b.WriteString("\t\"github.com/oapi-codegen/runtime\"\n")
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
			signature += ", params " + name + "Params"
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
			b.WriteString("\t\terr = runtime.BindStyledParameterWithOptions(\"simple\", \"" + p.Name + "\", chi.URLParam(r, \"" + p.Name + "\"), &" + varName + ", runtime.BindStyledParameterOptions{ParamLocation: runtime.ParamLocationPath, Explode: false, Required: " + required + "})\n")
			b.WriteString("\t\tif err != nil {\n")
			b.WriteString("\t\t\thttp.Error(w, err.Error(), http.StatusBadRequest)\n")
			b.WriteString("\t\t\treturn true\n")
			b.WriteString("\t\t}\n")
		}

		if len(queryParams) > 0 {
			b.WriteString("\t\tvar params " + name + "Params\n")
			for _, p := range queryParams {
				fieldName := exportedName(p.Name)
				required := "false"
				if p.Required {
					required = "true"
				}
				b.WriteString("\t\terr = runtime.BindQueryParameter(\"form\", true, " + required + ", \"" + p.Name + "\", r.URL.Query(), &params." + fieldName + ")\n")
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
		b.WriteString("// Gen" + name + "Request represents the APIGen strict request contract for " + name + ".\n")
		b.WriteString("type Gen" + name + "Request struct {\n")
		for _, p := range pathParams {
			b.WriteString("\t" + exportedName(p.Name) + " " + pathParamTypeName(p) + "\n")
		}
		if len(queryParams) > 0 {
			b.WriteString("\tParams " + name + "Params\n")
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
			if isQueryFamilyOperation(endpoint.OperationID) {
				if response.Schema != nil {
					b.WriteString("// Gen" + name + statusCode + "JSONResponse is the APIGen concrete JSON response for " + name + " " + statusCode + ".\n")
					b.WriteString("type Gen" + name + statusCode + "JSONResponse " + name + statusCode + "JSONResponse\n\n")
					b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCode + " responses to the client.\n")
					b.WriteString("func (response Gen" + name + statusCode + "JSONResponse) Visit" + name + "Response(w http.ResponseWriter) error {\n")
					b.WriteString("\treturn " + name + statusCode + "JSONResponse(response).Visit" + name + "Response(w)\n")
					b.WriteString("}\n\n")
					continue
				}

				b.WriteString("// Gen" + name + statusCode + "Response is the APIGen concrete response for " + name + " " + statusCode + ".\n")
				b.WriteString("type Gen" + name + statusCode + "Response " + name + statusCode + "Response\n\n")
				b.WriteString("// Visit" + name + "Response writes " + name + " " + statusCode + " responses to the client.\n")
				b.WriteString("func (response Gen" + name + statusCode + "Response) Visit" + name + "Response(w http.ResponseWriter) error {\n")
				b.WriteString("\treturn " + name + statusCode + "Response(response).Visit" + name + "Response(w)\n")
				b.WriteString("}\n\n")
				continue
			}

			if response.Schema != nil {
				b.WriteString("// Gen" + name + statusCode + "JSONResponse aliases the APIGen concrete JSON response for " + name + " " + statusCode + ".\n")
				b.WriteString("type Gen" + name + statusCode + "JSONResponse = " + name + statusCode + "JSONResponse\n\n")
				continue
			}
			b.WriteString("// Gen" + name + statusCode + "Response aliases the APIGen concrete response for " + name + " " + statusCode + ".\n")
			b.WriteString("type Gen" + name + statusCode + "Response = " + name + statusCode + "Response\n\n")
		}
		if endpoint.RequestBody != nil {
			b.WriteString("// Gen" + name + "JSONBody aliases the APIGen strict JSON request body contract for " + name + ".\n")
			b.WriteString("type Gen" + name + "JSONBody = " + name + "JSONRequestBody\n\n")
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
			sig += ", params " + name + "Params"
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
	schemaType := strings.ToLower(strings.TrimSpace(param.Schema.Type))
	schemaFormat := strings.ToLower(strings.TrimSpace(param.Schema.Format))

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
	default:
		return "string"
	}
}

func isQueryFamilyOperation(operationID string) bool {
	switch operationID {
	case "executeQuery", "submitQuery", "getQuery", "getQueryResults", "cancelQuery", "deleteQuery":
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
