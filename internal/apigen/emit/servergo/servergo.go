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
	b.WriteString("package api\n\n")
	b.WriteString("import (\n")
	b.WriteString("\t\"context\"\n")
	b.WriteString("\t\"encoding/json\"\n")
	b.WriteString("\t\"net/http\"\n\n")
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
	b.WriteString("// GenServerInterface is generated from JSON IR.\n")
	b.WriteString("type GenServerInterface interface {\n")
	for _, endpoint := range doc.Endpoints {
		name := exportedName(endpoint.OperationID)
		b.WriteString("\t" + name + "(ctx context.Context, request *http.Request) (any, error)\n")
	}
	b.WriteString("}\n\n")
	b.WriteString("// RegisterAPIGenRoutes mounts generated routes on Chi router.\n")
	b.WriteString("func RegisterAPIGenRoutes(router chi.Router, server GenServerInterface) {\n")
	for _, endpoint := range doc.Endpoints {
		name := exportedName(endpoint.OperationID)
		method := strings.ToUpper(endpoint.Method)
		b.WriteString("\trouter.MethodFunc(\"" + method + "\", \"" + endpoint.Path + "\", func(w http.ResponseWriter, r *http.Request) {\n")
		b.WriteString("\t\tresponse, err := server." + name + "(r.Context(), r)\n")
		b.WriteString("\t\tif err != nil {\n")
		b.WriteString("\t\t\thttp.Error(w, err.Error(), http.StatusInternalServerError)\n")
		b.WriteString("\t\t\treturn\n")
		b.WriteString("\t\t}\n")
		b.WriteString("\t\tw.Header().Set(\"Content-Type\", \"application/json\")\n")
		b.WriteString("\t\tw.WriteHeader(http.StatusOK)\n")
		b.WriteString("\t\t_ = json.NewEncoder(w).Encode(response)\n")
		b.WriteString("\t})\n")
	}
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
