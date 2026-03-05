// Package cligo emits CLI metadata code from JSON IR.
package cligo

import (
	"strings"

	"duck-demo/internal/apigen/ir"
)

// Emit renders Go code for generated CLI endpoint metadata.
func Emit(doc ir.Document) ([]byte, error) {
	var b strings.Builder
	b.WriteString("package gen\n\n")
	b.WriteString("// APIGenEndpoint is generated from JSON IR.\n")
	b.WriteString("type APIGenEndpoint struct {\n")
	b.WriteString("\tOperationID string\n")
	b.WriteString("\tMethod string\n")
	b.WriteString("\tPath string\n")
	b.WriteString("\tTags []string\n")
	b.WriteString("}\n\n")
	b.WriteString("// APIGeneratedEndpoints contains operation metadata for tooling and discovery.\n")
	b.WriteString("var APIGeneratedEndpoints = []APIGenEndpoint{\n")
	for _, endpoint := range doc.Endpoints {
		b.WriteString("\t{OperationID: \"" + endpoint.OperationID + "\", Method: \"" + strings.ToUpper(endpoint.Method) + "\", Path: \"" + endpoint.Path + "\", Tags: []string{")
		for i, tag := range endpoint.Tags {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("\"" + tag + "\"")
		}
		b.WriteString("}},\n")
	}
	b.WriteString("}\n")
	return []byte(b.String()), nil
}
