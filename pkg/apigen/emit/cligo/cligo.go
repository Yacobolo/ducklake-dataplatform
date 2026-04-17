// Package cligo emits CLI metadata code from JSON IR.
package cligo

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Yacobolo/quackstack/pkg/apigen/ir"
)

// Options configures CLI metadata emission.
type Options struct {
	PackageName string
}

// Emit renders Go code for generated CLI endpoint metadata.
func Emit(doc ir.Document, opts Options) ([]byte, error) {
	var b strings.Builder
	b.WriteString("package ")
	b.WriteString(packageName(opts))
	b.WriteString("\n\n")
	b.WriteString("import apigencobra \"github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra\"\n\n")
	b.WriteString("// APIGenEndpoint is generated from JSON IR.\n")
	b.WriteString("type APIGenEndpoint = apigencobra.Endpoint\n\n")
	b.WriteString("// APIGenParam is generated parameter metadata from JSON IR.\n")
	b.WriteString("type APIGenParam = apigencobra.Param\n\n")
	b.WriteString("// APIGenField is generated request body field metadata from JSON IR.\n")
	b.WriteString("type APIGenField = apigencobra.Field\n\n")
	b.WriteString("// APIGeneratedEndpoints contains operation metadata for tooling and discovery.\n")
	b.WriteString("var APIGeneratedEndpoints = []APIGenEndpoint{\n")
	for _, endpoint := range doc.Endpoints {
		parameters := collectParameters(doc, endpoint)
		bodyFields := collectBodyFields(doc, endpoint)
		cliCommand := cliCommandFromExtensions(endpoint.Extensions)

		fmt.Fprintf(&b, "\t{OperationID: %q, Method: %q, Path: %q, Summary: %q, Description: %q, Tags: %s, Parameters: %s, BodyFields: %s, CLICommand: %q},\n",
			endpoint.OperationID,
			strings.ToUpper(endpoint.Method),
			ir.JoinAPIPath(doc.API.BasePath, endpoint.Path),
			endpoint.Summary,
			endpoint.Description,
			renderStringSlice(endpoint.Tags),
			renderParams(parameters),
			renderFields(bodyFields),
			cliCommand,
		)
	}
	b.WriteString("}\n")
	return []byte(b.String()), nil
}

func packageName(opts Options) string {
	if strings.TrimSpace(opts.PackageName) == "" {
		return "gen"
	}
	return opts.PackageName
}

func collectParameters(doc ir.Document, endpoint ir.Endpoint) []apiParam {
	params := make([]apiParam, 0, len(endpoint.Parameters))
	for _, parameter := range endpoint.Parameters {
		params = append(params, apiParam{
			Name:        parameter.Name,
			In:          parameter.In,
			Type:        schemaType(doc, parameter.Schema),
			Description: parameter.Description,
			Required:    parameter.Required,
			Enum:        schemaEnum(doc, parameter.Schema),
		})
	}
	return params
}

func collectBodyFields(doc ir.Document, endpoint ir.Endpoint) []apiField {
	if endpoint.RequestBody == nil {
		return nil
	}

	bodySchema, ok := resolveSchema(doc, endpoint.RequestBody.Schema)
	if !ok || bodySchema.Type != "object" {
		return nil
	}

	required := make(map[string]struct{}, len(bodySchema.Required))
	for _, name := range bodySchema.Required {
		required[name] = struct{}{}
	}

	names := make([]string, 0, len(bodySchema.Properties))
	for name := range bodySchema.Properties {
		names = append(names, name)
	}
	sort.Strings(names)

	fields := make([]apiField, 0, len(names))
	for _, name := range names {
		property := bodySchema.Properties[name]
		_, isRequired := required[name]
		fields = append(fields, apiField{
			Name:        name,
			Type:        schemaType(doc, property.Schema),
			Description: property.Description,
			Required:    isRequired,
			Enum:        schemaEnum(doc, property.Schema),
		})
	}
	return fields
}

func resolveSchema(doc ir.Document, schemaRef ir.SchemaRef) (ir.Schema, bool) {
	return ir.ResolveSchema(doc, schemaRef)
}

func schemaType(doc ir.Document, schemaRef ir.SchemaRef) string {
	if schemaRef.Type != "" {
		return schemaRef.Type
	}
	if schema, ok := resolveSchema(doc, schemaRef); ok && schema.Type != "" {
		return schema.Type
	}
	return "string"
}

func schemaEnum(doc ir.Document, schemaRef ir.SchemaRef) []string {
	if schema, ok := resolveSchema(doc, schemaRef); ok && len(schema.Enum) > 0 {
		values := make([]string, len(schema.Enum))
		copy(values, schema.Enum)
		return values
	}
	return nil
}

func cliCommandFromExtensions(extensions map[string]any) string {
	if len(extensions) == 0 {
		return ""
	}
	for _, key := range []string{"x-cli-command", "cli_command", "x_cli_command"} {
		raw, ok := extensions[key]
		if !ok {
			continue
		}
		value, ok := raw.(string)
		if !ok {
			continue
		}
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

type apiParam struct {
	Name        string
	In          string
	Type        string
	Description string
	Required    bool
	Enum        []string
}

type apiField struct {
	Name        string
	Type        string
	Description string
	Required    bool
	Enum        []string
}

func renderStringSlice(values []string) string {
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

func renderParams(params []apiParam) string {
	if len(params) == 0 {
		return "nil"
	}
	var b strings.Builder
	b.WriteString("[]apigencobra.Param{")
	for i, param := range params {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "{Name: %q, In: %q, Type: %q, Description: %q, Required: %t, Enum: %s}", param.Name, param.In, param.Type, param.Description, param.Required, renderStringSlice(param.Enum))
	}
	b.WriteString("}")
	return b.String()
}

func renderFields(fields []apiField) string {
	if len(fields) == 0 {
		return "nil"
	}
	var b strings.Builder
	b.WriteString("[]apigencobra.Field{")
	for i, field := range fields {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "{Name: %q, Type: %q, Description: %q, Required: %t, Enum: %s}", field.Name, field.Type, field.Description, field.Required, renderStringSlice(field.Enum))
	}
	b.WriteString("}")
	return b.String()
}
