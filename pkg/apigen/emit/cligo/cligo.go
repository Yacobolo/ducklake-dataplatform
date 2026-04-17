// Package cligo emits CLI metadata code from JSON IR.
package cligo

import (
	"fmt"
	"strings"

	"github.com/Yacobolo/quackstack/pkg/apigen/ir"
)

// Options configures CLI metadata emission.
type Options struct {
	PackageName string
}

// Emit renders Go code for generated CLI command metadata.
func Emit(doc ir.Document, opts Options) ([]byte, error) {
	normalized := doc
	ir.Normalize(&normalized)

	var b strings.Builder
	b.WriteString("package ")
	b.WriteString(packageName(opts))
	b.WriteString("\n\n")
	b.WriteString("import apigencobra \"github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra\"\n\n")
	b.WriteString("// APIGenCommandSpec is generated from JSON IR.\n")
	b.WriteString("type APIGenCommandSpec = apigencobra.CommandSpec\n\n")
	b.WriteString("// APIGenParam is generated parameter metadata from JSON IR.\n")
	b.WriteString("type APIGenParam = apigencobra.Param\n\n")
	b.WriteString("// APIGenField is generated request body field metadata from JSON IR.\n")
	b.WriteString("type APIGenField = apigencobra.Field\n\n")
	b.WriteString("// APIGenRequestBody is generated request body metadata from JSON IR.\n")
	b.WriteString("type APIGenRequestBody = apigencobra.RequestBodySpec\n\n")
	b.WriteString("// APIGenArgBinding is generated positional argument metadata from JSON IR.\n")
	b.WriteString("type APIGenArgBinding = apigencobra.ArgBinding\n\n")
	b.WriteString("// APIGenOutput is generated output rendering metadata from JSON IR.\n")
	b.WriteString("type APIGenOutput = apigencobra.OutputSpec\n\n")
	b.WriteString("// APIGenPagination is generated pagination metadata from JSON IR.\n")
	b.WriteString("type APIGenPagination = apigencobra.PaginationSpec\n\n")
	b.WriteString("// APIGeneratedCommandSpecs contains operation metadata for generated CLI execution.\n")
	b.WriteString("var APIGeneratedCommandSpecs = []APIGenCommandSpec{\n")
	for _, endpoint := range normalized.Endpoints {
		spec, ok := commandSpec(normalized, endpoint)
		if !ok {
			continue
		}
		fmt.Fprintf(&b, "\t%s,\n", spec)
	}
	b.WriteString("}\n\n")
	return []byte(b.String()), nil
}

func packageName(opts Options) string {
	if strings.TrimSpace(opts.PackageName) == "" {
		return "gen"
	}
	return opts.PackageName
}

func commandSpec(doc ir.Document, endpoint ir.Endpoint) (string, bool) {
	cli := endpoint.CLI
	if cli == nil || len(cli.Command) == 0 {
		return "", false
	}

	requestBody := renderRequestBody(doc, endpoint)
	return fmt.Sprintf("{OperationID: %q, Method: %q, Path: %q, Summary: %q, Description: %q, Tags: %s, Parameters: %s, RequestBody: %s, Command: %s, Args: %s, Confirm: %q, Output: %s, Pagination: %s}",
		endpoint.OperationID,
		strings.ToUpper(endpoint.Method),
		ir.JoinAPIPath(doc.API.BasePath, endpoint.Path),
		endpoint.Summary,
		endpoint.Description,
		renderStringSlice(endpoint.Tags),
		renderParams(doc, endpoint.Parameters),
		requestBody,
		renderCommand(cli.Command),
		renderArgs(cli.Args),
		cli.Confirm,
		renderOutput(cli.Output),
		renderPagination(cli.Pagination),
	), true
}

func renderRequestBody(doc ir.Document, endpoint ir.Endpoint) string {
	if endpoint.RequestBody == nil {
		return "nil"
	}
	bodyInput := "none"
	if endpoint.CLI != nil {
		bodyInput = endpoint.CLI.BodyInput
	}
	fields := collectBodyFields(doc, endpoint)
	schemaType := schemaType(doc, endpoint.RequestBody.Schema)
	return fmt.Sprintf("&apigencobra.RequestBodySpec{Required: %t, ContentType: %q, SchemaType: %q, InputMode: %q, Fields: %s}",
		endpoint.RequestBody.Required,
		defaultContentType(endpoint.RequestBody.ContentType),
		schemaType,
		bodyInput,
		renderFields(fields),
	)
}

func collectBodyFields(doc ir.Document, endpoint ir.Endpoint) []apiField {
	bodySchema, ok := ir.ResolveRequestBodySchema(doc, endpoint)
	if !ok || bodySchema.Type != "object" {
		return nil
	}

	required := make(map[string]struct{}, len(bodySchema.Required))
	for _, name := range bodySchema.Required {
		required[name] = struct{}{}
	}

	names := ir.OrderedPropertyNames(bodySchema)
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

func schemaType(doc ir.Document, schemaRef ir.SchemaRef) string {
	if schemaRef.Type != "" {
		return schemaRef.Type
	}
	if schema, ok := ir.ResolveSchema(doc, schemaRef); ok {
		if schema.Type != "" {
			return schema.Type
		}
	}
	return "string"
}

func schemaEnum(doc ir.Document, schemaRef ir.SchemaRef) []string {
	if schema, ok := ir.ResolveSchema(doc, schemaRef); ok && len(schema.Enum) > 0 {
		values := make([]string, len(schema.Enum))
		copy(values, schema.Enum)
		return values
	}
	return nil
}

func renderParams(doc ir.Document, params []ir.Parameter) string {
	if len(params) == 0 {
		return "nil"
	}
	rendered := make([]string, 0, len(params))
	for _, parameter := range params {
		rendered = append(rendered, fmt.Sprintf("{Name: %q, In: %q, Type: %q, Description: %q, Required: %t, Enum: %s}",
			parameter.Name,
			parameter.In,
			schemaType(doc, parameter.Schema),
			parameter.Description,
			parameter.Required,
			renderStringSlice(schemaEnum(doc, parameter.Schema)),
		))
	}
	return "[]apigencobra.Param{" + strings.Join(rendered, ", ") + "}"
}

func renderFields(fields []apiField) string {
	if len(fields) == 0 {
		return "nil"
	}
	rendered := make([]string, 0, len(fields))
	for _, field := range fields {
		rendered = append(rendered, fmt.Sprintf("{Name: %q, Type: %q, Description: %q, Required: %t, Enum: %s}",
			field.Name,
			field.Type,
			field.Description,
			field.Required,
			renderStringSlice(field.Enum),
		))
	}
	return "[]apigencobra.Field{" + strings.Join(rendered, ", ") + "}"
}

func renderCommand(command []string) string {
	if len(command) == 0 {
		return "nil"
	}
	return renderStringSlice(command)
}

func renderArgs(args []ir.CLIArg) string {
	if len(args) == 0 {
		return "nil"
	}
	rendered := make([]string, 0, len(args))
	for _, arg := range args {
		rendered = append(rendered, fmt.Sprintf("{Source: %q, Name: %q, DisplayName: %q}", arg.Source, arg.Name, arg.DisplayName))
	}
	return "[]apigencobra.ArgBinding{" + strings.Join(rendered, ", ") + "}"
}

func renderOutput(output *ir.CLIOutput) string {
	if output == nil {
		return "apigencobra.OutputSpec{}"
	}
	return fmt.Sprintf("apigencobra.OutputSpec{Mode: %q, TableColumns: %s, QuietFields: %s}",
		output.Mode,
		renderStringSlice(output.TableColumns),
		renderStringSlice(output.QuietFields),
	)
}

func renderPagination(pagination *ir.CLIPagination) string {
	if pagination == nil {
		return "nil"
	}
	return fmt.Sprintf("&apigencobra.PaginationSpec{ItemsField: %q, NextPageTokenField: %q}",
		pagination.ItemsField,
		pagination.NextPageTokenField,
	)
}

func renderStringSlice(values []string) string {
	if len(values) == 0 {
		return "nil"
	}
	rendered := make([]string, 0, len(values))
	for _, value := range values {
		rendered = append(rendered, fmt.Sprintf("%q", value))
	}
	return "[]string{" + strings.Join(rendered, ", ") + "}"
}

func defaultContentType(value string) string {
	if strings.TrimSpace(value) == "" {
		return "application/json"
	}
	return value
}

type apiField struct {
	Name        string
	Type        string
	Description string
	Required    bool
	Enum        []string
}
