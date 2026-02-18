// Package openapi renders markdown docs from an OpenAPI specification.
package openapi

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
)

type endpointDoc struct {
	Method       string
	Path         string
	OperationID  string
	Summary      string
	Description  string
	Deprecated   bool
	PathParams   []paramDoc
	QueryParams  []paramDoc
	HeaderParams []paramDoc
	RequestBody  *requestBodyDoc
	Responses    []responseDoc
}

type paramDoc struct {
	Name        string
	Required    bool
	Type        string
	Description string
}

type requestBodyDoc struct {
	Required     bool
	ContentTypes []string
}

type responseDoc struct {
	Code        string
	Description string
}

// Generate renders OpenAPI docs to markdown files.
func Generate(specPath, outDir string) error {
	loader := &openapi3.Loader{IsExternalRefsAllowed: true}
	spec, err := loader.LoadFromFile(specPath)
	if err != nil {
		return fmt.Errorf("load spec: %w", err)
	}

	if err := os.RemoveAll(outDir); err != nil {
		return fmt.Errorf("clean output dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(outDir, "endpoints"), 0o750); err != nil {
		return fmt.Errorf("create endpoints dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(outDir, "schemas"), 0o750); err != nil {
		return fmt.Errorf("create schemas dir: %w", err)
	}

	tagDescriptions := map[string]string{}
	for _, tag := range spec.Tags {
		tagDescriptions[tag.Name] = strings.TrimSpace(tag.Description)
	}

	tagEndpoints := map[string][]endpointDoc{}
	for path, pathItem := range spec.Paths.Map() {
		for method, op := range pathItem.Operations() {
			tags := op.Tags
			if len(tags) == 0 {
				tags = []string{"Untagged"}
			}

			for _, tag := range tags {
				tagEndpoints[tag] = append(tagEndpoints[tag], buildEndpointDoc(path, method, pathItem, op))
			}
		}
	}

	tags := sortedKeys(tagEndpoints)
	for _, tag := range tags {
		endpoints := tagEndpoints[tag]
		sortEndpoints(endpoints)
		if err := writeTagPage(filepath.Join(outDir, "endpoints", fileSlug(tag)+".md"), tag, tagDescriptions[tag], endpoints); err != nil {
			return err
		}
	}

	schemaNames := sortedKeys(spec.Components.Schemas)
	for _, name := range schemaNames {
		ref := spec.Components.Schemas[name]
		if err := writeSchemaPage(filepath.Join(outDir, "schemas", fileSlug(name)+".md"), name, ref); err != nil {
			return err
		}
	}

	if err := writeAPIIndex(filepath.Join(outDir, "index.md"), tags, tagEndpoints, schemaNames); err != nil {
		return err
	}
	if err := writeFeaturesPage(filepath.Join(outDir, "features.md"), tags, tagDescriptions, tagEndpoints); err != nil {
		return err
	}

	return nil
}

func buildEndpointDoc(path, method string, pathItem *openapi3.PathItem, op *openapi3.Operation) endpointDoc {
	params := append([]*openapi3.ParameterRef{}, pathItem.Parameters...)
	params = append(params, op.Parameters...)

	endpoint := endpointDoc{
		Method:      strings.ToUpper(method),
		Path:        path,
		OperationID: strings.TrimSpace(op.OperationID),
		Summary:     strings.TrimSpace(op.Summary),
		Description: strings.TrimSpace(op.Description),
		Deprecated:  op.Deprecated,
	}

	for _, p := range params {
		if p == nil || p.Value == nil {
			continue
		}
		pd := paramDoc{
			Name:        p.Value.Name,
			Required:    p.Value.Required,
			Type:        schemaTypeFromRef(p.Value.Schema),
			Description: cleanInline(p.Value.Description),
		}
		switch p.Value.In {
		case "path":
			endpoint.PathParams = append(endpoint.PathParams, pd)
		case "query":
			endpoint.QueryParams = append(endpoint.QueryParams, pd)
		case "header":
			endpoint.HeaderParams = append(endpoint.HeaderParams, pd)
		}
	}

	sortParams(endpoint.PathParams)
	sortParams(endpoint.QueryParams)
	sortParams(endpoint.HeaderParams)

	if op.RequestBody != nil && op.RequestBody.Value != nil {
		contentTypes := sortedKeys(op.RequestBody.Value.Content)
		endpoint.RequestBody = &requestBodyDoc{
			Required:     op.RequestBody.Value.Required,
			ContentTypes: contentTypes,
		}
	}

	for code, response := range op.Responses.Map() {
		desc := ""
		if response != nil && response.Value != nil {
			if response.Value.Description != nil {
				desc = cleanInline(*response.Value.Description)
			}
		}
		endpoint.Responses = append(endpoint.Responses, responseDoc{Code: code, Description: desc})
	}
	sortResponses(endpoint.Responses)

	return endpoint
}

func writeAPIIndex(path string, tags []string, tagEndpoints map[string][]endpointDoc, schemaNames []string) error {
	var b strings.Builder
	b.WriteString(generatedHeader())
	b.WriteString("# API Reference\n\n")
	b.WriteString("This section is generated from `internal/api/openapi.bundled.yaml`.\n\n")
	b.WriteString("- [Feature Overview](./features)\n\n")
	b.WriteString("## Endpoint Groups\n\n")
	for _, tag := range tags {
		count := len(tagEndpoints[tag])
		b.WriteString(fmt.Sprintf("- [%s](./endpoints/%s) (%d operations)\n", tag, fileSlug(tag), count))
	}
	b.WriteString("\n## Schemas\n\n")
	for _, schema := range schemaNames {
		b.WriteString(fmt.Sprintf("- [%s](./schemas/%s)\n", schema, fileSlug(schema)))
	}

	return writeFile(path, b.String())
}

func writeFeaturesPage(path string, tags []string, tagDescriptions map[string]string, tagEndpoints map[string][]endpointDoc) error {
	var b strings.Builder
	b.WriteString(generatedHeader())
	b.WriteString("# Platform Features\n\n")
	b.WriteString("This page is generated from OpenAPI tags and operations.\n\n")
	b.WriteString("| Feature | What you can do | API coverage |\n")
	b.WriteString("| --- | --- | --- |\n")
	for _, tag := range tags {
		desc := tagDescriptions[tag]
		if desc == "" {
			desc = "-"
		}
		b.WriteString(fmt.Sprintf("| [%s](./endpoints/%s) | %s | %d operations |\n", tag, fileSlug(tag), tableSafe(desc), len(tagEndpoints[tag])))
	}

	return writeFile(path, b.String())
}

func writeTagPage(path, tag, description string, endpoints []endpointDoc) error {
	var b strings.Builder
	b.WriteString(generatedHeader())
	b.WriteString(fmt.Sprintf("# %s Endpoints\n\n", tag))
	if description != "" {
		b.WriteString(description)
		b.WriteString("\n\n")
	}

	for _, endpoint := range endpoints {
		title := fmt.Sprintf("## `%s %s`", endpoint.Method, endpoint.Path)
		b.WriteString(title)
		b.WriteString("\n\n")

		if endpoint.Summary != "" {
			b.WriteString(endpoint.Summary)
			b.WriteString("\n\n")
		}
		if endpoint.Description != "" {
			b.WriteString(endpoint.Description)
			b.WriteString("\n\n")
		}

		if endpoint.OperationID != "" {
			b.WriteString(fmt.Sprintf("- Operation ID: `%s`\n", endpoint.OperationID))
		}
		if endpoint.Deprecated {
			b.WriteString("- Deprecated: `true`\n")
		}
		if endpoint.OperationID != "" || endpoint.Deprecated {
			b.WriteString("\n")
		}

		if len(endpoint.PathParams) > 0 {
			writeParamTable(&b, "Path Parameters", endpoint.PathParams)
		}
		if len(endpoint.QueryParams) > 0 {
			writeParamTable(&b, "Query Parameters", endpoint.QueryParams)
		}
		if len(endpoint.HeaderParams) > 0 {
			writeParamTable(&b, "Header Parameters", endpoint.HeaderParams)
		}

		if endpoint.RequestBody != nil {
			b.WriteString("### Request Body\n\n")
			b.WriteString(fmt.Sprintf("- Required: `%t`\n", endpoint.RequestBody.Required))
			if len(endpoint.RequestBody.ContentTypes) > 0 {
				b.WriteString("- Content types: ")
				for i, c := range endpoint.RequestBody.ContentTypes {
					if i > 0 {
						b.WriteString(", ")
					}
					b.WriteString("`")
					b.WriteString(c)
					b.WriteString("`")
				}
				b.WriteString("\n")
			}
			b.WriteString("\n")
		}

		if len(endpoint.Responses) > 0 {
			b.WriteString("### Responses\n\n")
			b.WriteString("| Code | Description |\n")
			b.WriteString("| --- | --- |\n")
			for _, response := range endpoint.Responses {
				b.WriteString(fmt.Sprintf("| `%s` | %s |\n", response.Code, tableSafe(response.Description)))
			}
			b.WriteString("\n")
		}
	}

	return writeFile(path, b.String())
}

func writeSchemaPage(path, name string, ref *openapi3.SchemaRef) error {
	var b strings.Builder
	b.WriteString(generatedHeader())
	b.WriteString(fmt.Sprintf("# Schema: `%s`\n\n", name))

	if ref == nil {
		b.WriteString("Schema definition is missing.\n")
		return writeFile(path, b.String())
	}

	if ref.Ref != "" {
		b.WriteString(fmt.Sprintf("- Source ref: `%s`\n\n", ref.Ref))
	}

	schema := ref.Value
	if schema == nil {
		b.WriteString("Schema body is empty.\n")
		return writeFile(path, b.String())
	}

	if schema.Description != "" {
		b.WriteString(cleanInline(schema.Description))
		b.WriteString("\n\n")
	}

	b.WriteString(fmt.Sprintf("- Type: `%s`\n", schemaType(schema)))
	if len(schema.Required) > 0 {
		required := slices.Clone(schema.Required)
		slices.Sort(required)
		b.WriteString("- Required fields: ")
		for i, field := range required {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString("`")
			b.WriteString(field)
			b.WriteString("`")
		}
		b.WriteString("\n")
	}
	b.WriteString("\n")

	if len(schema.Properties) > 0 {
		propNames := sortedKeys(schema.Properties)
		b.WriteString("## Properties\n\n")
		b.WriteString("| Name | Type | Required | Description |\n")
		b.WriteString("| --- | --- | --- | --- |\n")
		reqSet := make(map[string]struct{}, len(schema.Required))
		for _, field := range schema.Required {
			reqSet[field] = struct{}{}
		}
		for _, propName := range propNames {
			propRef := schema.Properties[propName]
			_, required := reqSet[propName]
			desc := ""
			if propRef != nil && propRef.Value != nil {
				desc = cleanInline(propRef.Value.Description)
			}
			b.WriteString(fmt.Sprintf("| `%s` | `%s` | `%t` | %s |\n", propName, schemaTypeFromRef(propRef), required, tableSafe(desc)))
		}
		b.WriteString("\n")
	}

	return writeFile(path, b.String())
}

func writeParamTable(b *strings.Builder, title string, params []paramDoc) {
	b.WriteString("### ")
	b.WriteString(title)
	b.WriteString("\n\n")
	b.WriteString("| Name | Type | Required | Description |\n")
	b.WriteString("| --- | --- | --- | --- |\n")
	for _, param := range params {
		_, _ = fmt.Fprintf(b, "| `%s` | `%s` | `%t` | %s |\n", param.Name, param.Type, param.Required, tableSafe(param.Description))
	}
	b.WriteString("\n")
}

func sortParams(params []paramDoc) {
	sort.Slice(params, func(i, j int) bool {
		return params[i].Name < params[j].Name
	})
}

func sortEndpoints(endpoints []endpointDoc) {
	methodOrder := map[string]int{
		"GET":     0,
		"POST":    1,
		"PUT":     2,
		"PATCH":   3,
		"DELETE":  4,
		"OPTIONS": 5,
		"HEAD":    6,
	}

	sort.Slice(endpoints, func(i, j int) bool {
		if endpoints[i].Path != endpoints[j].Path {
			return endpoints[i].Path < endpoints[j].Path
		}
		mi := methodOrder[endpoints[i].Method]
		mj := methodOrder[endpoints[j].Method]
		if mi != mj {
			return mi < mj
		}
		return endpoints[i].OperationID < endpoints[j].OperationID
	})
}

func sortResponses(responses []responseDoc) {
	sort.Slice(responses, func(i, j int) bool {
		ci := responses[i].Code
		cj := responses[j].Code
		if ci == "default" {
			return false
		}
		if cj == "default" {
			return true
		}
		return ci < cj
	})
}

func schemaTypeFromRef(ref *openapi3.SchemaRef) string {
	if ref == nil {
		return "unknown"
	}
	if ref.Ref != "" {
		parts := strings.Split(ref.Ref, "/")
		return parts[len(parts)-1]
	}
	if ref.Value == nil {
		return "unknown"
	}
	return schemaType(ref.Value)
}

func schemaType(schema *openapi3.Schema) string {
	if schema == nil || schema.Type == nil || len(*schema.Type) == 0 {
		return "object"
	}
	if (*schema.Type)[0] == "array" {
		if schema.Items != nil {
			return "array[" + schemaTypeFromRef(schema.Items) + "]"
		}
		return "array"
	}
	return (*schema.Type)[0]
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func fileSlug(value string) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	lower = strings.ReplaceAll(lower, " ", "-")
	lower = strings.ReplaceAll(lower, "/", "-")
	lower = strings.ReplaceAll(lower, "_", "-")
	lower = strings.ReplaceAll(lower, ".", "-")
	for strings.Contains(lower, "--") {
		lower = strings.ReplaceAll(lower, "--", "-")
	}
	return strings.Trim(lower, "-")
}

func generatedHeader() string {
	return "<!-- Code generated by cmd/docsgen. DO NOT EDIT. -->\n\n"
}

func writeFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create directory %q: %w", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write %q: %w", path, err)
	}
	return nil
}

func cleanInline(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\n", " ")
	for strings.Contains(value, "  ") {
		value = strings.ReplaceAll(value, "  ", " ")
	}
	return value
}

func tableSafe(value string) string {
	value = cleanInline(value)
	value = strings.ReplaceAll(value, "|", "\\|")
	if value == "" {
		return "-"
	}
	return value
}
