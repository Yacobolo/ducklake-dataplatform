// Package cuegen compiles authored CUE API definitions into APIGen IR and
// canonical OpenAPI artifacts.
package cuegen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/load"

	openapiemit "github.com/Yacobolo/quackstack/pkg/apigen/emit/openapi"
	"github.com/Yacobolo/quackstack/pkg/apigen/ir"
)

// Source is the parity-first CUE representation for the APIGen contract.
type Source struct {
	SchemaVersion string               `json:"schema_version"`
	API           ir.API               `json:"api"`
	Info          ir.Info              `json:"info"`
	OpenAPI       ir.OpenAPI           `json:"openapi,omitempty"`
	Servers       []ir.Server          `json:"servers,omitempty"`
	Tags          []ir.Tag             `json:"tags,omitempty"`
	Schemas       map[string]ir.Schema `json:"schemas,omitempty"`
	Endpoints     []ir.Endpoint        `json:"endpoints"`
	Extensions    map[string]any       `json:"extensions,omitempty"`
}

// Bundle contains the IR document plus the canonical OpenAPI artifact emitted
// from the CUE source tree.
type Bundle struct {
	Document         ir.Document
	CanonicalOpenAPI []byte
}

// CompileDir loads a CUE package directory and compiles it into APIGen inputs.
func CompileDir(dir string) (Bundle, error) {
	instances := load.Instances([]string{"."}, &load.Config{Dir: dir})
	if len(instances) == 0 {
		return Bundle{}, fmt.Errorf("load cue package %q: no instances found", dir)
	}
	if instances[0].Err != nil {
		return Bundle{}, fmt.Errorf("load cue package %q: %w", dir, instances[0].Err)
	}

	ctx := cuecontext.New()
	value := ctx.BuildInstance(instances[0])
	if err := value.Validate(); err != nil {
		return Bundle{}, fmt.Errorf("validate cue package %q: %w", dir, err)
	}

	var source Source
	if err := value.Decode(&source); err != nil {
		return Bundle{}, fmt.Errorf("decode cue package %q: %w", dir, err)
	}
	applySchemaPropertyOrderFromSource(value, source.Schemas, "schemas")

	fullDoc := ir.Document{
		SchemaVersion: source.SchemaVersion,
		API:           source.API,
		Info:          source.Info,
		OpenAPI:       source.OpenAPI,
		Servers:       source.Servers,
		Tags:          source.Tags,
		Schemas:       cloneSchemas(source.Schemas),
		Endpoints:     cloneEndpoints(source.Endpoints),
		Extensions:    source.Extensions,
	}
	doc := ir.Document{
		SchemaVersion: source.SchemaVersion,
		API:           source.API,
		Info:          source.Info,
		OpenAPI:       source.OpenAPI,
		Servers:       source.Servers,
		Tags:          source.Tags,
		Schemas:       cloneSchemas(source.Schemas),
		Endpoints:     filterGeneratedEndpoints(cloneEndpoints(source.Endpoints)),
		Extensions:    source.Extensions,
	}
	if err := ir.Validate(doc); err != nil {
		return Bundle{}, fmt.Errorf("validate cue-derived ir: %w", err)
	}
	ir.Normalize(&doc)
	if err := ir.Validate(fullDoc); err != nil {
		return Bundle{}, fmt.Errorf("validate cue-derived canonical doc: %w", err)
	}
	ir.Normalize(&fullDoc)

	canonicalOpenAPI, err := openapiemit.EmitYAML(fullDoc, openapiemit.Options{})
	if err != nil {
		return Bundle{}, fmt.Errorf("emit canonical openapi yaml: %w", err)
	}
	canonicalOpenAPI = normalizeCanonicalYAML(canonicalOpenAPI)
	canonicalOpenAPI = append(bytes.TrimSpace(canonicalOpenAPI), '\n')

	return Bundle{
		Document:         doc,
		CanonicalOpenAPI: canonicalOpenAPI,
	}, nil
}

func applySchemaPropertyOrderFromSource(root cue.Value, schemas map[string]ir.Schema, field string) {
	if len(schemas) == 0 {
		return
	}
	sourceSchemas := root.LookupPath(cue.ParsePath(field))
	if !sourceSchemas.Exists() {
		return
	}
	schemaIter, err := sourceSchemas.Fields(cue.Definitions(false), cue.Optional(true), cue.Hidden(false))
	if err != nil {
		return
	}
	for schemaIter.Next() {
		name := schemaIter.Selector().Unquoted()
		schema, ok := schemas[name]
		if !ok || len(schema.PropertyOrder) > 0 || len(schema.Properties) == 0 {
			continue
		}
		properties := schemaIter.Value().LookupPath(cue.ParsePath("properties"))
		if !properties.Exists() {
			continue
		}
		propertyIter, err := properties.Fields(cue.Definitions(false), cue.Optional(true), cue.Hidden(false))
		if err != nil {
			continue
		}
		order := make([]string, 0, len(schema.Properties))
		for propertyIter.Next() {
			propertyName := propertyIter.Selector().Unquoted()
			if _, ok := schema.Properties[propertyName]; ok {
				order = append(order, propertyName)
			}
		}
		if len(order) > 0 {
			schema.PropertyOrder = order
			schemas[name] = schema
		}
	}
}

func filterGeneratedEndpoints(endpoints []ir.Endpoint) []ir.Endpoint {
	filtered := make([]ir.Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if isManualEndpoint(endpoint) {
			continue
		}
		filtered = append(filtered, endpoint)
	}
	return filtered
}

func cloneSchemas(in map[string]ir.Schema) map[string]ir.Schema {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]ir.Schema, len(in))
	for key, value := range in {
		out[key] = cloneSchema(value)
	}
	return out
}

func cloneSchema(in ir.Schema) ir.Schema {
	out := in
	if len(in.Required) > 0 {
		out.Required = append([]string(nil), in.Required...)
	}
	if len(in.Enum) > 0 {
		out.Enum = append([]string(nil), in.Enum...)
	}
	if len(in.PropertyOrder) > 0 {
		out.PropertyOrder = append([]string(nil), in.PropertyOrder...)
	}
	if in.Items != nil {
		item := cloneSchemaRef(*in.Items)
		out.Items = &item
	}
	if len(in.Properties) > 0 {
		out.Properties = make(map[string]ir.SchemaProperty, len(in.Properties))
		for key, value := range in.Properties {
			out.Properties[key] = ir.SchemaProperty{
				Description: value.Description,
				Example:     cloneAny(value.Example),
				Schema:      cloneSchemaRef(value.Schema),
			}
		}
	}
	out.Example = cloneAny(in.Example)
	return out
}

func cloneSchemaRef(in ir.SchemaRef) ir.SchemaRef {
	out := in
	if in.Items != nil {
		item := cloneSchemaRef(*in.Items)
		out.Items = &item
	}
	if in.AdditionalProperties != nil {
		additional := cloneAdditionalProperties(*in.AdditionalProperties)
		out.AdditionalProperties = &additional
	}
	return out
}

func cloneAdditionalProperties(in ir.AdditionalProperties) ir.AdditionalProperties {
	out := in
	if in.Schema != nil {
		schema := cloneSchemaRef(*in.Schema)
		out.Schema = &schema
	}
	return out
}

func cloneEndpoints(in []ir.Endpoint) []ir.Endpoint {
	if len(in) == 0 {
		return nil
	}
	out := make([]ir.Endpoint, len(in))
	for i, endpoint := range in {
		out[i] = cloneEndpoint(endpoint)
	}
	return out
}

func cloneEndpoint(in ir.Endpoint) ir.Endpoint {
	out := in
	if len(in.Tags) > 0 {
		out.Tags = append([]string(nil), in.Tags...)
	}
	if len(in.Parameters) > 0 {
		out.Parameters = make([]ir.Parameter, len(in.Parameters))
		for i, parameter := range in.Parameters {
			out.Parameters[i] = parameter
			out.Parameters[i].Example = cloneAny(parameter.Example)
			out.Parameters[i].Schema = cloneSchemaRef(parameter.Schema)
			if parameter.Explode != nil {
				value := *parameter.Explode
				out.Parameters[i].Explode = &value
			}
		}
	}
	if in.RequestBody != nil {
		body := *in.RequestBody
		body.Example = cloneAny(in.RequestBody.Example)
		body.Schema = cloneSchemaRef(in.RequestBody.Schema)
		out.RequestBody = &body
	}
	if len(in.Responses) > 0 {
		out.Responses = make([]ir.Response, len(in.Responses))
		for i, response := range in.Responses {
			out.Responses[i] = response
			out.Responses[i].Example = cloneAny(response.Example)
			if response.Schema != nil {
				schema := cloneSchemaRef(*response.Schema)
				out.Responses[i].Schema = &schema
			}
			if len(response.AnyOf) > 0 {
				out.Responses[i].AnyOf = make([]ir.SchemaRef, len(response.AnyOf))
				for j, ref := range response.AnyOf {
					out.Responses[i].AnyOf[j] = cloneSchemaRef(ref)
				}
			}
			if len(response.Headers) > 0 {
				out.Responses[i].Headers = make([]ir.Header, len(response.Headers))
				for j, header := range response.Headers {
					out.Responses[i].Headers[j] = header
					out.Responses[i].Headers[j].Schema = cloneSchemaRef(header.Schema)
				}
			}
		}
	}
	if len(in.Security) > 0 {
		out.Security = cloneSecurityRequirements(in.Security)
	}
	if len(in.Extensions) > 0 {
		out.Extensions = cloneMapAny(in.Extensions)
	}
	return out
}

func cloneSecurityRequirements(in []ir.SecurityRequirement) []ir.SecurityRequirement {
	out := make([]ir.SecurityRequirement, len(in))
	for i, requirement := range in {
		cloned := make(ir.SecurityRequirement, len(requirement))
		for key, scopes := range requirement {
			cloned[key] = append([]string(nil), scopes...)
		}
		out[i] = cloned
	}
	return out
}

func cloneMapAny(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneAny(value)
	}
	return out
}

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMapAny(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneAny(item)
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	default:
		return typed
	}
}

func isManualEndpoint(endpoint ir.Endpoint) bool {
	if endpoint.Extensions == nil {
		return false
	}
	value, ok := endpoint.Extensions["x-apigen-manual"]
	if !ok {
		return false
	}
	manual, ok := value.(bool)
	return ok && manual
}

// WriteBundle persists a compiled CUE bundle to disk.
func WriteBundle(bundle Bundle, irPath string, openAPIPath string) error {
	if err := writeJSON(irPath, bundle.Document); err != nil {
		return err
	}
	if err := writeBytes(openAPIPath, bundle.CanonicalOpenAPI); err != nil {
		return err
	}
	return nil
}

// Bootstrap writes a CUE source tree from an IR document and the canonical
// OpenAPI artifact. The output keeps the APIGen contract editable in CUE
// without relying on any legacy source-layout metadata.
func Bootstrap(doc ir.Document, outDir string) error {
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return fmt.Errorf("create cue output directory: %w", err)
	}
	if err := cleanupStructuredLayout(outDir); err != nil {
		return err
	}

	if err := writeBytes(filepath.Join(outDir, "schema.cue"), []byte(schemaFile)); err != nil {
		return err
	}
	if err := writeFieldFile(filepath.Join(outDir, "metadata.cue"), "schema_version", doc.SchemaVersion); err != nil {
		return err
	}
	if err := appendFieldFile(filepath.Join(outDir, "metadata.cue"), "api", doc.API); err != nil {
		return err
	}
	if err := appendFieldFile(filepath.Join(outDir, "metadata.cue"), "info", doc.Info); err != nil {
		return err
	}
	if len(doc.Servers) > 0 {
		if err := appendFieldFile(filepath.Join(outDir, "metadata.cue"), "servers", doc.Servers); err != nil {
			return err
		}
	}
	if len(doc.Tags) > 0 {
		if err := appendFieldFile(filepath.Join(outDir, "metadata.cue"), "tags", doc.Tags); err != nil {
			return err
		}
	}
	if len(doc.Extensions) > 0 {
		if err := appendFieldFile(filepath.Join(outDir, "metadata.cue"), "extensions", doc.Extensions); err != nil {
			return err
		}
	}
	if !isZeroOpenAPI(doc.OpenAPI) {
		if err := appendFieldFile(filepath.Join(outDir, "metadata.cue"), "openapi", doc.OpenAPI); err != nil {
			return err
		}
	}
	if err := writeSchemaFile(filepath.Join(outDir, "schemas.cue"), "schemas", doc.Schemas, "authored APIGen schema set"); err != nil {
		return err
	}
	if err := writeEndpointFile(filepath.Join(outDir, "endpoints.cue"), "endpoints", doc.Endpoints, "authored APIGen endpoint set"); err != nil {
		return err
	}

	return nil
}

func isZeroOpenAPI(openapi ir.OpenAPI) bool {
	return openapi.Version == "" &&
		len(openapi.TagOrder) == 0 &&
		len(openapi.Security) == 0 &&
		len(openapi.SecuritySchemes) == 0
}

func cleanupStructuredLayout(outDir string) error {
	patterns := []string{
		filepath.Join(outDir, "models_*.cue"),
		filepath.Join(outDir, "operations_*.cue"),
	}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return fmt.Errorf("glob cue layout %s: %w", pattern, err)
		}
		for _, match := range matches {
			if err := os.Remove(match); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove stale cue layout file %s: %w", match, err)
			}
		}
	}
	for _, dir := range []string{filepath.Join(outDir, "models"), filepath.Join(outDir, "operations")} {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove stale cue layout directory %s: %w", dir, err)
		}
	}
	return nil
}

func writeJSON(path string, value any) error {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal json %s: %w", path, err)
	}
	content = append(bytes.TrimSpace(content), '\n')
	return writeBytes(path, content)
}

func writeBytes(path string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create output directory for %s: %w", path, err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func writeFieldFile(path string, field string, value any) error {
	content, err := renderFieldFile(field, value)
	if err != nil {
		return err
	}
	return writeBytes(path, content)
}

func appendFieldFile(path string, field string, value any) error {
	content, err := renderField(field, value)
	if err != nil {
		return err
	}
	//nolint:gosec // Path is a repo-local generated output path chosen by the caller.
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open %s for append: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()
	if _, err := file.Write(content); err != nil {
		return fmt.Errorf("append %s: %w", path, err)
	}
	return nil
}

func renderFieldFile(field string, value any) ([]byte, error) {
	rendered, err := renderField(field, value)
	if err != nil {
		return nil, err
	}
	return append([]byte("package api\n\n"), rendered...), nil
}

func renderField(field string, value any) ([]byte, error) {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal cue field %s: %w", field, err)
	}
	var builder strings.Builder
	builder.WriteString(field)
	builder.WriteString(": ")
	builder.Write(content)
	builder.WriteString("\n\n")
	return []byte(builder.String()), nil
}

var (
	quotedYAMLKeyPattern      = regexp.MustCompile(`^(\s*)"([A-Za-z0-9_]+)":(.*)$`)
	quotedYAMLListItemPattern = regexp.MustCompile(`^(\s*)- "([A-Za-z0-9_]+)"\s*$`)
)

func normalizeCanonicalYAML(content []byte) []byte {
	lines := strings.Split(string(content), "\n")
	for i, line := range lines {
		if match := quotedYAMLKeyPattern.FindStringSubmatch(line); len(match) == 4 {
			lines[i] = match[1] + "'" + match[2] + "':" + match[3]
			continue
		}
		if match := quotedYAMLListItemPattern.FindStringSubmatch(line); len(match) == 3 {
			lines[i] = match[1] + "- '" + match[2] + "'"
		}
	}
	return []byte(strings.Join(lines, "\n"))
}

func writeSchemaFile(path string, field string, value any, source string) error {
	return writeNamedFieldFile(path, field, value, source)
}

func writeEndpointFile(path string, field string, value any, source string) error {
	return writeNamedFieldFile(path, field, value, source)
}

func writeNamedFieldFile(path string, field string, value any, source string) error {
	content, err := renderFieldFile(field, value)
	if err != nil {
		return err
	}
	if source != "" {
		comment := fmt.Sprintf("// %s.\n", source)
		content = append([]byte("package api\n\n"+comment+"\n"), content[len("package api\n\n"):]...)
	}
	return writeBytes(path, content)
}

const schemaFile = `package api

// #Source defines the authored CUE contract consumed by APIGen.
#SchemaRef: {
	ref?: string
	type?: string
	format?: string
	items?: #SchemaRef
	additional_properties?: #AdditionalProperties
}

#AdditionalProperties: {
	any?: bool
	schema?: #SchemaRef
}

#SchemaProperty: {
	description?: string
	example?: _
	schema: #SchemaRef
}

#Schema: {
	type: string
	title?: string
	description?: string
	example?: _
	properties?: [string]: #SchemaProperty
	property_order?: [...string]
	required?: [...string]
	items?: #SchemaRef
	enum?: [...string]
}

#Header: {
	name: string
	required?: bool
	description?: string
	schema: #SchemaRef
}

#Response: {
	status_code: int
	description: string
	headers?: [...#Header]
	content_type?: string
	example?: _
	schema?: #SchemaRef
	any_of?: [...#SchemaRef]
	extensions?: [string]: _
}

#Parameter: {
	name: string
	in: string
	required?: bool
	description?: string
	example?: _
	explode?: bool
	schema: #SchemaRef
}

#RequestBody: {
	required?: bool
	description?: string
	content_type?: string
	example?: _
	schema: #SchemaRef
}

#SecurityRequirement: [string]: [...string]

#SecurityScheme: {
	type: string
	in?: string
	name?: string
	scheme?: string
}

#ServerVariable: {
	default?: string
	description?: string
	enum?: [...string]
}

#Server: {
	url: string
	description?: string
	variables?: [string]: #ServerVariable
}

#OpenAPI: {
	version?: string
	tag_order?: [...string]
	security?: [...#SecurityRequirement]
	security_schemes?: [string]: #SecurityScheme
}

#Endpoint: {
	method: string
	path: string
	operation_id: string
	summary?: string
	description?: string
	tags?: [...string]
	parameters?: [...#Parameter]
	request_body?: #RequestBody
	responses: [...#Response]
	extensions?: [string]: _
}

#Source: {
	schema_version: string
	api: {
		base_path: string
	}
	info: {
		title: string
		version: string
		description?: string
	}
	openapi?: #OpenAPI
	servers?: [...#Server]
	tags?: [..._]
	schemas?: [string]: #Schema
	endpoints: [...#Endpoint]
	extensions?: [string]: _
}
`
