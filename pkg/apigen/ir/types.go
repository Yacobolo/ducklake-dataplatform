// Package ir defines the JSON IR schema consumed by apigen emitters.
package ir

// Document is the root JSON IR payload.
type Document struct {
	SchemaVersion string            `json:"schema_version"`
	API           API               `json:"api"`
	Info          Info              `json:"info"`
	OpenAPI       OpenAPI           `json:"openapi,omitempty"`
	Servers       []Server          `json:"servers,omitempty"`
	Tags          []Tag             `json:"tags,omitempty"`
	Schemas       map[string]Schema `json:"schemas,omitempty"`
	Endpoints     []Endpoint        `json:"endpoints"`
	Extensions    map[string]any    `json:"extensions,omitempty"`
}

// API contains APIGen-owned API metadata.
type API struct {
	BasePath string `json:"base_path"`
}

// Info contains API metadata.
type Info struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

// Server describes a server URL entry.
type Server struct {
	URL         string                    `json:"url"`
	Description string                    `json:"description,omitempty"`
	Variables   map[string]ServerVariable `json:"variables,omitempty"`
}

// ServerVariable describes an OpenAPI server variable.
type ServerVariable struct {
	Default     string   `json:"default,omitempty"`
	Description string   `json:"description,omitempty"`
	Enum        []string `json:"enum,omitempty"`
}

// Tag describes a logical operation grouping.
type Tag struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// OpenAPI contains canonical OpenAPI document metadata that is not directly
// represented by the generator-oriented endpoint/schema model.
type OpenAPI struct {
	Version         string                    `json:"version,omitempty"`
	TagOrder        []string                  `json:"tag_order,omitempty"`
	Security        []SecurityRequirement     `json:"security,omitempty"`
	SecuritySchemes map[string]SecurityScheme `json:"security_schemes,omitempty"`
}

// SecurityRequirement is an OpenAPI security requirement object.
type SecurityRequirement map[string][]string

// SecurityScheme describes one named OpenAPI security scheme.
type SecurityScheme struct {
	Type   string `json:"type"`
	In     string `json:"in,omitempty"`
	Name   string `json:"name,omitempty"`
	Scheme string `json:"scheme,omitempty"`
}

// Endpoint describes one API operation.
type Endpoint struct {
	Method      string                `json:"method"`
	Path        string                `json:"path"`
	OperationID string                `json:"operation_id"`
	Summary     string                `json:"summary,omitempty"`
	Description string                `json:"description,omitempty"`
	Tags        []string              `json:"tags,omitempty"`
	Parameters  []Parameter           `json:"parameters,omitempty"`
	RequestBody *RequestBody          `json:"request_body,omitempty"`
	Responses   []Response            `json:"responses"`
	CLI         *CLI                  `json:"cli,omitempty"`
	Security    []SecurityRequirement `json:"security,omitempty"`
	Extensions  map[string]any        `json:"extensions,omitempty"`
}

// CLI describes APIGen-owned CLI metadata for one operation.
type CLI struct {
	Command    []string       `json:"command,omitempty"`
	Args       []CLIArg       `json:"args,omitempty"`
	BodyInput  string         `json:"body_input,omitempty"`
	Confirm    string         `json:"confirm,omitempty"`
	Output     *CLIOutput     `json:"output,omitempty"`
	Pagination *CLIPagination `json:"pagination,omitempty"`
}

// CLIArg binds one positional CLI argument to a request source field.
type CLIArg struct {
	Source      string `json:"source"`
	Name        string `json:"name"`
	DisplayName string `json:"display_name,omitempty"`
}

// CLIOutput controls generated response rendering.
type CLIOutput struct {
	Mode         string   `json:"mode,omitempty"`
	TableColumns []string `json:"table_columns,omitempty"`
	QuietFields  []string `json:"quiet_fields,omitempty"`
}

// CLIPagination declares the collection envelope used for --all style paging.
type CLIPagination struct {
	ItemsField         string `json:"items_field,omitempty"`
	NextPageTokenField string `json:"next_page_token_field,omitempty"`
}

// Parameter describes an operation parameter.
type Parameter struct {
	Name        string    `json:"name"`
	In          string    `json:"in"`
	Required    bool      `json:"required,omitempty"`
	Description string    `json:"description,omitempty"`
	Example     any       `json:"example,omitempty"`
	Explode     *bool     `json:"explode,omitempty"`
	Schema      SchemaRef `json:"schema"`
}

// RequestBody describes the JSON request payload.
type RequestBody struct {
	Required    bool      `json:"required,omitempty"`
	Description string    `json:"description,omitempty"`
	ContentType string    `json:"content_type,omitempty"`
	Example     any       `json:"example,omitempty"`
	Schema      SchemaRef `json:"schema"`
}

// Response describes one operation response.
type Response struct {
	StatusCode  int            `json:"status_code"`
	Description string         `json:"description"`
	Headers     []Header       `json:"headers,omitempty"`
	ContentType string         `json:"content_type,omitempty"`
	Example     any            `json:"example,omitempty"`
	Schema      *SchemaRef     `json:"schema,omitempty"`
	AnyOf       []SchemaRef    `json:"any_of,omitempty"`
	Extensions  map[string]any `json:"extensions,omitempty"`
}

// Header describes one response header.
type Header struct {
	Name        string    `json:"name"`
	Required    bool      `json:"required,omitempty"`
	Description string    `json:"description,omitempty"`
	Schema      SchemaRef `json:"schema"`
}

// ResponseShapeExtensionKey stores APIGen-owned response shape metadata.
const ResponseShapeExtensionKey = "x-apigen-response-shape"

// ResponseShape describes the APIGen-owned response transport shape.
type ResponseShape struct {
	Kind     string `json:"kind"`
	BodyType string `json:"body_type,omitempty"`
}

// SchemaRef references or describes a schema.
type SchemaRef struct {
	Ref                  string                `json:"ref,omitempty"`
	Type                 string                `json:"type,omitempty"`
	Format               string                `json:"format,omitempty"`
	Items                *SchemaRef            `json:"items,omitempty"`
	AdditionalProperties *AdditionalProperties `json:"additional_properties,omitempty"`
}

// AdditionalProperties captures OpenAPI object-map semantics for inline schema refs.
type AdditionalProperties struct {
	Any    bool       `json:"any,omitempty"`
	Schema *SchemaRef `json:"schema,omitempty"`
}

// Schema is a JSON schema subset used by apigen.
type Schema struct {
	Type          string                    `json:"type"`
	Title         string                    `json:"title,omitempty"`
	Description   string                    `json:"description,omitempty"`
	Example       any                       `json:"example,omitempty"`
	Properties    map[string]SchemaProperty `json:"properties,omitempty"`
	PropertyOrder []string                  `json:"property_order,omitempty"`
	Required      []string                  `json:"required,omitempty"`
	Items         *SchemaRef                `json:"items,omitempty"`
	Enum          []string                  `json:"enum,omitempty"`
}

// SchemaProperty describes one schema property.
type SchemaProperty struct {
	Description string    `json:"description,omitempty"`
	Example     any       `json:"example,omitempty"`
	Schema      SchemaRef `json:"schema"`
}
