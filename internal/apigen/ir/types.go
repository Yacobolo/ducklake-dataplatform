// Package ir defines the JSON IR schema consumed by apigen emitters.
package ir

// Document is the root JSON IR payload.
type Document struct {
	SchemaVersion string            `json:"schema_version"`
	Info          Info              `json:"info"`
	Servers       []Server          `json:"servers,omitempty"`
	Tags          []Tag             `json:"tags,omitempty"`
	Schemas       map[string]Schema `json:"schemas,omitempty"`
	Endpoints     []Endpoint        `json:"endpoints"`
	Extensions    map[string]any    `json:"extensions,omitempty"`
}

// Info contains API metadata.
type Info struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

// Server describes a server URL entry.
type Server struct {
	URL         string `json:"url"`
	Description string `json:"description,omitempty"`
}

// Tag describes a logical operation grouping.
type Tag struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// Endpoint describes one API operation.
type Endpoint struct {
	Method      string         `json:"method"`
	Path        string         `json:"path"`
	OperationID string         `json:"operation_id"`
	Summary     string         `json:"summary,omitempty"`
	Description string         `json:"description,omitempty"`
	Tags        []string       `json:"tags,omitempty"`
	Parameters  []Parameter    `json:"parameters,omitempty"`
	RequestBody *RequestBody   `json:"request_body,omitempty"`
	Responses   []Response     `json:"responses"`
	Extensions  map[string]any `json:"extensions,omitempty"`
}

// Parameter describes an operation parameter.
type Parameter struct {
	Name        string    `json:"name"`
	In          string    `json:"in"`
	Required    bool      `json:"required,omitempty"`
	Description string    `json:"description,omitempty"`
	Schema      SchemaRef `json:"schema"`
}

// RequestBody describes the JSON request payload.
type RequestBody struct {
	Required    bool      `json:"required,omitempty"`
	Description string    `json:"description,omitempty"`
	Schema      SchemaRef `json:"schema"`
}

// Response describes one operation response.
type Response struct {
	StatusCode  int            `json:"status_code"`
	Description string         `json:"description"`
	Headers     []Header       `json:"headers,omitempty"`
	Schema      *SchemaRef     `json:"schema,omitempty"`
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
	Ref    string     `json:"ref,omitempty"`
	Type   string     `json:"type,omitempty"`
	Format string     `json:"format,omitempty"`
	Items  *SchemaRef `json:"items,omitempty"`
}

// Schema is a JSON schema subset used by apigen.
type Schema struct {
	Type        string                    `json:"type"`
	Description string                    `json:"description,omitempty"`
	Properties  map[string]SchemaProperty `json:"properties,omitempty"`
	Required    []string                  `json:"required,omitempty"`
	Items       *SchemaRef                `json:"items,omitempty"`
	Enum        []string                  `json:"enum,omitempty"`
}

// SchemaProperty describes one schema property.
type SchemaProperty struct {
	Description string    `json:"description,omitempty"`
	Schema      SchemaRef `json:"schema"`
}
