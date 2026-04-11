// Package cobra defines the supported Cobra runtime boundary for generated CLIs.
package cobra

// Endpoint is generated CLI metadata for one API operation.
type Endpoint struct {
	OperationID string   `json:"operation_id"`
	Method      string   `json:"method"`
	Path        string   `json:"path"`
	Summary     string   `json:"summary"`
	Description string   `json:"description,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Parameters  []Param  `json:"parameters,omitempty"`
	BodyFields  []Field  `json:"body_fields,omitempty"`
	CLICommand  string   `json:"cli_command,omitempty"`
}

// Param describes one generated endpoint parameter.
type Param struct {
	Name        string   `json:"name"`
	In          string   `json:"in"`
	Type        string   `json:"type"`
	Description string   `json:"description,omitempty"`
	Required    bool     `json:"required,omitempty"`
	Enum        []string `json:"enum,omitempty"`
}

// Field describes one generated JSON request body field.
type Field struct {
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Description string   `json:"description,omitempty"`
	Required    bool     `json:"required,omitempty"`
	Enum        []string `json:"enum,omitempty"`
}

// PaginatedResponse is the minimal envelope used by FetchAllPages.
type PaginatedResponse struct {
	Data          []interface{} `json:"data"`
	NextPageToken string        `json:"next_page_token"`
}
