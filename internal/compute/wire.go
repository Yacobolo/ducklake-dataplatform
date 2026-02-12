package compute

// ExecuteRequest is the JSON body sent to POST /execute on a compute agent.
// Used by both the remote executor client (internal/compute/remote.go) and
// the agent handler (internal/agent/handler.go) to ensure the wire contract
// stays in sync at compile time.
type ExecuteRequest struct {
	SQL       string `json:"sql"`
	RequestID string `json:"request_id"`
}

// ExecuteResponse is the JSON body returned from POST /execute on success.
type ExecuteResponse struct {
	Columns   []string        `json:"columns"`
	Rows      [][]interface{} `json:"rows"`
	RowCount  int             `json:"row_count"`
	RequestID string          `json:"request_id,omitempty"`
	Error     string          `json:"error,omitempty"`
	Code      string          `json:"code,omitempty"`
}

// ErrorResponse is the JSON error body returned by the agent on failures.
type ErrorResponse struct {
	Error     string `json:"error"`
	Code      string `json:"code"`
	RequestID string `json:"request_id,omitempty"`
}
