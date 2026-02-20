package compute

import (
	"encoding/base64"
	"strconv"
)

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

// Query lifecycle statuses.
const (
	QueryStatusQueued    = "QUEUED"
	QueryStatusRunning   = "RUNNING"
	QueryStatusSucceeded = "SUCCEEDED"
	QueryStatusFailed    = "FAILED"
	QueryStatusCanceled  = "CANCELED"
)

// SubmitQueryRequest creates a query job on the compute agent.
type SubmitQueryRequest struct {
	SQL       string `json:"sql"`
	RequestID string `json:"request_id,omitempty"`
}

// SubmitQueryResponse is returned when query job is accepted.
type SubmitQueryResponse struct {
	QueryID   string `json:"query_id"`
	Status    string `json:"status"`
	RequestID string `json:"request_id,omitempty"`
}

// QueryStatusResponse returns current lifecycle status.
type QueryStatusResponse struct {
	QueryID     string   `json:"query_id"`
	Status      string   `json:"status"`
	Columns     []string `json:"columns,omitempty"`
	RowCount    int      `json:"row_count,omitempty"`
	Error       string   `json:"error,omitempty"`
	CompletedAt string   `json:"completed_at,omitempty"`
	RequestID   string   `json:"request_id,omitempty"`
}

// FetchQueryResultsResponse returns a page of query results.
type FetchQueryResultsResponse struct {
	QueryID       string          `json:"query_id"`
	Columns       []string        `json:"columns"`
	Rows          [][]interface{} `json:"rows"`
	RowCount      int             `json:"row_count"`
	NextPageToken string          `json:"next_page_token,omitempty"`
	RequestID     string          `json:"request_id,omitempty"`
}

// CancelQueryResponse is returned after cancel or delete requests.
type CancelQueryResponse struct {
	QueryID   string `json:"query_id"`
	Status    string `json:"status"`
	RequestID string `json:"request_id,omitempty"`
}

// DecodePageToken converts opaque page token into an integer offset.
func DecodePageToken(token string) int {
	if token == "" {
		return 0
	}
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0
	}
	offset, err := strconv.Atoi(string(decoded))
	if err != nil || offset < 0 {
		return 0
	}
	return offset
}

// EncodePageToken converts a positive integer offset into an opaque token.
func EncodePageToken(offset int) string {
	if offset <= 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}
