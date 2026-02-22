package compute

import (
	"encoding/base64"
	"strconv"
)

// ExecuteRequest is the payload sent to ComputeWorker.Execute on a compute agent.
// It is shared by the remote executor client and the compute worker gRPC server
// to keep the internal transport contract in sync at compile time.
type ExecuteRequest struct {
	SQL       string `json:"sql"`
	RequestID string `json:"request_id"`
}

// ExecuteResponse is the payload returned by ComputeWorker.Execute on success.
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

// GetQueryStatusRequest requests current query lifecycle status.
type GetQueryStatusRequest struct {
	QueryID string `json:"query_id"`
}

// FetchQueryResultsRequest requests a page of lifecycle query results.
type FetchQueryResultsRequest struct {
	QueryID    string `json:"query_id"`
	PageToken  string `json:"page_token,omitempty"`
	MaxResults int    `json:"max_results,omitempty"`
}

// CancelQueryRequest requests cancellation for a lifecycle query.
type CancelQueryRequest struct {
	QueryID string `json:"query_id"`
}

// DeleteQueryRequest removes query lifecycle state and retained results.
type DeleteQueryRequest struct {
	QueryID string `json:"query_id"`
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

// HealthRequest requests worker health details.
type HealthRequest struct{}

// HealthResponse mirrors worker readiness and queue pressure signals.
type HealthResponse struct {
	Status                string `json:"status"`
	UptimeSeconds         int    `json:"uptime_seconds,omitempty"`
	DuckDBVersion         string `json:"duckdb_version,omitempty"`
	MemoryUsedMB          int64  `json:"memory_used_mb,omitempty"`
	MaxMemoryGB           int    `json:"max_memory_gb,omitempty"`
	ActiveQueries         int64  `json:"active_queries,omitempty"`
	QueuedJobs            int64  `json:"queued_jobs,omitempty"`
	RunningJobs           int64  `json:"running_jobs,omitempty"`
	CompletedJobs         int64  `json:"completed_jobs,omitempty"`
	StoredJobs            int64  `json:"stored_jobs,omitempty"`
	CleanedJobs           int64  `json:"cleaned_jobs,omitempty"`
	QueryResultTTLSeconds int    `json:"query_result_ttl_seconds,omitempty"`
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
