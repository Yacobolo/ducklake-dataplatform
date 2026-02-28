package computeproto

type RequestContext struct {
	RequestId     string `json:"request_id,omitempty"`
	PrincipalName string `json:"principal_name,omitempty"`
}

type ExecuteRequest struct {
	Sql     string          `json:"sql,omitempty"`
	Context *RequestContext `json:"context,omitempty"`
}

type ExecuteResponse struct {
	Columns  []string     `json:"columns,omitempty"`
	Rows     []*ResultRow `json:"rows,omitempty"`
	RowCount int64        `json:"row_count,omitempty"`
}

type SubmitQueryRequest struct {
	Sql     string          `json:"sql,omitempty"`
	Context *RequestContext `json:"context,omitempty"`
}

type SubmitQueryResponse struct {
	QueryId string `json:"query_id,omitempty"`
	Status  string `json:"status,omitempty"`
}

type GetQueryStatusRequest struct {
	QueryId string `json:"query_id,omitempty"`
}

type QueryStatusResponse struct {
	QueryId            string   `json:"query_id,omitempty"`
	Status             string   `json:"status,omitempty"`
	Error              string   `json:"error,omitempty"`
	Columns            []string `json:"columns,omitempty"`
	RowCount           int64    `json:"row_count,omitempty"`
	CompletedAtRfc3339 string   `json:"completed_at_rfc3339,omitempty"`
}

type FetchQueryResultsRequest struct {
	QueryId    string `json:"query_id,omitempty"`
	PageToken  string `json:"page_token,omitempty"`
	MaxResults int32  `json:"max_results,omitempty"`
}

type FetchQueryResultsResponse struct {
	QueryId       string       `json:"query_id,omitempty"`
	Columns       []string     `json:"columns,omitempty"`
	Rows          []*ResultRow `json:"rows,omitempty"`
	RowCount      int64        `json:"row_count,omitempty"`
	NextPageToken string       `json:"next_page_token,omitempty"`
}

type CancelQueryRequest struct {
	QueryId string `json:"query_id,omitempty"`
}

type CancelQueryResponse struct {
	QueryId string `json:"query_id,omitempty"`
	Status  string `json:"status,omitempty"`
}

type DeleteQueryRequest struct {
	QueryId string `json:"query_id,omitempty"`
}

type DeleteQueryResponse struct {
	QueryId string `json:"query_id,omitempty"`
	Status  string `json:"status,omitempty"`
}

type ResultRow struct {
	Values []string `json:"values,omitempty"`
}

type HealthRequest struct{}

type HealthResponse struct {
	Status        string `json:"status,omitempty"`
	UptimeSeconds int64  `json:"uptime_seconds,omitempty"`
	ActiveQueries int64  `json:"active_queries,omitempty"`
	QueuedJobs    int64  `json:"queued_jobs,omitempty"`
	RunningJobs   int64  `json:"running_jobs,omitempty"`
	CompletedJobs int64  `json:"completed_jobs,omitempty"`
	StoredJobs    int64  `json:"stored_jobs,omitempty"`
	CleanedJobs   int64  `json:"cleaned_jobs,omitempty"`
	DuckdbVersion string `json:"duckdb_version,omitempty"`
	MemoryUsedMb  int64  `json:"memory_used_mb,omitempty"`
	MaxMemoryGb   int32  `json:"max_memory_gb,omitempty"`
	ResultTtlSecs int32  `json:"query_result_ttl_seconds,omitempty"`
}
