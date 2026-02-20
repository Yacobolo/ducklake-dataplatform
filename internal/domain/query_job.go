package domain

import "time"

// QueryJobStatus represents the lifecycle state of an async query job.
type QueryJobStatus string

// Query job lifecycle statuses.
const (
	QueryJobStatusQueued    QueryJobStatus = "QUEUED"
	QueryJobStatusRunning   QueryJobStatus = "RUNNING"
	QueryJobStatusSucceeded QueryJobStatus = "SUCCEEDED"
	QueryJobStatusFailed    QueryJobStatus = "FAILED"
	QueryJobStatusCanceled  QueryJobStatus = "CANCELED"
)

// QueryJob stores durable state for asynchronous query execution.
type QueryJob struct {
	ID            string
	PrincipalName string
	RequestID     string
	SQLText       string
	Status        QueryJobStatus
	Columns       []string
	Rows          [][]interface{}
	RowCount      int
	ErrorMessage  *string
	CreatedAt     time.Time
	StartedAt     *time.Time
	CompletedAt   *time.Time
	UpdatedAt     time.Time
}
