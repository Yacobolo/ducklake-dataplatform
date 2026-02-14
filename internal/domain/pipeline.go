package domain

import "time"

// Pipeline status constants.
const (
	PipelineRunStatusPending   = "PENDING"
	PipelineRunStatusRunning   = "RUNNING"
	PipelineRunStatusSuccess   = "SUCCESS"
	PipelineRunStatusFailed    = "FAILED"
	PipelineRunStatusCancelled = "CANCELLED"

	PipelineJobRunStatusPending   = "PENDING"
	PipelineJobRunStatusRunning   = "RUNNING"
	PipelineJobRunStatusSuccess   = "SUCCESS"
	PipelineJobRunStatusFailed    = "FAILED"
	PipelineJobRunStatusSkipped   = "SKIPPED"
	PipelineJobRunStatusCancelled = "CANCELLED"

	TriggerTypeManual    = "MANUAL"
	TriggerTypeScheduled = "SCHEDULED"
)

// Pipeline represents a workflow definition.
type Pipeline struct {
	ID               string
	Name             string
	Description      string
	ScheduleCron     *string
	IsPaused         bool
	ConcurrencyLimit int
	CreatedBy        string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// PipelineJob represents a single job within a pipeline (DAG node).
type PipelineJob struct {
	ID                string
	PipelineID        string
	Name              string
	ComputeEndpointID *string
	DependsOn         []string // job names
	NotebookID        string
	TimeoutSeconds    *int64
	RetryCount        int
	JobOrder          int
	CreatedAt         time.Time
}

// PipelineRun represents an execution of a pipeline.
type PipelineRun struct {
	ID            string
	PipelineID    string
	Status        string
	TriggerType   string
	TriggeredBy   string
	Parameters    map[string]string
	GitCommitHash *string
	StartedAt     *time.Time
	FinishedAt    *time.Time
	ErrorMessage  *string
	CreatedAt     time.Time
}

// PipelineJobRun represents the execution of a single job within a pipeline run.
type PipelineJobRun struct {
	ID           string
	RunID        string
	JobID        string
	JobName      string
	Status       string
	StartedAt    *time.Time
	FinishedAt   *time.Time
	ErrorMessage *string
	RetryAttempt int
	CreatedAt    time.Time
}

// CreatePipelineRequest holds parameters for creating a pipeline.
type CreatePipelineRequest struct {
	Name             string
	Description      string
	ScheduleCron     *string
	IsPaused         bool
	ConcurrencyLimit int
}

// Validate checks that the request is well-formed.
func (r *CreatePipelineRequest) Validate() error {
	if r.Name == "" {
		return ErrValidation("name is required")
	}
	if r.ConcurrencyLimit < 0 {
		return ErrValidation("concurrency_limit must be non-negative")
	}
	return nil
}

// UpdatePipelineRequest holds partial-update parameters for a pipeline.
type UpdatePipelineRequest struct {
	Description      *string
	ScheduleCron     *string // pointer-to-pointer semantics: nil=no change, non-nil sets
	IsPaused         *bool
	ConcurrencyLimit *int
}

// CreatePipelineJobRequest holds parameters for creating a pipeline job.
type CreatePipelineJobRequest struct {
	Name              string
	ComputeEndpointID *string
	DependsOn         []string
	NotebookID        string
	TimeoutSeconds    *int64
	RetryCount        int
	JobOrder          int
}

// Validate checks that the request is well-formed.
func (r *CreatePipelineJobRequest) Validate() error {
	if r.Name == "" {
		return ErrValidation("name is required")
	}
	if r.NotebookID == "" {
		return ErrValidation("notebook_id is required")
	}
	if r.RetryCount < 0 {
		return ErrValidation("retry_count must be non-negative")
	}
	return nil
}

// PipelineRunFilter holds filter parameters for querying pipeline runs.
type PipelineRunFilter struct {
	PipelineID *string
	Status     *string
	Page       PageRequest
}
