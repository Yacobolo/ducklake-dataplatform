package domain

import (
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

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

	PipelineAdmissionModeReject = "REJECT"
	PipelineAdmissionModeQueue  = "QUEUE"

	PipelineRunEventAdmitted     = "ADMITTED"
	PipelineRunEventQueued       = "QUEUED"
	PipelineRunEventStarted      = "STARTED"
	PipelineRunEventRetried      = "RETRIED"
	PipelineRunEventSucceeded    = "SUCCEEDED"
	PipelineRunEventFailed       = "FAILED"
	PipelineRunEventCancelled    = "CANCELLED"
	PipelineRunEventSkipped      = "SKIPPED"
	PipelineRunEventRepaired     = "REPAIRED"
	PipelineRunEventSLABreach    = "SLA_BREACHED"
	PipelineRunEventRepairFailed = "REPAIR_FAILED"
	PipelineRepairModeFailedOnly = "FAILED_ONLY"
	PipelineRepairModeFromJob    = "FROM_JOB"
)

// Pipeline represents a workflow definition.
type Pipeline struct {
	ID                       string
	Name                     string
	Description              string
	ScheduleCron             *string
	IsPaused                 bool
	ConcurrencyLimit         int
	RunAsPrincipal           *string
	AdmissionMode            string
	MaxRunDurationSeconds    *int64
	NotificationWebhooks     []PipelineNotificationWebhook
	DefaultRetryCount        *int
	DefaultTimeoutSeconds    *int64
	DefaultComputeEndpointID *string
	CreatedBy                string
	FolderID                 string
	CreatedAt                time.Time
	UpdatedAt                time.Time
}

// PipelineNotificationWebhook defines an outbound webhook subscription.
type PipelineNotificationWebhook struct {
	URL    string   `json:"url"`
	Events []string `json:"events,omitempty"`
}

// Pipeline job type constants.
const (
	PipelineJobTypeNotebook = "NOTEBOOK"
	PipelineJobTypeModelRun = "MODEL_RUN"
)

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
	JobType           string // NOTEBOOK or MODEL_RUN
	ModelSelector     string // for MODEL_RUN jobs
	CreatedAt         time.Time
}

// PipelineRun represents an execution of a pipeline.
type PipelineRun struct {
	ID                 string
	PipelineID         string
	Status             string
	TriggerType        string
	TriggeredBy        string
	EffectivePrincipal string
	Parameters         map[string]string
	GitCommitHash      *string
	QueuedAt           *time.Time
	QueueStartedAt     *time.Time
	StartedAt          *time.Time
	FinishedAt         *time.Time
	ErrorMessage       *string
	RepairedFromRunID  *string
	Provenance         *PipelineRunProvenance
	SLABreachedAt      *time.Time
	CreatedAt          time.Time
}

// PipelineJobRun represents the execution of a single job within a pipeline run.
type PipelineJobRun struct {
	ID                         string
	RunID                      string
	JobID                      string
	JobName                    string
	Status                     string
	StartedAt                  *time.Time
	FinishedAt                 *time.Time
	ErrorMessage               *string
	RetryAttempt               int
	EffectiveComputeEndpointID *string
	AttemptCount               int
	LastErrorCode              *string
	CreatedAt                  time.Time
}

// PipelineRunProvenance captures run reproducibility metadata.
type PipelineRunProvenance struct {
	TriggerType               string                       `json:"trigger_type,omitempty"`
	TriggeredBy               string                       `json:"triggered_by,omitempty"`
	EffectivePrincipal        string                       `json:"effective_principal,omitempty"`
	PipelineDefinitionVersion string                       `json:"pipeline_definition_version,omitempty"`
	Notebooks                 []PipelineNotebookProvenance `json:"notebooks,omitempty"`
	Models                    []PipelineModelProvenance    `json:"models,omitempty"`
}

// PipelineNotebookProvenance captures notebook-specific provenance.
type PipelineNotebookProvenance struct {
	NotebookID    string     `json:"notebook_id"`
	GitRepoID     *string    `json:"git_repo_id,omitempty"`
	GitCommitSHA  *string    `json:"git_commit_sha,omitempty"`
	LastUpdatedAt *time.Time `json:"last_updated_at,omitempty"`
}

// PipelineModelProvenance captures model-specific provenance.
type PipelineModelProvenance struct {
	Selector      string     `json:"selector"`
	ModelID       *string    `json:"model_id,omitempty"`
	LastUpdatedAt *time.Time `json:"last_updated_at,omitempty"`
}

// PipelineRunEvent records durable run/job lifecycle events.
type PipelineRunEvent struct {
	ID        string
	RunID     string
	JobRunID  *string
	EventType string
	Message   *string
	ErrorCode *string
	Metadata  map[string]any
	CreatedAt time.Time
}

// CreatePipelineRequest holds parameters for creating a pipeline.
type CreatePipelineRequest struct {
	Name                     string
	Description              string
	ScheduleCron             *string
	IsPaused                 bool
	ConcurrencyLimit         int
	RunAsPrincipal           *string
	AdmissionMode            string
	MaxRunDurationSeconds    *int64
	NotificationWebhooks     []PipelineNotificationWebhook
	DefaultRetryCount        *int
	DefaultTimeoutSeconds    *int64
	DefaultComputeEndpointID *string
	FolderID                 *string
}

// Validate checks that the request is well-formed.
func (r *CreatePipelineRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("name is required")
	}
	if r.ScheduleCron != nil && *r.ScheduleCron != "" {
		if _, err := cron.ParseStandard(*r.ScheduleCron); err != nil {
			return ErrValidation("schedule_cron is invalid: %v", err)
		}
	}
	if r.ConcurrencyLimit < 0 {
		return ErrValidation("concurrency_limit must be non-negative")
	}
	switch strings.TrimSpace(r.AdmissionMode) {
	case "", PipelineAdmissionModeReject, PipelineAdmissionModeQueue:
	default:
		return ErrValidation("admission_mode must be REJECT or QUEUE")
	}
	if r.DefaultRetryCount != nil && *r.DefaultRetryCount < 0 {
		return ErrValidation("default_retry_count must be non-negative")
	}
	if r.MaxRunDurationSeconds != nil && *r.MaxRunDurationSeconds <= 0 {
		return ErrValidation("max_run_duration_seconds must be positive")
	}
	if r.DefaultTimeoutSeconds != nil && *r.DefaultTimeoutSeconds <= 0 {
		return ErrValidation("default_timeout_seconds must be positive")
	}
	return nil
}

// UpdatePipelineRequest holds partial-update parameters for a pipeline.
type UpdatePipelineRequest struct {
	Description              *string
	ScheduleCron             *string // pointer-to-pointer semantics: nil=no change, non-nil sets
	IsPaused                 *bool
	ConcurrencyLimit         *int
	RunAsPrincipal           *string
	AdmissionMode            *string
	MaxRunDurationSeconds    *int64
	NotificationWebhooks     *[]PipelineNotificationWebhook
	DefaultRetryCount        *int
	DefaultTimeoutSeconds    *int64
	DefaultComputeEndpointID *string
	FolderID                 *string
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
	JobType           string
	ModelSelector     string
}

// UpdatePipelineJobRequest holds partial-update parameters for a pipeline job.
type UpdatePipelineJobRequest struct {
	Name              *string
	ComputeEndpointID *string
	DependsOn         *[]string
	NotebookID        *string
	TimeoutSeconds    *int64
	RetryCount        *int
	JobOrder          *int
	JobType           *string
	ModelSelector     *string
}

// Validate checks that the request is well-formed.
func (r *CreatePipelineJobRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("name is required")
	}
	if r.JobType == "" {
		r.JobType = PipelineJobTypeNotebook
	}
	if r.JobType == PipelineJobTypeNotebook && r.NotebookID == "" {
		return ErrValidation("notebook_id is required for NOTEBOOK jobs")
	}
	if r.JobType == PipelineJobTypeModelRun && r.ModelSelector == "" {
		return ErrValidation("model_selector is required for MODEL_RUN jobs")
	}
	if r.RetryCount < 0 {
		return ErrValidation("retry_count must be non-negative")
	}
	return nil
}

// Validate checks that the update request is well-formed.
func (r *UpdatePipelineJobRequest) Validate() error {
	if r.Name != nil && strings.TrimSpace(*r.Name) == "" {
		return ErrValidation("name cannot be empty")
	}
	if r.RetryCount != nil && *r.RetryCount < 0 {
		return ErrValidation("retry_count must be non-negative")
	}
	if r.JobType != nil {
		switch *r.JobType {
		case PipelineJobTypeNotebook:
			if r.NotebookID != nil && *r.NotebookID == "" {
				return ErrValidation("notebook_id is required for NOTEBOOK jobs")
			}
		case PipelineJobTypeModelRun:
			if r.ModelSelector != nil && *r.ModelSelector == "" {
				return ErrValidation("model_selector is required for MODEL_RUN jobs")
			}
		case "":
			return ErrValidation("job_type cannot be empty")
		default:
			return ErrValidation("job_type must be NOTEBOOK or MODEL_RUN")
		}
	}
	return nil
}

// PipelineRunFilter holds filter parameters for querying pipeline runs.
type PipelineRunFilter struct {
	PipelineID *string
	Status     *string
	Page       PageRequest
}

// RepairPipelineRunRequest describes a run repair action.
type RepairPipelineRunRequest struct {
	Mode      string
	FromJobID *string
}

// Validate checks that the repair request is well-formed.
func (r *RepairPipelineRunRequest) Validate() error {
	switch r.Mode {
	case PipelineRepairModeFailedOnly:
		return nil
	case PipelineRepairModeFromJob:
		if r.FromJobID == nil || strings.TrimSpace(*r.FromJobID) == "" {
			return ErrValidation("from_job_id is required for FROM_JOB repair mode")
		}
		return nil
	default:
		return ErrValidation("repair mode must be FAILED_ONLY or FROM_JOB")
	}
}
