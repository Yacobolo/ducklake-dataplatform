package domain

import (
	"strings"
	"time"
)

// CellType represents the type of a notebook cell.
type CellType string

// CellType constants define the supported notebook cell types.
const (
	CellTypeSQL      CellType = "sql"
	CellTypeMarkdown CellType = "markdown"
)

// CellRole represents the role of a notebook cell.
type CellRole string

// CellRole constants define supported notebook cell roles.
const (
	CellRoleTransform CellRole = "transform"
	CellRoleOutput    CellRole = "output"
	CellRoleTest      CellRole = "test"
	CellRoleMarkdown  CellRole = "markdown"
)

// NotebookTestSeverity defines notebook test-cell severity.
type NotebookTestSeverity string

// NotebookTestSeverity constants define severity gates for test cells.
const (
	NotebookTestSeverityError NotebookTestSeverity = "error"
	NotebookTestSeverityWarn  NotebookTestSeverity = "warn"
)

// NotebookCellTestConfig defines settings for notebook test cells.
type NotebookCellTestConfig struct {
	Severity NotebookTestSeverity `json:"severity"`
}

// Notebook represents a SQL notebook document.
type Notebook struct {
	ID          string
	Name        string
	Description *string
	Owner       string
	GitRepoID   *string
	GitPath     *string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// NotebookModelLink stores the 1:1 relationship between a notebook and a published model.
type NotebookModelLink struct {
	ID           string
	NotebookID   string
	ModelID      string
	OutputCellID string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Cell represents a single cell within a notebook.
type Cell struct {
	ID         string
	NotebookID string
	CellType   CellType
	Name       *string
	Role       CellRole
	Disabled   bool
	Test       *NotebookCellTestConfig
	Content    string
	Position   int
	LastResult *string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// CreateNotebookRequest holds parameters for creating a notebook.
type CreateNotebookRequest struct {
	Name        string
	Description *string
	Source      *string
}

// Validate validates the create notebook request.
func (r *CreateNotebookRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return ErrValidation("notebook name is required")
	}
	return nil
}

// UpdateNotebookRequest holds partial-update parameters for a notebook.
type UpdateNotebookRequest struct {
	Name        *string
	Description *string
}

// CreateCellRequest holds parameters for creating a cell.
type CreateCellRequest struct {
	CellType CellType
	Name     *string
	Role     *CellRole
	Disabled bool
	Test     *NotebookCellTestConfig
	Content  string
	Position *int
}

// Validate validates the create cell request.
func (r *CreateCellRequest) Validate() error {
	switch r.CellType {
	case CellTypeSQL, CellTypeMarkdown:
	default:
		return ErrValidation("cell_type must be 'sql' or 'markdown', got %q", string(r.CellType))
	}
	if r.Name != nil && strings.TrimSpace(*r.Name) == "" {
		return ErrValidation("name cannot be empty")
	}
	role := CellRoleTransform
	if r.CellType == CellTypeMarkdown {
		role = CellRoleMarkdown
	}
	if r.Role != nil {
		role = *r.Role
	}
	if err := validateCellRoleForType(role, r.CellType); err != nil {
		return err
	}
	if role == CellRoleTest {
		if r.Test == nil {
			return ErrValidation("test config is required for test cells")
		}
		if err := r.Test.Validate(); err != nil {
			return err
		}
	}
	if role != CellRoleTest && r.Test != nil {
		return ErrValidation("test config is only allowed for test cells")
	}
	return nil
}

// UpdateCellRequest holds partial-update parameters for a cell.
type UpdateCellRequest struct {
	Name     *string
	Role     *CellRole
	Disabled *bool
	Test     *NotebookCellTestConfig
	Content  *string
	Position *int
}

// Validate validates notebook test cell config.
func (c *NotebookCellTestConfig) Validate() error {
	if c == nil {
		return ErrValidation("test config is required")
	}
	if c.Severity == "" {
		c.Severity = NotebookTestSeverityError
	}
	switch c.Severity {
	case NotebookTestSeverityError, NotebookTestSeverityWarn:
		return nil
	default:
		return ErrValidation("test severity must be 'error' or 'warn', got %q", string(c.Severity))
	}
}

func validateCellRoleForType(role CellRole, cellType CellType) error {
	switch role {
	case CellRoleTransform, CellRoleOutput, CellRoleTest:
		if cellType != CellTypeSQL {
			return ErrValidation("role %q requires cell_type 'sql'", string(role))
		}
	case CellRoleMarkdown:
		if cellType != CellTypeMarkdown {
			return ErrValidation("role %q requires cell_type 'markdown'", string(role))
		}
	default:
		return ErrValidation("role must be 'transform', 'output', 'test', or 'markdown', got %q", string(role))
	}
	return nil
}

// ReorderCellsRequest holds a list of cell IDs in the desired order.
type ReorderCellsRequest struct {
	CellIDs []string
}

// === Session types (Phase 2) ===

// NotebookSession represents an active notebook execution session.
type NotebookSession struct {
	ID         string
	NotebookID string
	Principal  string
	State      string // "active" | "closed"
	CreatedAt  time.Time
	LastUsedAt time.Time
}

// CellExecutionResult holds the output of executing a single cell.
type CellExecutionResult struct {
	CellID     string
	Columns    []string
	Rows       [][]interface{}
	RowCount   int
	Error      *string
	Duration   time.Duration
	ExecutedAt *time.Time
}

// RunAllResult holds the aggregated output from executing all cells.
type RunAllResult struct {
	NotebookID    string
	Results       []CellExecutionResult
	TotalDuration time.Duration
}

// === Job types (Phase 3) ===

// JobState represents the lifecycle of an async job.
type JobState string

// JobState constants define the lifecycle of an async job.
const (
	JobStatePending  JobState = "pending"
	JobStateRunning  JobState = "running"
	JobStateComplete JobState = "complete"
	JobStateFailed   JobState = "failed"
)

// NotebookJob represents an async notebook execution job.
type NotebookJob struct {
	ID         string
	NotebookID string
	SessionID  string
	State      JobState
	Result     *string
	Error      *string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// === Git types (Phase 4) ===

// GitRepo represents a registered Git repository for notebook sync.
type GitRepo struct {
	ID            string
	URL           string
	Branch        string
	Path          string
	AuthToken     string
	WebhookSecret *string
	Owner         string
	LastSyncAt    *time.Time
	LastCommit    *string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// CreateGitRepoRequest holds parameters for registering a Git repo.
type CreateGitRepoRequest struct {
	URL       string
	Branch    string
	Path      string
	AuthToken string
}

// Validate validates the create git repo request.
func (r *CreateGitRepoRequest) Validate() error {
	if r.URL == "" {
		return ErrValidation("url is required")
	}
	if r.Branch == "" {
		return ErrValidation("branch is required")
	}
	return nil
}

// GitSyncResult holds the result of a sync operation.
type GitSyncResult struct {
	NotebooksCreated int
	NotebooksUpdated int
	NotebooksDeleted int
	CommitSHA        string
}
