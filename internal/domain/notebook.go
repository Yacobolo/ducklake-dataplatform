package domain

import "time"

// CellType represents the type of a notebook cell.
type CellType string

// CellType constants define the supported notebook cell types.
const (
	CellTypeSQL      CellType = "sql"
	CellTypeMarkdown CellType = "markdown"
)

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

// Cell represents a single cell within a notebook.
type Cell struct {
	ID         string
	NotebookID string
	CellType   CellType
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
}

// Validate validates the create notebook request.
func (r *CreateNotebookRequest) Validate() error {
	if r.Name == "" {
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
	return nil
}

// UpdateCellRequest holds partial-update parameters for a cell.
type UpdateCellRequest struct {
	Content  *string
	Position *int
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
	CellID   string
	Columns  []string
	Rows     [][]interface{}
	RowCount int
	Error    *string
	Duration time.Duration
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
