package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.PipelineRunRepository = (*PipelineRunRepo)(nil)

// PipelineRunRepo implements PipelineRunRepository using SQLite.
type PipelineRunRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewPipelineRunRepo creates a new PipelineRunRepo.
func NewPipelineRunRepo(db *sql.DB) *PipelineRunRepo {
	return &PipelineRunRepo{q: dbstore.New(db), db: db}
}

// CreateRun inserts a new pipeline run.
func (r *PipelineRunRepo) CreateRun(ctx context.Context, run *domain.PipelineRun) (*domain.PipelineRun, error) {
	paramsJSON, err := json.Marshal(run.Parameters)
	if err != nil {
		return nil, fmt.Errorf("marshal parameters: %w", err)
	}

	row, err := r.q.CreatePipelineRun(ctx, dbstore.CreatePipelineRunParams{
		ID:            newID(),
		PipelineID:    run.PipelineID,
		Status:        run.Status,
		TriggerType:   run.TriggerType,
		TriggeredBy:   run.TriggeredBy,
		Parameters:    string(paramsJSON),
		GitCommitHash: nullStringPtr(run.GitCommitHash),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineRunFromDB(row), nil
}

// GetRunByID returns a pipeline run by its ID.
func (r *PipelineRunRepo) GetRunByID(ctx context.Context, id string) (*domain.PipelineRun, error) {
	row, err := r.q.GetPipelineRunByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineRunFromDB(row), nil
}

// ListRuns returns a filtered, paginated list of pipeline runs.
func (r *PipelineRunRepo) ListRuns(ctx context.Context, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error) {
	pipelineIDFilter := ""
	if filter.PipelineID != nil {
		pipelineIDFilter = *filter.PipelineID
	}
	statusFilter := ""
	if filter.Status != nil {
		statusFilter = *filter.Status
	}

	total, err := r.q.CountPipelineRuns(ctx, dbstore.CountPipelineRunsParams{
		Column1:    pipelineIDFilter,
		PipelineID: pipelineIDFilter,
		Column3:    statusFilter,
		Status:     statusFilter,
	})
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListPipelineRuns(ctx, dbstore.ListPipelineRunsParams{
		Column1:    pipelineIDFilter,
		PipelineID: pipelineIDFilter,
		Column3:    statusFilter,
		Status:     statusFilter,
		Limit:      int64(filter.Page.Limit()),
		Offset:     int64(filter.Page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	runs := make([]domain.PipelineRun, 0, len(rows))
	for _, row := range rows {
		runs = append(runs, *pipelineRunFromDB(row))
	}
	return runs, total, nil
}

// UpdateRunStatus updates the status and optional error message of a pipeline run.
func (r *PipelineRunRepo) UpdateRunStatus(ctx context.Context, id string, status string, errorMsg *string) error {
	return mapDBError(r.q.UpdatePipelineRunStatus(ctx, dbstore.UpdatePipelineRunStatusParams{
		Status:       status,
		ErrorMessage: nullStrFromPtr(errorMsg),
		ID:           id,
	}))
}

// UpdateRunStarted marks a pipeline run as started.
func (r *PipelineRunRepo) UpdateRunStarted(ctx context.Context, id string) error {
	return mapDBError(r.q.UpdatePipelineRunStarted(ctx, id))
}

// UpdateRunFinished marks a pipeline run as finished with a final status.
func (r *PipelineRunRepo) UpdateRunFinished(ctx context.Context, id string, status string, errorMsg *string) error {
	return mapDBError(r.q.UpdatePipelineRunFinished(ctx, dbstore.UpdatePipelineRunFinishedParams{
		Status:       status,
		ErrorMessage: nullStrFromPtr(errorMsg),
		ID:           id,
	}))
}

// CountActiveRuns returns the number of active (PENDING or RUNNING) runs for a pipeline.
func (r *PipelineRunRepo) CountActiveRuns(ctx context.Context, pipelineID string) (int64, error) {
	return r.q.CountActivePipelineRuns(ctx, pipelineID)
}

// CancelPendingRuns cancels all pending runs for a pipeline.
func (r *PipelineRunRepo) CancelPendingRuns(ctx context.Context, pipelineID string) (int64, error) {
	err := r.q.CancelPendingPipelineRuns(ctx, pipelineID)
	if err != nil {
		return 0, err
	}
	// CancelPendingPipelineRuns is :exec, so we don't get rows affected.
	// Return 0 as a best-effort count.
	return 0, nil
}

// CreateJobRun inserts a new pipeline job run.
func (r *PipelineRunRepo) CreateJobRun(ctx context.Context, jr *domain.PipelineJobRun) (*domain.PipelineJobRun, error) {
	row, err := r.q.CreatePipelineJobRun(ctx, dbstore.CreatePipelineJobRunParams{
		ID:           newID(),
		RunID:        jr.RunID,
		JobID:        jr.JobID,
		JobName:      jr.JobName,
		Status:       jr.Status,
		RetryAttempt: int64(jr.RetryAttempt),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineJobRunFromDB(row), nil
}

// GetJobRunByID returns a pipeline job run by its ID.
func (r *PipelineRunRepo) GetJobRunByID(ctx context.Context, id string) (*domain.PipelineJobRun, error) {
	row, err := r.q.GetPipelineJobRunByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineJobRunFromDB(row), nil
}

// ListJobRunsByRun returns all job runs for a pipeline run.
func (r *PipelineRunRepo) ListJobRunsByRun(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
	rows, err := r.q.ListPipelineJobRunsByRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	jobRuns := make([]domain.PipelineJobRun, 0, len(rows))
	for _, row := range rows {
		jobRuns = append(jobRuns, *pipelineJobRunFromDB(row))
	}
	return jobRuns, nil
}

// UpdateJobRunStatus updates the status and optional error message of a job run.
func (r *PipelineRunRepo) UpdateJobRunStatus(ctx context.Context, id string, status string, errorMsg *string) error {
	return mapDBError(r.q.UpdatePipelineJobRunStatus(ctx, dbstore.UpdatePipelineJobRunStatusParams{
		Status:       status,
		ErrorMessage: nullStrFromPtr(errorMsg),
		ID:           id,
	}))
}

// UpdateJobRunStarted marks a job run as started.
func (r *PipelineRunRepo) UpdateJobRunStarted(ctx context.Context, id string) error {
	return mapDBError(r.q.UpdatePipelineJobRunStarted(ctx, id))
}

// UpdateJobRunFinished marks a job run as finished with a final status.
func (r *PipelineRunRepo) UpdateJobRunFinished(ctx context.Context, id string, status string, errorMsg *string) error {
	return mapDBError(r.q.UpdatePipelineJobRunFinished(ctx, dbstore.UpdatePipelineJobRunFinishedParams{
		Status:       status,
		ErrorMessage: nullStrFromPtr(errorMsg),
		ID:           id,
	}))
}

// === Private mappers ===

func pipelineRunFromDB(row dbstore.PipelineRun) *domain.PipelineRun {
	createdAt, _ := time.Parse("2006-01-02 15:04:05", row.CreatedAt)

	var params map[string]string
	_ = json.Unmarshal([]byte(row.Parameters), &params)
	if params == nil {
		params = map[string]string{}
	}

	var startedAt *time.Time
	if row.StartedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.StartedAt.String)
		startedAt = &t
	}

	var finishedAt *time.Time
	if row.FinishedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.FinishedAt.String)
		finishedAt = &t
	}

	var errMsg *string
	if row.ErrorMessage.Valid {
		errMsg = &row.ErrorMessage.String
	}

	var gitHash *string
	if row.GitCommitHash.Valid {
		gitHash = &row.GitCommitHash.String
	}

	return &domain.PipelineRun{
		ID:            row.ID,
		PipelineID:    row.PipelineID,
		Status:        row.Status,
		TriggerType:   row.TriggerType,
		TriggeredBy:   row.TriggeredBy,
		Parameters:    params,
		GitCommitHash: gitHash,
		StartedAt:     startedAt,
		FinishedAt:    finishedAt,
		ErrorMessage:  errMsg,
		CreatedAt:     createdAt,
	}
}

func pipelineJobRunFromDB(row dbstore.PipelineJobRun) *domain.PipelineJobRun {
	createdAt, _ := time.Parse("2006-01-02 15:04:05", row.CreatedAt)

	var startedAt *time.Time
	if row.StartedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.StartedAt.String)
		startedAt = &t
	}

	var finishedAt *time.Time
	if row.FinishedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.FinishedAt.String)
		finishedAt = &t
	}

	var errMsg *string
	if row.ErrorMessage.Valid {
		errMsg = &row.ErrorMessage.String
	}

	return &domain.PipelineJobRun{
		ID:           row.ID,
		RunID:        row.RunID,
		JobID:        row.JobID,
		JobName:      row.JobName,
		Status:       row.Status,
		StartedAt:    startedAt,
		FinishedAt:   finishedAt,
		ErrorMessage: errMsg,
		RetryAttempt: int(row.RetryAttempt),
		CreatedAt:    createdAt,
	}
}

func nullStrFromPtr(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *s, Valid: true}
}
