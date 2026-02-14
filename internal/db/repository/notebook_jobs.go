package repository

import (
	"context"
	"database/sql"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/mapper"
	"duck-demo/internal/domain"
)

var _ domain.NotebookJobRepository = (*NotebookJobRepo)(nil)

// NotebookJobRepo implements domain.NotebookJobRepository using sqlc-generated queries.
type NotebookJobRepo struct {
	q *dbstore.Queries
}

// NewNotebookJobRepo creates a new NotebookJobRepo.
func NewNotebookJobRepo(db *sql.DB) *NotebookJobRepo {
	return &NotebookJobRepo{q: dbstore.New(db)}
}

// CreateJob inserts a new notebook job.
func (r *NotebookJobRepo) CreateJob(ctx context.Context, job *domain.NotebookJob) (*domain.NotebookJob, error) {
	row, err := r.q.CreateNotebookJob(ctx, dbstore.CreateNotebookJobParams{
		ID:         domain.NewID(),
		NotebookID: job.NotebookID,
		SessionID:  job.SessionID,
		State:      string(job.State),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.NotebookJobFromDB(row), nil
}

// GetJob returns a notebook job by its ID.
func (r *NotebookJobRepo) GetJob(ctx context.Context, id string) (*domain.NotebookJob, error) {
	row, err := r.q.GetNotebookJob(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return mapper.NotebookJobFromDB(row), nil
}

// ListJobs returns a paginated list of jobs for a notebook.
func (r *NotebookJobRepo) ListJobs(ctx context.Context, notebookID string, page domain.PageRequest) ([]domain.NotebookJob, int64, error) {
	total, err := r.q.CountNotebookJobs(ctx, notebookID)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListNotebookJobs(ctx, dbstore.ListNotebookJobsParams{
		NotebookID: notebookID,
		Limit:      int64(page.Limit()),
		Offset:     int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	return mapper.NotebookJobsFromDB(rows), total, nil
}

// UpdateJobState updates the state, result, and error fields of a job.
func (r *NotebookJobRepo) UpdateJobState(ctx context.Context, id string, state domain.JobState, result *string, errMsg *string) error {
	return r.q.UpdateNotebookJobState(ctx, dbstore.UpdateNotebookJobStateParams{
		State:  string(state),
		Result: mapper.NullStrFromPtr(result),
		Error:  mapper.NullStrFromPtr(errMsg),
		ID:     id,
	})
}
