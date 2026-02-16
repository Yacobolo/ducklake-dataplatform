package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.PipelineRepository = (*PipelineRepo)(nil)

// PipelineRepo implements PipelineRepository using SQLite.
type PipelineRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewPipelineRepo creates a new PipelineRepo.
func NewPipelineRepo(db *sql.DB) *PipelineRepo {
	return &PipelineRepo{q: dbstore.New(db), db: db}
}

// CreatePipeline inserts a new pipeline.
func (r *PipelineRepo) CreatePipeline(ctx context.Context, p *domain.Pipeline) (*domain.Pipeline, error) {
	row, err := r.q.CreatePipeline(ctx, dbstore.CreatePipelineParams{
		ID:               newID(),
		Name:             p.Name,
		Description:      p.Description,
		ScheduleCron:     nullStringPtr(p.ScheduleCron),
		IsPaused:         boolToInt(p.IsPaused),
		ConcurrencyLimit: int64(p.ConcurrencyLimit),
		CreatedBy:        p.CreatedBy,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineFromDB(row), nil
}

// GetPipelineByID returns a pipeline by its ID.
func (r *PipelineRepo) GetPipelineByID(ctx context.Context, id string) (*domain.Pipeline, error) {
	row, err := r.q.GetPipelineByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineFromDB(row), nil
}

// GetPipelineByName returns a pipeline by its name.
func (r *PipelineRepo) GetPipelineByName(ctx context.Context, name string) (*domain.Pipeline, error) {
	row, err := r.q.GetPipelineByName(ctx, name)
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineFromDB(row), nil
}

// ListPipelines returns a paginated list of pipelines.
func (r *PipelineRepo) ListPipelines(ctx context.Context, page domain.PageRequest) ([]domain.Pipeline, int64, error) {
	total, err := r.q.CountPipelines(ctx)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListPipelines(ctx, dbstore.ListPipelinesParams{
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	pipelines := make([]domain.Pipeline, 0, len(rows))
	for _, row := range rows {
		pipelines = append(pipelines, *pipelineFromDB(row))
	}
	return pipelines, total, nil
}

// UpdatePipeline applies partial updates to a pipeline.
func (r *PipelineRepo) UpdatePipeline(ctx context.Context, id string, req domain.UpdatePipelineRequest) (*domain.Pipeline, error) {
	current, err := r.GetPipelineByID(ctx, id)
	if err != nil {
		return nil, err
	}

	desc := current.Description
	if req.Description != nil {
		desc = *req.Description
	}
	paused := current.IsPaused
	if req.IsPaused != nil {
		paused = *req.IsPaused
	}
	concLimit := current.ConcurrencyLimit
	if req.ConcurrencyLimit != nil {
		concLimit = *req.ConcurrencyLimit
	}
	sched := nullStringPtr(current.ScheduleCron)
	if req.ScheduleCron != nil {
		sched = sql.NullString{String: *req.ScheduleCron, Valid: *req.ScheduleCron != ""}
	}

	err = r.q.UpdatePipeline(ctx, dbstore.UpdatePipelineParams{
		Description:      desc,
		ScheduleCron:     sched,
		IsPaused:         boolToInt(paused),
		ConcurrencyLimit: int64(concLimit),
		ID:               id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetPipelineByID(ctx, id)
}

// DeletePipeline removes a pipeline by ID.
func (r *PipelineRepo) DeletePipeline(ctx context.Context, id string) error {
	return mapDBError(r.q.DeletePipeline(ctx, id))
}

// ListScheduledPipelines returns all pipelines with a schedule that are not paused.
func (r *PipelineRepo) ListScheduledPipelines(ctx context.Context) ([]domain.Pipeline, error) {
	rows, err := r.q.ListScheduledPipelines(ctx)
	if err != nil {
		return nil, err
	}

	pipelines := make([]domain.Pipeline, 0, len(rows))
	for _, row := range rows {
		pipelines = append(pipelines, *pipelineFromDB(row))
	}
	return pipelines, nil
}

// CreateJob inserts a new pipeline job.
func (r *PipelineRepo) CreateJob(ctx context.Context, job *domain.PipelineJob) (*domain.PipelineJob, error) {
	depsJSON, err := json.Marshal(job.DependsOn)
	if err != nil {
		return nil, fmt.Errorf("marshal depends_on: %w", err)
	}

	jobType := job.JobType
	if jobType == "" {
		jobType = domain.PipelineJobTypeNotebook
	}

	row, err := r.q.CreatePipelineJob(ctx, dbstore.CreatePipelineJobParams{
		ID:                newID(),
		PipelineID:        job.PipelineID,
		Name:              job.Name,
		ComputeEndpointID: nullStringPtr(job.ComputeEndpointID),
		DependsOn:         string(depsJSON),
		NotebookID:        job.NotebookID,
		TimeoutSeconds:    nullInt64Ptr(job.TimeoutSeconds),
		RetryCount:        int64(job.RetryCount),
		JobOrder:          int64(job.JobOrder),
		JobType:           jobType,
		ModelSelector:     job.ModelSelector,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineJobFromDB(row), nil
}

// GetJobByID returns a pipeline job by its ID.
func (r *PipelineRepo) GetJobByID(ctx context.Context, id string) (*domain.PipelineJob, error) {
	row, err := r.q.GetPipelineJobByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineJobFromDB(row), nil
}

// ListJobsByPipeline returns all jobs for a pipeline, ordered by job_order.
func (r *PipelineRepo) ListJobsByPipeline(ctx context.Context, pipelineID string) ([]domain.PipelineJob, error) {
	rows, err := r.q.ListPipelineJobsByPipeline(ctx, pipelineID)
	if err != nil {
		return nil, err
	}

	jobs := make([]domain.PipelineJob, 0, len(rows))
	for _, row := range rows {
		jobs = append(jobs, *pipelineJobFromDB(row))
	}
	return jobs, nil
}

// DeleteJob removes a pipeline job by ID.
func (r *PipelineRepo) DeleteJob(ctx context.Context, id string) error {
	return mapDBError(r.q.DeletePipelineJob(ctx, id))
}

// DeleteJobsByPipeline removes all jobs for a pipeline.
func (r *PipelineRepo) DeleteJobsByPipeline(ctx context.Context, pipelineID string) error {
	return mapDBError(r.q.DeletePipelineJobsByPipeline(ctx, pipelineID))
}

// === Private mappers ===

func pipelineFromDB(row dbstore.Pipeline) *domain.Pipeline {
	createdAt, err := time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	if err != nil {
		slog.Default().Warn("failed to parse pipeline created_at", "value", row.CreatedAt, "error", err)
	}
	updatedAt, err := time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	if err != nil {
		slog.Default().Warn("failed to parse pipeline updated_at", "value", row.UpdatedAt, "error", err)
	}

	var sched *string
	if row.ScheduleCron.Valid {
		sched = &row.ScheduleCron.String
	}

	return &domain.Pipeline{
		ID:               row.ID,
		Name:             row.Name,
		Description:      row.Description,
		ScheduleCron:     sched,
		IsPaused:         row.IsPaused != 0,
		ConcurrencyLimit: int(row.ConcurrencyLimit),
		CreatedBy:        row.CreatedBy,
		CreatedAt:        createdAt,
		UpdatedAt:        updatedAt,
	}
}

func pipelineJobFromDB(row dbstore.PipelineJob) *domain.PipelineJob {
	createdAt, err := time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	if err != nil {
		slog.Default().Warn("failed to parse pipeline_job created_at", "value", row.CreatedAt, "error", err)
	}

	var deps []string
	_ = json.Unmarshal([]byte(row.DependsOn), &deps)
	if deps == nil {
		deps = []string{}
	}

	var ceid *string
	if row.ComputeEndpointID.Valid {
		ceid = &row.ComputeEndpointID.String
	}

	var ts *int64
	if row.TimeoutSeconds.Valid {
		ts = &row.TimeoutSeconds.Int64
	}

	return &domain.PipelineJob{
		ID:                row.ID,
		PipelineID:        row.PipelineID,
		Name:              row.Name,
		ComputeEndpointID: ceid,
		DependsOn:         deps,
		NotebookID:        row.NotebookID,
		TimeoutSeconds:    ts,
		RetryCount:        int(row.RetryCount),
		JobOrder:          int(row.JobOrder),
		JobType:           row.JobType,
		ModelSelector:     row.ModelSelector,
		CreatedAt:         createdAt,
	}
}

func nullStringPtr(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *s, Valid: true}
}
