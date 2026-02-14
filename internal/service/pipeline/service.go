package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"duck-demo/internal/domain"
)

// ScheduleReloader allows the service to notify the scheduler to reload.
type ScheduleReloader interface {
	Reload(ctx context.Context) error
}

// PipelineService provides business logic for pipeline management.
type PipelineService struct {
	pipelines domain.PipelineRepository
	runs      domain.PipelineRunRepository
	audit     domain.AuditRepository
	notebooks domain.NotebookProvider
	engine    domain.SessionEngine
	duckDB    *sql.DB
	logger    *slog.Logger
	reloader  ScheduleReloader
}

// NewPipelineService creates a new PipelineService.
func NewPipelineService(
	pipelines domain.PipelineRepository,
	runs domain.PipelineRunRepository,
	audit domain.AuditRepository,
	notebooks domain.NotebookProvider,
	engine domain.SessionEngine,
	duckDB *sql.DB,
	logger *slog.Logger,
) *PipelineService {
	return &PipelineService{
		pipelines: pipelines,
		runs:      runs,
		audit:     audit,
		notebooks: notebooks,
		engine:    engine,
		duckDB:    duckDB,
		logger:    logger,
	}
}

// SetScheduleReloader sets the schedule reloader (breaks circular dep).
func (s *PipelineService) SetScheduleReloader(r ScheduleReloader) {
	s.reloader = r
}

// === Pipeline CRUD ===

func (s *PipelineService) CreatePipeline(ctx context.Context, principal string, req domain.CreatePipelineRequest) (*domain.Pipeline, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	if req.ConcurrencyLimit == 0 {
		req.ConcurrencyLimit = 1
	}

	p := &domain.Pipeline{
		ID:               domain.NewID(),
		Name:             req.Name,
		Description:      req.Description,
		ScheduleCron:     req.ScheduleCron,
		IsPaused:         req.IsPaused,
		ConcurrencyLimit: req.ConcurrencyLimit,
		CreatedBy:        principal,
	}

	result, err := s.pipelines.CreatePipeline(ctx, p)
	if err != nil {
		return nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: principal,
		Action:        "pipeline.create",
		Status:        "success",
		CreatedAt:     time.Now(),
	})

	if s.reloader != nil {
		_ = s.reloader.Reload(ctx)
	}

	return result, nil
}

func (s *PipelineService) GetPipeline(ctx context.Context, name string) (*domain.Pipeline, error) {
	return s.pipelines.GetPipelineByName(ctx, name)
}

func (s *PipelineService) ListPipelines(ctx context.Context, page domain.PageRequest) ([]domain.Pipeline, int64, error) {
	return s.pipelines.ListPipelines(ctx, page)
}

func (s *PipelineService) UpdatePipeline(ctx context.Context, principal string, name string, req domain.UpdatePipelineRequest) (*domain.Pipeline, error) {
	p, err := s.pipelines.GetPipelineByName(ctx, name)
	if err != nil {
		return nil, err
	}

	result, err := s.pipelines.UpdatePipeline(ctx, p.ID, req)
	if err != nil {
		return nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: principal,
		Action:        "pipeline.update",
		Status:        "success",
		CreatedAt:     time.Now(),
	})

	if s.reloader != nil {
		_ = s.reloader.Reload(ctx)
	}

	return result, nil
}

func (s *PipelineService) DeletePipeline(ctx context.Context, principal string, name string) error {
	p, err := s.pipelines.GetPipelineByName(ctx, name)
	if err != nil {
		return err
	}

	if err := s.pipelines.DeletePipeline(ctx, p.ID); err != nil {
		return err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: principal,
		Action:        "pipeline.delete",
		Status:        "success",
		CreatedAt:     time.Now(),
	})

	if s.reloader != nil {
		_ = s.reloader.Reload(ctx)
	}

	return nil
}

// === Job CRUD ===

func (s *PipelineService) CreateJob(ctx context.Context, principal string, pipelineName string, req domain.CreatePipelineJobRequest) (*domain.PipelineJob, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, err
	}

	// Validate notebook exists and has SQL.
	_, err = s.notebooks.GetSQLBlocks(ctx, req.NotebookID)
	if err != nil {
		return nil, fmt.Errorf("validate notebook: %w", err)
	}

	job := &domain.PipelineJob{
		ID:                domain.NewID(),
		PipelineID:        p.ID,
		Name:              req.Name,
		ComputeEndpointID: req.ComputeEndpointID,
		DependsOn:         req.DependsOn,
		NotebookID:        req.NotebookID,
		TimeoutSeconds:    req.TimeoutSeconds,
		RetryCount:        req.RetryCount,
		JobOrder:          req.JobOrder,
	}

	return s.pipelines.CreateJob(ctx, job)
}

func (s *PipelineService) ListJobs(ctx context.Context, pipelineName string) ([]domain.PipelineJob, error) {
	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, err
	}
	return s.pipelines.ListJobsByPipeline(ctx, p.ID)
}

func (s *PipelineService) DeleteJob(ctx context.Context, principal string, pipelineName string, jobID string) error {
	// Verify the job exists (also validates jobID).
	_, err := s.pipelines.GetJobByID(ctx, jobID)
	if err != nil {
		return err
	}
	return s.pipelines.DeleteJob(ctx, jobID)
}

// === Run Operations ===

func (s *PipelineService) TriggerRun(ctx context.Context, principal string, pipelineName string,
	params map[string]string, triggerType string) (*domain.PipelineRun, error) {

	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, err
	}

	// Check concurrency limit.
	active, err := s.runs.CountActiveRuns(ctx, p.ID)
	if err != nil {
		return nil, fmt.Errorf("count active runs: %w", err)
	}
	if active >= int64(p.ConcurrencyLimit) {
		return nil, domain.ErrValidation("concurrency limit reached (%d active runs)", active)
	}

	// List jobs and validate DAG.
	jobs, err := s.pipelines.ListJobsByPipeline(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, domain.ErrValidation("pipeline has no jobs")
	}

	levels, err := ResolveExecutionOrder(jobs)
	if err != nil {
		return nil, err
	}

	if params == nil {
		params = map[string]string{}
	}

	// Create the run.
	run := &domain.PipelineRun{
		ID:          domain.NewID(),
		PipelineID:  p.ID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: triggerType,
		TriggeredBy: principal,
		Parameters:  params,
	}

	result, err := s.runs.CreateRun(ctx, run)
	if err != nil {
		return nil, err
	}

	// Create job runs for each job.
	for _, job := range jobs {
		jr := &domain.PipelineJobRun{
			ID:      domain.NewID(),
			RunID:   result.ID,
			JobID:   job.ID,
			JobName: job.Name,
			Status:  domain.PipelineJobRunStatusPending,
		}
		if _, err := s.runs.CreateJobRun(ctx, jr); err != nil {
			return nil, fmt.Errorf("create job run: %w", err)
		}
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: principal,
		Action:        "pipeline.trigger",
		Status:        "success",
		CreatedAt:     time.Now(),
	})

	// Launch background executor.
	go s.executeRun(result.ID, jobs, levels, params, principal)

	return result, nil
}

func (s *PipelineService) GetRun(ctx context.Context, runID string) (*domain.PipelineRun, error) {
	return s.runs.GetRunByID(ctx, runID)
}

func (s *PipelineService) ListRuns(ctx context.Context, pipelineName string, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error) {
	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, 0, err
	}
	filter.PipelineID = &p.ID
	return s.runs.ListRuns(ctx, filter)
}

func (s *PipelineService) ListJobRuns(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
	// Verify run exists.
	_, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		return nil, err
	}
	return s.runs.ListJobRunsByRun(ctx, runID)
}

func (s *PipelineService) CancelRun(ctx context.Context, principal string, runID string) error {
	run, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		return err
	}

	if run.Status != domain.PipelineRunStatusPending && run.Status != domain.PipelineRunStatusRunning {
		return domain.ErrValidation("cannot cancel run with status %s", run.Status)
	}

	errMsg := "cancelled by " + principal
	if err := s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusCancelled, &errMsg); err != nil {
		return err
	}

	// Cancel pending job runs.
	jobRuns, err := s.runs.ListJobRunsByRun(ctx, runID)
	if err != nil {
		return nil // best effort
	}
	for _, jr := range jobRuns {
		if jr.Status == domain.PipelineJobRunStatusPending {
			_ = s.runs.UpdateJobRunFinished(ctx, jr.ID, domain.PipelineJobRunStatusCancelled, nil)
		}
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: principal,
		Action:        "pipeline.cancel",
		Status:        "success",
		CreatedAt:     time.Now(),
	})

	return nil
}
