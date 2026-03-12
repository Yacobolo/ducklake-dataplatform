package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/orchestration"
	"github.com/robfig/cron/v3"
)

// ScheduleReloader allows the service to notify the scheduler to reload.
type ScheduleReloader interface {
	Reload(ctx context.Context) error
}

// Service provides business logic for pipeline management.
type Service struct {
	pipelines   domain.PipelineRepository
	runs        domain.PipelineRunRepository
	audit       domain.AuditRepository
	notebooks   domain.NotebookProvider
	modelRunner domain.ModelRunner
	engine      domain.SessionEngine
	duckDB      *sql.DB
	logger      *slog.Logger
	reloader    ScheduleReloader
	runCancels  sync.Map // maps run ID (string) → context.CancelFunc

	assetRepo      domain.DataAssetRepository
	assetDepRepo   domain.AssetDependencyRepository
	assetRunRepo   domain.AssetRunRepository
	assetProductID string
}

// NewService creates a new pipeline Service.
func NewService(
	pipelines domain.PipelineRepository,
	runs domain.PipelineRunRepository,
	audit domain.AuditRepository,
	notebooks domain.NotebookProvider,
	engine domain.SessionEngine,
	duckDB *sql.DB,
	logger *slog.Logger,
) *Service {
	return &Service{
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
func (s *Service) SetScheduleReloader(r ScheduleReloader) {
	s.reloader = r
}

// SetModelRunner sets the model runner for MODEL_RUN pipeline jobs.
func (s *Service) SetModelRunner(runner domain.ModelRunner) {
	s.modelRunner = runner
}

// SetAssetOrchestration wires asset-centric orchestration dependencies.
func (s *Service) SetAssetOrchestration(
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
	assetRunRepo domain.AssetRunRepository,
	assetProductID string,
) {
	s.assetRepo = assetRepo
	s.assetDepRepo = assetDepRepo
	s.assetRunRepo = assetRunRepo
	s.assetProductID = assetProductID
}

// SyncPipelinesToAssets maps existing pipeline/job definitions to asset graph state.
func (s *Service) SyncPipelinesToAssets(ctx context.Context) error {
	if s.assetRepo == nil || s.assetDepRepo == nil {
		return nil
	}

	page := domain.PageRequest{MaxResults: 500}
	for {
		pipelines, total, err := s.pipelines.ListPipelines(ctx, page)
		if err != nil {
			return fmt.Errorf("list pipelines: %w", err)
		}

		for i := range pipelines {
			if err := s.syncPipelineAssets(ctx, &pipelines[i]); err != nil {
				return err
			}
		}

		next := domain.NextPageToken(page.Offset(), page.Limit(), total)
		if next == "" {
			break
		}
		page.PageToken = next
	}

	return nil
}

func (s *Service) syncPipelineAssets(ctx context.Context, pipeline *domain.Pipeline) error {
	jobs, err := s.pipelines.ListJobsByPipeline(ctx, pipeline.ID)
	if err != nil {
		return fmt.Errorf("list pipeline jobs for %s: %w", pipeline.Name, err)
	}
	if len(jobs) == 0 {
		return nil
	}

	adapted, err := BuildPipelineAssetGraph(pipeline, jobs)
	if err != nil {
		return fmt.Errorf("build pipeline asset graph for %s: %w", pipeline.Name, err)
	}

	for i := range adapted.Assets {
		asset := adapted.Assets[i]
		if asset.ProductID == "" {
			asset.ProductID = s.assetProductID
		}
		if existing, getErr := s.assetRepo.GetByID(ctx, asset.ID); getErr == nil {
			if asset.ProductID == "" {
				asset.ProductID = existing.ProductID
			}
			if _, updateErr := s.assetRepo.Update(ctx, asset.ID, &asset); updateErr != nil {
				return fmt.Errorf("update asset %s: %w", asset.AssetKey, updateErr)
			}
		} else {
			var notFoundErr *domain.NotFoundError
			if !errors.As(getErr, &notFoundErr) {
				return fmt.Errorf("get asset %s: %w", asset.AssetKey, getErr)
			}
			if _, createErr := s.assetRepo.Create(ctx, &asset); createErr != nil {
				return fmt.Errorf("create asset %s: %w", asset.AssetKey, createErr)
			}
		}

		if depErr := s.assetDepRepo.DeleteByAsset(ctx, asset.ID); depErr != nil {
			return fmt.Errorf("clear dependencies for asset %s: %w", asset.AssetKey, depErr)
		}
	}

	for i := range adapted.Dependencies {
		dep := adapted.Dependencies[i]
		if _, depErr := s.assetDepRepo.Create(ctx, &dep); depErr != nil {
			var conflict *domain.ConflictError
			if !errors.As(depErr, &conflict) {
				return fmt.Errorf("create dependency %s->%s: %w", dep.UpstreamAssetID, dep.AssetID, depErr)
			}
		}
	}

	return nil
}

// === Pipeline CRUD ===

// CreatePipeline validates and persists a new pipeline, then reloads schedules.
func (s *Service) CreatePipeline(ctx context.Context, principal string, req domain.CreatePipelineRequest) (*domain.Pipeline, error) {
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
		Status:        "ALLOWED",
		CreatedAt:     time.Now(),
	})

	if s.reloader != nil {
		_ = s.reloader.Reload(ctx)
	}

	return result, nil
}

// GetPipeline returns a pipeline by name.
func (s *Service) GetPipeline(ctx context.Context, name string) (*domain.Pipeline, error) {
	return s.pipelines.GetPipelineByName(ctx, name)
}

// ListPipelines returns a paginated list of pipelines.
func (s *Service) ListPipelines(ctx context.Context, page domain.PageRequest) ([]domain.Pipeline, int64, error) {
	return s.pipelines.ListPipelines(ctx, page)
}

// UpdatePipeline applies changes to an existing pipeline and reloads schedules.
func (s *Service) UpdatePipeline(ctx context.Context, principal string, name string, req domain.UpdatePipelineRequest) (*domain.Pipeline, error) {
	if req.ScheduleCron != nil && *req.ScheduleCron != "" {
		if _, err := cron.ParseStandard(*req.ScheduleCron); err != nil {
			return nil, domain.ErrValidation("schedule_cron is invalid: %v", err)
		}
	}

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
		Status:        "ALLOWED",
		CreatedAt:     time.Now(),
	})

	if s.reloader != nil {
		_ = s.reloader.Reload(ctx)
	}

	return result, nil
}

// DeletePipeline removes a pipeline by name and reloads schedules.
func (s *Service) DeletePipeline(ctx context.Context, principal string, name string) error {
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
		Status:        "ALLOWED",
		CreatedAt:     time.Now(),
	})

	if s.reloader != nil {
		_ = s.reloader.Reload(ctx)
	}

	return nil
}

// === Job CRUD ===

// CreateJob adds a new job to the specified pipeline after validating the notebook.
func (s *Service) CreateJob(ctx context.Context, principal string, pipelineName string, req domain.CreatePipelineJobRequest) (*domain.PipelineJob, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, err
	}

	// Validate notebook exists and has SQL (only for NOTEBOOK jobs).
	if req.JobType == "" || req.JobType == domain.PipelineJobTypeNotebook {
		_, err = s.notebooks.GetSQLBlocks(ctx, req.NotebookID)
		if err != nil {
			return nil, fmt.Errorf("validate notebook: %w", err)
		}
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
		JobType:           req.JobType,
		ModelSelector:     req.ModelSelector,
	}

	created, err := s.pipelines.CreateJob(ctx, job)
	if err != nil {
		return nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: principal,
		Action:        "pipeline.job.create",
		Status:        "ALLOWED",
		CreatedAt:     time.Now(),
	})

	return created, nil
}

// ListJobs returns all jobs belonging to the named pipeline.
func (s *Service) ListJobs(ctx context.Context, pipelineName string) ([]domain.PipelineJob, error) {
	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, err
	}
	return s.pipelines.ListJobsByPipeline(ctx, p.ID)
}

// DeleteJob removes a job from a pipeline by ID.
func (s *Service) DeleteJob(ctx context.Context, principal string, _ string, jobID string) error {
	// Verify the job exists (also validates jobID).
	_, err := s.pipelines.GetJobByID(ctx, jobID)
	if err != nil {
		return err
	}
	if err := s.pipelines.DeleteJob(ctx, jobID); err != nil {
		return err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: principal,
		Action:        "pipeline.job.delete",
		Status:        "ALLOWED",
		CreatedAt:     time.Now(),
	})

	return nil
}

// === Run Operations ===

// TriggerRun validates the pipeline DAG and delegates execution to asset orchestration.
func (s *Service) TriggerRun(ctx context.Context, principal string, pipelineName string,
	params map[string]string, triggerType string) (*domain.PipelineRun, error) {
	runID := domain.NewID()
	paramsCopy := cloneParams(params)
	pipelineID, err := s.triggerAssets(ctx, runID, principal, pipelineName, paramsCopy)
	if err != nil {
		return nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: principal,
		Action:        "pipeline.trigger",
		Status:        "ALLOWED",
		CreatedAt:     time.Now(),
	})

	return &domain.PipelineRun{
		ID:          runID,
		PipelineID:  pipelineID,
		Status:      domain.PipelineRunStatusPending,
		TriggerType: triggerType,
		TriggeredBy: principal,
		Parameters:  cloneParams(paramsCopy),
		CreatedAt:   time.Now(),
	}, nil
}

func (s *Service) triggerAssets(ctx context.Context, runID string, principal string, pipelineName string,
	params map[string]string) (string, error) {

	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return "", err
	}

	// List jobs and validate DAG.
	jobs, err := s.pipelines.ListJobsByPipeline(ctx, p.ID)
	if err != nil {
		return "", err
	}
	if len(jobs) == 0 {
		return "", domain.ErrValidation("pipeline has no jobs")
	}

	levels, err := ResolveExecutionOrder(jobs)
	if err != nil {
		return "", err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	s.runCancels.Store(runID, cancel)

	go func() {
		defer s.runCancels.LoadAndDelete(runID)
		s.executeRunViaAssets(runCtx, p, jobs, levels, cloneParams(params), principal)
	}()

	return p.ID, nil
}

func (s *Service) executeRunViaAssets(
	ctx context.Context,
	pipeline *domain.Pipeline,
	jobs []domain.PipelineJob,
	levels [][]string,
	params map[string]string,
	principal string,
) {
	if s.assetRepo == nil || s.assetDepRepo == nil || s.assetRunRepo == nil {
		s.logger.Warn("asset orchestration not configured for pipeline run", "pipeline", pipeline.Name)
		return
	}

	adapted, err := BuildPipelineAssetGraph(pipeline, jobs)
	if err != nil {
		s.logger.Error("build pipeline asset graph", "pipeline", pipeline.Name, "error", err)
		return
	}

	for i := range adapted.Assets {
		if adapted.Assets[i].ProductID == "" {
			adapted.Assets[i].ProductID = s.assetProductID
		}
		_, getErr := s.assetRepo.GetByID(ctx, adapted.Assets[i].ID)
		if getErr == nil {
			continue
		}
		var notFoundErr *domain.NotFoundError
		if !errors.As(getErr, &notFoundErr) {
			s.logger.Error("get asset", "asset_id", adapted.Assets[i].ID, "error", getErr)
			return
		}
		if _, createErr := s.assetRepo.Create(ctx, &adapted.Assets[i]); createErr != nil {
			var conflictErr *domain.ConflictError
			if errors.As(createErr, &conflictErr) {
				if _, updateErr := s.assetRepo.Update(ctx, adapted.Assets[i].ID, &adapted.Assets[i]); updateErr != nil {
					s.logger.Error("ensure asset", "asset_id", adapted.Assets[i].ID, "error", updateErr)
					return
				}
				continue
			}
			s.logger.Error("create asset", "asset_id", adapted.Assets[i].ID, "error", createErr)
			return
		}
	}

	for i := range adapted.Dependencies {
		_, depErr := s.assetDepRepo.Create(ctx, &adapted.Dependencies[i])
		if depErr != nil {
			var conflict *domain.ConflictError
			if !errors.As(depErr, &conflict) {
				s.logger.Error("create asset dependency", "error", depErr)
				return
			}
		}
	}

	rootAssetID := levels[0][0]
	assetRun, err := s.assetRunRepo.CreateRun(ctx, &domain.AssetRun{
		ID:          domain.NewID(),
		AssetID:     rootAssetID,
		Status:      domain.AssetRunStatusQueued,
		TriggerType: domain.AssetTriggerTypePipeline,
		TriggeredBy: principal,
		MaxAttempts: 1,
	})
	if err != nil {
		s.logger.Error("create asset run", "asset_id", rootAssetID, "error", err)
		return
	}

	plan := &orchestration.AssetRunPlan{RootAssetID: rootAssetID, Levels: levels}
	state := orchestration.NewAssetRunStateMachine()
	limiter := orchestration.NewConcurrencyLimiter(16, 1)
	io := orchestration.NewInMemoryIOManager()

	jobByID := make(map[string]domain.PipelineJob, len(jobs))
	for _, job := range jobs {
		jobByID[job.ID] = job
	}

	stepper := &pipelineAssetStepper{
		svc:       s,
		jobByID:   jobByID,
		params:    params,
		principal: principal,
		logger:    s.logger.With("pipeline", pipeline.Name),
	}
	executor := orchestration.NewAssetExecutor(s.assetRunRepo, state, io, limiter, stepper)

	if err := executor.ExecutePlan(ctx, assetRun.ID, assetRun.Status, plan); err != nil {
		s.logger.Error("execute asset plan", "asset_run_id", assetRun.ID, "error", err)
		return
	}
}

func cloneParams(params map[string]string) map[string]string {
	if len(params) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(params))
	for k, v := range params {
		out[k] = v
	}
	return out
}

type pipelineAssetStepper struct {
	svc       *Service
	jobByID   map[string]domain.PipelineJob
	params    map[string]string
	principal string
	logger    *slog.Logger
}

func (p *pipelineAssetStepper) Execute(ctx context.Context, assetID string, _ orchestration.IOManager) (map[string]any, error) {
	job, ok := p.jobByID[assetID]
	if !ok {
		return nil, domain.ErrValidation("asset job not found for asset id %s", assetID)
	}
	if err := p.svc.executeJob(ctx, job, "", p.params, p.principal, p.logger); err != nil {
		return nil, err
	}
	return map[string]any{"asset_id": assetID, "status": "success"}, nil
}

// GetRun returns a pipeline run by ID.
func (s *Service) GetRun(ctx context.Context, runID string) (*domain.PipelineRun, error) {
	return s.runs.GetRunByID(ctx, runID)
}

// ListRuns returns a filtered, paginated list of runs for the named pipeline.
func (s *Service) ListRuns(ctx context.Context, pipelineName string, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error) {
	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, 0, err
	}
	filter.PipelineID = &p.ID
	return s.runs.ListRuns(ctx, filter)
}

// ListJobRuns returns all job runs for the given pipeline run.
func (s *Service) ListJobRuns(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
	// Verify run exists.
	_, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		return nil, err
	}
	return s.runs.ListJobRunsByRun(ctx, runID)
}

// CancelRun cancels a pending or running pipeline run and its pending job runs.
func (s *Service) CancelRun(ctx context.Context, principal string, runID string) error {
	run, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		return err
	}

	if run.Status != domain.PipelineRunStatusPending && run.Status != domain.PipelineRunStatusRunning {
		return domain.ErrValidation("cannot cancel run with status %s", run.Status)
	}

	// Signal the background goroutine to stop.
	if cancel, ok := s.runCancels.LoadAndDelete(runID); ok {
		cancel.(context.CancelFunc)()
	}

	errMsg := "cancelled by " + principal
	if err := s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusCancelled, &errMsg); err != nil {
		return err
	}

	// Cancel pending job runs.
	jobRuns, _ := s.runs.ListJobRunsByRun(ctx, runID) // best effort: run already cancelled
	for _, jr := range jobRuns {
		if jr.Status == domain.PipelineJobRunStatusPending {
			_ = s.runs.UpdateJobRunFinished(ctx, jr.ID, domain.PipelineJobRunStatusCancelled, nil)
		}
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: principal,
		Action:        "pipeline.cancel",
		Status:        "ALLOWED",
		CreatedAt:     time.Now(),
	})

	return nil
}
