package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/service/orchestration"
	servicepolicy "github.com/Yacobolo/quackstack/internal/service/policy"
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
	notebookRepo domain.NotebookRepository
	gitRepos    domain.GitRepoRepository
	models      domain.ModelRepository
	folders     domain.FolderRepository
	principals  domain.PrincipalRepository
	grants      domain.GrantRepository
	auth        domain.AuthorizationService
	computeRepo domain.ComputeEndpointRepository
	modelRunner domain.ModelRunner
	engine      domain.SessionEngine
	duckDB      *sql.DB
	httpClient  *http.Client
	logger      *slog.Logger
	reloader    ScheduleReloader
	runCancels  sync.Map // maps run ID (string) → context.CancelFunc
	dispatching sync.Map // maps pipeline ID (string) → struct{}

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
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
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

func (s *Service) requirePipelinesRepo() error {
	if s == nil || s.pipelines == nil {
		return domain.ErrNotImplemented("pipeline service is not configured")
	}
	return nil
}

func (s *Service) requireRunsRepo() error {
	if s.runs == nil {
		return domain.ErrNotImplemented("pipeline run service is not configured")
	}
	return nil
}

// SetScheduleReloader sets the schedule reloader (breaks circular dep).
func (s *Service) SetScheduleReloader(r ScheduleReloader) {
	s.reloader = r
}

// SetModelRunner sets the model runner for MODEL_RUN pipeline jobs.
func (s *Service) SetModelRunner(runner domain.ModelRunner) {
	s.modelRunner = runner
}

// SetFolderRepository enables folder-backed pipeline placement.
func (s *Service) SetFolderRepository(folders domain.FolderRepository) {
	s.folders = folders
}

// SetAuthorization enables pipeline privilege enforcement.
func (s *Service) SetAuthorization(auth domain.AuthorizationService) {
	s.auth = auth
}

// SetGrantRepository enables creator self-grants for new pipelines.
func (s *Service) SetGrantRepository(repo domain.GrantRepository) {
	s.grants = repo
}

// SetPrincipalRepository enables principal lookup for run_as validation and grants.
func (s *Service) SetPrincipalRepository(repo domain.PrincipalRepository) {
	s.principals = repo
}

// SetComputeEndpointRepository enables compute-endpoint validation and routing.
func (s *Service) SetComputeEndpointRepository(repo domain.ComputeEndpointRepository) {
	s.computeRepo = repo
}

// SetNotebookRepository enables pipeline provenance lookups.
func (s *Service) SetNotebookRepository(repo domain.NotebookRepository) {
	s.notebookRepo = repo
}

// SetGitRepoRepository enables Git-backed provenance lookups.
func (s *Service) SetGitRepoRepository(repo domain.GitRepoRepository) {
	s.gitRepos = repo
}

// SetModelRepository enables model provenance lookups.
func (s *Service) SetModelRepository(repo domain.ModelRepository) {
	s.models = repo
}

// SetHTTPClient configures the outbound webhook client.
func (s *Service) SetHTTPClient(client *http.Client) {
	s.httpClient = client
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
	if err := s.requirePipelinesRepo(); err != nil {
		return err
	}
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
	if err := s.requirePipelinesRepo(); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	req.AdmissionMode = normalizeAdmissionMode(req.AdmissionMode)
	if err := s.validateRunAsPrincipal(ctx, req.RunAsPrincipal, "setting run_as_principal"); err != nil {
		return nil, err
	}
	if err := s.validatePipelineDefaults(ctx, req.DefaultComputeEndpointID, req.NotificationWebhooks); err != nil {
		return nil, err
	}

	if req.ConcurrencyLimit == 0 {
		req.ConcurrencyLimit = 1
	}

	p := &domain.Pipeline{
		ID:                       domain.NewID(),
		Name:                     req.Name,
		Description:              req.Description,
		ScheduleCron:             req.ScheduleCron,
		IsPaused:                 req.IsPaused,
		ConcurrencyLimit:         req.ConcurrencyLimit,
		RunAsPrincipal:           req.RunAsPrincipal,
		AdmissionMode:            req.AdmissionMode,
		MaxRunDurationSeconds:    req.MaxRunDurationSeconds,
		NotificationWebhooks:     req.NotificationWebhooks,
		DefaultRetryCount:        req.DefaultRetryCount,
		DefaultTimeoutSeconds:    req.DefaultTimeoutSeconds,
		DefaultComputeEndpointID: req.DefaultComputeEndpointID,
		CreatedBy:                principal,
	}
	if req.FolderID != nil && *req.FolderID != "" {
		p.FolderID = *req.FolderID
	} else if s.folders != nil {
		root, err := s.folders.EnsurePersonalWorkspaceRoot(ctx, principal)
		if err != nil {
			return nil, err
		}
		p.FolderID = root.ID
	}

	result, err := s.pipelines.CreatePipeline(ctx, p)
	if err != nil {
		return nil, err
	}
	if err := s.grantCreatorPipelinePrivileges(ctx, result.ID, principal); err != nil {
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
	if err := s.requirePipelinesRepo(); err != nil {
		return nil, err
	}
	result, err := s.pipelines.GetPipelineByName(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := s.requirePipelineView(ctx, servicePrincipalName(ctx), result); err != nil {
		return nil, err
	}
	return result, nil
}

// ListPipelines returns a paginated list of pipelines.
func (s *Service) ListPipelines(ctx context.Context, page domain.PageRequest) ([]domain.Pipeline, int64, error) {
	if err := s.requirePipelinesRepo(); err != nil {
		return nil, 0, err
	}
	items, total, err := s.pipelines.ListPipelines(ctx, page)
	if err != nil {
		return nil, 0, err
	}
	principal := servicePrincipalName(ctx)
	if s.auth == nil && !servicepolicy.IsAdmin(ctx) {
		return items, total, nil
	}
	filtered := make([]domain.Pipeline, 0, len(items))
	for i := range items {
		allowed, checkErr := s.canViewPipeline(ctx, principal, &items[i])
		if checkErr != nil {
			return nil, 0, checkErr
		}
		if allowed {
			filtered = append(filtered, items[i])
		}
	}
	return filtered, int64(len(filtered)), nil
}

// UpdatePipeline applies changes to an existing pipeline and reloads schedules.
func (s *Service) UpdatePipeline(ctx context.Context, principal string, name string, req domain.UpdatePipelineRequest) (*domain.Pipeline, error) {
	if err := s.requirePipelinesRepo(); err != nil {
		return nil, err
	}
	if req.ScheduleCron != nil && *req.ScheduleCron != "" {
		if _, err := cron.ParseStandard(*req.ScheduleCron); err != nil {
			return nil, domain.ErrValidation("schedule_cron is invalid: %v", err)
		}
	}

	p, err := s.pipelines.GetPipelineByName(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := s.requirePipelineManage(ctx, principal, p); err != nil {
		return nil, err
	}
	if req.AdmissionMode != nil {
		value := normalizeAdmissionMode(*req.AdmissionMode)
		req.AdmissionMode = &value
	}
	if err := s.validateRunAsPrincipal(ctx, req.RunAsPrincipal, "updating run_as_principal"); err != nil {
		return nil, err
	}
	notificationWebhooks := p.NotificationWebhooks
	if req.NotificationWebhooks != nil {
		notificationWebhooks = *req.NotificationWebhooks
	}
	defaultComputeEndpointID := p.DefaultComputeEndpointID
	if req.DefaultComputeEndpointID != nil {
		defaultComputeEndpointID = req.DefaultComputeEndpointID
	}
	if err := s.validatePipelineDefaults(ctx, defaultComputeEndpointID, notificationWebhooks); err != nil {
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
	if err := s.requirePipelinesRepo(); err != nil {
		return err
	}
	p, err := s.pipelines.GetPipelineByName(ctx, name)
	if err != nil {
		return err
	}
	if err := s.requirePipelineManage(ctx, principal, p); err != nil {
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
	if err := s.requirePipelinesRepo(); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}

	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, err
	}
	if err := s.requirePipelineManage(ctx, principal, p); err != nil {
		return nil, err
	}

	// Validate notebook exists and has SQL (only for NOTEBOOK jobs).
	if req.JobType == "" || req.JobType == domain.PipelineJobTypeNotebook {
		_, err = s.notebooks.GetSQLBlocks(ctx, req.NotebookID)
		if err != nil {
			return nil, fmt.Errorf("validate notebook: %w", err)
		}
	}
	if err := s.validateComputeEndpoint(ctx, req.ComputeEndpointID); err != nil {
		return nil, err
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
	if err := s.requirePipelinesRepo(); err != nil {
		return nil, err
	}
	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, err
	}
	if err := s.requirePipelineView(ctx, servicePrincipalName(ctx), p); err != nil {
		return nil, err
	}
	return s.pipelines.ListJobsByPipeline(ctx, p.ID)
}

// GetJob returns a pipeline-scoped job by ID.
func (s *Service) GetJob(ctx context.Context, pipelineName, jobID string) (*domain.PipelineJob, error) {
	if err := s.requirePipelinesRepo(); err != nil {
		return nil, err
	}
	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, err
	}
	if err := s.requirePipelineView(ctx, servicePrincipalName(ctx), p); err != nil {
		return nil, err
	}
	job, err := s.pipelines.GetJobByID(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if job.PipelineID != p.ID {
		return nil, domain.ErrNotFound("pipeline job %q not found", jobID)
	}
	return job, nil
}

// UpdateJob applies partial changes to a pipeline-scoped job.
func (s *Service) UpdateJob(ctx context.Context, principal string, pipelineName, jobID string, req domain.UpdatePipelineJobRequest) (*domain.PipelineJob, error) {
	if err := s.requirePipelinesRepo(); err != nil {
		return nil, err
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	job, err := s.GetJob(ctx, pipelineName, jobID)
	if err != nil {
		return nil, err
	}
	pipelineDef, err := s.pipelines.GetPipelineByID(ctx, job.PipelineID)
	if err != nil {
		return nil, err
	}
	if err := s.requirePipelineManage(ctx, principal, pipelineDef); err != nil {
		return nil, err
	}

	jobType := job.JobType
	if req.JobType != nil {
		jobType = *req.JobType
	}
	notebookID := job.NotebookID
	if req.NotebookID != nil {
		notebookID = *req.NotebookID
	}
	modelSelector := job.ModelSelector
	if req.ModelSelector != nil {
		modelSelector = *req.ModelSelector
	}

	switch jobType {
	case "", domain.PipelineJobTypeNotebook:
		if strings.TrimSpace(notebookID) == "" {
			return nil, domain.ErrValidation("notebook_id is required for NOTEBOOK jobs")
		}
		if _, err := s.notebooks.GetSQLBlocks(ctx, notebookID); err != nil {
			return nil, fmt.Errorf("validate notebook: %w", err)
		}
	case domain.PipelineJobTypeModelRun:
		if strings.TrimSpace(modelSelector) == "" {
			return nil, domain.ErrValidation("model_selector is required for MODEL_RUN jobs")
		}
	default:
		return nil, domain.ErrValidation("job_type must be NOTEBOOK or MODEL_RUN")
	}
	computeEndpointID := job.ComputeEndpointID
	if req.ComputeEndpointID != nil {
		computeEndpointID = req.ComputeEndpointID
	}
	if err := s.validateComputeEndpoint(ctx, computeEndpointID); err != nil {
		return nil, err
	}

	result, err := s.pipelines.UpdateJob(ctx, job.ID, req)
	if err != nil {
		return nil, err
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: principal,
		Action:        "pipeline.job.update",
		Status:        "ALLOWED",
		CreatedAt:     time.Now(),
	})
	return result, nil
}

// DeleteJob removes a job from a pipeline by ID.
func (s *Service) DeleteJob(ctx context.Context, principal string, pipelineName string, jobID string) error {
	if err := s.requirePipelinesRepo(); err != nil {
		return err
	}
	if s.auth != nil {
		pipelineDef, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
		if err != nil {
			return err
		}
		if err := s.requirePipelineManage(ctx, principal, pipelineDef); err != nil {
			return err
		}
	}
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

// TriggerRun persists a real pipeline run, pre-creates job runs, and dispatches execution.
func (s *Service) TriggerRun(ctx context.Context, principal string, pipelineName string,
	params map[string]string, triggerType string) (*domain.PipelineRun, error) {
	if err := s.requirePipelinesRepo(); err != nil {
		return nil, err
	}
	if err := s.requireRunsRepo(); err != nil {
		return nil, err
	}

	pipelineDef, jobs, levels, err := s.loadPipelineExecution(ctx, pipelineName)
	if err != nil {
		return nil, err
	}
	if err := s.requirePipelineRun(ctx, principal, pipelineDef); err != nil {
		return nil, err
	}
	effectivePrincipal := principal
	if pipelineDef.RunAsPrincipal != nil && strings.TrimSpace(*pipelineDef.RunAsPrincipal) != "" {
		effectivePrincipal = strings.TrimSpace(*pipelineDef.RunAsPrincipal)
	}
	queued, err := s.shouldQueueRun(ctx, pipelineDef, triggerType)
	if err != nil {
		return nil, err
	}

	run, jobRunsByJobID, err := s.createRunRecords(ctx, pipelineDef, jobs, principal, effectivePrincipal, params, triggerType, queued, nil, nil)
	if err != nil {
		return nil, err
	}
	if !queued {
		s.dispatchRunExecution(*run, pipelineDef, jobs, levels, jobRunsByJobID)
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		ID:            domain.NewID(),
		PrincipalName: principal,
		Action:        "pipeline.trigger",
		Status:        "ALLOWED",
		CreatedAt:     time.Now(),
	})

	return run, nil
}

func (s *Service) executeRunViaAssets(
	ctx context.Context,
	run *domain.PipelineRun,
	pipeline *domain.Pipeline,
	jobs []domain.PipelineJob,
	levels [][]string,
	params map[string]string,
	principal string,
	jobRunsByJobID map[string]*domain.PipelineJobRun,
) {
	persistCtx := context.Background()

	if s.assetRepo == nil || s.assetDepRepo == nil || s.assetRunRepo == nil {
		s.failActiveJobRuns(persistCtx, run.ID, "asset orchestration not configured for pipeline run")
		s.failRun(persistCtx, run.ID, "asset orchestration not configured for pipeline run")
		s.logger.Warn("asset orchestration not configured for pipeline run", "pipeline", pipeline.Name, "run_id", run.ID)
		return
	}

	if s.runCancelled(persistCtx, run.ID) || ctx.Err() != nil {
		s.cancelPendingJobRuns(persistCtx, run.ID)
		msg := "pipeline run cancelled"
		_ = s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusCancelled, &msg)
		run.Status = domain.PipelineRunStatusCancelled
		run.ErrorMessage = &msg
		s.logRunEventAndNotify(persistCtx, run, domain.PipelineRunEventCancelled, &msg, pipelineMessagePtr("CANCELLED"), nil)
		go s.dispatchQueuedRuns(context.Background(), run.PipelineID)
		return
	}

	if err := s.runs.UpdateRunStarted(persistCtx, run.ID); err != nil {
		s.logger.Error("mark pipeline run started", "run_id", run.ID, "error", err)
		return
	}
	run.Status = domain.PipelineRunStatusRunning
	startMsg := "pipeline run started"
	s.logRunEventAndNotify(persistCtx, run, domain.PipelineRunEventStarted, &startMsg, nil, nil)

	if err := s.syncPipelineAssets(persistCtx, pipeline); err != nil {
		s.failActiveJobRuns(persistCtx, run.ID, err.Error())
		s.failRun(persistCtx, run.ID, err.Error())
		s.logger.Error("sync pipeline assets", "pipeline", pipeline.Name, "run_id", run.ID, "error", err)
		return
	}

	rootAssetID := levels[0][0]
	assetRun, err := s.assetRunRepo.CreateRun(ctx, &domain.AssetRun{
		ID:          domain.NewID(),
		AssetID:     rootAssetID,
		RunGroupID:  &run.ID,
		Status:      domain.AssetRunStatusQueued,
		TriggerType: domain.AssetTriggerTypePipeline,
		TriggeredBy: principal,
		MaxAttempts: 1,
	})
	if err != nil {
		s.failActiveJobRuns(persistCtx, run.ID, err.Error())
		s.failRun(persistCtx, run.ID, err.Error())
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
		svc:            s,
		pipeline:       pipeline,
		jobByID:        jobByID,
		jobRunsByJobID: jobRunsByJobID,
		runID:          run.ID,
		params:         params,
		principal:      principal,
		logger:         s.logger.With("pipeline", pipeline.Name, "run_id", run.ID),
	}
	executor := orchestration.NewAssetExecutor(s.assetRunRepo, state, io, limiter, stepper)

	if err := executor.ExecutePlan(ctx, assetRun.ID, assetRun.Status, plan); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			msg := "pipeline run exceeded max_run_duration_seconds"
			_ = s.runs.MarkRunSLABreached(persistCtx, run.ID, &msg)
			run.SLABreachedAt = timePtr(time.Now().UTC())
			s.logRunEventAndNotify(persistCtx, run, domain.PipelineRunEventSLABreach, &msg, pipelineMessagePtr("TIMEOUT"), nil)
			s.finishFailedRunJobRuns(persistCtx, run.ID, msg)
			s.failRun(persistCtx, run.ID, msg)
			_ = s.assetRunRepo.UpdateRunFinished(persistCtx, assetRun.ID, domain.AssetRunStatusFailed, &msg)
			return
		}
		if errors.Is(err, context.Canceled) || s.runCancelled(persistCtx, run.ID) {
			msg := "pipeline run cancelled"
			_ = s.assetRunRepo.UpdateRunFinished(persistCtx, assetRun.ID, domain.AssetRunStatusCancelled, &msg)
			s.cancelPendingJobRuns(persistCtx, run.ID)
			_ = s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusCancelled, &msg)
			run.Status = domain.PipelineRunStatusCancelled
			run.ErrorMessage = &msg
			s.logRunEventAndNotify(persistCtx, run, domain.PipelineRunEventCancelled, &msg, pipelineMessagePtr("CANCELLED"), nil)
			go s.dispatchQueuedRuns(context.Background(), run.PipelineID)
			return
		}
		s.finishFailedRunJobRuns(persistCtx, run.ID, err.Error())
		s.failRun(persistCtx, run.ID, err.Error())
		s.logger.Error("execute asset plan", "asset_run_id", assetRun.ID, "error", err)
		return
	}

	if s.runCancelled(persistCtx, run.ID) {
		msg := "pipeline run cancelled"
		_ = s.assetRunRepo.UpdateRunFinished(persistCtx, assetRun.ID, domain.AssetRunStatusCancelled, &msg)
		s.cancelPendingJobRuns(persistCtx, run.ID)
		_ = s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusCancelled, &msg)
		run.Status = domain.PipelineRunStatusCancelled
		run.ErrorMessage = &msg
		s.logRunEventAndNotify(persistCtx, run, domain.PipelineRunEventCancelled, &msg, pipelineMessagePtr("CANCELLED"), nil)
		go s.dispatchQueuedRuns(context.Background(), run.PipelineID)
		return
	}

	if err := s.runs.UpdateRunFinished(persistCtx, run.ID, domain.PipelineRunStatusSuccess, nil); err != nil {
		s.logger.Error("mark pipeline run success", "run_id", run.ID, "error", err)
		return
	}
	run.Status = domain.PipelineRunStatusSuccess
	s.logRunEventAndNotify(persistCtx, run, domain.PipelineRunEventSucceeded, nil, nil, nil)
	go s.dispatchQueuedRuns(context.Background(), run.PipelineID)
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
	svc            *Service
	pipeline       *domain.Pipeline
	jobByID        map[string]domain.PipelineJob
	jobRunsByJobID map[string]*domain.PipelineJobRun
	runID          string
	params         map[string]string
	principal      string
	logger         *slog.Logger
}

func (p *pipelineAssetStepper) Execute(ctx context.Context, assetID string, _ orchestration.IOManager) (map[string]any, error) {
	job, ok := p.jobByID[assetID]
	if !ok {
		return nil, domain.ErrValidation("asset job not found for asset id %s", assetID)
	}
	jobRunID := ""
	if jobRun, ok := p.jobRunsByJobID[job.ID]; ok && jobRun != nil {
		if jobRun.Status == domain.PipelineJobRunStatusSkipped {
			return map[string]any{"asset_id": assetID, "status": "skipped"}, nil
		}
		jobRunID = jobRun.ID
	}
	if err := p.svc.executeJob(ctx, p.pipeline, job, p.runID, jobRunID, p.params, p.principal, p.logger); err != nil {
		return nil, err
	}
	return map[string]any{"asset_id": assetID, "status": "success"}, nil
}

// GetRun returns a pipeline run by ID.
func (s *Service) GetRun(ctx context.Context, runID string) (*domain.PipelineRun, error) {
	if err := s.requireRunsRepo(); err != nil {
		return nil, err
	}
	run, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		return nil, err
	}
	pipelineDef, err := s.pipelines.GetPipelineByID(ctx, run.PipelineID)
	if err != nil {
		return nil, err
	}
	if err := s.requirePipelineRun(ctx, servicePrincipalName(ctx), pipelineDef); err != nil {
		return nil, err
	}
	return run, nil
}

// ListRuns returns a filtered, paginated list of runs for the named pipeline.
func (s *Service) ListRuns(ctx context.Context, pipelineName string, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error) {
	if err := s.requirePipelinesRepo(); err != nil {
		return nil, 0, err
	}
	if err := s.requireRunsRepo(); err != nil {
		return nil, 0, err
	}
	p, err := s.pipelines.GetPipelineByName(ctx, pipelineName)
	if err != nil {
		return nil, 0, err
	}
	if err := s.requirePipelineRun(ctx, servicePrincipalName(ctx), p); err != nil {
		return nil, 0, err
	}
	filter.PipelineID = &p.ID
	return s.runs.ListRuns(ctx, filter)
}

// ListJobRuns returns all job runs for the given pipeline run.
func (s *Service) ListJobRuns(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
	if err := s.requireRunsRepo(); err != nil {
		return nil, err
	}
	// Verify run exists.
	run, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		return nil, err
	}
	pipelineDef, err := s.pipelines.GetPipelineByID(ctx, run.PipelineID)
	if err != nil {
		return nil, err
	}
	if err := s.requirePipelineRun(ctx, servicePrincipalName(ctx), pipelineDef); err != nil {
		return nil, err
	}
	return s.runs.ListJobRunsByRun(ctx, runID)
}

// CancelRun cancels a pending or running pipeline run and its pending job runs.
func (s *Service) CancelRun(ctx context.Context, principal string, runID string) error {
	if err := s.requireRunsRepo(); err != nil {
		return err
	}
	run, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		return err
	}
	pipelineDef, err := s.pipelines.GetPipelineByID(ctx, run.PipelineID)
	if err != nil {
		return err
	}
	if err := s.requirePipelineManage(ctx, principal, pipelineDef); err != nil {
		return err
	}

	if run.Status != domain.PipelineRunStatusPending && run.Status != domain.PipelineRunStatusRunning {
		return domain.ErrValidation("cannot cancel run with status %s", run.Status)
	}

	errMsg := "cancelled by " + principal
	// Queued runs can be terminalized immediately because no work has started.
	if run.Status == domain.PipelineRunStatusPending && run.QueueStartedAt == nil {
		if err := s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusCancelled, &errMsg); err != nil {
			return err
		}
		jobRuns, _ := s.runs.ListJobRunsByRun(ctx, runID)
		for _, jr := range jobRuns {
			if jr.Status == domain.PipelineJobRunStatusPending {
				_ = s.runs.UpdateJobRunFinished(ctx, jr.ID, domain.PipelineJobRunStatusCancelled, &errMsg, pipelineMessagePtr("CANCELLED"), jr.AttemptCount)
			}
		}
		run.Status = domain.PipelineRunStatusCancelled
		run.ErrorMessage = &errMsg
		s.logRunEventAndNotify(ctx, run, domain.PipelineRunEventCancelled, &errMsg, pipelineMessagePtr("CANCELLED"), nil)
		go s.dispatchQueuedRuns(context.Background(), run.PipelineID)
	} else if cancel, ok := s.runCancels.LoadAndDelete(runID); ok {
		s.cancelPendingJobRuns(ctx, runID)
		cancel.(context.CancelFunc)()
		s.logRunEventAndNotify(ctx, run, domain.PipelineRunEventCancelled, &errMsg, pipelineMessagePtr("CANCELLED"), map[string]any{"requested": true})
	} else {
		s.cancelPendingJobRuns(ctx, runID)
		if err := s.runs.UpdateRunFinished(ctx, runID, domain.PipelineRunStatusCancelled, &errMsg); err != nil {
			return err
		}
		run.Status = domain.PipelineRunStatusCancelled
		run.ErrorMessage = &errMsg
		s.logRunEventAndNotify(ctx, run, domain.PipelineRunEventCancelled, &errMsg, pipelineMessagePtr("CANCELLED"), map[string]any{"forced": true})
		go s.dispatchQueuedRuns(context.Background(), run.PipelineID)
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
