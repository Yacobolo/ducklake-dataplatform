package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/Yacobolo/quackstack/internal/domain"
)

// Service provides business logic for model management.
type Service struct {
	models        domain.ModelRepository
	runs          domain.ModelRunRepository
	projects      domain.ProjectRepository
	environments  domain.EnvironmentRepository
	projectDeps   domain.ProjectDependencyRepository
	sources       domain.SourceDefinitionRepository
	seeds         domain.SeedRepository
	builds        domain.BuildRepository
	tests         domain.ModelTestRepository
	testResults   domain.ModelTestResultRepository
	audit         domain.AuditRepository
	lineage       domain.LineageRepository
	colLineage    domain.ColumnLineageRepository
	introspection domain.IntrospectionRepository
	tags          domain.TagRepository
	macros        domain.MacroRepository
	notebooks     domain.NotebookProvider
	notebookLinks domain.NotebookModelLinkRepository
	engine        domain.SessionEngine
	duckDB        *sql.DB
	logger        *slog.Logger
	runCancels    sync.Map
}

// ServiceDeps defines the collaborators required to construct a model service.
type ServiceDeps struct {
	Models        domain.ModelRepository
	Runs          domain.ModelRunRepository
	Projects      domain.ProjectRepository
	Environments  domain.EnvironmentRepository
	ProjectDeps   domain.ProjectDependencyRepository
	Sources       domain.SourceDefinitionRepository
	Seeds         domain.SeedRepository
	Builds        domain.BuildRepository
	Tests         domain.ModelTestRepository
	TestResults   domain.ModelTestResultRepository
	Audit         domain.AuditRepository
	Lineage       domain.LineageRepository
	ColumnLineage domain.ColumnLineageRepository
	Introspection domain.IntrospectionRepository
	Tags          domain.TagRepository
	Macros        domain.MacroRepository
	Notebooks     domain.NotebookProvider
	NotebookLinks domain.NotebookModelLinkRepository
	Engine        domain.SessionEngine
	DuckDB        *sql.DB
	Logger        *slog.Logger
}

// NewService creates a new model Service.
func NewService(deps ServiceDeps) *Service {
	return &Service{
		models:        deps.Models,
		runs:          deps.Runs,
		projects:      deps.Projects,
		environments:  deps.Environments,
		projectDeps:   deps.ProjectDeps,
		sources:       deps.Sources,
		seeds:         deps.Seeds,
		builds:        deps.Builds,
		tests:         deps.Tests,
		testResults:   deps.TestResults,
		audit:         deps.Audit,
		lineage:       deps.Lineage,
		colLineage:    deps.ColumnLineage,
		introspection: deps.Introspection,
		tags:          deps.Tags,
		macros:        deps.Macros,
		notebooks:     deps.Notebooks,
		notebookLinks: deps.NotebookLinks,
		engine:        deps.Engine,
		duckDB:        deps.DuckDB,
		logger:        deps.Logger,
	}
}

// CreateModel creates a new transformation model and auto-extracts its dependencies.
func (s *Service) CreateModel(ctx context.Context, principal string, req domain.CreateModelRequest) (*domain.Model, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	allModels, err := s.models.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("list models for dep extraction: %w", err)
	}

	deps, err := ExtractDependencies(req.SQL, req.ProjectName, allModels)
	if err != nil {
		s.logger.Warn("dependency extraction failed", "project", req.ProjectName, "model", req.Name, "error", err)
		deps = []string{}
	}

	m := &domain.Model{
		ProjectName:     req.ProjectName,
		Name:            req.Name,
		SQL:             req.SQL,
		Materialization: req.Materialization,
		Description:     req.Description,
		Tags:            req.Tags,
		DependsOn:       deps,
		Config:          req.Config,
		Contract:        req.Contract,
		Freshness:       req.Freshness,
		CreatedBy:       principal,
	}

	result, err := s.models.Create(ctx, m)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "create_model", result.QualifiedName())
	return result, nil
}

// GetModel retrieves a model by project and name.
func (s *Service) GetModel(ctx context.Context, projectName, name string) (*domain.Model, error) {
	return s.models.GetByName(ctx, projectName, name)
}

// ListModels returns a paginated list of models, optionally filtered by project.
func (s *Service) ListModels(ctx context.Context, projectName *string, page domain.PageRequest) ([]domain.Model, int64, error) {
	return s.models.List(ctx, projectName, page)
}

// UpdateModel updates a model and re-extracts dependencies if SQL changed.
func (s *Service) UpdateModel(ctx context.Context, principal, projectName, name string, req domain.UpdateModelRequest) (*domain.Model, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	existing, err := s.models.GetByName(ctx, projectName, name)
	if err != nil {
		return nil, err
	}

	result, err := s.models.Update(ctx, existing.ID, req)
	if err != nil {
		return nil, err
	}

	// Re-extract deps if SQL changed
	if req.SQL != nil {
		allModels, err := s.models.ListAll(ctx)
		if err == nil {
			deps, depErr := ExtractDependencies(*req.SQL, projectName, allModels)
			if depErr != nil {
				s.logger.Warn("dependency re-extraction failed", "model", result.QualifiedName(), "error", depErr)
			} else {
				if err := s.models.UpdateDependencies(ctx, result.ID, deps); err != nil {
					return nil, fmt.Errorf("update dependencies for %s: %w", result.QualifiedName(), err)
				}
				result.DependsOn = deps
			}
		}
	}

	s.logAudit(ctx, principal, "update_model", result.QualifiedName())
	return result, nil
}

// DeleteModel deletes a model.
func (s *Service) DeleteModel(ctx context.Context, principal, projectName, name string) error {
	existing, err := s.models.GetByName(ctx, projectName, name)
	if err != nil {
		return err
	}

	if err := s.models.Delete(ctx, existing.ID); err != nil {
		return err
	}

	s.logAudit(ctx, principal, "delete_model", existing.QualifiedName())
	return nil
}

// GetDAG computes the DAG for all models, optionally filtered by project.
func (s *Service) GetDAG(ctx context.Context, projectName *string) ([][]DAGNode, error) {
	var models []domain.Model
	var err error
	if projectName != nil {
		models, _, err = s.models.List(ctx, projectName, domain.PageRequest{MaxResults: 10000})
	} else {
		models, err = s.models.ListAll(ctx)
	}
	if err != nil {
		return nil, err
	}
	return ResolveDAG(models)
}

// TriggerRun starts a model run.
func (s *Service) TriggerRun(ctx context.Context, principal string, req domain.TriggerModelRunRequest) (*domain.ModelRun, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if req.TriggerType == "" {
		req.TriggerType = domain.ModelTriggerTypeManual
	}
	runCtx, err := s.resolveExecutionContext(ctx, req.ProjectName, req.EnvironmentName, req)
	if err != nil {
		return nil, err
	}
	req.TargetCatalog = runCtx.targetCatalog
	req.TargetSchema = runCtx.targetSchema
	req.Variables = cloneStringMap(runCtx.variables)

	allModels, scopeWarnings, err := s.loadCompilationModelScope(ctx, runCtx)
	if err != nil {
		return nil, fmt.Errorf("load scoped models: %w", err)
	}
	if len(allModels) == 0 {
		return nil, domain.ErrValidation("no models defined")
	}

	selected := filterByProject(allModels, runCtx.project.Name)
	if len(selected) == 0 {
		return nil, domain.ErrValidation("no enabled models defined in project %s", runCtx.project.Name)
	}
	selected, selectorWarnings, err := s.selectModelsForRun(ctx, principal, req, selected, allModels, runCtx)
	if err != nil {
		return nil, err
	}

	compiledArtifacts, compileWarnings, err := s.compileSelectedModels(ctx, principal, selected, allModels, runCtx, req)
	if err != nil {
		return nil, err
	}
	compileWarnings = append(compileWarnings, scopeWarnings...)
	compileWarnings = append(compileWarnings, selectorWarnings...)
	if err := s.persistCompileDependencyLineage(ctx, selected, compiledArtifacts, req, principal); err != nil {
		return nil, fmt.Errorf("persist compile dependency lineage: %w", err)
	}

	// Resolve ephemeral models: inject CTEs and remove from execution set.
	// This can alter SQL for downstream models, so keep artifacts in sync.
	selected = resolveEphemeralModels(selected)
	if err := s.syncCompiledArtifacts(selected, compiledArtifacts, req); err != nil {
		return nil, err
	}
	analysisResult, err := s.analyzeCompiledModels(ctx, principal, selected, compiledArtifacts, runCtx, req)
	if err != nil {
		return nil, fmt.Errorf("analyze compiled models: %w", err)
	}
	manifestJSON, err := buildCompileManifest(selected, compiledArtifacts, runCtx, analysisResult.coverageByModel)
	if err != nil {
		return nil, fmt.Errorf("build compile manifest: %w", err)
	}
	diagnosticsJSON, err := buildCompileDiagnostics(compileWarnings, nil, analysisResult.diagnostics)
	if err != nil {
		return nil, fmt.Errorf("build compile diagnostics: %w", err)
	}

	// Resolve DAG
	tiers, err := ResolveDAG(selected)
	if err != nil {
		return nil, fmt.Errorf("resolve DAG: %w", err)
	}

	// Create run
	run := &domain.ModelRun{
		Status:             domain.ModelRunStatusPending,
		TriggerType:        req.TriggerType,
		TriggeredBy:        principal,
		ProjectName:        runCtx.project.Name,
		EnvironmentName:    runCtx.environment.Name,
		TargetCatalog:      req.TargetCatalog,
		TargetSchema:       req.TargetSchema,
		ModelSelector:      req.Selector,
		Variables:          req.Variables,
		FullRefresh:        req.FullRefresh,
		CompileManifest:    strPtrOrNil(manifestJSON),
		CompileDiagnostics: diagnosticsFromJSONOrNil(diagnosticsJSON),
	}
	run, err = s.runs.CreateRun(ctx, run)
	if err != nil {
		return nil, fmt.Errorf("create run: %w", err)
	}
	stateSnapshotJSON, err := marshalStateSnapshot(analysisResult.stateSnapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal build state snapshot: %w", err)
	}
	if build, err := s.createRunBuild(ctx, principal, runCtx.project, runCtx.environment, run.ID, req, manifestJSON, diagnosticsJSON, stateSnapshotJSON); err != nil {
		return nil, err
	} else if build != nil {
		run.BuildID = &build.ID
		if err := s.persistCompiledColumnLineage(ctx, build.ID, analysisResult.lineage); err != nil {
			return nil, fmt.Errorf("persist compiled column lineage: %w", err)
		}
	}

	// Create steps
	for _, tier := range tiers {
		for _, node := range tier {
			artifact := compiledArtifacts[node.Model.ID]
			step := &domain.ModelRunStep{
				RunID:        run.ID,
				ModelID:      node.Model.ID,
				ModelName:    node.Model.QualifiedName(),
				CompiledSQL:  strPtrOrNil(artifact.sql),
				CompiledHash: strPtrOrNil(artifact.compiledHash),
				DependsOn:    artifact.dependsOn,
				VarsUsed:     artifact.varsUsed,
				MacrosUsed:   artifact.macrosUsed,
				Status:       domain.ModelRunStatusPending,
				Tier:         node.Tier,
			}
			if _, err := s.runs.CreateStep(ctx, step); err != nil {
				return nil, fmt.Errorf("create step for %s: %w", node.Model.QualifiedName(), err)
			}
		}
	}

	// Launch execution goroutine
	execCtx, cancel := context.WithCancel(context.Background())
	s.runCancels.Store(run.ID, cancel)
	config := ExecutionConfig{
		TargetCatalog: req.TargetCatalog,
		TargetSchema:  req.TargetSchema,
		Variables:     req.Variables,
		FullRefresh:   req.FullRefresh,
	}
	go s.executeRun(execCtx, run.ID, selected, tiers, config, principal)

	s.logAudit(ctx, principal, "trigger_model_run", run.ID)
	return run, nil
}

// TriggerRunSync starts a model run and waits for it to complete synchronously.
// Used by the pipeline executor for MODEL_RUN jobs.
func (s *Service) TriggerRunSync(ctx context.Context, principal string, req domain.TriggerModelRunRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	if req.TriggerType == "" {
		req.TriggerType = domain.ModelTriggerTypePipeline
	}
	runCtx, err := s.resolveExecutionContext(ctx, req.ProjectName, req.EnvironmentName, req)
	if err != nil {
		return err
	}
	req.TargetCatalog = runCtx.targetCatalog
	req.TargetSchema = runCtx.targetSchema
	req.Variables = cloneStringMap(runCtx.variables)

	allModels, scopeWarnings, err := s.loadCompilationModelScope(ctx, runCtx)
	if err != nil {
		return fmt.Errorf("load scoped models: %w", err)
	}
	if len(allModels) == 0 {
		return domain.ErrValidation("no models defined")
	}

	selected := filterByProject(allModels, runCtx.project.Name)
	if len(selected) == 0 {
		return domain.ErrValidation("no enabled models defined in project %s", runCtx.project.Name)
	}
	selected, selectorWarnings, err := s.selectModelsForRun(ctx, principal, req, selected, allModels, runCtx)
	if err != nil {
		return err
	}

	compiledArtifacts, compileWarnings, err := s.compileSelectedModels(ctx, principal, selected, allModels, runCtx, req)
	if err != nil {
		return err
	}
	compileWarnings = append(compileWarnings, scopeWarnings...)
	compileWarnings = append(compileWarnings, selectorWarnings...)
	if err := s.persistCompileDependencyLineage(ctx, selected, compiledArtifacts, req, principal); err != nil {
		return fmt.Errorf("persist compile dependency lineage: %w", err)
	}

	selected = resolveEphemeralModels(selected)
	if err := s.syncCompiledArtifacts(selected, compiledArtifacts, req); err != nil {
		return err
	}
	analysisResult, err := s.analyzeCompiledModels(ctx, principal, selected, compiledArtifacts, runCtx, req)
	if err != nil {
		return fmt.Errorf("analyze compiled models: %w", err)
	}
	manifestJSON, err := buildCompileManifest(selected, compiledArtifacts, runCtx, analysisResult.coverageByModel)
	if err != nil {
		return fmt.Errorf("build compile manifest: %w", err)
	}
	diagnosticsJSON, err := buildCompileDiagnostics(compileWarnings, nil, analysisResult.diagnostics)
	if err != nil {
		return fmt.Errorf("build compile diagnostics: %w", err)
	}

	tiers, err := ResolveDAG(selected)
	if err != nil {
		return fmt.Errorf("resolve DAG: %w", err)
	}

	// Create run
	run := &domain.ModelRun{
		Status:             domain.ModelRunStatusPending,
		TriggerType:        req.TriggerType,
		TriggeredBy:        principal,
		ProjectName:        runCtx.project.Name,
		EnvironmentName:    runCtx.environment.Name,
		TargetCatalog:      req.TargetCatalog,
		TargetSchema:       req.TargetSchema,
		ModelSelector:      req.Selector,
		Variables:          req.Variables,
		FullRefresh:        req.FullRefresh,
		CompileManifest:    strPtrOrNil(manifestJSON),
		CompileDiagnostics: diagnosticsFromJSONOrNil(diagnosticsJSON),
	}
	run, err = s.runs.CreateRun(ctx, run)
	if err != nil {
		return fmt.Errorf("create run: %w", err)
	}
	stateSnapshotJSON, err := marshalStateSnapshot(analysisResult.stateSnapshot)
	if err != nil {
		return fmt.Errorf("marshal build state snapshot: %w", err)
	}
	if build, err := s.createRunBuild(ctx, principal, runCtx.project, runCtx.environment, run.ID, req, manifestJSON, diagnosticsJSON, stateSnapshotJSON); err != nil {
		return err
	} else if build != nil {
		run.BuildID = &build.ID
		if err := s.persistCompiledColumnLineage(ctx, build.ID, analysisResult.lineage); err != nil {
			return fmt.Errorf("persist compiled column lineage: %w", err)
		}
	}

	// Create steps
	for _, tier := range tiers {
		for _, node := range tier {
			artifact := compiledArtifacts[node.Model.ID]
			step := &domain.ModelRunStep{
				RunID:        run.ID,
				ModelID:      node.Model.ID,
				ModelName:    node.Model.QualifiedName(),
				CompiledSQL:  strPtrOrNil(artifact.sql),
				CompiledHash: strPtrOrNil(artifact.compiledHash),
				DependsOn:    artifact.dependsOn,
				VarsUsed:     artifact.varsUsed,
				MacrosUsed:   artifact.macrosUsed,
				Status:       domain.ModelRunStatusPending,
				Tier:         node.Tier,
			}
			if _, err := s.runs.CreateStep(ctx, step); err != nil {
				return fmt.Errorf("create step for %s: %w", node.Model.QualifiedName(), err)
			}
		}
	}

	// Execute synchronously (no goroutine)
	config := ExecutionConfig{
		TargetCatalog: req.TargetCatalog,
		TargetSchema:  req.TargetSchema,
		Variables:     req.Variables,
		FullRefresh:   req.FullRefresh,
	}
	s.executeRun(ctx, run.ID, selected, tiers, config, principal)

	// Check final status
	finalRun, err := s.runs.GetRunByID(ctx, run.ID)
	if err != nil {
		return fmt.Errorf("get final run status: %w", err)
	}
	if finalRun.Status != domain.ModelRunStatusSuccess {
		msg := "model run failed"
		if finalRun.ErrorMessage != nil {
			msg = *finalRun.ErrorMessage
		}
		return fmt.Errorf("model run %s: %s", finalRun.Status, msg)
	}

	s.logAudit(ctx, principal, "trigger_model_run_sync", run.ID)
	return nil
}

// GetRun retrieves a model run.
func (s *Service) GetRun(ctx context.Context, runID string) (*domain.ModelRun, error) {
	return s.runs.GetRunByID(ctx, runID)
}

// ListRuns returns a filtered list of model runs.
func (s *Service) ListRuns(ctx context.Context, filter domain.ModelRunFilter) ([]domain.ModelRun, int64, error) {
	return s.runs.ListRuns(ctx, filter)
}

// ListRunSteps returns the steps for a model run.
func (s *Service) ListRunSteps(ctx context.Context, runID string) ([]domain.ModelRunStep, error) {
	if _, err := s.runs.GetRunByID(ctx, runID); err != nil {
		return nil, err
	}
	return s.runs.ListStepsByRun(ctx, runID)
}

// CancelRun cancels a running model execution.
func (s *Service) CancelRun(ctx context.Context, principal, runID string) error {
	run, err := s.runs.GetRunByID(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status != domain.ModelRunStatusPending && run.Status != domain.ModelRunStatusRunning {
		return domain.ErrValidation("cannot cancel run in status %s", run.Status)
	}

	if cancelFn, ok := s.runCancels.LoadAndDelete(runID); ok {
		cancelFn.(context.CancelFunc)()
	}

	errMsg := "cancelled by " + principal
	if err := s.runs.UpdateRunFinished(ctx, runID, domain.ModelRunStatusCancelled, &errMsg); err != nil {
		return err
	}

	s.logAudit(ctx, principal, "cancel_model_run", runID)
	return nil
}

// CreateTest creates a new test assertion for a model.
func (s *Service) CreateTest(ctx context.Context, principal, projectName, modelName string, req domain.CreateModelTestRequest) (*domain.ModelTest, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	model, err := s.models.GetByName(ctx, projectName, modelName)
	if err != nil {
		return nil, err
	}
	test := &domain.ModelTest{
		ModelID:  model.ID,
		Name:     req.Name,
		TestType: req.TestType,
		Column:   req.Column,
		Config:   req.Config,
	}
	result, err := s.tests.Create(ctx, test)
	if err != nil {
		return nil, err
	}
	s.logAudit(ctx, principal, "create_model_test", model.QualifiedName()+"/"+req.Name)
	return result, nil
}

// ListTests returns all tests for a model.
func (s *Service) ListTests(ctx context.Context, projectName, modelName string) ([]domain.ModelTest, error) {
	model, err := s.models.GetByName(ctx, projectName, modelName)
	if err != nil {
		return nil, err
	}
	return s.tests.ListByModel(ctx, model.ID)
}

// DeleteTest deletes a model test.
func (s *Service) DeleteTest(ctx context.Context, principal, projectName, modelName, testID string) error {
	// Verify the model exists.
	_, err := s.models.GetByName(ctx, projectName, modelName)
	if err != nil {
		return err
	}
	if err := s.tests.Delete(ctx, testID); err != nil {
		return err
	}
	s.logAudit(ctx, principal, "delete_model_test", testID)
	return nil
}

// ListTestResults returns all test results for a model run step.
func (s *Service) ListTestResults(ctx context.Context, runID, stepID string) ([]domain.ModelTestResult, error) {
	if _, err := s.runs.GetRunByID(ctx, runID); err != nil {
		return nil, err
	}
	return s.testResults.ListByStep(ctx, stepID)
}

// PromoteNotebook promotes a notebook output cell to a transformation model.
func (s *Service) PromoteNotebook(ctx context.Context, principal string, req domain.PromoteNotebookRequest) (*domain.Model, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	if s.notebooks == nil {
		return nil, domain.ErrValidation("notebook provider not configured")
	}

	// Compile SQL from selected output cell (graph-aware, tree-shaken).
	sqlBody, err := s.notebooks.CompileOutputCellSQL(ctx, req.NotebookID, req.OutputCellID)
	if err != nil {
		return nil, fmt.Errorf("compile notebook output SQL: %w", err)
	}

	// Compute dependencies from compiled SQL.
	allModels, err := s.models.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("list models for dep extraction: %w", err)
	}
	deps, err := ExtractDependencies(sqlBody, req.ProjectName, allModels)
	if err != nil {
		s.logger.Warn("dependency extraction failed", "project", req.ProjectName, "model", req.Name, "error", err)
		deps = []string{}
	}

	if existing, err := s.models.GetByName(ctx, req.ProjectName, req.Name); err == nil {
		updateReq := domain.UpdateModelRequest{
			SQL:             &sqlBody,
			Materialization: &req.Materialization,
		}
		updated, err := s.models.Update(ctx, existing.ID, updateReq)
		if err != nil {
			return nil, err
		}
		if err := s.models.UpdateDependencies(ctx, updated.ID, deps); err != nil {
			return nil, fmt.Errorf("update dependencies for %s: %w", updated.QualifiedName(), err)
		}
		updated.DependsOn = deps

		if s.notebookLinks == nil {
			return nil, domain.ErrValidation("notebook-model link repository not configured")
		}
		if err := s.notebookLinks.Upsert(ctx, &domain.NotebookModelLink{
			NotebookID:   req.NotebookID,
			ModelID:      updated.ID,
			OutputCellID: req.OutputCellID,
		}); err != nil {
			return nil, fmt.Errorf("upsert notebook-model link: %w", err)
		}

		s.logAudit(ctx, principal, "update_model", updated.QualifiedName())
		return updated, nil
	} else if !errors.As(err, new(*domain.NotFoundError)) {
		return nil, err
	}

	created, err := s.models.CreateWithNotebookLink(ctx, &domain.Model{
		ProjectName:     req.ProjectName,
		Name:            req.Name,
		SQL:             sqlBody,
		Materialization: req.Materialization,
		DependsOn:       deps,
		CreatedBy:       principal,
	}, req.NotebookID, req.OutputCellID)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "create_model", created.QualifiedName())
	return created, nil
}

// UnpublishNotebook removes a notebook-backed model publication and its link.
func (s *Service) UnpublishNotebook(ctx context.Context, principal, notebookID string) error {
	if strings.TrimSpace(notebookID) == "" {
		return domain.ErrValidation("notebook_id is required")
	}
	if s.notebookLinks == nil {
		return domain.ErrValidation("notebook-model link repository not configured")
	}

	link, err := s.notebookLinks.GetByNotebookID(ctx, notebookID)
	if err != nil {
		var notFound *domain.NotFoundError
		if errors.As(err, &notFound) {
			return nil
		}
		return err
	}

	if _, err := s.models.GetByID(ctx, link.ModelID); err != nil {
		var notFound *domain.NotFoundError
		if errors.As(err, &notFound) {
			if err := s.notebookLinks.DeleteByNotebookID(ctx, notebookID); err != nil {
				return err
			}
			return nil
		}
		return err
	}

	if err := s.models.Delete(ctx, link.ModelID); err != nil {
		return err
	}

	s.logAudit(ctx, principal, "delete_model", link.ModelID)
	return nil
}

func (s *Service) logAudit(ctx context.Context, principal, action, _ string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
	})
}

func (s *Service) compileSelectedModels(
	ctx context.Context,
	principal string,
	selected []domain.Model,
	allModels []domain.Model,
	runCtx *resolvedRunContext,
	req domain.TriggerModelRunRequest,
) (map[string]compileResult, []string, error) {
	byQualified := make(map[string]domain.Model, len(allModels))
	for _, m := range allModels {
		byQualified[m.QualifiedName()] = m
	}

	sources, sourceWarnings, err := s.loadSourceRegistry(ctx, runCtx)
	if err != nil {
		return nil, nil, fmt.Errorf("load source registry: %w", err)
	}
	compileWarnings := append([]string(nil), sourceWarnings...)

	type macroBundle struct {
		defs     map[string]compileMacroDefinition
		runtimes map[string]*starlarkMacroRuntime
		warnings []string
	}
	bundleByProject := make(map[string]macroBundle)

	artifacts := make(map[string]compileResult, len(selected))
	usedSourceKeys := make(map[string]struct{})
	for i := range selected {
		m := selected[i]
		bundle, ok := bundleByProject[m.ProjectName]
		if !ok {
			defs, runtimes, warnings, err := s.loadCompileMacros(ctx, principal, m.ProjectName, runCtx)
			if err != nil {
				return nil, nil, fmt.Errorf("load compile macros for project %s: %w", m.ProjectName, err)
			}
			bundle = macroBundle{defs: defs, runtimes: runtimes, warnings: warnings}
			bundleByProject[m.ProjectName] = bundle
			compileWarnings = append(compileWarnings, warnings...)
		}

		ctx := compileContext{
			targetCatalog: req.TargetCatalog,
			targetSchema:  effectiveSchema(req.TargetSchema, m.Config.Schema),
			vars:          req.Variables,
			fullRefresh:   req.FullRefresh,
			projectName:   m.ProjectName,
			modelName:     m.Name,
			materialize:   m.Materialization,
			allowedRefs:   runCtx.allowedRefProjects,
			models:        byQualified,
			sources:       sources,
			macros:        bundle.defs,
			macroRuntimes: bundle.runtimes,
		}
		compiled, err := compileModelSQL(m.SQL, ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("compile model %s: %w", m.QualifiedName(), err)
		}

		selected[i].SQL = compiled.sql
		selected[i].DependsOn = compiled.dependsOn
		for sourceKey := range compiled.sourcesUsed {
			usedSourceKeys[sourceKey] = struct{}{}
		}
		for _, macroName := range compiled.macrosUsed {
			if def, ok := bundle.defs[macroName]; ok && def.status == domain.MacroStatusDeprecated {
				compileWarnings = append(compileWarnings, fmt.Sprintf("deprecated macro %q used by model %s", macroName, m.QualifiedName()))
			}
		}
		artifacts[m.ID] = *compiled
	}

	for key := range runCtx.sourceOverrides {
		if isSourceOverrideUsed(key, usedSourceKeys, runCtx.project.Name) {
			continue
		}
		compileWarnings = append(compileWarnings, fmt.Sprintf("environment source override %q was not used during compilation", key))
	}

	return artifacts, compileWarnings, nil
}

func (s *Service) selectModelsForRun(
	ctx context.Context,
	principal string,
	req domain.TriggerModelRunRequest,
	projectModels []domain.Model,
	allModels []domain.Model,
	runCtx *resolvedRunContext,
) ([]domain.Model, []string, error) {
	selector := strings.TrimSpace(req.Selector)
	if selector == "" || selector == "*" {
		return projectModels, nil, nil
	}
	if selector != "state:modified" {
		selected, err := SelectModels(selector, allModels)
		if err != nil {
			return nil, nil, err
		}
		for _, item := range selected {
			if item.ProjectName != runCtx.project.Name {
				return nil, nil, domain.ErrValidation("selector %q crossed project boundaries via %s; project-scoped runs only support models in %s", selector, item.QualifiedName(), runCtx.project.Name)
			}
		}
		return selected, nil, nil
	}

	artifacts, warnings, err := s.compileSelectedModels(ctx, principal, projectModels, allModels, runCtx, req)
	if err != nil {
		return nil, nil, fmt.Errorf("compile models for state selector: %w", err)
	}

	baseline, err := s.latestSuccessfulRunHashes(ctx, runCtx.project.Name, runCtx.stateEnvironment.Name)
	if err != nil {
		return nil, nil, err
	}

	selected := selectStateModifiedModels(projectModels, artifacts, baseline)
	if len(selected) == 0 {
		return nil, nil, domain.ErrValidation("selector state:modified matched no models")
	}

	return selected, warnings, nil
}

func (s *Service) latestSuccessfulRunHashes(ctx context.Context, projectName, environmentName string) (map[string]string, error) {
	status := domain.ModelRunStatusSuccess
	runs, _, err := s.runs.ListRuns(ctx, domain.ModelRunFilter{
		Status: &status,
		Page:   domain.PageRequest{MaxResults: domain.MaxMaxResults},
	})
	if err != nil {
		return nil, fmt.Errorf("list successful runs for state selector: %w", err)
	}
	var matched *domain.ModelRun
	for i := range runs {
		if strings.TrimSpace(runs[i].ProjectName) != strings.TrimSpace(projectName) {
			continue
		}
		if strings.TrimSpace(runs[i].EnvironmentName) != strings.TrimSpace(environmentName) {
			continue
		}
		matched = &runs[i]
		break
	}
	if matched == nil || matched.CompileManifest == nil || strings.TrimSpace(*matched.CompileManifest) == "" {
		return map[string]string{}, nil
	}

	hashes, err := modelHashByNameFromManifest(*matched.CompileManifest)
	if err != nil {
		return nil, domain.ErrValidation("invalid compile manifest in latest successful run: %v", err)
	}
	return hashes, nil
}

func selectStateModifiedModels(
	projectModels []domain.Model,
	artifacts map[string]compileResult,
	baseline map[string]string,
) []domain.Model {
	out := make([]domain.Model, 0, len(projectModels))
	for _, m := range projectModels {
		artifact, ok := artifacts[m.ID]
		if !ok {
			continue
		}
		if baseline[m.QualifiedName()] != artifact.compiledHash {
			out = append(out, m)
		}
	}
	return out
}

func modelHashByNameFromManifest(manifestJSON string) (map[string]string, error) {
	type manifestModel struct {
		ModelName    string `json:"model_name"`
		CompiledHash string `json:"compiled_hash"`
	}
	type manifest struct {
		Models []manifestModel `json:"models"`
	}

	var m manifest
	if err := json.Unmarshal([]byte(manifestJSON), &m); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(m.Models))
	for _, model := range m.Models {
		if strings.TrimSpace(model.ModelName) == "" {
			continue
		}
		out[model.ModelName] = model.CompiledHash
	}
	return out, nil
}

func (s *Service) loadCompileMacros(
	ctx context.Context,
	_ string,
	projectName string,
	runCtx *resolvedRunContext,
) (map[string]compileMacroDefinition, map[string]*starlarkMacroRuntime, []string, error) {
	known := make(map[string]compileMacroDefinition)
	runtimes := make(map[string]*starlarkMacroRuntime)
	warnings := make([]string, 0)

	layers, err := s.compileMacroLayers(ctx, projectName, runCtx)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, layer := range layers {
		for name, existing := range layer.defs {
			if prior, ok := known[name]; ok {
				warnings = append(warnings, fmt.Sprintf("macro %q from %s shadows lower-precedence macro from %s", name, existing.origin, prior.origin))
			}
			known[name] = existing
		}
		for runtimeKey, runtime := range layer.runtimes {
			runtimes[runtimeKey] = runtime
		}
	}
	return known, runtimes, dedupeSorted(warnings), nil
}

func loadStarModules(scopeDir string) (map[string]string, error) {
	moduleSources := make(map[string]string)
	if _, err := os.Stat(scopeDir); err != nil {
		if os.IsNotExist(err) {
			return moduleSources, nil
		}
		return nil, fmt.Errorf("stat star scope directory %q: %w", scopeDir, err)
	}

	err := filepath.WalkDir(scopeDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".star") {
			return nil
		}

		rel, err := filepath.Rel(scopeDir, path)
		if err != nil {
			return fmt.Errorf("relative star path for %q: %w", path, err)
		}
		module := strings.TrimSuffix(filepath.ToSlash(rel), ".star")
		module = strings.ReplaceAll(module, "/", ".")
		if module == "" {
			return domain.ErrValidation("invalid star macro module for %q", path)
		}
		content, err := os.ReadFile(path) // #nosec G304 -- path is constrained to walked scopeDir .star files
		if err != nil {
			return fmt.Errorf("read star macro file %q: %w", path, err)
		}
		moduleSources[module] = string(content)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk star scope %q: %w", scopeDir, err)
	}

	return moduleSources, nil
}

var starlarkDefRe = regexp.MustCompile(`(?m)^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(`)

func topLevelFunctionNames(moduleSource string) []string {
	matches := starlarkDefRe.FindAllStringSubmatch(moduleSource, -1)
	seen := make(map[string]struct{}, len(matches))
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		name := m[1]
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func (s *Service) loadSourceRegistry(ctx context.Context, runCtx *resolvedRunContext) (map[string]compileSourceDefinition, []string, error) {
	registry := make(map[string]compileSourceDefinition)
	warnings := make([]string, 0)
	if s.sources == nil {
		return registry, warnings, nil
	}

	projects := append([]string{runCtx.project.Name}, runCtx.dependencyProjects...)
	for _, projectName := range projects {
		sources, err := s.sources.ListByProject(ctx, projectName)
		if err != nil {
			return nil, nil, fmt.Errorf("list sources for project %s: %w", projectName, err)
		}
		for _, source := range sources {
			relationRef := strings.TrimSpace(source.RelationRef)
			if projectName == runCtx.project.Name {
				if override, ok := sourceOverrideForKey(runCtx.sourceOverrides, projectName, source.SourceName, source.TableName); ok {
					relationRef = strings.TrimSpace(override)
				}
			}
			if relationRef == "" {
				warnings = append(warnings, fmt.Sprintf("source %s.%s in project %s has no relation reference", source.SourceName, source.TableName, source.ProjectName))
				continue
			}
			key := sourceLookupKey(projectName, source.SourceName, source.TableName)
			registry[key] = compileSourceDefinition{
				key:         key,
				projectName: projectName,
				sourceName:  source.SourceName,
				tableName:   source.TableName,
				relation:    renderRelationParts(relationRef),
				relationRef: relationRef,
				freshness:   source.Freshness,
			}
		}
	}

	return registry, dedupeSorted(warnings), nil
}

func (s *Service) syncCompiledArtifacts(selected []domain.Model, artifacts map[string]compileResult, req domain.TriggerModelRunRequest) error {
	for _, m := range selected {
		artifact := artifacts[m.ID]
		if artifact.sql == m.SQL {
			continue
		}
		hash, err := computeCompiledHash(m.SQL, compileContext{
			targetCatalog: req.TargetCatalog,
			targetSchema:  effectiveSchema(req.TargetSchema, m.Config.Schema),
			vars:          req.Variables,
			fullRefresh:   req.FullRefresh,
			projectName:   m.ProjectName,
			modelName:     m.Name,
			materialize:   m.Materialization,
		})
		if err != nil {
			return fmt.Errorf("hash compiled SQL for %s: %w", m.QualifiedName(), err)
		}
		artifact.sql = m.SQL
		artifact.compiledHash = hash
		artifacts[m.ID] = artifact
	}
	return nil
}

func strPtrOrNil(v string) *string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return &v
}

func buildCompileManifest(
	selected []domain.Model,
	artifacts map[string]compileResult,
	runCtx *resolvedRunContext,
	coverageByModel map[string]string,
) (string, error) {
	type manifestModel struct {
		ModelName       string             `json:"model_name"`
		CompiledHash    string             `json:"compiled_hash"`
		DependsOn       []string           `json:"depends_on,omitempty"`
		VarsUsed        []string           `json:"vars_used,omitempty"`
		MacrosUsed      []string           `json:"macros_used,omitempty"`
		ResolvedSources map[string]string  `json:"resolved_sources,omitempty"`
		EffectiveConfig domain.ModelConfig `json:"effective_config,omitempty"`
		LineageCoverage string             `json:"lineage_coverage,omitempty"`
	}
	type manifest struct {
		Version             int             `json:"version"`
		ProjectName         string          `json:"project_name,omitempty"`
		EnvironmentName     string          `json:"environment_name,omitempty"`
		TargetCatalog       string          `json:"target_catalog,omitempty"`
		TargetSchema        string          `json:"target_schema,omitempty"`
		ProjectDependencies []string        `json:"project_dependencies,omitempty"`
		Models              []manifestModel `json:"models"`
	}

	models := make([]manifestModel, 0, len(selected))
	for _, m := range selected {
		artifact, ok := artifacts[m.ID]
		if !ok {
			continue
		}
		models = append(models, manifestModel{
			ModelName:       m.QualifiedName(),
			CompiledHash:    artifact.compiledHash,
			DependsOn:       artifact.dependsOn,
			VarsUsed:        artifact.varsUsed,
			MacrosUsed:      artifact.macrosUsed,
			ResolvedSources: artifact.sourcesUsed,
			EffectiveConfig: m.Config,
			LineageCoverage: coverageByModel[m.QualifiedName()],
		})
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ModelName < models[j].ModelName })

	payload := manifest{
		Version: 2,
		Models:  models,
	}
	if runCtx != nil {
		payload.ProjectDependencies = append([]string(nil), runCtx.dependencyProjects...)
		payload.ProjectName = runCtx.project.Name
		payload.EnvironmentName = runCtx.environment.Name
		payload.TargetCatalog = runCtx.targetCatalog
		payload.TargetSchema = runCtx.targetSchema
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func buildCompileDiagnostics(warnings, errors []string, items []domain.CompileDiagnostic) (string, error) {
	diagnostics := domain.ModelCompileDiagnostics{
		Items:    dedupeDiagnostics(items),
		Warnings: dedupeSorted(warnings),
		Errors:   dedupeSorted(errors),
	}
	b, err := json.Marshal(diagnostics)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func diagnosticsFromJSONOrNil(raw string) *domain.ModelCompileDiagnostics {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out domain.ModelCompileDiagnostics
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil
	}
	return &out
}

func dedupeDiagnostics(items []domain.CompileDiagnostic) []domain.CompileDiagnostic {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	out := make([]domain.CompileDiagnostic, 0, len(items))
	for _, item := range items {
		key := strings.Join([]string{
			string(item.Severity),
			item.Code,
			item.Message,
			item.ModelName,
			item.ColumnName,
			strings.Join(item.RelatedObjects, ","),
		}, "|")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Severity != out[j].Severity {
			return out[i].Severity < out[j].Severity
		}
		if out[i].Code != out[j].Code {
			return out[i].Code < out[j].Code
		}
		if out[i].ModelName != out[j].ModelName {
			return out[i].ModelName < out[j].ModelName
		}
		if out[i].ColumnName != out[j].ColumnName {
			return out[i].ColumnName < out[j].ColumnName
		}
		return out[i].Message < out[j].Message
	})
	return out
}

func marshalStateSnapshot(snapshot *domain.BuildStateSnapshot) (string, error) {
	if snapshot == nil {
		return "", nil
	}
	b, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (s *Service) persistCompileDependencyLineage(
	ctx context.Context,
	selected []domain.Model,
	artifacts map[string]compileResult,
	req domain.TriggerModelRunRequest,
	principal string,
) error {
	if s.lineage == nil {
		return nil
	}

	for _, m := range selected {
		artifact, ok := artifacts[m.ID]
		if !ok {
			continue
		}
		targetSchema := effectiveSchema(req.TargetSchema, m.Config.Schema)
		targetTable := m.Name
		targetName := makeLineageTableName(req.TargetCatalog, targetSchema, targetTable)

		for _, dep := range artifact.dependsOn {
			if strings.HasPrefix(dep, "source:") {
				sourceKey := strings.TrimPrefix(dep, "source:")
				if relationRef, ok := artifact.sourcesUsed[sourceKey]; ok {
					sourceCatalog, sourceSchema, sourceTable := parseRelationRef(relationRef, req.TargetCatalog, req.TargetSchema)
					sourceName := makeLineageTableName(sourceCatalog, sourceSchema, sourceTable)
					edge := &domain.LineageEdge{
						SourceTable:   sourceName,
						TargetTable:   strPtr(targetName),
						SourceSchema:  sourceSchema,
						TargetSchema:  targetSchema,
						EdgeType:      "READ",
						PrincipalName: principal,
						QueryHash:     strPtrOrNil(artifact.compiledHash),
					}
					if err := s.lineage.InsertEdge(ctx, edge); err != nil {
						return fmt.Errorf("insert lineage edge %s -> %s: %w", sourceName, targetName, err)
					}
					continue
				}
			}
			sourceSchema, sourceTable := depToLineageSource(dep, req.TargetSchema)
			if sourceTable == "" {
				continue
			}
			sourceName := makeLineageTableName(req.TargetCatalog, sourceSchema, sourceTable)
			edge := &domain.LineageEdge{
				SourceTable:   sourceName,
				TargetTable:   strPtr(targetName),
				SourceSchema:  sourceSchema,
				TargetSchema:  targetSchema,
				EdgeType:      "READ",
				PrincipalName: principal,
				QueryHash:     strPtrOrNil(artifact.compiledHash),
			}
			if err := s.lineage.InsertEdge(ctx, edge); err != nil {
				return fmt.Errorf("insert lineage edge %s -> %s: %w", sourceName, targetName, err)
			}
		}

		for _, macroName := range artifact.macrosUsed {
			edge := &domain.LineageEdge{
				SourceTable:   "macro." + macroName,
				TargetTable:   strPtr(targetName),
				SourceSchema:  "macro",
				TargetSchema:  targetSchema,
				EdgeType:      "MACRO",
				PrincipalName: principal,
				QueryHash:     strPtrOrNil(artifact.compiledHash),
			}
			if err := s.lineage.InsertEdge(ctx, edge); err != nil {
				return fmt.Errorf("insert macro lineage edge %s -> %s: %w", macroName, targetName, err)
			}
		}
	}

	return nil
}

func (s *Service) createRunBuild(
	ctx context.Context,
	principal string,
	project *domain.Project,
	environment *domain.Environment,
	runID string,
	req domain.TriggerModelRunRequest,
	manifestJSON string,
	diagnosticsJSON string,
	stateSnapshotJSON string,
) (*domain.Build, error) {
	if s.builds == nil {
		return nil, nil
	}
	build := &domain.Build{
		ProjectID:          project.ID,
		ProductID:          project.ProductID,
		EnvironmentID:      environment.ID,
		State:              domain.BuildStateReady,
		GitRef:             "refs/heads/" + project.DefaultBranch,
		Selector:           req.Selector,
		TargetCatalog:      environment.TargetCatalog,
		TargetSchema:       environment.TargetSchema,
		SourceModelRunID:   &runID,
		CompileManifest:    manifestJSON,
		CompileDiagnostics: strPtrOrNil(diagnosticsJSON),
		StateSnapshot:      strPtrOrNil(stateSnapshotJSON),
		CreatedBy:          principal,
	}
	created, err := s.builds.Create(ctx, build)
	if err != nil {
		return nil, fmt.Errorf("create build for model run %s: %w", runID, err)
	}
	if s.runs != nil {
		if err := s.runs.UpdateRunBuild(ctx, runID, created.ID); err != nil {
			return nil, fmt.Errorf("link model run %s to build %s: %w", runID, created.ID, err)
		}
	}
	return created, nil
}

func (s *Service) persistCompiledColumnLineage(ctx context.Context, buildID string, items []domain.CompiledColumnLineage) error {
	if s.colLineage == nil {
		return nil
	}
	for i := range items {
		items[i].BuildID = buildID
	}
	return s.colLineage.ReplaceBuildLineage(ctx, buildID, items)
}

func (s *Service) loadProjectModels(ctx context.Context, projectName string) ([]domain.Model, error) {
	projectFilter := strings.TrimSpace(projectName)
	models, _, err := s.models.List(ctx, &projectFilter, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return nil, fmt.Errorf("load models for project %s: %w", projectFilter, err)
	}
	if len(models) == 0 {
		return nil, domain.ErrValidation("no models defined in project %s", projectFilter)
	}
	return models, nil
}

func selectDefaultDevelopmentEnvironment(environments []domain.Environment) (*domain.Environment, error) {
	var namedDev *domain.Environment
	devEnvironments := make([]domain.Environment, 0)
	for i := range environments {
		if environments[i].Kind != domain.EnvironmentKindDevelopment {
			continue
		}
		devEnvironments = append(devEnvironments, environments[i])
		if environments[i].Name == "dev" {
			env := environments[i]
			namedDev = &env
		}
	}
	if namedDev != nil {
		return namedDev, nil
	}
	if len(devEnvironments) == 1 {
		return &devEnvironments[0], nil
	}
	if len(devEnvironments) == 0 {
		return nil, domain.ErrValidation("project has no development environment")
	}
	return nil, domain.ErrValidation("project has multiple development environments; environment_name is required")
}

func depToLineageSource(dep, defaultSchema string) (schema, table string) {
	if strings.HasPrefix(dep, "source:") {
		dep = strings.TrimPrefix(dep, "source:")
		parts := strings.Split(dep, ".")
		switch len(parts) {
		case 1:
			return defaultSchema, parts[0]
		default:
			return parts[len(parts)-2], parts[len(parts)-1]
		}
	}

	parts := strings.Split(dep, ".")
	if len(parts) == 1 {
		return defaultSchema, parts[0]
	}
	return defaultSchema, parts[len(parts)-1]
}

func makeLineageTableName(catalog, schema, table string) string {
	if strings.TrimSpace(catalog) == "" {
		return schema + "." + table
	}
	return catalog + "." + schema + "." + table
}
