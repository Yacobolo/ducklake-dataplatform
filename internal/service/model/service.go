package model

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"duck-demo/internal/domain"
)

// Service provides business logic for model management.
type Service struct {
	models      domain.ModelRepository
	runs        domain.ModelRunRepository
	tests       domain.ModelTestRepository
	testResults domain.ModelTestResultRepository
	audit       domain.AuditRepository
	lineage     domain.LineageRepository
	colLineage  domain.ColumnLineageRepository
	macros      domain.MacroRepository
	notebooks   domain.NotebookProvider
	engine      domain.SessionEngine
	duckDB      *sql.DB
	logger      *slog.Logger
	runCancels  sync.Map
}

// NewService creates a new model Service.
func NewService(
	models domain.ModelRepository,
	runs domain.ModelRunRepository,
	tests domain.ModelTestRepository,
	testResults domain.ModelTestResultRepository,
	audit domain.AuditRepository,
	lineage domain.LineageRepository,
	colLineage domain.ColumnLineageRepository,
	engine domain.SessionEngine,
	duckDB *sql.DB,
	logger *slog.Logger,
) *Service {
	return &Service{
		models:      models,
		runs:        runs,
		tests:       tests,
		testResults: testResults,
		audit:       audit,
		lineage:     lineage,
		colLineage:  colLineage,
		engine:      engine,
		duckDB:      duckDB,
		logger:      logger,
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

	// Load models
	allModels, err := s.models.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("load models: %w", err)
	}
	if len(allModels) == 0 {
		return nil, domain.ErrValidation("no models defined")
	}

	selected := allModels
	if req.Selector != "" {
		selected, err = SelectModels(req.Selector, allModels)
		if err != nil {
			return nil, err
		}
	}

	compiledArtifacts, err := s.compileSelectedModels(selected, allModels, req)
	if err != nil {
		return nil, err
	}

	// Resolve ephemeral models: inject CTEs and remove from execution set.
	// This can alter SQL for downstream models, so keep artifacts in sync.
	selected = resolveEphemeralModels(selected)
	if err := s.syncCompiledArtifacts(selected, compiledArtifacts, req); err != nil {
		return nil, err
	}

	// Resolve DAG
	tiers, err := ResolveDAG(selected)
	if err != nil {
		return nil, fmt.Errorf("resolve DAG: %w", err)
	}

	// Create run
	run := &domain.ModelRun{
		Status:        domain.ModelRunStatusPending,
		TriggerType:   req.TriggerType,
		TriggeredBy:   principal,
		TargetCatalog: req.TargetCatalog,
		TargetSchema:  req.TargetSchema,
		ModelSelector: req.Selector,
		Variables:     req.Variables,
		FullRefresh:   req.FullRefresh,
	}
	run, err = s.runs.CreateRun(ctx, run)
	if err != nil {
		return nil, fmt.Errorf("create run: %w", err)
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
	runCtx, cancel := context.WithCancel(context.Background())
	s.runCancels.Store(run.ID, cancel)
	config := ExecutionConfig{
		TargetCatalog: req.TargetCatalog,
		TargetSchema:  req.TargetSchema,
		Variables:     req.Variables,
		FullRefresh:   req.FullRefresh,
	}
	go s.executeRun(runCtx, run.ID, selected, tiers, config, principal)

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

	// Load models
	allModels, err := s.models.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("load models: %w", err)
	}
	if len(allModels) == 0 {
		return domain.ErrValidation("no models defined")
	}

	selected := allModels
	if req.Selector != "" {
		selected, err = SelectModels(req.Selector, allModels)
		if err != nil {
			return err
		}
	}

	compiledArtifacts, err := s.compileSelectedModels(selected, allModels, req)
	if err != nil {
		return err
	}

	selected = resolveEphemeralModels(selected)
	if err := s.syncCompiledArtifacts(selected, compiledArtifacts, req); err != nil {
		return err
	}

	tiers, err := ResolveDAG(selected)
	if err != nil {
		return fmt.Errorf("resolve DAG: %w", err)
	}

	// Create run
	run := &domain.ModelRun{
		Status:        domain.ModelRunStatusPending,
		TriggerType:   req.TriggerType,
		TriggeredBy:   principal,
		TargetCatalog: req.TargetCatalog,
		TargetSchema:  req.TargetSchema,
		ModelSelector: req.Selector,
		Variables:     req.Variables,
		FullRefresh:   req.FullRefresh,
	}
	run, err = s.runs.CreateRun(ctx, run)
	if err != nil {
		return fmt.Errorf("create run: %w", err)
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

// SetTestRepos sets the test and test result repositories on the service.
// This allows injecting these optional dependencies after construction.
func (s *Service) SetTestRepos(tests domain.ModelTestRepository, testResults domain.ModelTestResultRepository) {
	s.tests = tests
	s.testResults = testResults
}

// SetMacroRepo sets the macro repository for loading macros during model runs.
func (s *Service) SetMacroRepo(macros domain.MacroRepository) {
	s.macros = macros
}

// SetNotebookProvider sets the notebook provider for notebook-to-model promotion.
func (s *Service) SetNotebookProvider(notebooks domain.NotebookProvider) {
	s.notebooks = notebooks
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
func (s *Service) ListTestResults(ctx context.Context, _, stepID string) ([]domain.ModelTestResult, error) {
	return s.testResults.ListByStep(ctx, stepID)
}

// PromoteNotebook promotes a notebook cell to a transformation model.
func (s *Service) PromoteNotebook(ctx context.Context, principal string, req domain.PromoteNotebookRequest) (*domain.Model, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	if s.notebooks == nil {
		return nil, domain.ErrValidation("notebook provider not configured")
	}

	// Get SQL blocks from the notebook.
	blocks, err := s.notebooks.GetSQLBlocks(ctx, req.NotebookID)
	if err != nil {
		return nil, fmt.Errorf("get notebook SQL: %w", err)
	}

	if req.CellIndex >= len(blocks) {
		return nil, domain.ErrValidation("cell_index %d out of range (notebook has %d SQL cells)", req.CellIndex, len(blocks))
	}

	sqlBody := blocks[req.CellIndex]
	if sqlBody == "" {
		return nil, domain.ErrValidation("selected cell has empty SQL")
	}

	// Create the model with the extracted SQL.
	return s.CreateModel(ctx, principal, domain.CreateModelRequest{
		ProjectName:     req.ProjectName,
		Name:            req.Name,
		SQL:             sqlBody,
		Materialization: req.Materialization,
	})
}

func (s *Service) logAudit(ctx context.Context, principal, action, _ string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
	})
}

func (s *Service) compileSelectedModels(selected []domain.Model, allModels []domain.Model, req domain.TriggerModelRunRequest) (map[string]compileResult, error) {
	byQualified := make(map[string]domain.Model, len(allModels))
	byName := make(map[string][]domain.Model)
	for _, m := range allModels {
		byQualified[m.QualifiedName()] = m
		byName[m.Name] = append(byName[m.Name], m)
	}

	artifacts := make(map[string]compileResult, len(selected))
	for i := range selected {
		m := selected[i]
		ctx := compileContext{
			targetCatalog: req.TargetCatalog,
			targetSchema:  req.TargetSchema,
			vars:          req.Variables,
			fullRefresh:   req.FullRefresh,
			projectName:   m.ProjectName,
			modelName:     m.Name,
			materialize:   m.Materialization,
			models:        byQualified,
			byName:        byName,
		}
		compiled, err := compileModelSQL(m.SQL, ctx)
		if err != nil {
			return nil, fmt.Errorf("compile model %s: %w", m.QualifiedName(), err)
		}

		selected[i].SQL = compiled.sql
		selected[i].DependsOn = compiled.dependsOn
		artifacts[m.ID] = *compiled
	}

	return artifacts, nil
}

func (s *Service) syncCompiledArtifacts(selected []domain.Model, artifacts map[string]compileResult, req domain.TriggerModelRunRequest) error {
	for _, m := range selected {
		artifact := artifacts[m.ID]
		if artifact.sql == m.SQL {
			continue
		}
		hash, err := computeCompiledHash(m.SQL, compileContext{
			targetCatalog: req.TargetCatalog,
			targetSchema:  req.TargetSchema,
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
