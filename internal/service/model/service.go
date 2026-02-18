package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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
		selected, err = s.selectModelsForRun(ctx, principal, req, allModels)
		if err != nil {
			return nil, err
		}
	}

	compiledArtifacts, compileWarnings, err := s.compileSelectedModels(ctx, principal, selected, allModels, req)
	if err != nil {
		return nil, err
	}
	if err := s.persistCompileDependencyLineage(ctx, selected, compiledArtifacts, req, principal); err != nil {
		return nil, fmt.Errorf("persist compile dependency lineage: %w", err)
	}

	// Resolve ephemeral models: inject CTEs and remove from execution set.
	// This can alter SQL for downstream models, so keep artifacts in sync.
	selected = resolveEphemeralModels(selected)
	if err := s.syncCompiledArtifacts(selected, compiledArtifacts, req); err != nil {
		return nil, err
	}
	manifestJSON, err := buildCompileManifest(selected, compiledArtifacts)
	if err != nil {
		return nil, fmt.Errorf("build compile manifest: %w", err)
	}
	diagnosticsJSON, err := buildCompileDiagnostics(compileWarnings, nil)
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
		selected, err = s.selectModelsForRun(ctx, principal, req, allModels)
		if err != nil {
			return err
		}
	}

	compiledArtifacts, compileWarnings, err := s.compileSelectedModels(ctx, principal, selected, allModels, req)
	if err != nil {
		return err
	}
	if err := s.persistCompileDependencyLineage(ctx, selected, compiledArtifacts, req, principal); err != nil {
		return fmt.Errorf("persist compile dependency lineage: %w", err)
	}

	selected = resolveEphemeralModels(selected)
	if err := s.syncCompiledArtifacts(selected, compiledArtifacts, req); err != nil {
		return err
	}
	manifestJSON, err := buildCompileManifest(selected, compiledArtifacts)
	if err != nil {
		return fmt.Errorf("build compile manifest: %w", err)
	}
	diagnosticsJSON, err := buildCompileDiagnostics(compileWarnings, nil)
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

func (s *Service) compileSelectedModels(
	ctx context.Context,
	principal string,
	selected []domain.Model,
	allModels []domain.Model,
	req domain.TriggerModelRunRequest,
) (map[string]compileResult, []string, error) {
	byQualified := make(map[string]domain.Model, len(allModels))
	byName := make(map[string][]domain.Model)
	for _, m := range allModels {
		byQualified[m.QualifiedName()] = m
		byName[m.Name] = append(byName[m.Name], m)
	}

	sources, err := s.loadSourceRegistry(ctx, principal)
	if err != nil {
		return nil, nil, fmt.Errorf("load source registry: %w", err)
	}
	compileWarnings := make([]string, 0)

	type macroBundle struct {
		defs     map[string]compileMacroDefinition
		runtimes map[string]*starlarkMacroRuntime
	}
	bundleByProject := make(map[string]macroBundle)

	artifacts := make(map[string]compileResult, len(selected))
	for i := range selected {
		m := selected[i]
		bundle, ok := bundleByProject[m.ProjectName]
		if !ok {
			defs, runtimes, err := s.loadCompileMacros(ctx, principal, m.ProjectName)
			if err != nil {
				return nil, nil, fmt.Errorf("load compile macros for project %s: %w", m.ProjectName, err)
			}
			bundle = macroBundle{defs: defs, runtimes: runtimes}
			bundleByProject[m.ProjectName] = bundle
		}

		ctx := compileContext{
			targetCatalog: req.TargetCatalog,
			targetSchema:  req.TargetSchema,
			vars:          req.Variables,
			fullRefresh:   req.FullRefresh,
			strictSources: true,
			projectName:   m.ProjectName,
			modelName:     m.Name,
			materialize:   m.Materialization,
			models:        byQualified,
			byName:        byName,
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
		artifacts[m.ID] = *compiled
	}

	return artifacts, compileWarnings, nil
}

func (s *Service) selectModelsForRun(
	ctx context.Context,
	principal string,
	req domain.TriggerModelRunRequest,
	allModels []domain.Model,
) ([]domain.Model, error) {
	selector := strings.TrimSpace(req.Selector)
	if selector != "state:modified" {
		return SelectModels(selector, allModels)
	}

	artifacts, _, err := s.compileSelectedModels(ctx, principal, allModels, allModels, req)
	if err != nil {
		return nil, fmt.Errorf("compile models for state selector: %w", err)
	}

	baseline, err := s.latestSuccessfulRunHashes(ctx)
	if err != nil {
		return nil, err
	}

	selected := selectStateModifiedModels(allModels, artifacts, baseline)
	if len(selected) == 0 {
		return nil, domain.ErrValidation("selector state:modified matched no models")
	}

	return selected, nil
}

func (s *Service) latestSuccessfulRunHashes(ctx context.Context) (map[string]string, error) {
	status := domain.ModelRunStatusSuccess
	runs, _, err := s.runs.ListRuns(ctx, domain.ModelRunFilter{
		Status: &status,
		Page:   domain.PageRequest{MaxResults: 1},
	})
	if err != nil {
		return nil, fmt.Errorf("list successful runs for state selector: %w", err)
	}
	if len(runs) == 0 || runs[0].CompileManifest == nil || strings.TrimSpace(*runs[0].CompileManifest) == "" {
		return map[string]string{}, nil
	}

	hashes, err := modelHashByNameFromManifest(*runs[0].CompileManifest)
	if err != nil {
		return nil, domain.ErrValidation("invalid compile manifest in latest successful run: %v", err)
	}
	return hashes, nil
}

func selectStateModifiedModels(
	allModels []domain.Model,
	artifacts map[string]compileResult,
	baseline map[string]string,
) []domain.Model {
	out := make([]domain.Model, 0, len(allModels))
	for _, m := range allModels {
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
) (map[string]compileMacroDefinition, map[string]*starlarkMacroRuntime, error) {
	known := make(map[string]compileMacroDefinition)
	runtimes := make(map[string]*starlarkMacroRuntime)
	if s.macros == nil {
		return known, runtimes, nil
	}

	macros, err := s.macros.ListAll(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list macros: %w", err)
	}
	dbDefs := make(map[string]compileMacroDefinition)
	for _, m := range macros {
		def := compileMacroDefinition{
			name:       m.Name,
			parameters: m.Parameters,
			body:       m.Body,
			starlark:   strings.Contains(m.Name, "."),
			runtimeKey: "db",
		}
		dbDefs[m.Name] = def
		known[m.Name] = def
	}

	dbRuntime, err := newStarlarkMacroRuntime(dbDefs)
	if err != nil {
		return nil, nil, err
	}
	runtimes["db"] = dbRuntime

	starDefs, starRuntimes, err := s.loadStarMacroScopes(projectName)
	if err != nil {
		return nil, nil, err
	}
	for runtimeKey, rt := range starRuntimes {
		runtimes[runtimeKey] = rt
	}
	for name, def := range starDefs {
		known[name] = def
	}

	return known, runtimes, nil
}

func (s *Service) loadStarMacroScopes(projectName string) (map[string]compileMacroDefinition, map[string]*starlarkMacroRuntime, error) {
	root, err := os.Getwd()
	if err != nil {
		return nil, nil, fmt.Errorf("resolve working directory for star macros: %w", err)
	}
	macrosDir := filepath.Join(root, "macros")
	if _, err := os.Stat(macrosDir); err != nil {
		if os.IsNotExist(err) {
			return map[string]compileMacroDefinition{}, map[string]*starlarkMacroRuntime{}, nil
		}
		return nil, nil, fmt.Errorf("stat macros directory: %w", err)
	}

	defs := make(map[string]compileMacroDefinition)
	runtimes := make(map[string]*starlarkMacroRuntime)
	scopes := []struct {
		key       string
		dir       string
		isProject bool
	}{
		{key: "system", dir: filepath.Join(macrosDir, "system")},
		{key: "catalog_global", dir: filepath.Join(macrosDir, "catalog_global")},
		{key: "project", dir: filepath.Join(macrosDir, projectName), isProject: true},
	}

	for _, scope := range scopes {
		moduleSources, err := loadStarModules(scope.dir)
		if err != nil {
			return nil, nil, err
		}
		if len(moduleSources) == 0 {
			continue
		}

		runtime, err := newStarlarkMacroRuntimeFromModules(moduleSources)
		if err != nil {
			return nil, nil, fmt.Errorf("load %s star runtime: %w", scope.key, err)
		}
		runtimes[scope.key] = runtime

		moduleNames := make([]string, 0, len(moduleSources))
		for module := range moduleSources {
			moduleNames = append(moduleNames, module)
		}
		sort.Strings(moduleNames)

		for _, module := range moduleNames {
			fnNames := topLevelFunctionNames(moduleSources[module])
			for _, fn := range fnNames {
				name := module + "." + fn
				defs[name] = compileMacroDefinition{
					name:       name,
					starlark:   true,
					runtimeKey: scope.key,
				}
			}
		}
	}

	return defs, runtimes, nil
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

func (s *Service) loadSourceRegistry(ctx context.Context, principal string) (map[string]string, error) {
	registry := make(map[string]string)
	conn, err := s.duckDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection for source registry: %w", err)
	}
	defer func() { _ = conn.Close() }()

	rows, err := s.engine.QueryOnConn(ctx, conn, principal, "SELECT table_schema, table_name FROM information_schema.tables")
	if err != nil {
		return nil, fmt.Errorf("query source registry: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var schemaName string
		var tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("scan source registry row: %w", err)
		}
		registry[schemaName+"."+tableName] = renderRelationParts(schemaName, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source registry rows: %w", err)
	}

	return registry, nil
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

func buildCompileManifest(selected []domain.Model, artifacts map[string]compileResult) (string, error) {
	type manifestModel struct {
		ModelName    string   `json:"model_name"`
		CompiledHash string   `json:"compiled_hash"`
		DependsOn    []string `json:"depends_on,omitempty"`
		VarsUsed     []string `json:"vars_used,omitempty"`
		MacrosUsed   []string `json:"macros_used,omitempty"`
	}
	type manifest struct {
		Version int             `json:"version"`
		Models  []manifestModel `json:"models"`
	}

	models := make([]manifestModel, 0, len(selected))
	for _, m := range selected {
		artifact, ok := artifacts[m.ID]
		if !ok {
			continue
		}
		models = append(models, manifestModel{
			ModelName:    m.QualifiedName(),
			CompiledHash: artifact.compiledHash,
			DependsOn:    artifact.dependsOn,
			VarsUsed:     artifact.varsUsed,
			MacrosUsed:   artifact.macrosUsed,
		})
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ModelName < models[j].ModelName })

	b, err := json.Marshal(manifest{Version: 1, Models: models})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func buildCompileDiagnostics(warnings, errors []string) (string, error) {
	diagnostics := domain.ModelCompileDiagnostics{
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
		targetSchema := req.TargetSchema
		targetTable := m.Name
		targetName := makeLineageTableName(req.TargetCatalog, targetSchema, targetTable)

		for _, dep := range artifact.dependsOn {
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
