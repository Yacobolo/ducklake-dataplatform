package model

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
)

// GetCompilation loads a persisted compilation artifact by id.
func (s *Service) GetCompilation(ctx context.Context, compilationID string) (*domain.Compilation, error) {
	if s.compilations == nil {
		return nil, domain.ErrNotImplemented("compilations are not configured")
	}
	return s.compilations.GetByID(ctx, strings.TrimSpace(compilationID))
}

// ListCompilationsForEnvironment lists persisted compilations for one project environment.
func (s *Service) ListCompilationsForEnvironment(ctx context.Context, projectName, environmentName string, page domain.PageRequest) ([]domain.Compilation, int64, error) {
	if s.compilations == nil {
		return nil, 0, domain.ErrNotImplemented("compilations are not configured")
	}
	project, err := s.projects.GetByName(ctx, strings.TrimSpace(projectName))
	if err != nil {
		return nil, 0, err
	}
	environment, err := s.environments.GetByName(ctx, project.ID, strings.TrimSpace(environmentName))
	if err != nil {
		return nil, 0, err
	}
	return s.compilations.ListByEnvironment(ctx, project.ID, environment.ID, page)
}

// CreateCompilation creates an immutable compilation artifact for preview and analysis.
func (s *Service) CreateCompilation(
	ctx context.Context,
	principal string,
	projectName string,
	environmentName string,
	req domain.CreateCompilationRequest,
) (*domain.Compilation, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.compilations == nil {
		return nil, domain.ErrNotImplemented("compilations are not configured")
	}
	selected, artifacts, analysisResult, runCtx, err := s.prepareBuildAnalysis(ctx, principal, projectName, environmentName, strings.TrimSpace(req.Selector))
	if err != nil {
		return nil, err
	}
	manifestJSON, err := buildCompileManifest(selected, artifacts, runCtx, analysisResult.coverageByModel)
	if err != nil {
		return nil, fmt.Errorf("build compile manifest: %w", err)
	}
	diagnosticsJSON, err := buildCompileDiagnostics(nil, nil, analysisResult.diagnostics)
	if err != nil {
		return nil, fmt.Errorf("build compile diagnostics: %w", err)
	}
	stateSnapshotJSON, err := marshalStateSnapshot(analysisResult.stateSnapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal build state snapshot: %w", err)
	}
	created, err := s.compilations.Create(ctx, &domain.Compilation{
		ProjectID:          runCtx.project.ID,
		EnvironmentID:      runCtx.environment.ID,
		GitRef:             strings.TrimSpace(req.GitRef),
		CommitSHA:          req.CommitSHA,
		Selector:           strings.TrimSpace(req.Selector),
		TargetCatalog:      runCtx.targetCatalog,
		TargetSchema:       runCtx.targetSchema,
		CompileManifest:    manifestJSON,
		CompileDiagnostics: strPtrOrNil(diagnosticsJSON),
		StateSnapshot:      strPtrOrNil(stateSnapshotJSON),
		CreatedBy:          principal,
	})
	if err != nil {
		return nil, err
	}
	if err := s.persistCompiledCompilationLineage(ctx, created.ID, analysisResult.lineage); err != nil {
		return nil, fmt.Errorf("persist compiled compilation lineage: %w", err)
	}
	s.logAudit(ctx, principal, "create_model_compilation", created.ID)
	return created, nil
}

// CreateEnvironmentBuild compiles the current project state into an immutable build artifact.
func (s *Service) CreateEnvironmentBuild(
	ctx context.Context,
	principal string,
	projectName string,
	environmentName string,
	req domain.CreateCompilationRequest,
) (*domain.Build, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if s.builds == nil {
		return nil, domain.ErrNotImplemented("builds are not configured")
	}
	selected, artifacts, analysisResult, runCtx, err := s.prepareBuildAnalysis(ctx, principal, projectName, environmentName, strings.TrimSpace(req.Selector))
	if err != nil {
		return nil, err
	}
	manifestJSON, err := buildCompileManifest(selected, artifacts, runCtx, analysisResult.coverageByModel)
	if err != nil {
		return nil, fmt.Errorf("build compile manifest: %w", err)
	}
	diagnosticsJSON, err := buildCompileDiagnostics(nil, nil, analysisResult.diagnostics)
	if err != nil {
		return nil, fmt.Errorf("build compile diagnostics: %w", err)
	}
	stateSnapshotJSON, err := marshalStateSnapshot(analysisResult.stateSnapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal build state snapshot: %w", err)
	}
	build, err := s.builds.Create(ctx, &domain.Build{
		ProjectID:          runCtx.project.ID,
		ProductID:          runCtx.project.ProductID,
		EnvironmentID:      runCtx.environment.ID,
		State:              domain.BuildStateReady,
		GitRef:             strings.TrimSpace(req.GitRef),
		CommitSHA:          req.CommitSHA,
		Selector:           strings.TrimSpace(req.Selector),
		TargetCatalog:      runCtx.targetCatalog,
		TargetSchema:       runCtx.targetSchema,
		CompileManifest:    manifestJSON,
		CompileDiagnostics: strPtrOrNil(diagnosticsJSON),
		StateSnapshot:      strPtrOrNil(stateSnapshotJSON),
		CreatedBy:          principal,
	})
	if err != nil {
		return nil, err
	}
	if err := s.persistCompiledColumnLineage(ctx, build.ID, analysisResult.lineage); err != nil {
		return nil, fmt.Errorf("persist compiled column lineage: %w", err)
	}
	s.logAudit(ctx, principal, "create_model_build", build.ID)
	return build, nil
}

// ListRunsForBuild lists model runs linked to a build.
func (s *Service) ListRunsForBuild(ctx context.Context, buildID string, page domain.PageRequest) ([]domain.ModelRun, int64, error) {
	runs, _, err := s.runs.ListRuns(ctx, domain.ModelRunFilter{Page: domain.PageRequest{MaxResults: domain.MaxMaxResults}})
	if err != nil {
		return nil, 0, err
	}
	filtered := make([]domain.ModelRun, 0)
	for i := range runs {
		if runs[i].BuildID != nil && strings.TrimSpace(*runs[i].BuildID) == strings.TrimSpace(buildID) {
			filtered = append(filtered, runs[i])
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
	})
	total := int64(len(filtered))
	start := page.Offset()
	if start > len(filtered) {
		start = len(filtered)
	}
	end := start + page.Limit()
	if end > len(filtered) {
		end = len(filtered)
	}
	return append([]domain.ModelRun(nil), filtered[start:end]...), total, nil
}

// CreateRunForBuild executes a fresh run using a build's configuration and links the run to that build.
func (s *Service) CreateRunForBuild(ctx context.Context, principal string, build *domain.Build) (*domain.ModelRun, error) {
	if build == nil {
		return nil, domain.ErrValidation("build is required")
	}
	req := domain.TriggerModelRunRequest{
		ProjectName:     build.ProjectName,
		EnvironmentName: build.EnvironmentName,
		Selector:        build.Selector,
		TriggerType:     domain.ModelTriggerTypeManual,
		Variables:       map[string]string{},
		TargetCatalog:   build.TargetCatalog,
		TargetSchema:    build.TargetSchema,
	}
	runCtx, err := s.resolveExecutionContext(ctx, build.ProjectName, build.EnvironmentName, req)
	if err != nil {
		return nil, err
	}
	req.TargetCatalog = runCtx.targetCatalog
	req.TargetSchema = runCtx.targetSchema
	req.Variables = cloneStringMap(runCtx.variables)
	allModels, scopeWarnings, err := s.loadCompilationModelScope(ctx, runCtx)
	if err != nil {
		return nil, err
	}
	selected := filterByProject(allModels, runCtx.project.Name)
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
	selected = resolveEphemeralModels(selected)
	if err := s.syncCompiledArtifacts(selected, compiledArtifacts, req); err != nil {
		return nil, err
	}
	analysisResult, err := s.analyzeCompiledModels(ctx, principal, selected, compiledArtifacts, runCtx, req)
	if err != nil {
		return nil, err
	}
	manifestJSON, err := buildCompileManifest(selected, compiledArtifacts, runCtx, analysisResult.coverageByModel)
	if err != nil {
		return nil, err
	}
	diagnosticsJSON, err := buildCompileDiagnostics(compileWarnings, nil, analysisResult.diagnostics)
	if err != nil {
		return nil, err
	}
	tiers, err := ResolveDAG(selected)
	if err != nil {
		return nil, err
	}
	run := &domain.ModelRun{
		Status:             domain.ModelRunStatusPending,
		TriggerType:        req.TriggerType,
		TriggeredBy:        principal,
		ProjectName:        runCtx.project.Name,
		EnvironmentName:    runCtx.environment.Name,
		BuildID:            &build.ID,
		TargetCatalog:      req.TargetCatalog,
		TargetSchema:       req.TargetSchema,
		ModelSelector:      req.Selector,
		Variables:          req.Variables,
		CompileManifest:    strPtrOrNil(manifestJSON),
		CompileDiagnostics: diagnosticsFromJSONOrNil(diagnosticsJSON),
	}
	run, err = s.runs.CreateRun(ctx, run)
	if err != nil {
		return nil, fmt.Errorf("create run: %w", err)
	}
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
	execCtx, cancel := context.WithCancel(context.Background())
	s.runCancels.Store(run.ID, cancel)
	config := ExecutionConfig{
		TargetCatalog: req.TargetCatalog,
		TargetSchema:  req.TargetSchema,
		Variables:     req.Variables,
		FullRefresh:   req.FullRefresh,
	}
	s.logAudit(ctx, principal, "create_model_build_run", run.ID)
	go s.executeRun(execCtx, run.ID, selected, tiers, config, principal)
	return run, nil
}

func (s *Service) persistCompiledCompilationLineage(ctx context.Context, compilationID string, items []domain.CompiledColumnLineage) error {
	if s.colLineage == nil {
		return nil
	}
	for i := range items {
		items[i].CompilationID = compilationID
	}
	return s.colLineage.ReplaceCompilationLineage(ctx, compilationID, items)
}
