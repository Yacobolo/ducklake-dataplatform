package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
	modelsvc "github.com/Yacobolo/quackstack/internal/service/model"
)

type mockModelService struct {
	triggerRunFn           func(ctx context.Context, principal string, req domain.TriggerModelRunRequest) (*domain.ModelRun, error)
	getRunFn               func(ctx context.Context, runID string) (*domain.ModelRun, error)
	listRunsFn             func(ctx context.Context, filter domain.ModelRunFilter) ([]domain.ModelRun, int64, error)
	listRunStepsFn         func(ctx context.Context, runID string) ([]domain.ModelRunStep, error)
	listTestResultsFn      func(ctx context.Context, runID, stepID string) ([]domain.ModelTestResult, error)
	checkSourceFreshnessFn func(ctx context.Context, principal, sourceSchema, sourceTable, timestampColumn string, maxLagSeconds int64) (*domain.SourceFreshnessStatus, error)
	unpublishNotebookFn    func(ctx context.Context, principal, notebookID string) error
	getBuildLineageFn      func(ctx context.Context, buildID string, modelName *string) ([]domain.CompiledColumnLineage, error)
	getCompilationLineageFn func(ctx context.Context, compilationID string, modelName *string) ([]domain.CompiledColumnLineage, error)
	getBuildDiagnosticsFn  func(ctx context.Context, buildID string, filter domain.BuildDiagnosticsFilter) ([]domain.CompileDiagnostic, error)
	getCompilationDiagnosticsFn func(ctx context.Context, compilationID string, filter domain.BuildDiagnosticsFilter) ([]domain.CompileDiagnostic, error)
	getBuildImpactFn       func(ctx context.Context, buildID, schema, table, column string) ([]domain.CompiledColumnLineage, error)
	getCompilationImpactFn func(ctx context.Context, compilationID, schema, table, column string) ([]domain.CompiledColumnLineage, error)
	planRebuildFn          func(ctx context.Context, principal string, req domain.PlanRebuildRequest) (*domain.RebuildPlan, error)
	compareBuildsFn        func(ctx context.Context, principal string, req domain.CompareBuildsRequest) (*domain.BuildCompareResult, error)
	getModelImpactFn       func(ctx context.Context, projectName string, buildID *string, modelName string) (*domain.BuildImpactResult, error)
	getMacroImpactFn       func(ctx context.Context, projectName string, buildID *string, macroName string) (*domain.BuildImpactResult, error)
	getCompilationFn       func(ctx context.Context, compilationID string) (*domain.Compilation, error)
	listCompilationsFn     func(ctx context.Context, projectName, environmentName string, page domain.PageRequest) ([]domain.Compilation, int64, error)
	getCompilationModelImpactFn func(ctx context.Context, compilationID string, modelName string) (*domain.BuildImpactResult, error)
	getCompilationMacroImpactFn func(ctx context.Context, compilationID string, macroName string) (*domain.BuildImpactResult, error)
	createCompilationFn    func(ctx context.Context, principal string, projectName string, environmentName string, req domain.CreateCompilationRequest) (*domain.Compilation, error)
	createEnvironmentBuildFn func(ctx context.Context, principal string, projectName string, environmentName string, req domain.CreateCompilationRequest) (*domain.Build, error)
	listRunsForBuildFn     func(ctx context.Context, buildID string, page domain.PageRequest) ([]domain.ModelRun, int64, error)
	createRunForBuildFn    func(ctx context.Context, principal string, build *domain.Build) (*domain.ModelRun, error)
}

func (m *mockModelService) CreateModel(context.Context, string, domain.CreateModelRequest) (*domain.Model, error) {
	panic("not implemented")
}
func (m *mockModelService) GetModel(context.Context, string, string) (*domain.Model, error) {
	panic("not implemented")
}
func (m *mockModelService) ListModels(context.Context, *string, domain.PageRequest) ([]domain.Model, int64, error) {
	panic("not implemented")
}
func (m *mockModelService) UpdateModel(context.Context, string, string, string, domain.UpdateModelRequest) (*domain.Model, error) {
	panic("not implemented")
}
func (m *mockModelService) DeleteModel(context.Context, string, string, string) error {
	panic("not implemented")
}
func (m *mockModelService) GetDAG(context.Context, *string) ([][]modelsvc.DAGNode, error) {
	panic("not implemented")
}
func (m *mockModelService) TriggerRun(ctx context.Context, principal string, req domain.TriggerModelRunRequest) (*domain.ModelRun, error) {
	if m.triggerRunFn == nil {
		panic("not implemented")
	}
	return m.triggerRunFn(ctx, principal, req)
}
func (m *mockModelService) GetRun(ctx context.Context, runID string) (*domain.ModelRun, error) {
	if m.getRunFn == nil {
		panic("not implemented")
	}
	return m.getRunFn(ctx, runID)
}
func (m *mockModelService) ListRuns(ctx context.Context, filter domain.ModelRunFilter) ([]domain.ModelRun, int64, error) {
	if m.listRunsFn == nil {
		panic("not implemented")
	}
	return m.listRunsFn(ctx, filter)
}
func (m *mockModelService) ListRunSteps(ctx context.Context, runID string) ([]domain.ModelRunStep, error) {
	if m.listRunStepsFn == nil {
		panic("not implemented")
	}
	return m.listRunStepsFn(ctx, runID)
}
func (m *mockModelService) CancelRun(context.Context, string, string) error {
	panic("not implemented")
}
func (m *mockModelService) CreateTest(context.Context, string, string, string, domain.CreateModelTestRequest) (*domain.ModelTest, error) {
	panic("not implemented")
}
func (m *mockModelService) ListTests(context.Context, string, string) ([]domain.ModelTest, error) {
	panic("not implemented")
}
func (m *mockModelService) DeleteTest(context.Context, string, string, string, string) error {
	panic("not implemented")
}
func (m *mockModelService) ListTestResults(ctx context.Context, runID, stepID string) ([]domain.ModelTestResult, error) {
	if m.listTestResultsFn == nil {
		panic("not implemented")
	}
	return m.listTestResultsFn(ctx, runID, stepID)
}
func (m *mockModelService) CheckFreshness(context.Context, string, string) (*domain.FreshnessStatus, error) {
	panic("not implemented")
}
func (m *mockModelService) CheckSourceFreshness(ctx context.Context, principal, sourceSchema, sourceTable, timestampColumn string, maxLagSeconds int64) (*domain.SourceFreshnessStatus, error) {
	if m.checkSourceFreshnessFn == nil {
		panic("not implemented")
	}
	return m.checkSourceFreshnessFn(ctx, principal, sourceSchema, sourceTable, timestampColumn, maxLagSeconds)
}
func (m *mockModelService) PromoteNotebook(context.Context, string, domain.PromoteNotebookRequest) (*domain.Model, error) {
	panic("not implemented")
}
func (m *mockModelService) UnpublishNotebook(ctx context.Context, principal, notebookID string) error {
	if m.unpublishNotebookFn == nil {
		panic("not implemented")
	}
	return m.unpublishNotebookFn(ctx, principal, notebookID)
}
func (m *mockModelService) GetBuildLineage(ctx context.Context, buildID string, modelName *string) ([]domain.CompiledColumnLineage, error) {
	if m.getBuildLineageFn == nil {
		return nil, nil
	}
	return m.getBuildLineageFn(ctx, buildID, modelName)
}
func (m *mockModelService) GetCompilationLineage(ctx context.Context, compilationID string, modelName *string) ([]domain.CompiledColumnLineage, error) {
	if m.getCompilationLineageFn == nil {
		return nil, nil
	}
	return m.getCompilationLineageFn(ctx, compilationID, modelName)
}
func (m *mockModelService) GetBuildDiagnostics(ctx context.Context, buildID string, filter domain.BuildDiagnosticsFilter) ([]domain.CompileDiagnostic, error) {
	if m.getBuildDiagnosticsFn == nil {
		return nil, nil
	}
	return m.getBuildDiagnosticsFn(ctx, buildID, filter)
}
func (m *mockModelService) GetCompilationDiagnostics(ctx context.Context, compilationID string, filter domain.BuildDiagnosticsFilter) ([]domain.CompileDiagnostic, error) {
	if m.getCompilationDiagnosticsFn == nil {
		return nil, nil
	}
	return m.getCompilationDiagnosticsFn(ctx, compilationID, filter)
}
func (m *mockModelService) GetBuildSourceColumnImpact(ctx context.Context, buildID, schema, table, column string) ([]domain.CompiledColumnLineage, error) {
	if m.getBuildImpactFn == nil {
		return nil, nil
	}
	return m.getBuildImpactFn(ctx, buildID, schema, table, column)
}
func (m *mockModelService) GetCompilationSourceColumnImpact(ctx context.Context, compilationID, schema, table, column string) ([]domain.CompiledColumnLineage, error) {
	if m.getCompilationImpactFn == nil {
		return nil, nil
	}
	return m.getCompilationImpactFn(ctx, compilationID, schema, table, column)
}
func (m *mockModelService) PlanRebuild(ctx context.Context, principal string, req domain.PlanRebuildRequest) (*domain.RebuildPlan, error) {
	if m.planRebuildFn == nil {
		return nil, nil
	}
	return m.planRebuildFn(ctx, principal, req)
}
func (m *mockModelService) CompareBuilds(ctx context.Context, principal string, req domain.CompareBuildsRequest) (*domain.BuildCompareResult, error) {
	if m.compareBuildsFn == nil {
		return nil, nil
	}
	return m.compareBuildsFn(ctx, principal, req)
}
func (m *mockModelService) GetModelImpact(ctx context.Context, projectName string, buildID *string, modelName string) (*domain.BuildImpactResult, error) {
	if m.getModelImpactFn == nil {
		return nil, nil
	}
	return m.getModelImpactFn(ctx, projectName, buildID, modelName)
}
func (m *mockModelService) GetMacroImpact(ctx context.Context, projectName string, buildID *string, macroName string) (*domain.BuildImpactResult, error) {
	if m.getMacroImpactFn == nil {
		return nil, nil
	}
	return m.getMacroImpactFn(ctx, projectName, buildID, macroName)
}
func (m *mockModelService) GetCompilation(ctx context.Context, compilationID string) (*domain.Compilation, error) {
	if m.getCompilationFn == nil {
		return nil, nil
	}
	return m.getCompilationFn(ctx, compilationID)
}
func (m *mockModelService) ListCompilationsForEnvironment(ctx context.Context, projectName, environmentName string, page domain.PageRequest) ([]domain.Compilation, int64, error) {
	if m.listCompilationsFn == nil {
		return nil, 0, nil
	}
	return m.listCompilationsFn(ctx, projectName, environmentName, page)
}
func (m *mockModelService) GetCompilationModelImpact(ctx context.Context, compilationID string, modelName string) (*domain.BuildImpactResult, error) {
	if m.getCompilationModelImpactFn == nil {
		return nil, nil
	}
	return m.getCompilationModelImpactFn(ctx, compilationID, modelName)
}
func (m *mockModelService) GetCompilationMacroImpact(ctx context.Context, compilationID string, macroName string) (*domain.BuildImpactResult, error) {
	if m.getCompilationMacroImpactFn == nil {
		return nil, nil
	}
	return m.getCompilationMacroImpactFn(ctx, compilationID, macroName)
}
func (m *mockModelService) CreateCompilation(ctx context.Context, principal string, projectName string, environmentName string, req domain.CreateCompilationRequest) (*domain.Compilation, error) {
	if m.createCompilationFn == nil {
		return nil, nil
	}
	return m.createCompilationFn(ctx, principal, projectName, environmentName, req)
}
func (m *mockModelService) CreateEnvironmentBuild(ctx context.Context, principal string, projectName string, environmentName string, req domain.CreateCompilationRequest) (*domain.Build, error) {
	if m.createEnvironmentBuildFn == nil {
		return nil, nil
	}
	return m.createEnvironmentBuildFn(ctx, principal, projectName, environmentName, req)
}
func (m *mockModelService) ListRunsForBuild(ctx context.Context, buildID string, page domain.PageRequest) ([]domain.ModelRun, int64, error) {
	if m.listRunsForBuildFn == nil {
		return nil, 0, nil
	}
	return m.listRunsForBuildFn(ctx, buildID, page)
}
func (m *mockModelService) CreateRunForBuild(ctx context.Context, principal string, build *domain.Build) (*domain.ModelRun, error) {
	if m.createRunForBuildFn == nil {
		return nil, nil
	}
	return m.createRunForBuildFn(ctx, principal, build)
}

func TestHandler_UnpublishNotebookModel(t *testing.T) {
	t.Parallel()

	var called bool
	h := &APIHandler{
		models: &mockModelService{
			unpublishNotebookFn: func(_ context.Context, principal, notebookID string) error {
				called = true
				assert.Equal(t, "admin-user", principal)
				assert.Equal(t, "nb-1", notebookID)
				return nil
			},
		},
		notebooks: &mockNotebookService{
			getNotebookForPrincipalFn: func(_ context.Context, principal string, isAdmin bool, id string) (*domain.Notebook, []domain.Cell, error) {
				assert.Equal(t, "admin-user", principal)
				assert.True(t, isAdmin)
				assert.Equal(t, "nb-1", id)
				return &domain.Notebook{ID: id, Name: "notebook"}, nil, nil
			},
		},
	}

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin-user", IsAdmin: true})
	resp, err := h.UnpublishNotebookModel(ctx, GenUnpublishNotebookModelRequest{NotebookId: "nb-1"})
	require.NoError(t, err)
	require.True(t, called)
	_, ok := resp.(GenUnpublishNotebookModel204Response)
	require.True(t, ok)
}

func TestHandler_TriggerModelRun_UsesAllModelNames(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 2, 16, 10, 0, 0, 0, time.UTC)
	var gotReq domain.TriggerModelRunRequest
	h := &APIHandler{
		models: &mockModelService{
			triggerRunFn: func(_ context.Context, principal string, req domain.TriggerModelRunRequest) (*domain.ModelRun, error) {
				gotReq = req
				assert.Equal(t, "admin-user", principal)
				return &domain.ModelRun{
					ID:              "run-1",
					Status:          domain.ModelRunStatusPending,
					TriggerType:     domain.ModelTriggerTypeManual,
					TriggeredBy:     principal,
					ProjectName:     req.ProjectName,
					EnvironmentName: req.EnvironmentName,
					ModelSelector:   req.Selector,
					CreatedAt:       fixed,
				}, nil
			},
		},
	}

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin-user", IsAdmin: true})
	modelNames := []string{"stg_orders", "fct_orders"}
	resp, err := h.TriggerModelRun(ctx, GenTriggerModelRunRequest{
		Body: &GenTriggerModelRunJSONBody{
			ProjectName: "analytics",
			ModelNames:  &modelNames,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "stg_orders,fct_orders", gotReq.Selector)
	assert.Equal(t, "analytics", gotReq.ProjectName)

	created, ok := resp.(GenTriggerModelRun201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)
	require.NotNil(t, created.Body.ProjectName)
	assert.Equal(t, "analytics", *created.Body.ProjectName)
	require.NotNil(t, created.Body.ModelNames)
	assert.Equal(t, []string{"stg_orders", "fct_orders"}, *created.Body.ModelNames)
}

func TestHandler_TriggerModelRun_MapsPayloadFields(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})

	reqBody := GenTriggerModelRunJSONBody{
		ProjectName:     "proj_a",
		EnvironmentName: strPtr("staging"),
		ModelNames:      &[]string{"stg_orders", "+fct_orders"},
		FullRefresh:     boolPtr(true),
	}

	var gotPrincipal string
	var gotReq domain.TriggerModelRunRequest
	h := &APIHandler{
		models: &mockModelService{triggerRunFn: func(_ context.Context, principal string, req domain.TriggerModelRunRequest) (*domain.ModelRun, error) {
			gotPrincipal = principal
			gotReq = req
			now := time.Now().UTC()
			return &domain.ModelRun{ID: "run-1", Status: domain.ModelRunStatusPending, TriggerType: domain.ModelTriggerTypeManual, TriggeredBy: principal, CreatedAt: now}, nil
		}},
	}

	resp, err := h.TriggerModelRun(ctx, GenTriggerModelRunRequest{Body: &reqBody})
	require.NoError(t, err)
	_, ok := resp.(GenTriggerModelRun201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)

	assert.Equal(t, "alice", gotPrincipal)
	assert.Equal(t, "proj_a", gotReq.ProjectName)
	assert.Equal(t, "staging", gotReq.EnvironmentName)
	assert.Equal(t, "stg_orders,+fct_orders", gotReq.Selector)
	assert.True(t, gotReq.FullRefresh)
	assert.Equal(t, domain.ModelTriggerTypeManual, gotReq.TriggerType)
}

func TestHandler_TriggerModelRun_DefaultEnvironmentIsEmpty(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})

	reqBody := GenTriggerModelRunJSONBody{ProjectName: "proj_a"}

	var gotReq domain.TriggerModelRunRequest
	h := &APIHandler{
		models: &mockModelService{triggerRunFn: func(_ context.Context, _ string, req domain.TriggerModelRunRequest) (*domain.ModelRun, error) {
			gotReq = req
			now := time.Now().UTC()
			return &domain.ModelRun{ID: "run-2", Status: domain.ModelRunStatusPending, TriggerType: domain.ModelTriggerTypeManual, TriggeredBy: "alice", CreatedAt: now}, nil
		}},
	}

	resp, err := h.TriggerModelRun(ctx, GenTriggerModelRunRequest{Body: &reqBody})
	require.NoError(t, err)
	_, ok := resp.(GenTriggerModelRun201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)

	assert.Equal(t, "proj_a", gotReq.ProjectName)
	assert.Empty(t, gotReq.EnvironmentName)
	assert.Empty(t, gotReq.Selector)
	assert.False(t, gotReq.FullRefresh)
}

func TestHandler_ListModelRuns_InvalidStatusReturns400(t *testing.T) {
	t.Parallel()

	called := false
	h := &APIHandler{
		models: &mockModelService{
			listRunsFn: func(_ context.Context, _ domain.ModelRunFilter) ([]domain.ModelRun, int64, error) {
				called = true
				return nil, 0, nil
			},
		},
	}

	invalid := "INVALID"
	resp, err := h.ListModelRuns(context.Background(), GenListModelRunsRequest{Params: GenListModelRunsParams{Status: &invalid}})
	require.NoError(t, err)
	assert.False(t, called)

	badReq, ok := resp.(ListModelRuns400JSONResponse)
	require.True(t, ok, "expected 400 response, got %T", resp)
	assert.Contains(t, badReq.Body.Message, "status must be one of")
}

func TestHandler_ListModelRuns_IncludesModelNamesAndProject(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 2, 16, 11, 0, 0, 0, time.UTC)
	h := &APIHandler{
		models: &mockModelService{
			listRunsFn: func(_ context.Context, _ domain.ModelRunFilter) ([]domain.ModelRun, int64, error) {
				return []domain.ModelRun{{
					ID:            "run-2",
					Status:        domain.ModelRunStatusSuccess,
					TriggerType:   domain.ModelTriggerTypeManual,
					TriggeredBy:   "admin-user",
					ProjectName:   "analytics",
					ModelSelector: "stg_orders,fct_orders",
					CreatedAt:     fixed,
				}}, 1, nil
			},
		},
	}

	resp, err := h.ListModelRuns(context.Background(), GenListModelRunsRequest{})
	require.NoError(t, err)

	okResp, ok := resp.(GenListModelRuns200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.NotNil(t, okResp.Body.Data)
	require.Len(t, okResp.Body.Data, 1)
	run := okResp.Body.Data[0]
	require.NotNil(t, run.ProjectName)
	assert.Equal(t, "analytics", *run.ProjectName)
	require.NotNil(t, run.ModelNames)
	assert.Equal(t, []string{"stg_orders", "fct_orders"}, *run.ModelNames)
}

func TestHandler_ListModelRunSteps_MissingRunReturns404(t *testing.T) {
	t.Parallel()

	h := &APIHandler{
		models: &mockModelService{
			listRunStepsFn: func(_ context.Context, _ string) ([]domain.ModelRunStep, error) {
				return nil, domain.ErrNotFound("run not found")
			},
		},
	}

	resp, err := h.ListModelRunSteps(context.Background(), GenListModelRunStepsRequest{RunId: "00000000-0000-0000-0000-000000000000"})
	require.NoError(t, err)

	notFound, ok := resp.(ListModelRunSteps404JSONResponse)
	require.True(t, ok, "expected 404 response, got %T", resp)
	assert.Equal(t, int32(404), notFound.Body.Code)
}

func TestHandler_ListModelTestResults_MissingRunReturns404(t *testing.T) {
	t.Parallel()

	h := &APIHandler{
		models: &mockModelService{
			listTestResultsFn: func(_ context.Context, _, _ string) ([]domain.ModelTestResult, error) {
				return nil, domain.ErrNotFound("run not found")
			},
		},
	}

	resp, err := h.ListModelTestResults(context.Background(), GenListModelTestResultsRequest{
		RunId:  "00000000-0000-0000-0000-000000000000",
		StepId: "00000000-0000-0000-0000-000000000001",
	})
	require.NoError(t, err)

	notFound, ok := resp.(ListModelTestResults404JSONResponse)
	require.True(t, ok, "expected 404 response, got %T", resp)
	assert.Equal(t, int32(404), notFound.Body.Code)
}

func boolPtr(v bool) *bool { return &v }

func TestModelRunToAPI_CompileDiagnosticsStableEmptyArrays(t *testing.T) {
	t.Parallel()

	run := domain.ModelRun{
		ID:              "run-empty-diags",
		Status:          domain.ModelRunStatusSuccess,
		TriggerType:     domain.ModelTriggerTypeManual,
		TriggeredBy:     "admin",
		ProjectName:     "analytics",
		EnvironmentName: "dev",
		BuildID:         strPtr("build-1"),
		CreatedAt:       time.Date(2026, 2, 17, 12, 0, 0, 0, time.UTC),
		CompileDiagnostics: &domain.ModelCompileDiagnostics{
			Warnings: nil,
			Errors:   nil,
		},
	}

	got := modelRunToAPI(run)
	require.NotNil(t, got.ProjectName)
	assert.Equal(t, "analytics", *got.ProjectName)
	require.NotNil(t, got.EnvironmentName)
	assert.Equal(t, "dev", *got.EnvironmentName)
	require.NotNil(t, got.BuildId)
	assert.Equal(t, "build-1", *got.BuildId)
	require.NotNil(t, got.CompileDiagnostics)
	require.NotNil(t, got.CompileDiagnostics.Warnings)
	require.NotNil(t, got.CompileDiagnostics.Errors)
	assert.Empty(t, *got.CompileDiagnostics.Warnings)
	assert.Empty(t, *got.CompileDiagnostics.Errors)
}

func TestSelectorToModelNames_IgnoresStateSelector(t *testing.T) {
	t.Parallel()
	assert.Nil(t, selectorToModelNames("state:modified"))
}

func TestSourceFreshnessStatusToAPI_MapsFields(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	status := domain.SourceFreshnessStatus{
		IsFresh:       true,
		SourceSchema:  "raw",
		SourceTable:   "orders",
		TimestampCol:  "updated_at",
		LastLoadedAt:  &now,
		MaxLagSeconds: 3600,
	}

	apiStatus := sourceFreshnessStatusToAPI(status)
	require.NotNil(t, apiStatus.IsFresh)
	assert.True(t, *apiStatus.IsFresh)
	require.NotNil(t, apiStatus.SourceSchema)
	assert.Equal(t, "raw", *apiStatus.SourceSchema)
	require.NotNil(t, apiStatus.SourceTable)
	assert.Equal(t, "orders", *apiStatus.SourceTable)
	require.NotNil(t, apiStatus.TimestampColumn)
	assert.Equal(t, "updated_at", *apiStatus.TimestampColumn)
	require.NotNil(t, apiStatus.LastLoadedAt)
	require.NotNil(t, apiStatus.MaxLagSeconds)
	assert.EqualValues(t, 3600, *apiStatus.MaxLagSeconds)
}

func TestHandler_CheckSourceFreshness_DefaultsAndMapping(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
	called := false
	h := &APIHandler{models: &mockModelService{checkSourceFreshnessFn: func(_ context.Context, principal, sourceSchema, sourceTable, timestampColumn string, maxLagSeconds int64) (*domain.SourceFreshnessStatus, error) {
		called = true
		assert.Equal(t, "alice", principal)
		assert.Equal(t, "raw", sourceSchema)
		assert.Equal(t, "orders", sourceTable)
		assert.Empty(t, timestampColumn)
		assert.EqualValues(t, 3600, maxLagSeconds)
		now := time.Now().UTC()
		return &domain.SourceFreshnessStatus{
			IsFresh:       true,
			SourceSchema:  sourceSchema,
			SourceTable:   sourceTable,
			TimestampCol:  "updated_at",
			LastLoadedAt:  &now,
			MaxLagSeconds: maxLagSeconds,
		}, nil
	}}}

	resp, err := h.CheckSourceFreshness(ctx, GenCheckSourceFreshnessRequest{
		SourceSchema: "raw",
		SourceTable:  "orders",
		Params:       GenCheckSourceFreshnessParams{},
	})
	require.NoError(t, err)
	require.True(t, called)

	okResp, ok := resp.(GenCheckSourceFreshness200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.NotNil(t, okResp.Body.SourceSchema)
	assert.Equal(t, "raw", *okResp.Body.SourceSchema)
	require.NotNil(t, okResp.Body.SourceTable)
	assert.Equal(t, "orders", *okResp.Body.SourceTable)
	require.NotNil(t, okResp.Body.TimestampColumn)
	assert.Equal(t, "updated_at", *okResp.Body.TimestampColumn)
}
