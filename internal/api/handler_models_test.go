package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	modelsvc "duck-demo/internal/service/model"
)

type mockModelService struct {
	triggerRunFn func(ctx context.Context, principal string, req domain.TriggerModelRunRequest) (*domain.ModelRun, error)
	listRunsFn   func(ctx context.Context, filter domain.ModelRunFilter) ([]domain.ModelRun, int64, error)
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
func (m *mockModelService) GetRun(context.Context, string) (*domain.ModelRun, error) {
	panic("not implemented")
}
func (m *mockModelService) ListRuns(ctx context.Context, filter domain.ModelRunFilter) ([]domain.ModelRun, int64, error) {
	if m.listRunsFn == nil {
		panic("not implemented")
	}
	return m.listRunsFn(ctx, filter)
}
func (m *mockModelService) ListRunSteps(context.Context, string) ([]domain.ModelRunStep, error) {
	panic("not implemented")
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
func (m *mockModelService) ListTestResults(context.Context, string, string) ([]domain.ModelTestResult, error) {
	panic("not implemented")
}
func (m *mockModelService) CheckFreshness(context.Context, string, string) (*domain.FreshnessStatus, error) {
	panic("not implemented")
}
func (m *mockModelService) PromoteNotebook(context.Context, string, domain.PromoteNotebookRequest) (*domain.Model, error) {
	panic("not implemented")
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
					ID:            "run-1",
					Status:        domain.ModelRunStatusPending,
					TriggerType:   domain.ModelTriggerTypeManual,
					TriggeredBy:   principal,
					TargetSchema:  req.TargetSchema,
					ModelSelector: req.Selector,
					CreatedAt:     fixed,
				}, nil
			},
		},
	}

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin-user", IsAdmin: true})
	modelNames := []string{"stg_orders", "fct_orders"}
	resp, err := h.TriggerModelRun(ctx, TriggerModelRunRequestObject{
		Body: &TriggerModelRunJSONRequestBody{
			ProjectName: "analytics",
			ModelNames:  &modelNames,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "stg_orders,fct_orders", gotReq.Selector)

	created, ok := resp.(TriggerModelRun201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)
	require.NotNil(t, created.Body.ProjectName)
	assert.Equal(t, "analytics", *created.Body.ProjectName)
	require.NotNil(t, created.Body.ModelNames)
	assert.Equal(t, []string{"stg_orders", "fct_orders"}, *created.Body.ModelNames)
}

func TestHandler_TriggerModelRun_MapsPayloadFields(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})

	reqBody := TriggerModelRunJSONRequestBody{
		ProjectName:   "proj_a",
		ModelNames:    &[]string{"stg_orders", "+fct_orders"},
		FullRefresh:   boolPtr(true),
		TargetCatalog: strPtr("analytics"),
		TargetSchema:  strPtr("mart"),
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

	resp, err := h.TriggerModelRun(ctx, TriggerModelRunRequestObject{Body: &reqBody})
	require.NoError(t, err)
	_, ok := resp.(TriggerModelRun201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)

	assert.Equal(t, "alice", gotPrincipal)
	assert.Equal(t, "analytics", gotReq.TargetCatalog)
	assert.Equal(t, "mart", gotReq.TargetSchema)
	assert.Equal(t, "stg_orders,+fct_orders", gotReq.Selector)
	assert.True(t, gotReq.FullRefresh)
	assert.Equal(t, domain.ModelTriggerTypeManual, gotReq.TriggerType)
}

func TestHandler_TriggerModelRun_DefaultTargetValues(t *testing.T) {
	t.Parallel()

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})

	reqBody := TriggerModelRunJSONRequestBody{ProjectName: "proj_a"}

	var gotReq domain.TriggerModelRunRequest
	h := &APIHandler{
		models: &mockModelService{triggerRunFn: func(_ context.Context, _ string, req domain.TriggerModelRunRequest) (*domain.ModelRun, error) {
			gotReq = req
			now := time.Now().UTC()
			return &domain.ModelRun{ID: "run-2", Status: domain.ModelRunStatusPending, TriggerType: domain.ModelTriggerTypeManual, TriggeredBy: "alice", CreatedAt: now}, nil
		}},
	}

	resp, err := h.TriggerModelRun(ctx, TriggerModelRunRequestObject{Body: &reqBody})
	require.NoError(t, err)
	_, ok := resp.(TriggerModelRun201JSONResponse)
	require.True(t, ok, "expected 201 response, got %T", resp)

	assert.Equal(t, "memory", gotReq.TargetCatalog)
	assert.Equal(t, "proj_a", gotReq.TargetSchema)
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

	invalid := ListModelRunsParamsStatus("INVALID")
	resp, err := h.ListModelRuns(context.Background(), ListModelRunsRequestObject{Params: ListModelRunsParams{Status: &invalid}})
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
					TargetSchema:  "analytics",
					ModelSelector: "stg_orders,fct_orders",
					CreatedAt:     fixed,
				}}, 1, nil
			},
		},
	}

	resp, err := h.ListModelRuns(context.Background(), ListModelRunsRequestObject{})
	require.NoError(t, err)

	okResp, ok := resp.(ListModelRuns200JSONResponse)
	require.True(t, ok, "expected 200 response, got %T", resp)
	require.NotNil(t, okResp.Body.Data)
	require.Len(t, *okResp.Body.Data, 1)
	run := (*okResp.Body.Data)[0]
	require.NotNil(t, run.ProjectName)
	assert.Equal(t, "analytics", *run.ProjectName)
	require.NotNil(t, run.ModelNames)
	assert.Equal(t, []string{"stg_orders", "fct_orders"}, *run.ModelNames)
}

func boolPtr(v bool) *bool { return &v }

func TestModelRunToAPI_CompileDiagnosticsStableEmptyArrays(t *testing.T) {
	t.Parallel()

	run := domain.ModelRun{
		ID:          "run-empty-diags",
		Status:      domain.ModelRunStatusSuccess,
		TriggerType: domain.ModelTriggerTypeManual,
		TriggeredBy: "admin",
		CreatedAt:   time.Date(2026, 2, 17, 12, 0, 0, 0, time.UTC),
		CompileDiagnostics: &domain.ModelCompileDiagnostics{
			Warnings: nil,
			Errors:   nil,
		},
	}

	got := modelRunToAPI(run)
	require.NotNil(t, got.CompileDiagnostics)
	require.NotNil(t, got.CompileDiagnostics.Warnings)
	require.NotNil(t, got.CompileDiagnostics.Errors)
	assert.Empty(t, *got.CompileDiagnostics.Warnings)
	assert.Empty(t, *got.CompileDiagnostics.Errors)
}
