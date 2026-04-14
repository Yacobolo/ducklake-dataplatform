package orchestration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
)

func TestMaterializingAssetStepper_ExecuteModel(t *testing.T) {
	stepper := NewMaterializingAssetStepper(
		&fakeAssets{items: map[string]*domain.DataAsset{
			"asset-1": {ID: "asset-1", AssetType: domain.AssetTypeModel, AssetKey: "model.sales.orders", Owner: "alice"},
		}},
		&fakeDeps{},
		modelRepoStub{byID: map[string]*domain.Model{
			"asset-1": {ID: "asset-1", ProjectName: "sales", Name: "orders"},
		}},
		&modelRunnerStub{},
		notebookRepoStub{},
		&notebookSessionRunnerStub{},
		&semanticMaterializerStub{},
	)

	result, err := stepper.Execute(context.Background(), "asset-1", nil)
	require.NoError(t, err)
	assert.Equal(t, domain.AssetTypeModel, result["asset_type"])
	assert.Equal(t, "sales.orders", result["model"])
	assert.Equal(t, "alice", result["principal"])
}

func TestMaterializingAssetStepper_ExecuteNotebookOutput(t *testing.T) {
	sessions := &notebookSessionRunnerStub{
		runAllResult: &domain.RunAllResult{
			NotebookID: "nb-1",
			Results: []domain.CellExecutionResult{
				{CellID: "cell-out"},
			},
		},
	}
	stepper := NewMaterializingAssetStepper(
		&fakeAssets{items: map[string]*domain.DataAsset{
			"cell-out": {ID: "cell-out", AssetType: domain.AssetTypeNotebookOutput, AssetKey: "notebook_output.nb-1.cell-out", Owner: "alice"},
		}},
		&fakeDeps{upstream: map[string][]domain.AssetDependency{
			"cell-out": {{AssetID: "cell-out", UpstreamAssetID: "nb-1", DependencyType: domain.DependencyTypeHard}},
		}},
		modelRepoStub{},
		&modelRunnerStub{},
		notebookRepoStub{byID: map[string]*domain.Notebook{
			"nb-1": {ID: "nb-1", Name: "Revenue", Owner: "alice"},
		}},
		sessions,
		&semanticMaterializerStub{},
	)

	result, err := stepper.Execute(context.Background(), "cell-out", nil)
	require.NoError(t, err)
	assert.Equal(t, "nb-1", result["notebook_id"])
	assert.Equal(t, "cell-out", result["output_cell"])
	assert.Equal(t, "alice", sessions.createPrincipal)
	assert.Equal(t, "alice", sessions.runPrincipal)
}

func TestMaterializingAssetStepper_ExecuteSemanticPreAggregation(t *testing.T) {
	semantic := &semanticMaterializerStub{
		preAgg: &domain.SemanticPreAggregation{ID: "preagg-1", Name: "daily_revenue"},
		metadata: map[string]any{
			"generated_sql": "select 1",
		},
	}
	stepper := NewMaterializingAssetStepper(
		&fakeAssets{items: map[string]*domain.DataAsset{
			"preagg-1": {ID: "preagg-1", AssetType: domain.AssetTypeSemanticPreAggregation, AssetKey: "semantic_pre_aggregation.sales.orders.daily_revenue", Owner: "alice"},
		}},
		&fakeDeps{},
		modelRepoStub{},
		&modelRunnerStub{},
		notebookRepoStub{},
		&notebookSessionRunnerStub{},
		semantic,
	)

	result, err := stepper.Execute(context.Background(), "preagg-1", nil)
	require.NoError(t, err)
	assert.Equal(t, "select 1", result["generated_sql"])
	assert.Equal(t, "preagg-1", semantic.lastID)
	assert.Equal(t, "alice", semantic.lastPrincipal)
}

func TestMaterializingAssetStepper_ExecuteLogicalAssetsSkipMaterialization(t *testing.T) {
	stepper := NewMaterializingAssetStepper(
		&fakeAssets{items: map[string]*domain.DataAsset{
			"metric-1": {ID: "metric-1", AssetType: domain.AssetTypeMetric, AssetKey: "metric.sales.orders.revenue"},
		}},
		&fakeDeps{},
		modelRepoStub{},
		&modelRunnerStub{},
		notebookRepoStub{},
		&notebookSessionRunnerStub{},
		&semanticMaterializerStub{},
	)

	result, err := stepper.Execute(context.Background(), "metric-1", nil)
	require.NoError(t, err)
	assert.Equal(t, true, result[assetExecutionSkipMaterializationKey])
	assert.Equal(t, "skipped", result["status"])
}

func TestAssetExecutor_SkipsMaterializationWhenStepperRequestsIt(t *testing.T) {
	runs := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	state := NewAssetRunStateMachine()
	io := NewInMemoryIOManager()
	limiter := NewConcurrencyLimiter(1, 1)
	stepper := &fakeStepper{results: map[string]map[string]any{
		"a": {
			assetExecutionSkipMaterializationKey: true,
			"status":                             "skipped",
		},
	}}

	executor := NewAssetExecutor(runs, state, io, limiter, stepper)
	runID := "run-skip"
	runs.runs[runID] = &domain.AssetRun{ID: runID, Status: domain.AssetRunStatusQueued, MaxAttempts: 1}

	err := executor.ExecutePlan(context.Background(), runID, domain.AssetRunStatusQueued, &AssetRunPlan{
		RootAssetID: "a",
		Levels:      [][]string{{"a"}},
	})
	require.NoError(t, err)
	assert.Empty(t, runs.materialize["a"])
}

type modelRepoStub struct {
	byID map[string]*domain.Model
}

func (s modelRepoStub) Create(context.Context, *domain.Model) (*domain.Model, error) {
	panic("unexpected call")
}

func (s modelRepoStub) CreateWithNotebookLink(context.Context, *domain.Model, string, string) (*domain.Model, error) {
	panic("unexpected call")
}

func (s modelRepoStub) GetByID(_ context.Context, id string) (*domain.Model, error) {
	if model, ok := s.byID[id]; ok {
		return model, nil
	}
	return nil, domain.ErrNotFound("model %s not found", id)
}

func (s modelRepoStub) GetByName(context.Context, string, string) (*domain.Model, error) {
	panic("unexpected call")
}
func (s modelRepoStub) List(context.Context, *string, domain.PageRequest) ([]domain.Model, int64, error) {
	panic("unexpected call")
}
func (s modelRepoStub) Update(context.Context, string, domain.UpdateModelRequest) (*domain.Model, error) {
	panic("unexpected call")
}
func (s modelRepoStub) Delete(context.Context, string) error { panic("unexpected call") }
func (s modelRepoStub) ListAll(context.Context) ([]domain.Model, error) {
	panic("unexpected call")
}
func (s modelRepoStub) UpdateDependencies(context.Context, string, []string) error {
	panic("unexpected call")
}

type modelRunnerStub struct {
	lastReq domain.TriggerModelRunRequest
}

func (s *modelRunnerStub) TriggerRunSync(_ context.Context, _ string, req domain.TriggerModelRunRequest) error {
	s.lastReq = req
	return nil
}

type notebookRepoStub struct {
	byID map[string]*domain.Notebook
}

func (s notebookRepoStub) CreateNotebook(context.Context, *domain.Notebook) (*domain.Notebook, error) {
	panic("unexpected call")
}
func (s notebookRepoStub) GetNotebook(_ context.Context, id string) (*domain.Notebook, error) {
	if notebook, ok := s.byID[id]; ok {
		return notebook, nil
	}
	return nil, domain.ErrNotFound("notebook %s not found", id)
}
func (s notebookRepoStub) ListNotebooks(context.Context, *string, domain.PageRequest) ([]domain.Notebook, int64, error) {
	panic("unexpected call")
}
func (s notebookRepoStub) ListByFolders(context.Context, []string) ([]domain.Notebook, error) {
	panic("unexpected call")
}
func (s notebookRepoStub) UpdateNotebook(context.Context, string, domain.UpdateNotebookRequest) (*domain.Notebook, error) {
	panic("unexpected call")
}
func (s notebookRepoStub) UpdateNotebookSync(context.Context, *domain.Notebook) (*domain.Notebook, error) {
	panic("unexpected call")
}
func (s notebookRepoStub) DeleteNotebook(context.Context, string) error { panic("unexpected call") }
func (s notebookRepoStub) CreateCell(context.Context, *domain.Cell) (*domain.Cell, error) {
	panic("unexpected call")
}
func (s notebookRepoStub) GetCell(context.Context, string) (*domain.Cell, error) {
	panic("unexpected call")
}
func (s notebookRepoStub) ListCells(context.Context, string) ([]domain.Cell, error) {
	panic("unexpected call")
}
func (s notebookRepoStub) UpdateCell(context.Context, string, domain.UpdateCellRequest) (*domain.Cell, error) {
	panic("unexpected call")
}
func (s notebookRepoStub) UpdateCellSync(context.Context, *domain.Cell) (*domain.Cell, error) {
	panic("unexpected call")
}
func (s notebookRepoStub) DeleteCell(context.Context, string) error { panic("unexpected call") }
func (s notebookRepoStub) UpdateCellResult(context.Context, string, *string) error {
	panic("unexpected call")
}
func (s notebookRepoStub) ReorderCells(context.Context, string, []string) error {
	panic("unexpected call")
}
func (s notebookRepoStub) GetMaxPosition(context.Context, string) (int, error) {
	panic("unexpected call")
}

type notebookSessionRunnerStub struct {
	createPrincipal string
	runPrincipal    string
	runAllResult    *domain.RunAllResult
}

func (s *notebookSessionRunnerStub) CreateSession(_ context.Context, notebookID, principal string) (*domain.NotebookSession, error) {
	s.createPrincipal = principal
	return &domain.NotebookSession{ID: "session-1", NotebookID: notebookID, Principal: principal}, nil
}

func (s *notebookSessionRunnerStub) RunAll(_ context.Context, _ string, principalName ...string) (*domain.RunAllResult, error) {
	if len(principalName) > 0 {
		s.runPrincipal = principalName[0]
	}
	return s.runAllResult, nil
}

func (s *notebookSessionRunnerStub) CloseSession(context.Context, string, ...string) error {
	return nil
}

type semanticMaterializerStub struct {
	lastPrincipal string
	lastID        string
	preAgg        *domain.SemanticPreAggregation
	metadata      map[string]any
}

func (s *semanticMaterializerStub) MaterializePreAggregation(_ context.Context, principal, preAggregationID string) (*domain.SemanticPreAggregation, map[string]any, error) {
	s.lastPrincipal = principal
	s.lastID = preAggregationID
	return s.preAgg, s.metadata, nil
}
