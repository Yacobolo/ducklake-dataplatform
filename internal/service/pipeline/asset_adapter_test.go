package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestBuildPipelineAssetGraph(t *testing.T) {
	p := &domain.Pipeline{Name: "daily", CreatedBy: "admin"}
	jobs := []domain.PipelineJob{
		{ID: "j1", Name: "extract", JobType: domain.PipelineJobTypeNotebook},
		{ID: "j2", Name: "transform", JobType: domain.PipelineJobTypeModelRun, DependsOn: []string{"extract"}},
	}

	adapted, err := BuildPipelineAssetGraph(p, jobs)
	require.NoError(t, err)
	require.Len(t, adapted.Assets, 2)
	require.Len(t, adapted.Dependencies, 1)

	assert.Equal(t, "pipeline.daily.extract", adapted.Assets[0].AssetKey)
	assert.Equal(t, domain.AssetTypeNotebook, adapted.Assets[0].AssetType)
	assert.Equal(t, domain.AssetTypeModel, adapted.Assets[1].AssetType)

	dep := adapted.Dependencies[0]
	assert.Equal(t, "j2", dep.AssetID)
	assert.Equal(t, "j1", dep.UpstreamAssetID)
	assert.Equal(t, domain.DependencyTypeHard, dep.DependencyType)
}

func TestBuildPipelineAssetGraph_InvalidDependency(t *testing.T) {
	p := &domain.Pipeline{Name: "daily", CreatedBy: "admin"}
	jobs := []domain.PipelineJob{{ID: "j1", Name: "extract", DependsOn: []string{"missing"}}}

	_, err := BuildPipelineAssetGraph(p, jobs)
	require.Error(t, err)
	var validationErr *domain.ValidationError
	assert.ErrorAs(t, err, &validationErr)
}

func TestBuildModelAssetGraph(t *testing.T) {
	models := []domain.Model{
		{
			ID:          "m1",
			ProjectName: "sales",
			Name:        "stg_orders",
			CreatedBy:   "alice",
		},
		{
			ID:          "m2",
			ProjectName: "sales",
			Name:        "fct_orders",
			Owner:       "analytics",
			CreatedBy:   "bob",
			DependsOn:   []string{"sales.stg_orders", "raw.orders"},
		},
	}

	adapted, err := BuildModelAssetGraph(models)
	require.NoError(t, err)
	require.Len(t, adapted.Assets, 2)
	require.Len(t, adapted.Dependencies, 1)

	assert.Equal(t, "model.sales.stg_orders", adapted.Assets[0].AssetKey)
	assert.Equal(t, "alice", adapted.Assets[0].Owner)
	assert.Equal(t, "model.sales.fct_orders", adapted.Assets[1].AssetKey)
	assert.Equal(t, "analytics", adapted.Assets[1].Owner)

	dep := adapted.Dependencies[0]
	assert.Equal(t, "m2", dep.AssetID)
	assert.Equal(t, "m1", dep.UpstreamAssetID)
	assert.Equal(t, domain.DependencyTypeHard, dep.DependencyType)
}

func TestBuildNotebookAssetGraph(t *testing.T) {
	notebooks := []domain.Notebook{
		{ID: "nb-1", Name: "Exec Summary", Owner: "alice"},
	}

	adapted, err := BuildNotebookAssetGraph(notebooks)
	require.NoError(t, err)
	require.Len(t, adapted.Assets, 1)
	assert.Equal(t, "notebook.nb-1", adapted.Assets[0].AssetKey)
	assert.Equal(t, domain.AssetTypeNotebook, adapted.Assets[0].AssetType)
}

func TestBuildSemanticAssetGraph(t *testing.T) {
	models := []domain.Model{
		{ID: "model-1", ProjectName: "sales", Name: "fct_orders", CreatedBy: "alice"},
	}
	semanticModels := []domain.SemanticModel{
		{ID: "sem-1", ProjectName: "sales", Name: "orders", BaseModelRef: "sales.fct_orders", CreatedBy: "alice"},
	}
	metricsByModel := map[string][]domain.SemanticMetric{
		"sem-1": {{ID: "metric-1", Name: "revenue", CreatedBy: "alice"}},
	}
	preAggsByModel := map[string][]domain.SemanticPreAggregation{
		"sem-1": {{ID: "preagg-1", Name: "daily_revenue", CreatedBy: "alice", TargetRelation: "analytics.daily_revenue"}},
	}

	adapted, err := BuildSemanticAssetGraph(semanticModels, metricsByModel, preAggsByModel, models)
	require.NoError(t, err)
	require.Len(t, adapted.Assets, 3)
	assert.Equal(t, "semantic_model.sales.orders", adapted.Assets[0].AssetKey)
	assert.Equal(t, domain.AssetTypeSemanticModel, adapted.Assets[0].AssetType)
	assert.Equal(t, "metric.sales.orders.revenue", adapted.Assets[1].AssetKey)
	assert.Equal(t, domain.AssetTypeMetric, adapted.Assets[1].AssetType)
	assert.Equal(t, "semantic_pre_aggregation.sales.orders.daily_revenue", adapted.Assets[2].AssetKey)
	assert.Equal(t, domain.AssetTypeSemanticPreAggregation, adapted.Assets[2].AssetType)

	assert.ElementsMatch(t, []string{"sem-1->model-1", "metric-1->sem-1", "preagg-1->sem-1"}, dependencyPairs(adapted.Dependencies))
}

func TestBuildDashboardAssetGraph(t *testing.T) {
	dashboards := []domain.Dashboard{
		{ID: "dash-1", Name: "Revenue", Owner: "alice"},
	}
	widgetsByDashboard := map[string][]domain.DashboardWidget{
		"dash-1": {
			{
				ID: "widget-1",
				Source: domain.DashboardWidgetSource{
					Kind: domain.DashboardWidgetSourceSemanticQuery,
					SemanticQuery: &domain.DashboardSemanticQuerySource{
						ProjectName:       "sales",
						SemanticModelName: "orders",
						Metrics:           []string{"revenue"},
					},
				},
			},
			{
				ID: "widget-2",
				Source: domain.DashboardWidgetSource{
					Kind: domain.DashboardWidgetSourceNotebookCell,
					NotebookCell: &domain.DashboardNotebookCellSource{
						NotebookID: "nb-1",
						CellID:     "cell-1",
					},
				},
			},
		},
	}
	notebooks := []domain.Notebook{{ID: "nb-1", Name: "Notebook", Owner: "alice"}}
	semanticModels := []domain.SemanticModel{{ID: "sem-1", ProjectName: "sales", Name: "orders"}}
	metricsByModel := map[string][]domain.SemanticMetric{
		"sem-1": {{ID: "metric-1", Name: "revenue"}},
	}

	adapted, err := BuildDashboardAssetGraph(dashboards, widgetsByDashboard, notebooks, semanticModels, metricsByModel)
	require.NoError(t, err)
	require.Len(t, adapted.Assets, 1)
	assert.Equal(t, "dashboard.dash-1", adapted.Assets[0].AssetKey)
	assert.Equal(t, domain.AssetTypeDashboard, adapted.Assets[0].AssetType)
	assert.ElementsMatch(t, []string{"dash-1->metric-1", "dash-1->nb-1"}, dependencyPairs(adapted.Dependencies))
}

func TestSyncModelsToAssets(t *testing.T) {
	modelRepo := &mockModelRepo{
		listAllFn: func(context.Context) ([]domain.Model, error) {
			return []domain.Model{
				{ID: "m1", ProjectName: "sales", Name: "stg_orders", CreatedBy: "alice"},
				{ID: "m2", ProjectName: "sales", Name: "fct_orders", CreatedBy: "alice", DependsOn: []string{"sales.stg_orders"}},
			}, nil
		},
	}
	assetRepo := &mockDataAssetRepo{existing: map[string]domain.DataAsset{"m1": {ID: "m1"}}}
	depRepo := &mockAssetDependencyRepo{}

	err := SyncModelsToAssets(context.Background(), modelRepo, assetRepo, depRepo)
	require.NoError(t, err)

	assert.Len(t, assetRepo.created, 1)
	assert.Equal(t, "m2", assetRepo.created[0].ID)
	assert.Len(t, assetRepo.updated, 1)
	assert.Equal(t, "m1", assetRepo.updated[0].ID)

	assert.ElementsMatch(t, []string{"m1", "m2"}, depRepo.deletedByAsset)
	require.Len(t, depRepo.created, 1)
	assert.Equal(t, "m2", depRepo.created[0].AssetID)
	assert.Equal(t, "m1", depRepo.created[0].UpstreamAssetID)
}

func TestSyncModelsToAssets_ListModelsError(t *testing.T) {
	modelRepo := &mockModelRepo{listAllFn: func(context.Context) ([]domain.Model, error) {
		return nil, errors.New("boom")
	}}

	err := SyncModelsToAssets(context.Background(), modelRepo, &mockDataAssetRepo{}, &mockAssetDependencyRepo{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list models")
}

func TestSyncModelsToAssets_GetAssetUnexpectedError(t *testing.T) {
	modelRepo := &mockModelRepo{
		listAllFn: func(context.Context) ([]domain.Model, error) {
			return []domain.Model{{ID: "m1", ProjectName: "sales", Name: "stg_orders", CreatedBy: "alice"}}, nil
		},
	}
	assetRepo := &mockDataAssetRepo{getByIDErr: errors.New("db unavailable")}

	err := SyncModelsToAssets(context.Background(), modelRepo, assetRepo, &mockAssetDependencyRepo{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get asset")
	assert.Empty(t, assetRepo.created)
	assert.Empty(t, assetRepo.updated)
}

type mockModelRepo struct {
	listAllFn func(ctx context.Context) ([]domain.Model, error)
}

func (m *mockModelRepo) Create(context.Context, *domain.Model) (*domain.Model, error) {
	panic("unexpected call")
}

func (m *mockModelRepo) CreateWithNotebookLink(context.Context, *domain.Model, string, string) (*domain.Model, error) {
	panic("unexpected call")
}

func (m *mockModelRepo) GetByID(context.Context, string) (*domain.Model, error) {
	panic("unexpected call")
}

func (m *mockModelRepo) GetByName(context.Context, string, string) (*domain.Model, error) {
	panic("unexpected call")
}

func (m *mockModelRepo) List(context.Context, *string, domain.PageRequest) ([]domain.Model, int64, error) {
	panic("unexpected call")
}

func (m *mockModelRepo) Update(context.Context, string, domain.UpdateModelRequest) (*domain.Model, error) {
	panic("unexpected call")
}

func (m *mockModelRepo) Delete(context.Context, string) error {
	panic("unexpected call")
}

func (m *mockModelRepo) ListAll(ctx context.Context) ([]domain.Model, error) {
	if m.listAllFn != nil {
		return m.listAllFn(ctx)
	}
	panic("unexpected call")
}

func (m *mockModelRepo) UpdateDependencies(context.Context, string, []string) error {
	panic("unexpected call")
}

type mockDataAssetRepo struct {
	existing   map[string]domain.DataAsset
	created    []domain.DataAsset
	updated    []domain.DataAsset
	getByIDErr error
}

func (m *mockDataAssetRepo) Create(_ context.Context, a *domain.DataAsset) (*domain.DataAsset, error) {
	m.created = append(m.created, *a)
	if m.existing == nil {
		m.existing = map[string]domain.DataAsset{}
	}
	m.existing[a.ID] = *a
	return a, nil
}

func (m *mockDataAssetRepo) GetByID(_ context.Context, id string) (*domain.DataAsset, error) {
	if m.getByIDErr != nil {
		return nil, m.getByIDErr
	}
	a, ok := m.existing[id]
	if !ok {
		return nil, domain.ErrNotFound("asset %s not found", id)
	}
	return &a, nil
}

func (m *mockDataAssetRepo) GetByKey(context.Context, string) (*domain.DataAsset, error) {
	panic("unexpected call")
}

func (m *mockDataAssetRepo) List(context.Context, domain.AssetFilter) ([]domain.DataAsset, int64, error) {
	panic("unexpected call")
}

func (m *mockDataAssetRepo) Update(_ context.Context, id string, a *domain.DataAsset) (*domain.DataAsset, error) {
	m.updated = append(m.updated, *a)
	m.existing[id] = *a
	return a, nil
}

func (m *mockDataAssetRepo) Delete(context.Context, string) error {
	panic("unexpected call")
}

type mockAssetDependencyRepo struct {
	created        []domain.AssetDependency
	deletedByAsset []string
}

func (m *mockAssetDependencyRepo) Create(_ context.Context, d *domain.AssetDependency) (*domain.AssetDependency, error) {
	m.created = append(m.created, *d)
	return d, nil
}

func (m *mockAssetDependencyRepo) ListUpstream(context.Context, string) ([]domain.AssetDependency, error) {
	panic("unexpected call")
}

func (m *mockAssetDependencyRepo) ListDownstream(context.Context, string) ([]domain.AssetDependency, error) {
	panic("unexpected call")
}

func (m *mockAssetDependencyRepo) Delete(context.Context, string) error {
	panic("unexpected call")
}

func (m *mockAssetDependencyRepo) DeleteByAsset(_ context.Context, assetID string) error {
	m.deletedByAsset = append(m.deletedByAsset, assetID)
	return nil
}

func dependencyPairs(deps []domain.AssetDependency) []string {
	out := make([]string, 0, len(deps))
	for _, dep := range deps {
		out = append(out, dep.AssetID+"->"+dep.UpstreamAssetID)
	}
	return out
}
