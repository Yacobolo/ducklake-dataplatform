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

	adapted, err := BuildModelAssetGraph(models, map[string]domain.NotebookModelLink{
		"m2": {NotebookID: "nb-1", OutputCellID: "cell-out"},
	})
	require.NoError(t, err)
	require.Len(t, adapted.Assets, 2)
	require.Len(t, adapted.Dependencies, 2)

	assert.Equal(t, "model.sales.stg_orders", adapted.Assets[0].AssetKey)
	assert.Equal(t, "alice", adapted.Assets[0].Owner)
	assert.Equal(t, "model.sales.fct_orders", adapted.Assets[1].AssetKey)
	assert.Equal(t, "analytics", adapted.Assets[1].Owner)

	assert.ElementsMatch(t, []string{"m2->m1", "m2->cell-out"}, dependencyPairs(adapted.Dependencies))
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

func TestBuildNotebookOutputAssetGraph(t *testing.T) {
	notebooks := []domain.Notebook{
		{ID: "nb-1", Name: "Exec Summary", Owner: "alice"},
	}
	links := []domain.NotebookModelLink{
		{NotebookID: "nb-1", ModelID: "model-1", OutputCellID: "cell-out"},
	}

	adapted, err := BuildNotebookOutputAssetGraph(notebooks, links)
	require.NoError(t, err)
	require.Len(t, adapted.Assets, 1)
	assert.Equal(t, "notebook_output.nb-1.cell-out", adapted.Assets[0].AssetKey)
	assert.Equal(t, domain.AssetTypeNotebookOutput, adapted.Assets[0].AssetType)
	assert.ElementsMatch(t, []string{"cell-out->nb-1"}, dependencyPairs(adapted.Dependencies))
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
		"sem-1": {{ID: "preagg-1", Name: "daily_revenue", CreatedBy: "alice", TargetRelation: "analytics.daily_revenue", MetricSet: []string{"revenue"}}},
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

	assert.ElementsMatch(t, []string{"sem-1->model-1", "metric-1->sem-1", "metric-1->preagg-1", "preagg-1->sem-1"}, dependencyPairs(adapted.Dependencies))
}

func TestBuildDashboardAssetGraph(t *testing.T) {
	dashboards := []domain.Dashboard{
		{ID: "dash-1", Name: "Revenue", Owner: "alice"},
	}
	widgetsByDashboard := map[string][]domain.DashboardWidget{
		"dash-1": {
			{
				ID: "widget-0",
				Source: domain.DashboardWidgetSource{
					Kind: domain.DashboardWidgetSourceSQLQuery,
					SQLQuery: &domain.DashboardSQLQuerySource{
						SQL:    "select * from fct_orders",
						Schema: stringPtr("sales"),
					},
				},
			},
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
	models := []domain.Model{{ID: "model-1", ProjectName: "sales", Name: "fct_orders"}}
	notebooks := []domain.Notebook{{ID: "nb-1", Name: "Notebook", Owner: "alice"}}
	linksByNotebookID := map[string]domain.NotebookModelLink{
		"nb-1": {NotebookID: "nb-1", OutputCellID: "cell-1", ModelID: "model-1"},
	}
	semanticModels := []domain.SemanticModel{{ID: "sem-1", ProjectName: "sales", Name: "orders"}}
	metricsByModel := map[string][]domain.SemanticMetric{
		"sem-1": {{ID: "metric-1", Name: "revenue"}},
	}
	preAggsByModel := map[string][]domain.SemanticPreAggregation{
		"sem-1": {{
			ID:           "preagg-1",
			Name:         "revenue_by_day",
			MetricSet:    []string{"revenue"},
			DimensionSet: []string{},
		}},
	}

	adapted, err := BuildDashboardAssetGraph(dashboards, widgetsByDashboard, models, notebooks, linksByNotebookID, semanticModels, metricsByModel, preAggsByModel)
	require.NoError(t, err)
	require.Len(t, adapted.Assets, 1)
	assert.Equal(t, "dashboard.dash-1", adapted.Assets[0].AssetKey)
	assert.Equal(t, domain.AssetTypeDashboard, adapted.Assets[0].AssetType)
	assert.ElementsMatch(t, []string{"dash-1->model-1", "dash-1->preagg-1", "dash-1->cell-1"}, dependencyPairs(adapted.Dependencies))
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

	err := SyncModelsToAssets(context.Background(), modelRepo, &mockNotebookModelLinkRepo{}, assetRepo, depRepo, "prod-models")
	require.NoError(t, err)

	assert.Len(t, assetRepo.created, 1)
	assert.Equal(t, "m2", assetRepo.created[0].ID)
	assert.Equal(t, "prod-models", assetRepo.created[0].ProductID)
	assert.Len(t, assetRepo.updated, 1)
	assert.Equal(t, "m1", assetRepo.updated[0].ID)
	assert.Equal(t, "prod-models", assetRepo.updated[0].ProductID)

	assert.ElementsMatch(t, []string{"m1", "m2"}, depRepo.deletedByAsset)
	require.Len(t, depRepo.created, 1)
	assert.Equal(t, "m2", depRepo.created[0].AssetID)
	assert.Equal(t, "m1", depRepo.created[0].UpstreamAssetID)
}

func TestSyncModelsToAssets_PreservesManagedPolicies(t *testing.T) {
	modelRepo := &mockModelRepo{
		listAllFn: func(context.Context) ([]domain.Model, error) {
			return []domain.Model{{
				ID:          "m1",
				ProjectName: "sales",
				Name:        "orders",
				CreatedBy:   "alice",
			}}, nil
		},
	}
	dynamicGroup := "tenant_id"
	assetRepo := &mockDataAssetRepo{existing: map[string]domain.DataAsset{
		"m1": {
			ID:         "m1",
			AssetKey:   "model.sales.orders",
			AssetType:  domain.AssetTypeModel,
			CreatedBy:  "alice",
			IOProfile:  "warehouse",
			SchemaJSON: map[string]any{"version": 1},
			FreshnessPolicy: &domain.AssetFreshnessPolicy{
				MaxLagSeconds: 1800,
				CronSchedule:  "*/30 * * * *",
			},
			MaterializationPolicy: &domain.AssetMaterializationPolicy{
				Mode:            "MANUAL",
				AllowConcurrent: true,
			},
			AutoMaterializePolicy: &domain.AssetAutoMaterializePolicy{
				Mode:                    "AUTO",
				MinIntervalSeconds:      300,
				OnFreshnessBreach:       true,
				DowntimeWindowsCronExpr: []string{"0 0 * * 0"},
			},
			PartitionDefinition: &domain.PartitionDefinition{
				Type:         domain.PartitionTypeDynamic,
				DynamicGroup: &dynamicGroup,
			},
		},
	}}
	depRepo := &mockAssetDependencyRepo{}

	err := SyncModelsToAssets(context.Background(), modelRepo, &mockNotebookModelLinkRepo{}, assetRepo, depRepo, "prod-models")
	require.NoError(t, err)
	require.Len(t, assetRepo.updated, 1)

	updated := assetRepo.updated[0]
	require.NotNil(t, updated.FreshnessPolicy)
	assert.EqualValues(t, 1800, updated.FreshnessPolicy.MaxLagSeconds)
	require.NotNil(t, updated.MaterializationPolicy)
	assert.Equal(t, "MANUAL", updated.MaterializationPolicy.Mode)
	require.NotNil(t, updated.AutoMaterializePolicy)
	assert.True(t, updated.AutoMaterializePolicy.OnFreshnessBreach)
	assert.Equal(t, []string{"0 0 * * 0"}, updated.AutoMaterializePolicy.DowntimeWindowsCronExpr)
	require.NotNil(t, updated.PartitionDefinition)
	require.NotNil(t, updated.PartitionDefinition.DynamicGroup)
	assert.Equal(t, "tenant_id", *updated.PartitionDefinition.DynamicGroup)
	assert.Equal(t, "warehouse", updated.IOProfile)
	assert.Equal(t, map[string]any{"version": 1}, updated.SchemaJSON)
}

func TestSyncModelsToAssets_ListModelsError(t *testing.T) {
	modelRepo := &mockModelRepo{listAllFn: func(context.Context) ([]domain.Model, error) {
		return nil, errors.New("boom")
	}}

	err := SyncModelsToAssets(context.Background(), modelRepo, &mockNotebookModelLinkRepo{}, &mockDataAssetRepo{}, &mockAssetDependencyRepo{}, "prod-models")
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

	err := SyncModelsToAssets(context.Background(), modelRepo, &mockNotebookModelLinkRepo{}, assetRepo, &mockAssetDependencyRepo{}, "prod-models")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get asset")
	assert.Empty(t, assetRepo.created)
	assert.Empty(t, assetRepo.updated)
}

func TestSyncNotebookOutputsToAssets(t *testing.T) {
	notebookRepo := &mockNotebookRepo{
		notebooks: []domain.Notebook{
			{ID: "nb-1", Name: "Exec Summary", Owner: "alice"},
		},
	}
	linkRepo := &mockNotebookModelLinkRepo{
		byNotebookID: map[string]domain.NotebookModelLink{
			"nb-1": {NotebookID: "nb-1", ModelID: "model-1", OutputCellID: "cell-out"},
		},
	}
	assetRepo := &mockDataAssetRepo{}
	depRepo := &mockAssetDependencyRepo{}

	err := SyncNotebookOutputsToAssets(context.Background(), notebookRepo, linkRepo, assetRepo, depRepo, "prod-notebook-outputs")
	require.NoError(t, err)
	require.Len(t, assetRepo.created, 1)
	assert.Equal(t, "cell-out", assetRepo.created[0].ID)
	assert.Equal(t, domain.AssetTypeNotebookOutput, assetRepo.created[0].AssetType)
	assert.Equal(t, "prod-notebook-outputs", assetRepo.created[0].ProductID)
	assert.ElementsMatch(t, []string{"cell-out->nb-1"}, dependencyPairs(depRepo.created))
}

func TestMatchingModelAssetIDsForSQLWidget(t *testing.T) {
	source := &domain.DashboardSQLQuerySource{
		SQL:    "select * from fct_orders join dim_customers on fct_orders.customer_id = dim_customers.id",
		Schema: stringPtr("sales"),
	}

	matches := matchingModelAssetIDsForSQLWidget(source, map[string]string{
		"sales.fct_orders":    "model-1",
		"sales.dim_customers": "model-2",
	})

	assert.Equal(t, []string{"model-1", "model-2"}, matches)
}

func stringPtr(v string) *string {
	return &v
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

type mockNotebookModelLinkRepo struct {
	byNotebookID map[string]domain.NotebookModelLink
	byModelID    map[string]domain.NotebookModelLink
}

func (m *mockNotebookModelLinkRepo) Upsert(context.Context, *domain.NotebookModelLink) error {
	panic("unexpected call")
}

func (m *mockNotebookModelLinkRepo) GetByNotebookID(_ context.Context, notebookID string) (*domain.NotebookModelLink, error) {
	link, ok := m.byNotebookID[notebookID]
	if !ok {
		return nil, domain.ErrNotFound("notebook link %s not found", notebookID)
	}
	return &link, nil
}

func (m *mockNotebookModelLinkRepo) GetByModelID(_ context.Context, modelID string) (*domain.NotebookModelLink, error) {
	link, ok := m.byModelID[modelID]
	if !ok {
		return nil, domain.ErrNotFound("notebook link for model %s not found", modelID)
	}
	return &link, nil
}

func (m *mockNotebookModelLinkRepo) DeleteByNotebookID(context.Context, string) error {
	panic("unexpected call")
}

type mockNotebookRepo struct {
	notebooks []domain.Notebook
}

func (m *mockNotebookRepo) CreateNotebook(context.Context, *domain.Notebook) (*domain.Notebook, error) {
	panic("unexpected call")
}

func (m *mockNotebookRepo) GetNotebook(context.Context, string) (*domain.Notebook, error) {
	panic("unexpected call")
}

func (m *mockNotebookRepo) ListNotebooks(_ context.Context, _ *string, _ domain.PageRequest) ([]domain.Notebook, int64, error) {
	return append([]domain.Notebook(nil), m.notebooks...), int64(len(m.notebooks)), nil
}

func (m *mockNotebookRepo) ListByFolders(_ context.Context, folderIDs []string) ([]domain.Notebook, error) {
	if len(folderIDs) == 0 {
		return []domain.Notebook{}, nil
	}
	folderSet := make(map[string]struct{}, len(folderIDs))
	for _, folderID := range folderIDs {
		folderSet[folderID] = struct{}{}
	}
	items := make([]domain.Notebook, 0, len(m.notebooks))
	for _, notebook := range m.notebooks {
		if _, ok := folderSet[notebook.FolderID]; ok {
			items = append(items, notebook)
		}
	}
	return items, nil
}

func (m *mockNotebookRepo) UpdateNotebook(context.Context, string, domain.UpdateNotebookRequest) (*domain.Notebook, error) {
	panic("unexpected call")
}

func (m *mockNotebookRepo) UpdateNotebookSync(context.Context, *domain.Notebook) (*domain.Notebook, error) {
	panic("unexpected call")
}

func (m *mockNotebookRepo) DeleteNotebook(context.Context, string) error {
	panic("unexpected call")
}

func (m *mockNotebookRepo) CreateCell(context.Context, *domain.Cell) (*domain.Cell, error) {
	panic("unexpected call")
}

func (m *mockNotebookRepo) GetCell(context.Context, string) (*domain.Cell, error) {
	panic("unexpected call")
}

func (m *mockNotebookRepo) ListCells(context.Context, string) ([]domain.Cell, error) {
	panic("unexpected call")
}

func (m *mockNotebookRepo) UpdateCell(context.Context, string, domain.UpdateCellRequest) (*domain.Cell, error) {
	panic("unexpected call")
}

func (m *mockNotebookRepo) UpdateCellSync(context.Context, *domain.Cell) (*domain.Cell, error) {
	panic("unexpected call")
}

func (m *mockNotebookRepo) DeleteCell(context.Context, string) error {
	panic("unexpected call")
}

func (m *mockNotebookRepo) UpdateCellResult(context.Context, string, *string) error {
	panic("unexpected call")
}

func (m *mockNotebookRepo) ReorderCells(context.Context, string, []string) error {
	panic("unexpected call")
}

func (m *mockNotebookRepo) GetMaxPosition(context.Context, string) (int, error) {
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
