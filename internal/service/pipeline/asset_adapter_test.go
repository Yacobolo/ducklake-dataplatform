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
	existing map[string]domain.DataAsset
	created  []domain.DataAsset
	updated  []domain.DataAsset
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
