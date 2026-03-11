package asset

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestService_CheckFreshness_ExecutableFresh(t *testing.T) {
	t.Parallel()

	assetRepo := &fakeAssetRepo{assetsByID: map[string]domain.DataAsset{}, idsByKey: map[string]string{}}
	depRepo := &fakeAssetDependencyRepo{}
	runRepo := &fakeAssetRunRepo{
		materializationsByAsset: map[string][]domain.AssetMaterialization{
			"asset-1": {{
				AssetID:        "asset-1",
				MaterializedAt: time.Now().UTC().Add(-5 * time.Minute),
			}},
		},
	}

	_, err := assetRepo.Create(context.Background(), &domain.DataAsset{
		ID:        "asset-1",
		AssetKey:  "model.sales.orders",
		AssetType: domain.AssetTypeModel,
		Owner:     "alice",
		FreshnessPolicy: &domain.AssetFreshnessPolicy{
			MaxLagSeconds: 1800,
		},
	})
	require.NoError(t, err)

	svc := &Service{assets: assetRepo, deps: depRepo, runs: runRepo}
	status, err := svc.CheckFreshness(context.Background(), "model.sales.orders")
	require.NoError(t, err)
	assert.Equal(t, domain.AssetFreshnessStatusFresh, status.FreshnessStatus)
}

func TestService_ExplainFreshness_LogicalStaleFromUpstream(t *testing.T) {
	t.Parallel()

	assetRepo := &fakeAssetRepo{assetsByID: map[string]domain.DataAsset{}, idsByKey: map[string]string{}}
	depRepo := &fakeAssetDependencyRepo{}
	runRepo := &fakeAssetRunRepo{
		materializationsByAsset: map[string][]domain.AssetMaterialization{
			"metric-1": {{
				AssetID:        "metric-1",
				MaterializedAt: time.Now().UTC().Add(-2 * time.Hour),
			}},
		},
	}

	_, err := assetRepo.Create(context.Background(), &domain.DataAsset{
		ID:        "dashboard-1",
		AssetKey:  "dashboard.dashboard-1",
		AssetType: domain.AssetTypeDashboard,
		Owner:     "alice",
		FreshnessPolicy: &domain.AssetFreshnessPolicy{
			MaxLagSeconds: 1800,
		},
	})
	require.NoError(t, err)
	_, err = assetRepo.Create(context.Background(), &domain.DataAsset{
		ID:        "metric-1",
		AssetKey:  "metric.sales.orders.revenue",
		AssetType: domain.AssetTypeModel,
		Owner:     "alice",
	})
	require.NoError(t, err)
	_, err = depRepo.Create(context.Background(), &domain.AssetDependency{
		AssetID:         "dashboard-1",
		UpstreamAssetID: "metric-1",
		DependencyType:  domain.DependencyTypeHard,
	})
	require.NoError(t, err)

	svc := &Service{assets: assetRepo, deps: depRepo, runs: runRepo}
	node, err := svc.ExplainFreshness(context.Background(), "dashboard.dashboard-1")
	require.NoError(t, err)
	assert.Equal(t, domain.AssetFreshnessStatusStale, node.FreshnessStatus)
	require.Len(t, node.Upstream, 1)
	assert.Equal(t, "metric.sales.orders.revenue", node.Upstream[0].AssetKey)
	assert.Equal(t, domain.AssetFreshnessStatusStale, node.Upstream[0].FreshnessStatus)
}

func TestService_ExplainFreshness_LogicalPrefersFreshSoftServingAsset(t *testing.T) {
	t.Parallel()

	assetRepo := &fakeAssetRepo{assetsByID: map[string]domain.DataAsset{}, idsByKey: map[string]string{}}
	depRepo := &fakeAssetDependencyRepo{}
	runRepo := &fakeAssetRunRepo{
		materializationsByAsset: map[string][]domain.AssetMaterialization{
			"preagg-1": {{
				AssetID:        "preagg-1",
				MaterializedAt: time.Now().UTC().Add(-5 * time.Minute),
			}},
		},
	}

	_, err := assetRepo.Create(context.Background(), &domain.DataAsset{
		ID:        "metric-1",
		AssetKey:  "metric.sales.orders.revenue",
		AssetType: domain.AssetTypeMetric,
		Owner:     "alice",
		FreshnessPolicy: &domain.AssetFreshnessPolicy{
			MaxLagSeconds: 1800,
		},
	})
	require.NoError(t, err)
	_, err = assetRepo.Create(context.Background(), &domain.DataAsset{
		ID:        "semantic-model-1",
		AssetKey:  "semantic_model.sales.orders",
		AssetType: domain.AssetTypeSemanticModel,
		Owner:     "alice",
	})
	require.NoError(t, err)
	_, err = assetRepo.Create(context.Background(), &domain.DataAsset{
		ID:        "preagg-1",
		AssetKey:  "semantic_pre_aggregation.sales.orders.daily_revenue",
		AssetType: domain.AssetTypeSemanticPreAggregation,
		Owner:     "alice",
	})
	require.NoError(t, err)
	_, err = assetRepo.Create(context.Background(), &domain.DataAsset{
		ID:        "model-1",
		AssetKey:  "model.sales.fct_orders",
		AssetType: domain.AssetTypeModel,
		Owner:     "alice",
	})
	require.NoError(t, err)

	_, err = depRepo.Create(context.Background(), &domain.AssetDependency{
		AssetID:         "metric-1",
		UpstreamAssetID: "semantic-model-1",
		DependencyType:  domain.DependencyTypeHard,
	})
	require.NoError(t, err)
	_, err = depRepo.Create(context.Background(), &domain.AssetDependency{
		AssetID:         "semantic-model-1",
		UpstreamAssetID: "model-1",
		DependencyType:  domain.DependencyTypeHard,
	})
	require.NoError(t, err)
	_, err = depRepo.Create(context.Background(), &domain.AssetDependency{
		AssetID:         "metric-1",
		UpstreamAssetID: "preagg-1",
		DependencyType:  domain.DependencyTypeSoft,
	})
	require.NoError(t, err)

	svc := &Service{assets: assetRepo, deps: depRepo, runs: runRepo}
	node, err := svc.ExplainFreshness(context.Background(), "metric.sales.orders.revenue")
	require.NoError(t, err)
	assert.Equal(t, domain.AssetFreshnessStatusFresh, node.FreshnessStatus)
	assert.Equal(t, "served by fresh upstream semantic_pre_aggregation.sales.orders.daily_revenue", node.Reason)
	assert.Contains(t, node.Basis, "semantic_pre_aggregation.sales.orders.daily_revenue")
	require.Len(t, node.Upstream, 2)
}

type fakeAssetRunRepo struct {
	materializationsByAsset map[string][]domain.AssetMaterialization
	runsByAsset             map[string][]domain.AssetRun
}

func (f *fakeAssetRunRepo) CreateRun(context.Context, *domain.AssetRun) (*domain.AssetRun, error) {
	panic("unexpected call")
}

func (f *fakeAssetRunRepo) GetRunByID(context.Context, string) (*domain.AssetRun, error) {
	panic("unexpected call")
}

func (f *fakeAssetRunRepo) ListRuns(_ context.Context, filter domain.AssetRunFilter) ([]domain.AssetRun, int64, error) {
	if filter.AssetID == nil {
		return nil, 0, nil
	}
	runs := f.runsByAsset[*filter.AssetID]
	if filter.Status == nil {
		return runs, int64(len(runs)), nil
	}
	out := make([]domain.AssetRun, 0, len(runs))
	for _, run := range runs {
		if run.Status == *filter.Status {
			out = append(out, run)
		}
	}
	return out, int64(len(out)), nil
}

func (f *fakeAssetRunRepo) UpdateRunStarted(context.Context, string) error {
	panic("unexpected call")
}

func (f *fakeAssetRunRepo) UpdateRunFinished(context.Context, string, string, *string) error {
	panic("unexpected call")
}

func (f *fakeAssetRunRepo) UpdateRunRetrying(context.Context, string, int, *string) error {
	panic("unexpected call")
}

func (f *fakeAssetRunRepo) CreateRunEvent(context.Context, *domain.AssetRunEvent) (*domain.AssetRunEvent, error) {
	panic("unexpected call")
}

func (f *fakeAssetRunRepo) ListRunEvents(context.Context, string, domain.PageRequest) ([]domain.AssetRunEvent, int64, error) {
	panic("unexpected call")
}

func (f *fakeAssetRunRepo) CreateMaterialization(context.Context, *domain.AssetMaterialization) (*domain.AssetMaterialization, error) {
	panic("unexpected call")
}

func (f *fakeAssetRunRepo) ListMaterializationsByAsset(_ context.Context, assetID string, _ domain.PageRequest) ([]domain.AssetMaterialization, int64, error) {
	items := f.materializationsByAsset[assetID]
	return items, int64(len(items)), nil
}
