package asset

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
)

func TestService_CreateAsset_ReconcilesDependenciesAndChecks(t *testing.T) {
	t.Parallel()

	assets := &fakeAssetRepo{assetsByID: map[string]domain.DataAsset{}, idsByKey: map[string]string{}}
	deps := &fakeAssetDependencyRepo{}
	checks := &fakeAssetCheckRepo{checksByAsset: map[string][]domain.AssetCheck{}}

	_, err := assets.Create(adminCtx(), &domain.DataAsset{AssetKey: "showcase.rides.raw", AssetType: domain.AssetTypeTable, Owner: "platform-admins", CreatedBy: "tester", IsActive: true, SchemaJSON: map[string]any{}})
	require.NoError(t, err)

	svc := &Service{assets: assets, deps: deps, checks: checks}
	created, err := svc.CreateAsset(adminCtx(), domain.CreateAssetRequest{
		AssetKey:              "showcase.rides.bronze",
		AssetType:             domain.AssetTypeTable,
		Owner:                 "data-engineers",
		Description:           "Bronze showcase asset",
		Tags:                  []string{"showcase", "bronze"},
		FreshnessPolicy:       &domain.AssetFreshnessPolicy{MaxLagSeconds: 1800},
		MaterializationPolicy: &domain.AssetMaterializationPolicy{Mode: "TABLE"},
		AutoMaterializePolicy: &domain.AssetAutoMaterializePolicy{Mode: "AUTO", OnFreshnessBreach: true},
		IOProfile:             "duckdb",
		IsActive:              true,
		UpstreamAssetKeys:     []string{"showcase.rides.raw"},
		Checks:                []domain.AssetCheckInput{{Name: "bronze_non_empty", CheckType: "SQL_ASSERT", Enabled: true}},
	})
	require.NoError(t, err)
	assert.Equal(t, "showcase.rides.bronze", created.AssetKey)
	require.NotNil(t, created.FreshnessPolicy)
	assert.EqualValues(t, 1800, created.FreshnessPolicy.MaxLagSeconds)
	require.NotNil(t, created.MaterializationPolicy)
	assert.Equal(t, "TABLE", created.MaterializationPolicy.Mode)
	require.Len(t, deps.byAsset[created.ID], 1)
	assert.Equal(t, assets.idsByKey["showcase.rides.raw"], deps.byAsset[created.ID][0].UpstreamAssetID)
	require.Len(t, checks.checksByAsset[created.ID], 1)
	assert.Equal(t, "bronze_non_empty", checks.checksByAsset[created.ID][0].Name)
}

func TestService_UpdateAsset_ReplacesChecks(t *testing.T) {
	t.Parallel()

	assets := &fakeAssetRepo{assetsByID: map[string]domain.DataAsset{}, idsByKey: map[string]string{}}
	deps := &fakeAssetDependencyRepo{}
	checks := &fakeAssetCheckRepo{checksByAsset: map[string][]domain.AssetCheck{}}

	asset, err := assets.Create(adminCtx(), &domain.DataAsset{AssetKey: "showcase.rides.gold", AssetType: domain.AssetTypeTable, Owner: "analytics", CreatedBy: "tester", IsActive: true, SchemaJSON: map[string]any{}})
	require.NoError(t, err)
	_, err = checks.CreateCheck(adminCtx(), &domain.AssetCheck{AssetID: asset.ID, Name: "old_check", CheckType: "SQL_ASSERT", Enabled: true, ConfigJSON: map[string]any{}})
	require.NoError(t, err)

	svc := &Service{assets: assets, deps: deps, checks: checks}
	updated, err := svc.UpdateAsset(adminCtx(), "showcase.rides.gold", domain.UpdateAssetRequest{
		AssetType:             domain.AssetTypeTable,
		Owner:                 "analytics",
		FreshnessPolicy:       &domain.AssetFreshnessPolicy{MaxLagSeconds: 600},
		AutoMaterializePolicy: &domain.AssetAutoMaterializePolicy{Mode: "AUTO", OnUpstreamMaterialized: true},
		Checks: []domain.AssetCheckInput{{
			Name:      "gold_non_empty",
			CheckType: "SQL_ASSERT",
			Severity:  "ERROR",
			Enabled:   true,
		}},
		IsActive: true,
	})
	require.NoError(t, err)
	assert.Equal(t, updated.ID, asset.ID)
	require.NotNil(t, updated.FreshnessPolicy)
	assert.EqualValues(t, 600, updated.FreshnessPolicy.MaxLagSeconds)
	require.Len(t, checks.checksByAsset[asset.ID], 1)
	assert.Equal(t, "gold_non_empty", checks.checksByAsset[asset.ID][0].Name)
}

func TestService_CreateAsset_RollsBackWhenUpstreamResolutionFails(t *testing.T) {
	t.Parallel()

	assets := &fakeAssetRepo{assetsByID: map[string]domain.DataAsset{}, idsByKey: map[string]string{}}
	deps := &fakeAssetDependencyRepo{}
	checks := &fakeAssetCheckRepo{checksByAsset: map[string][]domain.AssetCheck{}}

	svc := &Service{assets: assets, deps: deps, checks: checks}
	created, err := svc.CreateAsset(adminCtx(), domain.CreateAssetRequest{
		AssetKey:          "showcase.rides.failed",
		AssetType:         domain.AssetTypeTable,
		Owner:             "data-engineers",
		IsActive:          true,
		UpstreamAssetKeys: []string{"showcase.rides.missing"},
		Checks:            []domain.AssetCheckInput{{Name: "will_be_rolled_back", CheckType: "SQL_ASSERT", Enabled: true}},
	})
	require.Error(t, err)
	require.Nil(t, created)
	assert.Contains(t, err.Error(), "resolve upstream asset")

	_, getErr := assets.GetByKey(adminCtx(), "showcase.rides.failed")
	require.Error(t, getErr)
	var notFoundErr *domain.NotFoundError
	require.ErrorAs(t, getErr, &notFoundErr)
	assert.Empty(t, deps.byAsset)
	assert.Empty(t, checks.checksByAsset)
}

func TestService_ReconcileFreshness_EnqueuesNearestExecutableTarget(t *testing.T) {
	t.Parallel()

	assets := &fakeAssetRepo{assetsByID: map[string]domain.DataAsset{}, idsByKey: map[string]string{}}
	deps := &fakeAssetDependencyRepo{}
	runs := &fakeAssetRunRepo{materializationsByAsset: map[string][]domain.AssetMaterialization{}}
	events := &fakeOrchestrationEventRepo{}

	dashboard, err := assets.Create(adminCtx(), &domain.DataAsset{
		ID:        "dashboard-1",
		AssetKey:  "dashboard.exec",
		AssetType: domain.AssetTypeDashboard,
		Owner:     "alice",
		FreshnessPolicy: &domain.AssetFreshnessPolicy{
			MaxLagSeconds: 1800,
		},
		SchemaJSON: map[string]any{},
	})
	require.NoError(t, err)
	metric, err := assets.Create(adminCtx(), &domain.DataAsset{
		ID:         "metric-1",
		AssetKey:   "metric.exec.revenue",
		AssetType:  domain.AssetTypeMetric,
		Owner:      "alice",
		SchemaJSON: map[string]any{},
	})
	require.NoError(t, err)
	model, err := assets.Create(adminCtx(), &domain.DataAsset{
		ID:         "model-1",
		AssetKey:   "model.exec.revenue",
		AssetType:  domain.AssetTypeModel,
		Owner:      "alice",
		SchemaJSON: map[string]any{},
	})
	require.NoError(t, err)

	_, err = deps.Create(adminCtx(), &domain.AssetDependency{AssetID: dashboard.ID, UpstreamAssetID: metric.ID, DependencyType: domain.DependencyTypeHard})
	require.NoError(t, err)
	_, err = deps.Create(adminCtx(), &domain.AssetDependency{AssetID: metric.ID, UpstreamAssetID: model.ID, DependencyType: domain.DependencyTypeHard})
	require.NoError(t, err)

	svc := &Service{assets: assets, deps: deps, runs: runs, events: events}
	result, err := svc.ReconcileFreshness(context.Background(), dashboard.AssetKey)
	require.NoError(t, err)
	require.Len(t, result.Targets, 1)
	assert.Equal(t, model.AssetKey, result.Targets[0].AssetKey)
	require.Len(t, events.events, 1)
	assert.Equal(t, domain.AssetTriggerTypeFreshnessBreach, events.events[0].EventType)
	require.NotNil(t, events.events[0].AssetID)
	assert.Equal(t, model.ID, *events.events[0].AssetID)
}

func TestService_ReconcileFreshnessPolicies_DedupesSharedTargetEvents(t *testing.T) {
	t.Parallel()

	assets := &fakeAssetRepo{assetsByID: map[string]domain.DataAsset{}, idsByKey: map[string]string{}}
	deps := &fakeAssetDependencyRepo{}
	runs := &fakeAssetRunRepo{materializationsByAsset: map[string][]domain.AssetMaterialization{}}
	events := &fakeOrchestrationEventRepo{}

	model, err := assets.Create(adminCtx(), &domain.DataAsset{
		ID:         "model-1",
		AssetKey:   "model.shared.base",
		AssetType:  domain.AssetTypeModel,
		Owner:      "alice",
		SchemaJSON: map[string]any{},
	})
	require.NoError(t, err)
	for _, key := range []string{"dashboard.a", "dashboard.b"} {
		dash, createErr := assets.Create(adminCtx(), &domain.DataAsset{
			AssetKey:  key,
			AssetType: domain.AssetTypeDashboard,
			Owner:     "alice",
			FreshnessPolicy: &domain.AssetFreshnessPolicy{
				MaxLagSeconds: 1800,
			},
			SchemaJSON: map[string]any{},
		})
		require.NoError(t, createErr)
		_, createErr = deps.Create(adminCtx(), &domain.AssetDependency{
			AssetID:         dash.ID,
			UpstreamAssetID: model.ID,
			DependencyType:  domain.DependencyTypeHard,
		})
		require.NoError(t, createErr)
	}

	svc := &Service{assets: assets, deps: deps, runs: runs, events: events}
	count, err := svc.ReconcileFreshnessPolicies(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	require.Len(t, events.events, 1)
	require.NotNil(t, events.events[0].IdempotencyKey)
	assert.True(t, strings.HasPrefix(*events.events[0].IdempotencyKey, "freshness:model-1:"))
}

func TestService_ReconcileFreshness_SkipsStaleLineageWhenServingPreAggregationIsFresh(t *testing.T) {
	t.Parallel()

	assets := &fakeAssetRepo{assetsByID: map[string]domain.DataAsset{}, idsByKey: map[string]string{}}
	deps := &fakeAssetDependencyRepo{}
	runs := &fakeAssetRunRepo{
		materializationsByAsset: map[string][]domain.AssetMaterialization{
			"preagg-1": {{
				AssetID:        "preagg-1",
				MaterializedAt: time.Now().UTC().Add(-5 * time.Minute),
			}},
		},
	}
	events := &fakeOrchestrationEventRepo{}

	metric, err := assets.Create(adminCtx(), &domain.DataAsset{
		ID:        "metric-1",
		AssetKey:  "metric.exec.revenue",
		AssetType: domain.AssetTypeMetric,
		Owner:     "alice",
		FreshnessPolicy: &domain.AssetFreshnessPolicy{
			MaxLagSeconds: 1800,
		},
		SchemaJSON: map[string]any{},
	})
	require.NoError(t, err)
	semanticModel, err := assets.Create(adminCtx(), &domain.DataAsset{
		ID:         "semantic-model-1",
		AssetKey:   "semantic_model.exec.orders",
		AssetType:  domain.AssetTypeSemanticModel,
		Owner:      "alice",
		SchemaJSON: map[string]any{},
	})
	require.NoError(t, err)
	model, err := assets.Create(adminCtx(), &domain.DataAsset{
		ID:         "model-1",
		AssetKey:   "model.exec.orders",
		AssetType:  domain.AssetTypeModel,
		Owner:      "alice",
		SchemaJSON: map[string]any{},
	})
	require.NoError(t, err)
	preAgg, err := assets.Create(adminCtx(), &domain.DataAsset{
		ID:         "preagg-1",
		AssetKey:   "semantic_pre_aggregation.exec.orders.daily_revenue",
		AssetType:  domain.AssetTypeSemanticPreAggregation,
		Owner:      "alice",
		SchemaJSON: map[string]any{},
	})
	require.NoError(t, err)

	_, err = deps.Create(adminCtx(), &domain.AssetDependency{AssetID: metric.ID, UpstreamAssetID: semanticModel.ID, DependencyType: domain.DependencyTypeHard})
	require.NoError(t, err)
	_, err = deps.Create(adminCtx(), &domain.AssetDependency{AssetID: semanticModel.ID, UpstreamAssetID: model.ID, DependencyType: domain.DependencyTypeHard})
	require.NoError(t, err)
	_, err = deps.Create(adminCtx(), &domain.AssetDependency{AssetID: metric.ID, UpstreamAssetID: preAgg.ID, DependencyType: domain.DependencyTypeSoft})
	require.NoError(t, err)

	svc := &Service{assets: assets, deps: deps, runs: runs, events: events}
	result, err := svc.ReconcileFreshness(context.Background(), metric.AssetKey)
	require.NoError(t, err)
	assert.Empty(t, result.Targets)
	assert.Empty(t, events.events)
}

func TestService_TriggerMaterialization_RejectsUnsupportedAssetType(t *testing.T) {
	t.Parallel()

	assets := &fakeAssetRepo{assetsByID: map[string]domain.DataAsset{}, idsByKey: map[string]string{}}
	events := &fakeOrchestrationEventRepo{}

	dashboard, err := assets.Create(adminCtx(), &domain.DataAsset{
		ID:         "dashboard-1",
		AssetKey:   "dashboard.exec",
		AssetType:  domain.AssetTypeDashboard,
		Owner:      "alice",
		SchemaJSON: map[string]any{},
	})
	require.NoError(t, err)

	svc := &Service{assets: assets, events: events}
	_, err = svc.TriggerMaterialization(adminCtx(), dashboard.ID, nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support materialization")
	assert.Empty(t, events.events)
}

func adminCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "tester", IsAdmin: true, Type: "user"})
}

type fakeAssetRepo struct {
	assetsByID map[string]domain.DataAsset
	idsByKey   map[string]string
	seq        int
}

func (f *fakeAssetRepo) Create(_ context.Context, a *domain.DataAsset) (*domain.DataAsset, error) {
	f.seq++
	if _, exists := f.idsByKey[a.AssetKey]; exists {
		return nil, domain.ErrConflict("asset %q already exists", a.AssetKey)
	}
	id := a.ID
	if id == "" {
		id = domain.NewID()
	}
	now := time.Now().UTC()
	assetCopy := *a
	assetCopy.ID = id
	assetCopy.CreatedAt = now
	assetCopy.UpdatedAt = now
	if assetCopy.SchemaJSON == nil {
		assetCopy.SchemaJSON = map[string]any{}
	}
	f.assetsByID[id] = assetCopy
	f.idsByKey[assetCopy.AssetKey] = id
	return &assetCopy, nil
}

func (f *fakeAssetRepo) GetByID(_ context.Context, id string) (*domain.DataAsset, error) {
	asset, ok := f.assetsByID[id]
	if !ok {
		return nil, domain.ErrNotFound("asset %q not found", id)
	}
	assetCopy := asset
	return &assetCopy, nil
}

func (f *fakeAssetRepo) GetByKey(_ context.Context, assetKey string) (*domain.DataAsset, error) {
	id, ok := f.idsByKey[assetKey]
	if !ok {
		return nil, domain.ErrNotFound("asset %q not found", assetKey)
	}
	return f.GetByID(context.Background(), id)
}

func (f *fakeAssetRepo) List(_ context.Context, _ domain.AssetFilter) ([]domain.DataAsset, int64, error) {
	out := make([]domain.DataAsset, 0, len(f.assetsByID))
	for _, asset := range f.assetsByID {
		out = append(out, asset)
	}
	return out, int64(len(out)), nil
}

func (f *fakeAssetRepo) Update(_ context.Context, id string, a *domain.DataAsset) (*domain.DataAsset, error) {
	current, ok := f.assetsByID[id]
	if !ok {
		return nil, domain.ErrNotFound("asset %q not found", id)
	}
	updated := *a
	updated.ID = id
	updated.CreatedAt = current.CreatedAt
	updated.UpdatedAt = time.Now().UTC()
	f.assetsByID[id] = updated
	f.idsByKey[updated.AssetKey] = id
	return &updated, nil
}

func (f *fakeAssetRepo) Delete(_ context.Context, id string) error {
	asset, ok := f.assetsByID[id]
	if !ok {
		return domain.ErrNotFound("asset %q not found", id)
	}
	delete(f.assetsByID, id)
	delete(f.idsByKey, asset.AssetKey)
	return nil
}

type fakeAssetDependencyRepo struct {
	byAsset map[string][]domain.AssetDependency
}

func (f *fakeAssetDependencyRepo) Create(_ context.Context, d *domain.AssetDependency) (*domain.AssetDependency, error) {
	if f.byAsset == nil {
		f.byAsset = map[string][]domain.AssetDependency{}
	}
	depCopy := *d
	depCopy.ID = domain.NewID()
	f.byAsset[d.AssetID] = append(f.byAsset[d.AssetID], depCopy)
	return &depCopy, nil
}

func (f *fakeAssetDependencyRepo) ListUpstream(_ context.Context, assetID string) ([]domain.AssetDependency, error) {
	return append([]domain.AssetDependency{}, f.byAsset[assetID]...), nil
}

func (f *fakeAssetDependencyRepo) ListDownstream(_ context.Context, upstreamAssetID string) ([]domain.AssetDependency, error) {
	out := make([]domain.AssetDependency, 0)
	for _, deps := range f.byAsset {
		for _, dep := range deps {
			if dep.UpstreamAssetID == upstreamAssetID {
				out = append(out, dep)
			}
		}
	}
	return out, nil
}

func (f *fakeAssetDependencyRepo) Delete(_ context.Context, id string) error { return nil }

func (f *fakeAssetDependencyRepo) DeleteByAsset(_ context.Context, assetID string) error {
	if f.byAsset == nil {
		return nil
	}
	delete(f.byAsset, assetID)
	return nil
}

type fakeAssetCheckRepo struct {
	checksByAsset map[string][]domain.AssetCheck
}

func (f *fakeAssetCheckRepo) CreateCheck(_ context.Context, c *domain.AssetCheck) (*domain.AssetCheck, error) {
	checkCopy := *c
	checkCopy.ID = domain.NewID()
	checkCopy.CreatedAt = time.Now().UTC()
	checkCopy.UpdatedAt = checkCopy.CreatedAt
	if checkCopy.ConfigJSON == nil {
		checkCopy.ConfigJSON = map[string]any{}
	}
	f.checksByAsset[c.AssetID] = append(f.checksByAsset[c.AssetID], checkCopy)
	return &checkCopy, nil
}

func (f *fakeAssetCheckRepo) GetCheckByID(_ context.Context, id string) (*domain.AssetCheck, error) {
	for _, checks := range f.checksByAsset {
		for _, check := range checks {
			if check.ID == id {
				checkCopy := check
				return &checkCopy, nil
			}
		}
	}
	return nil, domain.ErrNotFound("check %q not found", id)
}

func (f *fakeAssetCheckRepo) ListChecksByAsset(_ context.Context, assetID string) ([]domain.AssetCheck, error) {
	return append([]domain.AssetCheck{}, f.checksByAsset[assetID]...), nil
}

func (f *fakeAssetCheckRepo) UpdateCheck(_ context.Context, id string, c *domain.AssetCheck) (*domain.AssetCheck, error) {
	for assetID, checks := range f.checksByAsset {
		for i := range checks {
			if checks[i].ID == id {
				checks[i].Name = c.Name
				checks[i].CheckType = c.CheckType
				checks[i].Severity = c.Severity
				checks[i].Enabled = c.Enabled
				checks[i].ConfigJSON = c.ConfigJSON
				checks[i].UpdatedAt = time.Now().UTC()
				f.checksByAsset[assetID] = checks
				checkCopy := checks[i]
				return &checkCopy, nil
			}
		}
	}
	return nil, domain.ErrNotFound("check %q not found", id)
}

func (f *fakeAssetCheckRepo) DeleteCheck(_ context.Context, id string) error {
	for assetID, checks := range f.checksByAsset {
		for i := range checks {
			if checks[i].ID == id {
				f.checksByAsset[assetID] = append(checks[:i], checks[i+1:]...)
				return nil
			}
		}
	}
	return domain.ErrNotFound("check %q not found", id)
}

func (f *fakeAssetCheckRepo) CreateCheckResult(_ context.Context, _ *domain.AssetCheckResult) (*domain.AssetCheckResult, error) {
	panic("not implemented")
}

func (f *fakeAssetCheckRepo) ListCheckResults(_ context.Context, _ string, _ domain.PageRequest) ([]domain.AssetCheckResult, int64, error) {
	panic("not implemented")
}

type fakeOrchestrationEventRepo struct {
	events []domain.OrchestrationEvent
}

func (f *fakeOrchestrationEventRepo) Enqueue(_ context.Context, event *domain.OrchestrationEvent) (*domain.OrchestrationEvent, error) {
	if event.IdempotencyKey != nil {
		key := strings.TrimSpace(*event.IdempotencyKey)
		if key != "" {
			for i := range f.events {
				if f.events[i].IdempotencyKey != nil && *f.events[i].IdempotencyKey == key {
					existing := f.events[i]
					return &existing, nil
				}
			}
		}
	}

	copyEvent := *event
	if copyEvent.ID == "" {
		copyEvent.ID = domain.NewID()
	}
	if copyEvent.AvailableAt.IsZero() {
		copyEvent.AvailableAt = time.Now().UTC()
	}
	f.events = append(f.events, copyEvent)
	return &copyEvent, nil
}

func (f *fakeOrchestrationEventRepo) ClaimNextPending(context.Context, time.Time) (*domain.OrchestrationEvent, error) {
	panic("not implemented")
}

func (f *fakeOrchestrationEventRepo) MarkProcessed(context.Context, string) error {
	panic("not implemented")
}

func (f *fakeOrchestrationEventRepo) MarkFailed(context.Context, string, string, *time.Time) error {
	panic("not implemented")
}

func (f *fakeOrchestrationEventRepo) List(context.Context, domain.OrchestrationEventFilter) ([]domain.OrchestrationEvent, int64, error) {
	panic("not implemented")
}
