package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

type mockAssetService struct {
	listAssetsFn             func(context.Context, domain.AssetFilter) ([]domain.DataAsset, int64, error)
	createAssetFn            func(context.Context, domain.CreateAssetRequest) (*domain.DataAsset, error)
	getAssetFn               func(context.Context, string) (*domain.DataAsset, error)
	updateAssetFn            func(context.Context, string, domain.UpdateAssetRequest) (*domain.DataAsset, error)
	deleteAssetFn            func(context.Context, string) error
	resolveAssetKeysFn       func(context.Context, []string) (map[string]string, error)
	getGraphFn               func(context.Context, string) ([]domain.AssetDependency, []domain.AssetDependency, error)
	listPartitionsFn         func(context.Context, string, domain.PageRequest) ([]domain.AssetPartition, int64, error)
	listRunsFn               func(context.Context, domain.AssetRunFilter) ([]domain.AssetRun, int64, error)
	listMaterializationsFn   func(context.Context, string, domain.PageRequest) ([]domain.AssetMaterialization, int64, error)
	listChecksFn             func(context.Context, string) ([]domain.AssetCheck, error)
	listCheckResultsFn       func(context.Context, string, domain.PageRequest) ([]domain.AssetCheckResult, int64, error)
	listBackfillsFn          func(context.Context, domain.BackfillFilter) ([]domain.BackfillRequest, int64, error)
	getBackfillFn            func(context.Context, string, string) (*domain.BackfillRequest, []domain.BackfillSlice, error)
	triggerMaterializationFn func(context.Context, string, *string, map[string]any, *string) (*domain.OrchestrationEvent, error)
}

func (m *mockAssetService) ListAssets(ctx context.Context, filter domain.AssetFilter) ([]domain.DataAsset, int64, error) {
	return m.listAssetsFn(ctx, filter)
}
func (m *mockAssetService) CreateAsset(ctx context.Context, req domain.CreateAssetRequest) (*domain.DataAsset, error) {
	return m.createAssetFn(ctx, req)
}
func (m *mockAssetService) GetAsset(ctx context.Context, key string) (*domain.DataAsset, error) {
	return m.getAssetFn(ctx, key)
}
func (m *mockAssetService) UpdateAsset(ctx context.Context, assetKey string, req domain.UpdateAssetRequest) (*domain.DataAsset, error) {
	return m.updateAssetFn(ctx, assetKey, req)
}
func (m *mockAssetService) DeleteAsset(ctx context.Context, assetKey string) error {
	return m.deleteAssetFn(ctx, assetKey)
}
func (m *mockAssetService) ResolveAssetKeys(ctx context.Context, assetIDs []string) (map[string]string, error) {
	if m.resolveAssetKeysFn == nil {
		return map[string]string{}, nil
	}
	return m.resolveAssetKeysFn(ctx, assetIDs)
}
func (m *mockAssetService) GetGraph(ctx context.Context, assetID string) ([]domain.AssetDependency, []domain.AssetDependency, error) {
	if m.getGraphFn == nil {
		return nil, nil, nil
	}
	return m.getGraphFn(ctx, assetID)
}
func (m *mockAssetService) ListPartitions(ctx context.Context, assetID string, page domain.PageRequest) ([]domain.AssetPartition, int64, error) {
	if m.listPartitionsFn == nil {
		return nil, 0, nil
	}
	return m.listPartitionsFn(ctx, assetID, page)
}
func (m *mockAssetService) ListRuns(ctx context.Context, filter domain.AssetRunFilter) ([]domain.AssetRun, int64, error) {
	if m.listRunsFn == nil {
		return nil, 0, nil
	}
	return m.listRunsFn(ctx, filter)
}
func (m *mockAssetService) ListMaterializations(ctx context.Context, assetID string, page domain.PageRequest) ([]domain.AssetMaterialization, int64, error) {
	if m.listMaterializationsFn == nil {
		return nil, 0, nil
	}
	return m.listMaterializationsFn(ctx, assetID, page)
}
func (m *mockAssetService) ListChecks(ctx context.Context, assetID string) ([]domain.AssetCheck, error) {
	if m.listChecksFn == nil {
		return nil, nil
	}
	return m.listChecksFn(ctx, assetID)
}
func (m *mockAssetService) ListCheckResults(ctx context.Context, assetID string, page domain.PageRequest) ([]domain.AssetCheckResult, int64, error) {
	if m.listCheckResultsFn == nil {
		return nil, 0, nil
	}
	return m.listCheckResultsFn(ctx, assetID, page)
}
func (m *mockAssetService) ListBackfills(ctx context.Context, filter domain.BackfillFilter) ([]domain.BackfillRequest, int64, error) {
	if m.listBackfillsFn == nil {
		return nil, 0, nil
	}
	return m.listBackfillsFn(ctx, filter)
}
func (m *mockAssetService) GetBackfill(ctx context.Context, assetID, backfillID string) (*domain.BackfillRequest, []domain.BackfillSlice, error) {
	if m.getBackfillFn == nil {
		return nil, nil, nil
	}
	return m.getBackfillFn(ctx, assetID, backfillID)
}
func (m *mockAssetService) TriggerMaterialization(ctx context.Context, assetID string, partitionKey *string, payload map[string]any, idempotencyKey *string) (*domain.OrchestrationEvent, error) {
	return m.triggerMaterializationFn(ctx, assetID, partitionKey, payload, idempotencyKey)
}

type mockAssetBackfillService struct {
	createFn func(context.Context, string, string, string, string, int) (*domain.BackfillRequest, []domain.BackfillSlice, error)
}

func (m *mockAssetBackfillService) Create(ctx context.Context, assetID, requestedBy, from, to string, maxParallelism int) (*domain.BackfillRequest, []domain.BackfillSlice, error) {
	return m.createFn(ctx, assetID, requestedBy, from, to, maxParallelism)
}

func assetTestCtx(isAdmin bool) context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "tester", IsAdmin: isAdmin, Type: "user"})
}

func TestHandler_ListAssets(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{listAssetsFn: func(_ context.Context, filter domain.AssetFilter) ([]domain.DataAsset, int64, error) {
		require.Equal(t, 0, filter.Page.Offset())
		return []domain.DataAsset{{ID: "a1", AssetKey: "sales.daily", AssetType: domain.AssetTypeModel, CreatedAt: time.Now(), UpdatedAt: time.Now()}}, 1, nil
	}}}

	resp, err := h.ListAssets(assetTestCtx(true), GenListAssetsRequest{})
	require.NoError(t, err)
	ok, cast := resp.(ListAssets200JSONResponse)
	require.True(t, cast)
	require.NotNil(t, ok.Body.Data)
	require.Len(t, ok.Body.Data, 1)
	assert.Equal(t, "sales.daily", *ok.Body.Data[0].AssetKey)
}

func TestHandler_CreateAsset(t *testing.T) {
	t.Parallel()
	createdAt := time.Now().UTC()
	h := &APIHandler{assets: &mockAssetService{createAssetFn: func(_ context.Context, req domain.CreateAssetRequest) (*domain.DataAsset, error) {
		require.Equal(t, "showcase.rides.gold", req.AssetKey)
		require.Equal(t, domain.AssetTypeTable, req.AssetType)
		require.Equal(t, []string{"showcase.rides.silver"}, req.UpstreamAssetKeys)
		require.Len(t, req.Checks, 1)
		require.NotNil(t, req.FreshnessPolicy)
		assert.EqualValues(t, 1800, req.FreshnessPolicy.MaxLagSeconds)
		require.NotNil(t, req.MaterializationPolicy)
		assert.Equal(t, "TABLE", req.MaterializationPolicy.Mode)
		require.NotNil(t, req.AutoMaterializePolicy)
		assert.True(t, req.AutoMaterializePolicy.OnFreshnessBreach)
		assert.True(t, req.IsActive)
		return &domain.DataAsset{
			ID:                    "asset-1",
			AssetKey:              req.AssetKey,
			AssetType:             req.AssetType,
			Owner:                 req.Owner,
			FreshnessPolicy:       req.FreshnessPolicy,
			MaterializationPolicy: req.MaterializationPolicy,
			AutoMaterializePolicy: req.AutoMaterializePolicy,
			CreatedAt:             createdAt,
			UpdatedAt:             createdAt,
		}, nil
	}}}

	resp, err := h.CreateAsset(assetTestCtx(true), GenCreateAssetRequest{Body: &CreateAssetJSONRequestBody{
		AssetKey:        "showcase.rides.gold",
		AssetType:       domain.AssetTypeTable,
		Owner:           "platform-admins",
		Description:     assetStrPtr("Gold showcase asset"),
		Tags:            &[]string{"showcase", "gold"},
		FreshnessPolicy: &AssetFreshnessPolicy{MaxLagSeconds: assetInt32Ptr(1800), CronSchedule: assetStrPtr("*/30 * * * *")},
		MaterializationPolicy: &AssetMaterializationPolicy{
			Mode: assetStrPtr("TABLE"),
		},
		AutoMaterializePolicy: &AssetAutoMaterializePolicy{
			Mode:               assetStrPtr("AUTO"),
			OnFreshnessBreach:  assetBoolPtr(true),
			MinIntervalSeconds: assetInt32Ptr(900),
		},
		IoProfile:         assetStrPtr("duckdb"),
		IsActive:          assetBoolPtr(true),
		UpstreamAssetKeys: &[]string{"showcase.rides.silver"},
		Checks: &[]AssetCheckInput{{
			Name:      "gold_non_empty",
			CheckType: "SQL_ASSERT",
			Enabled:   assetBoolPtr(true),
		}},
	}})
	require.NoError(t, err)
	created, ok := resp.(CreateAsset201JSONResponse)
	require.True(t, ok)
	assert.Equal(t, "showcase.rides.gold", *created.Body.AssetKey)
	require.NotNil(t, created.Body.FreshnessPolicy)
	assert.EqualValues(t, 1800, *created.Body.FreshnessPolicy.MaxLagSeconds)
}

func TestHandler_GetAsset_NotFound(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{getAssetFn: func(_ context.Context, _ string) (*domain.DataAsset, error) {
		return nil, domain.ErrNotFound("asset not found")
	}}}

	resp, err := h.GetAsset(assetTestCtx(true), GenGetAssetRequest{AssetKey: "missing"})
	require.NoError(t, err)
	_, cast := resp.(GetAsset404JSONResponse)
	require.True(t, cast)
}

func TestHandler_GetAsset(t *testing.T) {
	t.Parallel()
	createdAt := time.Now().UTC()
	h := &APIHandler{assets: &mockAssetService{getAssetFn: func(_ context.Context, key string) (*domain.DataAsset, error) {
		require.Equal(t, "sales.daily", key)
		return &domain.DataAsset{
			ID:        "asset-1",
			AssetKey:  key,
			AssetType: domain.AssetTypeModel,
			Owner:     "analytics",
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			FreshnessPolicy: &domain.AssetFreshnessPolicy{
				MaxLagSeconds: 300,
			},
		}, nil
	}}}

	resp, err := h.GetAsset(assetTestCtx(true), GenGetAssetRequest{AssetKey: "sales.daily"})
	require.NoError(t, err)
	ok, cast := resp.(GetAsset200JSONResponse)
	require.True(t, cast)
	assert.Equal(t, "sales.daily", *ok.Body.AssetKey)
	assert.Equal(t, domain.AssetTypeModel, *ok.Body.AssetType)
	assert.Equal(t, "analytics", *ok.Body.Owner)
	require.NotNil(t, ok.Body.FreshnessPolicy)
	assert.EqualValues(t, 300, *ok.Body.FreshnessPolicy.MaxLagSeconds)
}

func TestHandler_UpdateAsset(t *testing.T) {
	t.Parallel()
	updatedAt := time.Now().UTC()
	h := &APIHandler{assets: &mockAssetService{updateAssetFn: func(_ context.Context, assetKey string, req domain.UpdateAssetRequest) (*domain.DataAsset, error) {
		require.Equal(t, "showcase.rides.silver", assetKey)
		require.Equal(t, "analytics", req.Owner)
		require.Equal(t, []string{"showcase.rides.bronze"}, req.UpstreamAssetKeys)
		require.NotNil(t, req.FreshnessPolicy)
		require.NotNil(t, req.AutoMaterializePolicy)
		return &domain.DataAsset{
			ID:                    "asset-1",
			AssetKey:              assetKey,
			AssetType:             req.AssetType,
			Owner:                 req.Owner,
			FreshnessPolicy:       req.FreshnessPolicy,
			AutoMaterializePolicy: req.AutoMaterializePolicy,
			CreatedAt:             updatedAt,
			UpdatedAt:             updatedAt,
		}, nil
	}}}

	resp, err := h.UpdateAsset(assetTestCtx(true), GenUpdateAssetRequest{AssetKey: "showcase.rides.silver", Body: &UpdateAssetJSONRequestBody{
		AssetType:       domain.AssetTypeTable,
		Owner:           "analytics",
		Description:     assetStrPtr("Silver showcase asset"),
		FreshnessPolicy: &AssetFreshnessPolicy{MaxLagSeconds: assetInt32Ptr(600)},
		AutoMaterializePolicy: &AssetAutoMaterializePolicy{
			Mode:                   assetStrPtr("AUTO"),
			OnUpstreamMaterialized: assetBoolPtr(true),
		},
		IsActive:          assetBoolPtr(true),
		UpstreamAssetKeys: &[]string{"showcase.rides.bronze"},
	}})
	require.NoError(t, err)
	updated, ok := resp.(UpdateAsset200JSONResponse)
	require.True(t, ok)
	assert.Equal(t, "analytics", *updated.Body.Owner)
	require.NotNil(t, updated.Body.FreshnessPolicy)
	assert.EqualValues(t, 600, *updated.Body.FreshnessPolicy.MaxLagSeconds)
}

func TestHandler_DeleteAsset(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{deleteAssetFn: func(_ context.Context, assetKey string) error {
		require.Equal(t, "showcase.rides.raw", assetKey)
		return nil
	}}}

	resp, err := h.DeleteAsset(assetTestCtx(true), GenDeleteAssetRequest{AssetKey: "showcase.rides.raw"})
	require.NoError(t, err)
	_, ok := resp.(DeleteAsset204Response)
	require.True(t, ok)
}

func TestHandler_GetAssetGraph(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, key string) (*domain.DataAsset, error) {
			require.Equal(t, "sales.daily", key)
			return &domain.DataAsset{ID: "asset-1", AssetKey: key}, nil
		},
		getGraphFn: func(_ context.Context, assetID string) ([]domain.AssetDependency, []domain.AssetDependency, error) {
			require.Equal(t, "asset-1", assetID)
			return []domain.AssetDependency{{UpstreamAssetID: "asset-upstream"}}, []domain.AssetDependency{{AssetID: "asset-downstream"}}, nil
		},
		resolveAssetKeysFn: func(_ context.Context, assetIDs []string) (map[string]string, error) {
			assert.ElementsMatch(t, []string{"asset-upstream", "asset-downstream"}, assetIDs)
			return map[string]string{
				"asset-upstream":   "raw.orders",
				"asset-downstream": "analytics.orders",
			}, nil
		},
	}}

	resp, err := h.GetAssetGraph(assetTestCtx(true), GenGetAssetGraphRequest{AssetKey: "sales.daily"})
	require.NoError(t, err)
	ok, cast := resp.(GetAssetGraph200JSONResponse)
	require.True(t, cast)
	require.NotNil(t, ok.Body.UpstreamAssetKeys)
	require.NotNil(t, ok.Body.DownstreamAssetKeys)
	assert.Equal(t, []string{"raw.orders"}, *ok.Body.UpstreamAssetKeys)
	assert.Equal(t, []string{"analytics.orders"}, *ok.Body.DownstreamAssetKeys)
}

func TestHandler_ListAssetPartitions(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, key string) (*domain.DataAsset, error) {
			require.Equal(t, "sales.daily", key)
			return &domain.DataAsset{ID: "asset-1", AssetKey: key}, nil
		},
		listPartitionsFn: func(_ context.Context, assetID string, page domain.PageRequest) ([]domain.AssetPartition, int64, error) {
			require.Equal(t, "asset-1", assetID)
			require.Equal(t, 0, page.Offset())
			return []domain.AssetPartition{{ID: "partition-1", AssetID: assetID, PartitionKey: "2026-01-01", Status: "READY", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}}, 1, nil
		},
	}}

	resp, err := h.ListAssetPartitions(assetTestCtx(true), GenListAssetPartitionsRequest{AssetKey: "sales.daily"})
	require.NoError(t, err)
	ok, cast := resp.(ListAssetPartitions200JSONResponse)
	require.True(t, cast)
	require.NotNil(t, ok.Body.Data)
	require.Len(t, ok.Body.Data, 1)
	assert.Equal(t, "2026-01-01", *ok.Body.Data[0].PartitionKey)
	assert.Equal(t, "READY", *ok.Body.Data[0].Status)
}

func TestHandler_ListAssetMaterializations(t *testing.T) {
	t.Parallel()
	partitionKey := "2026-01-01"
	rowCount := int64(42)
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, key string) (*domain.DataAsset, error) {
			require.Equal(t, "sales.daily", key)
			return &domain.DataAsset{ID: "asset-1", AssetKey: key}, nil
		},
		listMaterializationsFn: func(_ context.Context, assetID string, page domain.PageRequest) ([]domain.AssetMaterialization, int64, error) {
			require.Equal(t, "asset-1", assetID)
			require.Equal(t, 0, page.Offset())
			return []domain.AssetMaterialization{{ID: "mat-1", AssetID: assetID, PartitionKey: &partitionKey, RowCount: &rowCount, MaterializedAt: time.Now().UTC(), CreatedAt: time.Now().UTC()}}, 1, nil
		},
	}}

	resp, err := h.ListAssetMaterializations(assetTestCtx(true), GenListAssetMaterializationsRequest{AssetKey: "sales.daily"})
	require.NoError(t, err)
	ok, cast := resp.(ListAssetMaterializations200JSONResponse)
	require.True(t, cast)
	require.NotNil(t, ok.Body.Data)
	require.Len(t, ok.Body.Data, 1)
	require.NotNil(t, ok.Body.Data[0].PartitionKey)
	require.NotNil(t, ok.Body.Data[0].RowCount)
	assert.Equal(t, partitionKey, *ok.Body.Data[0].PartitionKey)
	assert.Equal(t, int32(rowCount), *ok.Body.Data[0].RowCount)
}

func TestHandler_ListAssetChecks(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, key string) (*domain.DataAsset, error) {
			require.Equal(t, "sales.daily", key)
			return &domain.DataAsset{ID: "asset-1", AssetKey: key}, nil
		},
		listChecksFn: func(_ context.Context, assetID string) ([]domain.AssetCheck, error) {
			require.Equal(t, "asset-1", assetID)
			return []domain.AssetCheck{{ID: "check-1", AssetID: assetID, Name: "row_count", CheckType: "ROW_COUNT", Severity: "WARN", Enabled: true, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}}, nil
		},
	}}

	resp, err := h.ListAssetChecks(assetTestCtx(true), GenListAssetChecksRequest{AssetKey: "sales.daily"})
	require.NoError(t, err)
	ok, cast := resp.(ListAssetChecks200JSONResponse)
	require.True(t, cast)
	require.NotNil(t, ok.Body.Data)
	require.Len(t, ok.Body.Data, 1)
	assert.Equal(t, "row_count", *ok.Body.Data[0].Name)
	assert.Equal(t, "ROW_COUNT", *ok.Body.Data[0].CheckType)
}

func TestHandler_ListAssetBackfills(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, key string) (*domain.DataAsset, error) {
			require.Equal(t, "sales.daily", key)
			return &domain.DataAsset{ID: "asset-1", AssetKey: key}, nil
		},
		listBackfillsFn: func(_ context.Context, filter domain.BackfillFilter) ([]domain.BackfillRequest, int64, error) {
			require.NotNil(t, filter.AssetID)
			require.Equal(t, "asset-1", *filter.AssetID)
			return []domain.BackfillRequest{{ID: "550e8400-e29b-41d4-a716-446655440000", AssetID: "asset-1", PartitionFrom: "2026-01-01", PartitionTo: "2026-01-02", Status: domain.BackfillStatusPending, RequestedBy: "tester", CreatedAt: time.Now().UTC()}}, 1, nil
		},
	}}

	resp, err := h.ListAssetBackfills(assetTestCtx(true), GenListAssetBackfillsRequest{AssetKey: "sales.daily"})
	require.NoError(t, err)
	ok, cast := resp.(ListAssetBackfills200JSONResponse)
	require.True(t, cast)
	require.NotNil(t, ok.Body.Data)
	require.Len(t, ok.Body.Data, 1)
	assert.Equal(t, domain.BackfillStatusPending, *ok.Body.Data[0].Status)
	assert.Equal(t, "2026-01-01", *ok.Body.Data[0].PartitionFrom)
}

func TestHandler_ListAssetCheckResults(t *testing.T) {
	t.Parallel()
	createdAt := time.Now().UTC()
	partitionKey := "2026-01-01"
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, key string) (*domain.DataAsset, error) {
			require.Equal(t, "sales.daily", key)
			return &domain.DataAsset{ID: "asset-1", AssetKey: key}, nil
		},
		listCheckResultsFn: func(_ context.Context, assetID string, page domain.PageRequest) ([]domain.AssetCheckResult, int64, error) {
			require.Equal(t, "asset-1", assetID)
			require.Equal(t, 0, page.Offset())
			return []domain.AssetCheckResult{{
				ID:           "550e8400-e29b-41d4-a716-446655440000",
				CheckID:      "660e8400-e29b-41d4-a716-446655440000",
				Status:       "PASS",
				MetricsJSON:  map[string]any{"rows_checked": float64(42)},
				PartitionKey: &partitionKey,
				CreatedAt:    createdAt,
			}}, 1, nil
		},
	}}

	resp, err := h.ListAssetCheckResults(assetTestCtx(true), GenListAssetCheckResultsRequest{AssetKey: "sales.daily"})
	require.NoError(t, err)
	ok, cast := resp.(ListAssetCheckResults200JSONResponse)
	require.True(t, cast)
	require.NotNil(t, ok.Body.Data)
	require.Len(t, ok.Body.Data, 1)
	assert.Equal(t, "PASS", *ok.Body.Data[0].Status)
	assert.Equal(t, "660e8400-e29b-41d4-a716-446655440000", *ok.Body.Data[0].CheckId)
	require.NotNil(t, ok.Body.Data[0].MetricsJson)
	assert.InDelta(t, 42.0, (*ok.Body.Data[0].MetricsJson)["rows_checked"], 0.000001)
}

func TestHandler_ListAssetCheckResults_NotFound(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{getAssetFn: func(_ context.Context, _ string) (*domain.DataAsset, error) {
		return nil, domain.ErrNotFound("asset not found")
	}}}

	resp, err := h.ListAssetCheckResults(assetTestCtx(true), GenListAssetCheckResultsRequest{AssetKey: "missing"})
	require.NoError(t, err)
	_, cast := resp.(ListAssetCheckResults404JSONResponse)
	require.True(t, cast)
}

func TestDomainAssetChecks_Defaults(t *testing.T) {
	t.Parallel()
	checks := domainAssetChecks(&[]AssetCheckInput{{
		Name:      "gold_non_empty",
		CheckType: "SQL_ASSERT",
	}})
	require.Len(t, checks, 1)
	assert.True(t, checks[0].Enabled)
	assert.Empty(t, checks[0].Severity)
}

func assetBoolPtr(v bool) *bool {
	return &v
}

func assetStrPtr(v string) *string {
	return &v
}

func assetInt32Ptr(v int32) *int32 {
	return &v
}

func TestAssetRunToAPI_MapsPartitionKey(t *testing.T) {
	t.Parallel()
	partitionKey := "2026-01-02"
	partitionFrom := "2026-01-01"
	partitionTo := "2026-01-03"
	run := domain.AssetRun{
		ID:            "run-1",
		AssetID:       "asset-1",
		PartitionKey:  &partitionKey,
		PartitionFrom: &partitionFrom,
		PartitionTo:   &partitionTo,
		Status:        domain.AssetRunStatusQueued,
		TriggerType:   domain.AssetTriggerTypeBackfill,
		TriggeredBy:   "tester",
		AttemptCount:  0,
		MaxAttempts:   1,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}

	apiRun := assetRunToAPI(run)
	require.NotNil(t, apiRun.PartitionKey)
	require.NotNil(t, apiRun.PartitionFrom)
	require.NotNil(t, apiRun.PartitionTo)
	assert.Equal(t, partitionKey, *apiRun.PartitionKey)
	assert.Equal(t, partitionFrom, *apiRun.PartitionFrom)
	assert.Equal(t, partitionTo, *apiRun.PartitionTo)
}

func TestHandler_ListAssetRuns_MapsPartitionRange(t *testing.T) {
	t.Parallel()
	partitionKey := "2026-01-02"
	partitionFrom := "2026-01-01"
	partitionTo := "2026-01-03"
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, key string) (*domain.DataAsset, error) {
			require.Equal(t, "sales.daily", key)
			return &domain.DataAsset{ID: "asset-1", AssetKey: key}, nil
		},
		listRunsFn: func(_ context.Context, filter domain.AssetRunFilter) ([]domain.AssetRun, int64, error) {
			require.NotNil(t, filter.AssetID)
			require.Equal(t, "asset-1", *filter.AssetID)
			return []domain.AssetRun{{
				ID:            "run-1",
				AssetID:       "asset-1",
				PartitionKey:  &partitionKey,
				PartitionFrom: &partitionFrom,
				PartitionTo:   &partitionTo,
				Status:        domain.AssetRunStatusSuccess,
				TriggerType:   domain.AssetTriggerTypeBackfill,
				TriggeredBy:   "tester",
				MaxAttempts:   1,
				CreatedAt:     time.Now().UTC(),
				UpdatedAt:     time.Now().UTC(),
			}}, 1, nil
		},
	}}

	resp, err := h.ListAssetRuns(assetTestCtx(true), GenListAssetRunsRequest{AssetKey: "sales.daily"})
	require.NoError(t, err)
	ok, cast := resp.(ListAssetRuns200JSONResponse)
	require.True(t, cast)
	require.NotNil(t, ok.Body.Data)
	require.Len(t, ok.Body.Data, 1)
	apiRun := ok.Body.Data[0]
	require.NotNil(t, apiRun.PartitionFrom)
	require.NotNil(t, apiRun.PartitionTo)
	assert.Equal(t, partitionFrom, *apiRun.PartitionFrom)
	assert.Equal(t, partitionTo, *apiRun.PartitionTo)
}

func TestHandler_CreateAssetBackfill_AccessDenied(t *testing.T) {
	t.Parallel()
	h := &APIHandler{
		assets: &mockAssetService{getAssetFn: func(_ context.Context, _ string) (*domain.DataAsset, error) {
			return &domain.DataAsset{ID: "asset-1", AssetKey: "sales.daily"}, nil
		}},
		backfills: &mockAssetBackfillService{createFn: func(context.Context, string, string, string, string, int) (*domain.BackfillRequest, []domain.BackfillSlice, error) {
			return nil, nil, domain.ErrAccessDenied("\"tester\" lacks EXECUTE_ASSET_MATERIALIZATION on catalog")
		}},
	}
	body := CreateAssetBackfillJSONRequestBody{PartitionFrom: "2026-01-01", PartitionTo: "2026-01-02"}

	resp, err := h.CreateAssetBackfill(assetTestCtx(false), GenCreateAssetBackfillRequest{AssetKey: "sales.daily", Body: &body})
	require.NoError(t, err)
	_, cast := resp.(CreateAssetBackfill403JSONResponse)
	require.True(t, cast)
}

func TestHandler_TriggerAssetMaterialization(t *testing.T) {
	t.Parallel()
	partition := "2026-01-01"
	idem := "evt-1"
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, _ string) (*domain.DataAsset, error) {
			return &domain.DataAsset{ID: "asset-1", AssetKey: "sales.daily"}, nil
		},
		triggerMaterializationFn: func(_ context.Context, assetID string, partitionKey *string, payload map[string]any, idempotencyKey *string) (*domain.OrchestrationEvent, error) {
			require.Equal(t, "asset-1", assetID)
			require.Equal(t, "2026-01-01", *partitionKey)
			require.Equal(t, "evt-1", *idempotencyKey)
			assert.Equal(t, "manual", payload["source"])
			return &domain.OrchestrationEvent{ID: "event-1", Status: domain.OrchestrationEventStatusPending}, nil
		},
	}}
	payload := Record{"source": "manual"}
	body := TriggerAssetMaterializationJSONRequestBody{PartitionKey: &partition, IdempotencyKey: &idem, Payload: &payload}

	resp, err := h.TriggerAssetMaterialization(assetTestCtx(false), GenTriggerAssetMaterializationRequest{AssetKey: "sales.daily", Body: &body})
	require.NoError(t, err)
	accepted, cast := resp.(TriggerAssetMaterialization202JSONResponse)
	require.True(t, cast)
	assert.Equal(t, "event-1", *accepted.Body.EventId)
}

func TestHandler_TriggerAssetMaterialization_AccessDenied(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, _ string) (*domain.DataAsset, error) {
			return &domain.DataAsset{ID: "asset-1", AssetKey: "sales.daily"}, nil
		},
		triggerMaterializationFn: func(_ context.Context, _ string, _ *string, _ map[string]any, _ *string) (*domain.OrchestrationEvent, error) {
			return nil, domain.ErrAccessDenied("\"tester\" lacks EXECUTE_ASSET_MATERIALIZATION on catalog")
		},
	}}

	resp, err := h.TriggerAssetMaterialization(assetTestCtx(false), GenTriggerAssetMaterializationRequest{AssetKey: "sales.daily"})
	require.NoError(t, err)
	_, cast := resp.(TriggerAssetMaterialization403JSONResponse)
	require.True(t, cast)
}

func TestHandler_GetAssetBackfill(t *testing.T) {
	t.Parallel()
	createdAt := time.Now().UTC()
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, _ string) (*domain.DataAsset, error) {
			return &domain.DataAsset{ID: "asset-1", AssetKey: "sales.daily"}, nil
		},
		getBackfillFn: func(_ context.Context, assetID, backfillID string) (*domain.BackfillRequest, []domain.BackfillSlice, error) {
			require.Equal(t, "asset-1", assetID)
			require.Equal(t, "550e8400-e29b-41d4-a716-446655440000", backfillID)
			request := &domain.BackfillRequest{ID: backfillID, AssetID: assetID, PartitionFrom: "2026-01-01", PartitionTo: "2026-01-02", Status: domain.BackfillStatusPending, RequestedBy: "tester", CreatedAt: createdAt}
			slices := []domain.BackfillSlice{{ID: "slice-1", RequestID: backfillID, AssetID: assetID, PartitionKey: "2026-01-01", Status: domain.BackfillStatusPending}}
			return request, slices, nil
		},
	}}

	resp, err := h.GetAssetBackfill(assetTestCtx(true), GenGetAssetBackfillRequest{AssetKey: "sales.daily", BackfillId: "550e8400-e29b-41d4-a716-446655440000"})
	require.NoError(t, err)
	ok, cast := resp.(GetAssetBackfill200JSONResponse)
	require.True(t, cast)
	require.NotNil(t, ok.Body.Request)
	require.NotNil(t, ok.Body.Slices)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", *ok.Body.Request.Id)
	require.Len(t, *ok.Body.Slices, 1)
	assert.Equal(t, "2026-01-01", *(*ok.Body.Slices)[0].PartitionKey)
}

func TestHandler_GetAssetBackfill_NotFound(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, _ string) (*domain.DataAsset, error) {
			return &domain.DataAsset{ID: "asset-1", AssetKey: "sales.daily"}, nil
		},
		getBackfillFn: func(_ context.Context, _ string, _ string) (*domain.BackfillRequest, []domain.BackfillSlice, error) {
			return nil, nil, domain.ErrNotFound("backfill request not found")
		},
	}}

	resp, err := h.GetAssetBackfill(assetTestCtx(true), GenGetAssetBackfillRequest{AssetKey: "sales.daily", BackfillId: "550e8400-e29b-41d4-a716-446655440000"})
	require.NoError(t, err)
	_, cast := resp.(GetAssetBackfill404JSONResponse)
	require.True(t, cast)
}
