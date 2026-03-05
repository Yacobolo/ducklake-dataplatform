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
	getAssetFn               func(context.Context, string) (*domain.DataAsset, error)
	getGraphFn               func(context.Context, string) ([]domain.AssetDependency, []domain.AssetDependency, error)
	listPartitionsFn         func(context.Context, string, domain.PageRequest) ([]domain.AssetPartition, int64, error)
	listRunsFn               func(context.Context, domain.AssetRunFilter) ([]domain.AssetRun, int64, error)
	listMaterializationsFn   func(context.Context, string, domain.PageRequest) ([]domain.AssetMaterialization, int64, error)
	listChecksFn             func(context.Context, string) ([]domain.AssetCheck, error)
	listBackfillsFn          func(context.Context, domain.BackfillFilter) ([]domain.BackfillRequest, int64, error)
	triggerMaterializationFn func(context.Context, string, *string, map[string]any, *string) (*domain.OrchestrationEvent, error)
}

func (m *mockAssetService) ListAssets(ctx context.Context, filter domain.AssetFilter) ([]domain.DataAsset, int64, error) {
	return m.listAssetsFn(ctx, filter)
}
func (m *mockAssetService) GetAsset(ctx context.Context, key string) (*domain.DataAsset, error) {
	return m.getAssetFn(ctx, key)
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
func (m *mockAssetService) ListBackfills(ctx context.Context, filter domain.BackfillFilter) ([]domain.BackfillRequest, int64, error) {
	if m.listBackfillsFn == nil {
		return nil, 0, nil
	}
	return m.listBackfillsFn(ctx, filter)
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

	resp, err := h.ListAssets(assetTestCtx(true), ListAssetsRequestObject{})
	require.NoError(t, err)
	ok, cast := resp.(ListAssets200JSONResponse)
	require.True(t, cast)
	require.NotNil(t, ok.Data)
	require.Len(t, *ok.Data, 1)
	assert.Equal(t, "sales.daily", *(*ok.Data)[0].AssetKey)
}

func TestHandler_GetAsset_NotFound(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{getAssetFn: func(_ context.Context, _ string) (*domain.DataAsset, error) {
		return nil, domain.ErrNotFound("asset not found")
	}}}

	resp, err := h.GetAsset(assetTestCtx(true), GetAssetRequestObject{AssetKey: "missing"})
	require.NoError(t, err)
	_, cast := resp.(GetAsset404JSONResponse)
	require.True(t, cast)
}

func TestHandler_CreateAssetBackfill_RequiresAdmin(t *testing.T) {
	t.Parallel()
	h := &APIHandler{
		assets: &mockAssetService{getAssetFn: func(_ context.Context, _ string) (*domain.DataAsset, error) {
			return &domain.DataAsset{ID: "asset-1", AssetKey: "sales.daily"}, nil
		}},
		backfills: &mockAssetBackfillService{createFn: func(context.Context, string, string, string, string, int) (*domain.BackfillRequest, []domain.BackfillSlice, error) {
			t.Fatalf("create should not be called for non-admin")
			return nil, nil, nil
		}},
	}
	body := CreateAssetBackfillJSONRequestBody{PartitionFrom: "2026-01-01", PartitionTo: "2026-01-02"}

	resp, err := h.CreateAssetBackfill(assetTestCtx(false), CreateAssetBackfillRequestObject{AssetKey: "sales.daily", Body: &body})
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
	body := TriggerAssetMaterializationJSONRequestBody{PartitionKey: &partition, IdempotencyKey: &idem, Payload: &map[string]any{"source": "manual"}}

	resp, err := h.TriggerAssetMaterialization(assetTestCtx(true), TriggerAssetMaterializationRequestObject{AssetKey: "sales.daily", Body: &body})
	require.NoError(t, err)
	accepted, cast := resp.(TriggerAssetMaterialization202JSONResponse)
	require.True(t, cast)
	assert.Equal(t, "event-1", *accepted.EventId)
}

func TestHandler_TriggerAssetMaterialization_RequiresAdmin(t *testing.T) {
	t.Parallel()
	h := &APIHandler{assets: &mockAssetService{
		getAssetFn: func(_ context.Context, _ string) (*domain.DataAsset, error) {
			return &domain.DataAsset{ID: "asset-1", AssetKey: "sales.daily"}, nil
		},
		triggerMaterializationFn: func(_ context.Context, _ string, _ *string, _ map[string]any, _ *string) (*domain.OrchestrationEvent, error) {
			t.Fatalf("trigger should not be called for non-admin")
			return nil, nil
		},
	}}

	resp, err := h.TriggerAssetMaterialization(assetTestCtx(false), TriggerAssetMaterializationRequestObject{AssetKey: "sales.daily"})
	require.NoError(t, err)
	_, cast := resp.(TriggerAssetMaterialization403JSONResponse)
	require.True(t, cast)
}
