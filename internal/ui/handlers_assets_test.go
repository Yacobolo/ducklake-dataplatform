package ui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	assetsvc "duck-demo/internal/service/asset"
	"duck-demo/internal/service/orchestration"
)

func TestUIAssets_AssetMaterialize_UsesPrivilegeCheck(t *testing.T) {
	authz := &uiAuthzStub{allow: false}
	assetService := assetsvc.NewService(&uiAssetRepoStub{}, nil, nil, nil, nil, nil, nil, nil, authz)
	h := &Handler{Asset: assetService}

	req := httptest.NewRequest(http.MethodPost, "/ui/assets/orders.daily/materialize", strings.NewReader(""))
	req = req.WithContext(domain.WithPrincipal(req.Context(), domain.ContextPrincipal{Name: "alice", Type: "user", IsAdmin: false}))
	req = withAssetKey(req, "orders.daily")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()

	h.AssetMaterialize(rr, req)

	res := rr.Result()
	t.Cleanup(func() {
		require.NoError(t, res.Body.Close())
	})
	body := rr.Body.String()

	require.Equal(t, http.StatusForbidden, res.StatusCode)
	assert.Contains(t, body, domain.PrivExecuteAssetMaterialization)
	assert.NotContains(t, body, "requires admin privileges")
	require.Len(t, authz.calls, 1)
	assert.Equal(t, "alice", authz.calls[0].principal)
	assert.Equal(t, domain.SecurableCatalog, authz.calls[0].securableType)
	assert.Equal(t, domain.CatalogID, authz.calls[0].securableID)
	assert.Equal(t, domain.PrivExecuteAssetMaterialization, authz.calls[0].privilege)
}

func TestUIAssets_AssetMaterialize_SuccessRedirect(t *testing.T) {
	authz := &uiAuthzStub{allow: true}
	events := &uiOrchestrationEventRepoStub{}
	assetService := assetsvc.NewService(&uiAssetRepoStub{}, nil, nil, nil, nil, nil, events, nil, authz)
	h := &Handler{Asset: assetService}

	form := strings.NewReader("partition_key=2026-01-01")
	req := httptest.NewRequest(http.MethodPost, "/ui/assets/orders.daily/materialize", form)
	req = req.WithContext(domain.WithPrincipal(req.Context(), domain.ContextPrincipal{Name: "alice", Type: "user", IsAdmin: false}))
	req = withAssetKey(req, "orders.daily")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()

	h.AssetMaterialize(rr, req)

	res := rr.Result()
	t.Cleanup(func() {
		require.NoError(t, res.Body.Close())
	})

	require.Equal(t, http.StatusSeeOther, res.StatusCode)
	assert.Equal(t, "/ui/assets/orders.daily", res.Header.Get("Location"))
	require.Len(t, events.events, 1)
	assert.Equal(t, domain.AssetTriggerTypeManual, events.events[0].EventType)
	require.NotNil(t, events.events[0].AssetID)
	assert.Equal(t, "asset-1", *events.events[0].AssetID)
	require.NotNil(t, events.events[0].PartitionKey)
	assert.Equal(t, "2026-01-01", *events.events[0].PartitionKey)
}

func TestUIAssets_AssetBackfillCreate_UsesPrivilegeCheck(t *testing.T) {
	authz := &uiAuthzStub{allow: false}
	assetService := assetsvc.NewService(&uiAssetRepoStub{}, nil, nil, nil, nil, nil, nil, nil, authz)
	backfillService := orchestration.NewBackfillService(nil, orchestration.NewTriggerRouter(nil), nil, authz)
	h := &Handler{Asset: assetService, Backfill: backfillService}

	form := strings.NewReader("partition_from=2026-01-01&partition_to=2026-01-02")
	req := httptest.NewRequest(http.MethodPost, "/ui/assets/orders.daily/backfills", form)
	req = req.WithContext(domain.WithPrincipal(req.Context(), domain.ContextPrincipal{Name: "alice", Type: "user", IsAdmin: false}))
	req = withAssetKey(req, "orders.daily")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()

	h.AssetBackfillCreate(rr, req)

	res := rr.Result()
	t.Cleanup(func() {
		require.NoError(t, res.Body.Close())
	})
	body := rr.Body.String()

	require.Equal(t, http.StatusForbidden, res.StatusCode)
	assert.Contains(t, body, domain.PrivExecuteAssetMaterialization)
	assert.NotContains(t, body, "requires admin privileges")
	require.Len(t, authz.calls, 1)
	assert.Equal(t, "alice", authz.calls[0].principal)
	assert.Equal(t, domain.SecurableCatalog, authz.calls[0].securableType)
	assert.Equal(t, domain.CatalogID, authz.calls[0].securableID)
	assert.Equal(t, domain.PrivExecuteAssetMaterialization, authz.calls[0].privilege)
}

func TestUIAssets_AssetBackfillCreate_SuccessRedirect(t *testing.T) {
	authz := &uiAuthzStub{allow: true}
	backfills := &uiBackfillRepoStub{}
	events := &uiOrchestrationEventRepoStub{}
	assetService := assetsvc.NewService(&uiAssetRepoStub{}, nil, nil, nil, nil, nil, nil, nil, authz)
	backfillService := orchestration.NewBackfillService(backfills, orchestration.NewTriggerRouter(events), nil, authz)
	h := &Handler{Asset: assetService, Backfill: backfillService}

	form := strings.NewReader("partition_from=2026-01-01&partition_to=2026-01-02&max_parallelism=3")
	req := httptest.NewRequest(http.MethodPost, "/ui/assets/orders.daily/backfills", form)
	req = req.WithContext(domain.WithPrincipal(req.Context(), domain.ContextPrincipal{Name: "alice", Type: "user", IsAdmin: false}))
	req = withAssetKey(req, "orders.daily")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()

	h.AssetBackfillCreate(rr, req)

	res := rr.Result()
	t.Cleanup(func() {
		require.NoError(t, res.Body.Close())
	})

	require.Equal(t, http.StatusSeeOther, res.StatusCode)
	assert.Equal(t, "/ui/assets/orders.daily", res.Header.Get("Location"))
	require.NotNil(t, backfills.createdRequest)
	assert.Equal(t, "asset-1", backfills.createdRequest.AssetID)
	assert.Equal(t, "alice", backfills.createdRequest.RequestedBy)
	assert.Equal(t, "2026-01-01", backfills.createdRequest.PartitionFrom)
	assert.Equal(t, "2026-01-02", backfills.createdRequest.PartitionTo)
	assert.Equal(t, 3, backfills.createdRequest.MaxParallelism)
	require.NotEmpty(t, backfills.createdSlices)
	require.NotEmpty(t, events.events)
	for i := range events.events {
		assert.Equal(t, domain.AssetTriggerTypeBackfill, events.events[i].EventType)
		require.NotNil(t, events.events[i].AssetID)
		assert.Equal(t, "asset-1", *events.events[i].AssetID)
	}
}

func TestUIAssets_AssetBackfillCreate_InvalidInput(t *testing.T) {
	h := &Handler{Asset: assetsvc.NewService(&uiAssetRepoStub{}, nil, nil, nil, nil, nil, nil, nil, nil), Backfill: orchestration.NewBackfillService(nil, nil, nil, nil)}

	form := strings.NewReader("partition_from=2026-01-01&partition_to=2026-01-02&max_parallelism=abc")
	req := httptest.NewRequest(http.MethodPost, "/ui/assets/orders.daily/backfills", form)
	req = req.WithContext(domain.WithPrincipal(req.Context(), domain.ContextPrincipal{Name: "alice", Type: "user", IsAdmin: false}))
	req = withAssetKey(req, "orders.daily")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()

	h.AssetBackfillCreate(rr, req)

	res := rr.Result()
	t.Cleanup(func() {
		require.NoError(t, res.Body.Close())
	})

	require.Equal(t, http.StatusBadRequest, res.StatusCode)
	assert.Contains(t, rr.Body.String(), "max_parallelism must be an integer")
}

func TestUIAssets_AssetBackfillCreate_NotConfigured(t *testing.T) {
	h := &Handler{Asset: assetsvc.NewService(&uiAssetRepoStub{}, nil, nil, nil, nil, nil, nil, nil, nil)}

	form := strings.NewReader("partition_from=2026-01-01&partition_to=2026-01-02")
	req := httptest.NewRequest(http.MethodPost, "/ui/assets/orders.daily/backfills", form)
	req = req.WithContext(domain.WithPrincipal(req.Context(), domain.ContextPrincipal{Name: "alice", Type: "user", IsAdmin: false}))
	req = withAssetKey(req, "orders.daily")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()

	h.AssetBackfillCreate(rr, req)

	res := rr.Result()
	t.Cleanup(func() {
		require.NoError(t, res.Body.Close())
	})

	require.Equal(t, http.StatusNotFound, res.StatusCode)
	assert.Contains(t, rr.Body.String(), "Asset backfill UI is not configured")
}

func withAssetKey(r *http.Request, assetKey string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("assetKey", assetKey)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

type uiAssetRepoStub struct{}

func (s *uiAssetRepoStub) Create(context.Context, *domain.DataAsset) (*domain.DataAsset, error) {
	panic("unexpected call")
}

func (s *uiAssetRepoStub) GetByID(context.Context, string) (*domain.DataAsset, error) {
	now := time.Now().UTC()
	return &domain.DataAsset{
		ID:        "asset-1",
		AssetKey:  "orders.daily",
		AssetType: domain.AssetTypeTable,
		UpdatedAt: now,
	}, nil
}

func (s *uiAssetRepoStub) GetByKey(_ context.Context, assetKey string) (*domain.DataAsset, error) {
	now := time.Now().UTC()
	return &domain.DataAsset{ID: "asset-1", AssetKey: assetKey, UpdatedAt: now}, nil
}

func (s *uiAssetRepoStub) List(context.Context, domain.AssetFilter) ([]domain.DataAsset, int64, error) {
	panic("unexpected call")
}

func (s *uiAssetRepoStub) Update(context.Context, string, *domain.DataAsset) (*domain.DataAsset, error) {
	panic("unexpected call")
}

func (s *uiAssetRepoStub) Delete(context.Context, string) error {
	panic("unexpected call")
}

type uiBackfillRepoStub struct {
	createdRequest *domain.BackfillRequest
	createdSlices  []domain.BackfillSlice
}

func (s *uiBackfillRepoStub) CreateRequest(_ context.Context, req *domain.BackfillRequest) (*domain.BackfillRequest, error) {
	now := time.Now().UTC()
	copyReq := *req
	copyReq.CreatedAt = now
	s.createdRequest = &copyReq
	return &copyReq, nil
}

func (s *uiBackfillRepoStub) GetRequestByID(context.Context, string) (*domain.BackfillRequest, error) {
	panic("unexpected call")
}

func (s *uiBackfillRepoStub) ListRequests(context.Context, domain.BackfillFilter) ([]domain.BackfillRequest, int64, error) {
	return []domain.BackfillRequest{}, 0, nil
}

func (s *uiBackfillRepoStub) UpdateRequestStatus(context.Context, string, string, *string) error {
	panic("unexpected call")
}

func (s *uiBackfillRepoStub) CreateSlice(_ context.Context, slice *domain.BackfillSlice) (*domain.BackfillSlice, error) {
	copySlice := *slice
	s.createdSlices = append(s.createdSlices, copySlice)
	return &copySlice, nil
}

func (s *uiBackfillRepoStub) ListSlicesByRequest(context.Context, string) ([]domain.BackfillSlice, error) {
	panic("unexpected call")
}

func (s *uiBackfillRepoStub) UpdateSliceStatus(context.Context, string, string, *string, *string) error {
	panic("unexpected call")
}

type uiOrchestrationEventRepoStub struct {
	events []domain.OrchestrationEvent
}

func (s *uiOrchestrationEventRepoStub) Enqueue(_ context.Context, event *domain.OrchestrationEvent) (*domain.OrchestrationEvent, error) {
	now := time.Now().UTC()
	copyEvent := *event
	copyEvent.CreatedAt = now
	s.events = append(s.events, copyEvent)
	return &copyEvent, nil
}

func (s *uiOrchestrationEventRepoStub) ClaimNextPending(context.Context, time.Time) (*domain.OrchestrationEvent, error) {
	panic("unexpected call")
}

func (s *uiOrchestrationEventRepoStub) MarkProcessed(context.Context, string) error {
	panic("unexpected call")
}

func (s *uiOrchestrationEventRepoStub) MarkFailed(context.Context, string, string, *time.Time) error {
	panic("unexpected call")
}

func (s *uiOrchestrationEventRepoStub) List(context.Context, domain.OrchestrationEventFilter) ([]domain.OrchestrationEvent, int64, error) {
	panic("unexpected call")
}

type uiAuthzCheckCall struct {
	principal     string
	securableType string
	securableID   string
	privilege     string
}

type uiAuthzStub struct {
	allow bool
	calls []uiAuthzCheckCall
}

func (s *uiAuthzStub) LookupTableID(context.Context, string) (string, string, bool, error) {
	panic("unexpected call")
}

func (s *uiAuthzStub) CheckPrivilege(_ context.Context, principalName, securableType, securableID, privilege string) (bool, error) {
	s.calls = append(s.calls, uiAuthzCheckCall{principal: principalName, securableType: securableType, securableID: securableID, privilege: privilege})
	return s.allow, nil
}

func (s *uiAuthzStub) GetEffectiveRowFilters(context.Context, string, string) ([]string, error) {
	panic("unexpected call")
}

func (s *uiAuthzStub) GetEffectiveColumnMasks(context.Context, string, string) (map[string]string, error) {
	panic("unexpected call")
}

func (s *uiAuthzStub) GetTableColumnNames(context.Context, string) ([]string, error) {
	panic("unexpected call")
}
