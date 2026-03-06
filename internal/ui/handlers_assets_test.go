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
	panic("unexpected call")
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
