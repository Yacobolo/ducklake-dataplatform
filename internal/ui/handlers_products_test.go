package ui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	productsvc "duck-demo/internal/service/product"
)

func TestUIProducts_CreateAndList(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	assetRepo := repository.NewDataAssetRepo(writeDB)
	assetRunRepo := repository.NewAssetRunRepo(writeDB)
	assetCheckRepo := repository.NewAssetCheckRepo(writeDB)
	domainRepo := repository.NewDomainRepo(writeDB)
	teamRepo := repository.NewTeamRepo(writeDB)
	productRepo := repository.NewDataProductRepo(writeDB)
	productSvc := productsvc.NewService(domainRepo, teamRepo, assetRepo, assetRunRepo, assetCheckRepo, productRepo)

	_, err := assetRepo.Create(t.Context(), &domain.DataAsset{
		AssetKey:  "main.analytics.daily_orders",
		AssetType: domain.AssetTypeTable,
		Owner:     "analytics",
		CreatedBy: "alice",
		IsActive:  true,
	})
	require.NoError(t, err)

	h := &Handler{Product: productSvc}

	createReq := httptest.NewRequest(http.MethodPost, "/ui/products", strings.NewReader(strings.Join([]string{
		"slug=daily-orders",
		"name=Daily+Orders",
		"description=Published+orders+product",
		"domain_name=Revenue",
		"team_name=Analytics+Engineering",
		"steward_principal=alice",
		"contact_channel=%23rev-data",
		"docs_url=https%3A%2F%2Fdocs.example.com%2Fdaily-orders",
		"access_request_path=%2Faccess%2Fdaily-orders",
		"data_grain=one+row+per+order",
		"update_cadence=hourly",
		"retention_window=365d",
		"freshness_slo=60m",
		"latency_slo=5m",
		"breaking_change_policy=new+version+required",
		"primary_asset_key=main.analytics.daily_orders",
	}, "&")))
	createReq = createReq.WithContext(domain.WithPrincipal(createReq.Context(), domain.ContextPrincipal{Name: "alice", Type: "user"}))
	createReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	createRR := httptest.NewRecorder()

	h.ProductsCreate(createRR, createReq)

	createRes := createRR.Result()
	t.Cleanup(func() { require.NoError(t, createRes.Body.Close()) })
	require.Equal(t, http.StatusSeeOther, createRes.StatusCode)
	assert.Equal(t, "/ui/products/daily-orders", createRes.Header.Get("Location"))

	listReq := httptest.NewRequest(http.MethodGet, "/ui/products", nil)
	listReq = listReq.WithContext(domain.WithPrincipal(listReq.Context(), domain.ContextPrincipal{Name: "alice", Type: "user"}))
	listRR := httptest.NewRecorder()

	h.ProductsList(listRR, listReq)

	listRes := listRR.Result()
	t.Cleanup(func() { require.NoError(t, listRes.Body.Close()) })
	require.Equal(t, http.StatusOK, listRes.StatusCode)
	assert.Contains(t, listRR.Body.String(), "Daily Orders")
	assert.Contains(t, listRR.Body.String(), "main.analytics.daily_orders")

	detail, err := productSvc.GetProduct(t.Context(), "daily-orders")
	require.NoError(t, err)
	require.NotEmpty(t, detail.Versions)

	versionReq := httptest.NewRequest(http.MethodGet, "/ui/products/daily-orders/versions/"+strconv.Itoa(detail.Versions[0].Version), nil)
	versionReq = versionReq.WithContext(domain.WithPrincipal(versionReq.Context(), domain.ContextPrincipal{Name: "alice", Type: "user"}))
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("productSlug", "daily-orders")
	routeCtx.URLParams.Add("version", strconv.Itoa(detail.Versions[0].Version))
	versionReq = versionReq.WithContext(context.WithValue(versionReq.Context(), chi.RouteCtxKey, routeCtx))
	versionRR := httptest.NewRecorder()

	h.ProductsVersionDetail(versionRR, versionReq)

	versionRes := versionRR.Result()
	t.Cleanup(func() { require.NoError(t, versionRes.Body.Close()) })
	require.Equal(t, http.StatusOK, versionRes.StatusCode)
	assert.Contains(t, versionRR.Body.String(), "Contract snapshot")
	assert.Contains(t, versionRR.Body.String(), "Daily Orders v1")
}
