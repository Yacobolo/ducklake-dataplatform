//go:build integration

package api

import (
	"net/http"
	"net/http/httptest"
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

func TestProductRoutes_CreatePublishAndSubscribe(t *testing.T) {
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
		FreshnessPolicy: &domain.AssetFreshnessPolicy{
			MaxLagSeconds: 3600,
		},
	})
	require.NoError(t, err)

	handler := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	handler.SetProductService(productSvc)

	r := chi.NewRouter()
	r.Route("/v1", func(r chi.Router) {
		RegisterAPIGenStrictRoutes(r, handler)
	})

	createReq := httptest.NewRequest(http.MethodPost, "/v1/data-products", strings.NewReader(`{
		"slug":"daily-orders",
		"name":"Daily Orders",
		"domain_name":"Revenue",
		"team_name":"Analytics Engineering",
		"steward_principal":"alice",
		"contact_channel":"#rev-data",
		"docs_url":"https://docs.example.com/daily-orders",
		"access_request_path":"/access/daily-orders",
		"created_by":"alice",
		"contract":{"data_grain":"one row per order","update_cadence":"hourly","breaking_change_policy":"new version required"},
		"slo":{"freshness_slo":"60m"},
		"primary_asset_key":"main.analytics.daily_orders"
	}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRR := httptest.NewRecorder()
	r.ServeHTTP(createRR, createReq)
	require.Equal(t, http.StatusCreated, createRR.Code)
	assert.Contains(t, createRR.Body.String(), `"slug":"daily-orders"`)

	versionReq := httptest.NewRequest(http.MethodGet, "/v1/data-products/daily-orders/versions/1", nil)
	versionRR := httptest.NewRecorder()
	r.ServeHTTP(versionRR, versionReq)
	require.Equal(t, http.StatusOK, versionRR.Code)
	assert.Contains(t, versionRR.Body.String(), `"version":1`)

	publishReq := httptest.NewRequest(http.MethodPatch, "/v1/data-products/daily-orders/publish", strings.NewReader(`{"version":1}`))
	publishReq.Header.Set("Content-Type", "application/json")
	publishRR := httptest.NewRecorder()
	r.ServeHTTP(publishRR, publishReq)
	require.Equal(t, http.StatusOK, publishRR.Code)
	assert.Contains(t, publishRR.Body.String(), `"publication_state":"PUBLISHED"`)

	subscribeReq := httptest.NewRequest(http.MethodPost, "/v1/data-products/daily-orders/subscriptions", strings.NewReader(`{
		"principal_name":"alice",
		"event_type":"freshness_breach",
		"channel":"inbox"
	}`))
	subscribeReq.Header.Set("Content-Type", "application/json")
	subscribeRR := httptest.NewRecorder()
	r.ServeHTTP(subscribeRR, subscribeReq)
	require.Equal(t, http.StatusCreated, subscribeRR.Code)
	assert.Contains(t, subscribeRR.Body.String(), `"principal_name":"alice"`)

	eventsReq := httptest.NewRequest(http.MethodGet, "/v1/data-products/daily-orders/events", nil)
	eventsRR := httptest.NewRecorder()
	r.ServeHTTP(eventsRR, eventsReq)
	require.Equal(t, http.StatusOK, eventsRR.Code)
	assert.Contains(t, eventsRR.Body.String(), `"event_type":"publication"`)

	listReq := httptest.NewRequest(http.MethodGet, "/v1/data-products?q=daily&publication_state=PUBLISHED", nil)
	listRR := httptest.NewRecorder()
	r.ServeHTTP(listRR, listReq)
	require.Equal(t, http.StatusOK, listRR.Code)
	assert.Contains(t, listRR.Body.String(), `"slug":"daily-orders"`)
}

func TestProductRoutes_SemanticEntrypoints(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	assetRepo := repository.NewDataAssetRepo(writeDB)
	assetRunRepo := repository.NewAssetRunRepo(writeDB)
	assetCheckRepo := repository.NewAssetCheckRepo(writeDB)
	domainRepo := repository.NewDomainRepo(writeDB)
	teamRepo := repository.NewTeamRepo(writeDB)
	productRepo := repository.NewDataProductRepo(writeDB)
	semanticRepo := repository.NewSemanticModelRepo(writeDB)
	productSvc := productsvc.NewService(domainRepo, teamRepo, assetRepo, assetRunRepo, assetCheckRepo, productRepo)
	productSvc.SetSemanticModelRepository(semanticRepo)

	_, err := semanticRepo.Create(t.Context(), &domain.SemanticModel{
		ProjectName:  "sales",
		Name:         "orders",
		Description:  "Orders semantic model",
		BaseModelRef: "sales.orders",
		CreatedBy:    "alice",
	})
	require.NoError(t, err)

	handler := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	handler.SetProductService(productSvc)

	r := chi.NewRouter()
	r.Route("/v1", func(r chi.Router) {
		RegisterAPIGenStrictRoutes(r, handler)
	})

	createReq := httptest.NewRequest(http.MethodPost, "/v1/data-products", strings.NewReader(`{
		"slug":"orders-semantic",
		"name":"Orders Semantic",
		"domain_name":"Revenue",
		"team_name":"Analytics Engineering",
		"steward_principal":"alice",
		"contact_channel":"#rev-data",
		"docs_url":"https://docs.example.com/orders-semantic",
		"access_request_path":"/access/orders-semantic",
		"created_by":"alice",
		"contract":{"data_grain":"one row per order","update_cadence":"hourly","breaking_change_policy":"new version required"},
		"slo":{"freshness_slo":"60m"},
		"semantic_model_refs":["sales.orders"]
	}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRR := httptest.NewRecorder()
	r.ServeHTTP(createRR, createReq)
	require.Equal(t, http.StatusCreated, createRR.Code)

	entrypointsReq := httptest.NewRequest(http.MethodGet, "/v1/data-products/orders-semantic/semantic-entrypoints", nil)
	entrypointsRR := httptest.NewRecorder()
	r.ServeHTTP(entrypointsRR, entrypointsReq)
	require.Equal(t, http.StatusOK, entrypointsRR.Code)
	assert.Contains(t, entrypointsRR.Body.String(), `"project_name":"sales"`)
	assert.Contains(t, entrypointsRR.Body.String(), `"model_name":"orders"`)
}

func TestProductRoutes_PortfolioReport(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	assetRepo := repository.NewDataAssetRepo(writeDB)
	assetRunRepo := repository.NewAssetRunRepo(writeDB)
	assetCheckRepo := repository.NewAssetCheckRepo(writeDB)
	domainRepo := repository.NewDomainRepo(writeDB)
	teamRepo := repository.NewTeamRepo(writeDB)
	productRepo := repository.NewDataProductRepo(writeDB)
	productSvc := productsvc.NewService(domainRepo, teamRepo, assetRepo, assetRunRepo, assetCheckRepo, productRepo)

	_, err := assetRepo.Create(t.Context(), &domain.DataAsset{
		AssetKey:  "main.analytics.orders",
		AssetType: domain.AssetTypeTable,
		Owner:     "analytics",
		CreatedBy: "alice",
		IsActive:  true,
	})
	require.NoError(t, err)
	_, err = assetRepo.Create(t.Context(), &domain.DataAsset{
		AssetKey:  "main.analytics.orphaned",
		AssetType: domain.AssetTypeTable,
		Owner:     "analytics",
		CreatedBy: "alice",
		IsActive:  true,
	})
	require.NoError(t, err)

	handler := NewHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	handler.SetProductService(productSvc)

	r := chi.NewRouter()
	r.Route("/v1", func(r chi.Router) {
		RegisterAPIGenStrictRoutes(r, handler)
	})

	createReq := httptest.NewRequest(http.MethodPost, "/v1/data-products", strings.NewReader(`{
		"slug":"orders",
		"name":"Orders",
		"domain_name":"Revenue",
		"team_name":"Analytics Engineering",
		"steward_principal":"alice",
		"contact_channel":"#rev-data",
		"docs_url":"https://docs.example.com/orders",
		"access_request_path":"/access/orders",
		"created_by":"alice",
		"contract":{"data_grain":"one row per order","update_cadence":"hourly","breaking_change_policy":"new version required"},
		"slo":{"freshness_slo":"60m"},
		"primary_asset_key":"main.analytics.orders"
	}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRR := httptest.NewRecorder()
	r.ServeHTTP(createRR, createReq)
	require.Equal(t, http.StatusCreated, createRR.Code)

	subscribeReq := httptest.NewRequest(http.MethodPost, "/v1/data-products/orders/subscriptions", strings.NewReader(`{
		"principal_name":"alice",
		"event_type":"publication",
		"channel":"inbox"
	}`))
	subscribeReq.Header.Set("Content-Type", "application/json")
	subscribeRR := httptest.NewRecorder()
	r.ServeHTTP(subscribeRR, subscribeReq)
	require.Equal(t, http.StatusCreated, subscribeRR.Code)

	reportReq := httptest.NewRequest(http.MethodGet, "/v1/data-products/portfolio", nil)
	reportRR := httptest.NewRecorder()
	r.ServeHTTP(reportRR, reportReq)
	require.Equal(t, http.StatusOK, reportRR.Code)
	assert.Contains(t, reportRR.Body.String(), `"top_used"`)
	assert.Contains(t, reportRR.Body.String(), `"orders"`)
	assert.Contains(t, reportRR.Body.String(), `"main.analytics.orphaned"`)
}
