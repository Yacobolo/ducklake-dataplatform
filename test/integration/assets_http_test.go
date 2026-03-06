//go:build integration

package integration

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
)

func TestHTTP_AssetMaterialize_CreatesExecutableRun(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithAssets: true})
	require.NotNil(t, env.Reconciler)

	asset := createTestAsset(t, env.MetaDB, "analytics.materialize_target", domain.PartitionTypeUnpartitioned)

	resp := doRequest(t, "POST", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/materialize", env.Keys.Admin, map[string]any{})
	require.Equal(t, httpStatusAccepted, resp.StatusCode)
	_ = readBody(t, resp)

	require.NoError(t, env.Reconciler.Tick(ctx))

	runs := listAssetRuns(t, env, asset.AssetKey)
	require.NotEmpty(t, runs)
	assert.Equal(t, domain.AssetRunStatusSuccess, runs[0]["status"])
}

func TestHTTP_AssetMaterialize_AllowsGrantedAnalyst(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithAssets: true})
	asset := createTestAsset(t, env.MetaDB, "analytics.materialize_granted", domain.PartitionTypeUnpartitioned)
	grantCatalogPrivilege(t, env.MetaDB, "analyst1", domain.PrivExecuteAssetMaterialization)

	resp := doRequest(t, "POST", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/materialize", env.Keys.Analyst, map[string]any{})
	require.Equal(t, httpStatusAccepted, resp.StatusCode)
	_ = readBody(t, resp)
}

func TestHTTP_AssetMaterialize_DeniesWithoutPrivilege(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithAssets: true})
	asset := createTestAsset(t, env.MetaDB, "analytics.materialize_forbidden", domain.PartitionTypeUnpartitioned)

	resp := doRequest(t, "POST", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/materialize", env.Keys.Analyst, map[string]any{})
	require.Equal(t, httpStatusForbidden, resp.StatusCode)
	_ = readBody(t, resp)
}

func TestHTTP_AssetBackfill_CreateAndProcess(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithAssets: true})
	require.NotNil(t, env.Reconciler)

	asset := createTestAsset(t, env.MetaDB, "analytics.partitioned_backfill", domain.PartitionTypeDaily)

	resp := doRequest(t, "POST", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/backfills", env.Keys.Admin, map[string]any{
		"partition_from": "2026-03-01",
		"partition_to":   "2026-03-03",
	})
	require.Equal(t, httpStatusCreated, resp.StatusCode)

	var created map[string]any
	decodeJSON(t, resp, &created)
	request, ok := created["request"].(map[string]any)
	require.True(t, ok)
	backfillID, _ := request["id"].(string)
	require.NotEmpty(t, backfillID)

	require.NoError(t, env.Reconciler.Tick(ctx))

	detailResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/backfills/"+backfillID, env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, detailResp.StatusCode)
	var detail map[string]any
	decodeJSON(t, detailResp, &detail)

	request, ok = detail["request"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, domain.BackfillStatusSuccess, request["status"])

	slices, ok := detail["slices"].([]any)
	require.True(t, ok)
	require.Len(t, slices, 3)
	for i := range slices {
		slice, cast := slices[i].(map[string]any)
		require.True(t, cast)
		assert.Equal(t, domain.BackfillStatusSuccess, slice["status"])
		assert.NotEmpty(t, slice["run_id"])
	}

	runs := listAssetRuns(t, env, asset.AssetKey)
	require.Len(t, runs, 3)
	for i := range runs {
		assert.Equal(t, "2026-03-01", runs[i]["partition_from"])
		assert.Equal(t, "2026-03-03", runs[i]["partition_to"])
	}
}

func TestHTTP_AssetBackfill_AllowsGrantedAnalyst(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithAssets: true})
	asset := createTestAsset(t, env.MetaDB, "analytics.backfill_granted", domain.PartitionTypeDaily)
	grantCatalogPrivilege(t, env.MetaDB, "analyst1", domain.PrivExecuteAssetMaterialization)

	resp := doRequest(t, "POST", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/backfills", env.Keys.Analyst, map[string]any{
		"partition_from": "2026-03-01",
		"partition_to":   "2026-03-03",
	})
	require.Equal(t, httpStatusCreated, resp.StatusCode)
	_ = readBody(t, resp)
}

func TestHTTP_AssetBackfill_DeniesWithoutPrivilege(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithAssets: true})
	asset := createTestAsset(t, env.MetaDB, "analytics.backfill_forbidden", domain.PartitionTypeDaily)

	resp := doRequest(t, "POST", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/backfills", env.Keys.Analyst, map[string]any{
		"partition_from": "2026-03-01",
		"partition_to":   "2026-03-03",
	})
	require.Equal(t, httpStatusForbidden, resp.StatusCode)
	_ = readBody(t, resp)
}

func grantCatalogPrivilege(t *testing.T, db *sql.DB, principalName string, privilege string) {
	t.Helper()
	principal, err := repository.NewPrincipalRepo(db).GetByName(ctx, principalName)
	require.NoError(t, err)

	_, err = dbstore.New(db).GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID:            uuid.New().String(),
		PrincipalID:   principal.ID,
		PrincipalType: "user",
		SecurableType: domain.SecurableCatalog,
		SecurableID:   domain.CatalogID,
		Privilege:     privilege,
	})
	require.NoError(t, err)
}

func TestHTTP_AssetReconciler_WaitsForUpstreamThenExecutes(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithAssets: true})
	require.NotNil(t, env.Reconciler)

	upstream := createTestAsset(t, env.MetaDB, "analytics.upstream_orders", domain.PartitionTypeDaily)
	downstream := createTestAsset(t, env.MetaDB, "analytics.downstream_revenue", domain.PartitionTypeDaily)

	depRepo := repository.NewAssetDependencyRepo(env.MetaDB)
	_, err := depRepo.Create(ctx, &domain.AssetDependency{
		AssetID:         downstream.ID,
		UpstreamAssetID: upstream.ID,
		DependencyType:  domain.DependencyTypeHard,
	})
	require.NoError(t, err)

	partitionKey := "2026-04-01"
	resp := doRequest(t, "POST", env.Server.URL+"/v1/assets/"+downstream.AssetKey+"/materialize", env.Keys.Admin, map[string]any{
		"partition_key": partitionKey,
	})
	require.Equal(t, httpStatusAccepted, resp.StatusCode)
	_ = readBody(t, resp)

	require.NoError(t, env.Reconciler.Tick(ctx))
	assert.Empty(t, listAssetRuns(t, env, downstream.AssetKey))

	seedUpstreamMaterialization(t, env.MetaDB, upstream.ID, partitionKey)
	time.Sleep(2200 * time.Millisecond)
	require.NoError(t, env.Reconciler.Tick(ctx))

	runs := listAssetRuns(t, env, downstream.AssetKey)
	require.NotEmpty(t, runs)
	assert.Equal(t, domain.AssetRunStatusSuccess, runs[0]["status"])
	assert.Equal(t, partitionKey, runs[0]["partition_key"])
}

func createTestAsset(t *testing.T, db *sql.DB, key string, partitionType string) *domain.DataAsset {
	t.Helper()
	repo := repository.NewDataAssetRepo(db)

	asset := &domain.DataAsset{
		ID:        domain.NewID(),
		AssetKey:  key,
		AssetType: domain.AssetTypeModel,
		Owner:     "admin",
		IsActive:  true,
		CreatedBy: "admin",
	}
	if partitionType != "" && partitionType != domain.PartitionTypeUnpartitioned {
		asset.PartitionDefinition = &domain.PartitionDefinition{Type: partitionType}
	}

	created, err := repo.Create(ctx, asset)
	require.NoError(t, err)
	return created
}

func seedUpstreamMaterialization(t *testing.T, db *sql.DB, assetID string, partitionKey string) {
	t.Helper()
	runs := repository.NewAssetRunRepo(db)

	run, err := runs.CreateRun(ctx, &domain.AssetRun{
		ID:           domain.NewID(),
		AssetID:      assetID,
		PartitionKey: &partitionKey,
		Status:       domain.AssetRunStatusSuccess,
		TriggerType:  domain.AssetTriggerTypeManual,
		TriggeredBy:  "admin",
		MaxAttempts:  1,
	})
	require.NoError(t, err)

	_, err = runs.CreateMaterialization(ctx, &domain.AssetMaterialization{
		ID:             domain.NewID(),
		AssetID:        assetID,
		RunID:          &run.ID,
		PartitionKey:   &partitionKey,
		MaterializedAt: time.Now().UTC(),
		MetadataJSON:   map[string]any{"seeded": true},
	})
	require.NoError(t, err)
}

func listAssetRuns(t *testing.T, env *httpTestEnv, assetKey string) []map[string]any {
	t.Helper()

	resp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+assetKey+"/runs", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, resp.StatusCode)

	var result map[string]any
	decodeJSON(t, resp, &result)
	data, ok := result["data"].([]any)
	require.True(t, ok)

	out := make([]map[string]any, 0, len(data))
	for i := range data {
		row, cast := data[i].(map[string]any)
		require.True(t, cast)
		out = append(out, row)
	}
	return out
}

const (
	httpStatusOK        = 200
	httpStatusCreated   = 201
	httpStatusAccepted  = 202
	httpStatusForbidden = 403
)
