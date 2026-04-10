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

	resp := doRequest(t, "POST", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/materializations", env.Keys.Admin, map[string]any{})
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

	resp := doRequest(t, "POST", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/materializations", env.Keys.Analyst, map[string]any{})
	require.Equal(t, httpStatusAccepted, resp.StatusCode)
	_ = readBody(t, resp)
}

func TestHTTP_AssetMaterialize_DeniesWithoutPrivilege(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithAssets: true})
	asset := createTestAsset(t, env.MetaDB, "analytics.materialize_forbidden", domain.PartitionTypeUnpartitioned)

	resp := doRequest(t, "POST", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/materializations", env.Keys.Analyst, map[string]any{})
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

func TestHTTP_AssetReadEndpoints_ReturnExpectedData(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithAssets: true})

	upstream := createTestAsset(t, env.MetaDB, "analytics.read_upstream", domain.PartitionTypeDaily)
	asset := createTestAsset(t, env.MetaDB, "analytics.read_target", domain.PartitionTypeDaily)
	downstream := createTestAsset(t, env.MetaDB, "analytics.read_downstream", domain.PartitionTypeDaily)

	depRepo := repository.NewAssetDependencyRepo(env.MetaDB)
	_, err := depRepo.Create(ctx, &domain.AssetDependency{AssetID: asset.ID, UpstreamAssetID: upstream.ID, DependencyType: domain.DependencyTypeHard})
	require.NoError(t, err)
	_, err = depRepo.Create(ctx, &domain.AssetDependency{AssetID: downstream.ID, UpstreamAssetID: asset.ID, DependencyType: domain.DependencyTypeHard})
	require.NoError(t, err)

	partitionRepo := repository.NewAssetPartitionRepo(env.MetaDB)
	_, err = partitionRepo.Upsert(ctx, &domain.AssetPartition{ID: domain.NewID(), AssetID: asset.ID, PartitionKey: "2026-05-01", Status: "MATERIALIZED", MetadataJSON: map[string]any{"seeded": true}})
	require.NoError(t, err)

	runRepo := repository.NewAssetRunRepo(env.MetaDB)
	run, err := runRepo.CreateRun(ctx, &domain.AssetRun{ID: domain.NewID(), AssetID: asset.ID, Status: domain.AssetRunStatusSuccess, TriggerType: domain.AssetTriggerTypeManual, TriggeredBy: "admin", MaxAttempts: 1})
	require.NoError(t, err)
	_, err = runRepo.CreateMaterialization(ctx, &domain.AssetMaterialization{ID: domain.NewID(), AssetID: asset.ID, RunID: &run.ID, MaterializedAt: time.Now().UTC(), MetadataJSON: map[string]any{"source": "test"}})
	require.NoError(t, err)

	checkRepo := repository.NewAssetCheckRepo(env.MetaDB)
	check, err := checkRepo.CreateCheck(ctx, &domain.AssetCheck{ID: domain.NewID(), AssetID: asset.ID, Name: "row_count", CheckType: "ROW_COUNT", Severity: "WARN", Enabled: true, ConfigJSON: map[string]any{"min": 1}})
	require.NoError(t, err)
	_, err = checkRepo.CreateCheckResult(ctx, &domain.AssetCheckResult{ID: domain.NewID(), CheckID: check.ID, Status: "PASS", MetricsJSON: map[string]any{"rows": float64(12)}})
	require.NoError(t, err)

	backfillRepo := repository.NewBackfillRepo(env.MetaDB)
	backfill, err := backfillRepo.CreateRequest(ctx, &domain.BackfillRequest{ID: domain.NewID(), AssetID: asset.ID, PartitionFrom: "2026-05-01", PartitionTo: "2026-05-01", Status: domain.BackfillStatusPending, RequestedBy: "admin", MaxParallelism: 1})
	require.NoError(t, err)
	_, err = backfillRepo.CreateSlice(ctx, &domain.BackfillSlice{ID: domain.NewID(), RequestID: backfill.ID, AssetID: asset.ID, PartitionKey: "2026-05-01", Status: domain.BackfillStatusPending, MaxAttempts: 1})
	require.NoError(t, err)

	listResp := doRequest(t, "GET", env.Server.URL+"/v1/assets", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, listResp.StatusCode)
	var listResult map[string]any
	decodeJSON(t, listResp, &listResult)
	assetRows, ok := listResult["data"].([]any)
	require.True(t, ok)
	assert.True(t, hasAssetKey(assetRows, asset.AssetKey))

	getResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey, env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, getResp.StatusCode)
	var getResult map[string]any
	decodeJSON(t, getResp, &getResult)
	assert.Equal(t, asset.AssetKey, getResult["asset_key"])

	graphResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/graph", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, graphResp.StatusCode)
	var graphResult map[string]any
	decodeJSON(t, graphResp, &graphResult)
	assert.Contains(t, graphResult["upstream_asset_keys"], upstream.AssetKey)
	assert.Contains(t, graphResult["downstream_asset_keys"], downstream.AssetKey)

	partitionsResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/partitions", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, partitionsResp.StatusCode)
	var partitionsResult map[string]any
	decodeJSON(t, partitionsResp, &partitionsResult)
	partitions, ok := partitionsResult["data"].([]any)
	require.True(t, ok)
	require.Len(t, partitions, 1)

	materializationsResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/materializations", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, materializationsResp.StatusCode)
	var materializationsResult map[string]any
	decodeJSON(t, materializationsResp, &materializationsResult)
	materializations, ok := materializationsResult["data"].([]any)
	require.True(t, ok)
	require.Len(t, materializations, 1)

	checksResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/checks", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, checksResp.StatusCode)
	var checksResult map[string]any
	decodeJSON(t, checksResp, &checksResult)
	checks, ok := checksResult["data"].([]any)
	require.True(t, ok)
	require.Len(t, checks, 1)

	checkResultsResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/checks/results", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, checkResultsResp.StatusCode)
	var checkResultsResult map[string]any
	decodeJSON(t, checkResultsResp, &checkResultsResult)
	checkResults, ok := checkResultsResult["data"].([]any)
	require.True(t, ok)
	require.Len(t, checkResults, 1)

	backfillsResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/backfills", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, backfillsResp.StatusCode)
	var backfillsResult map[string]any
	decodeJSON(t, backfillsResp, &backfillsResult)
	backfills, ok := backfillsResult["data"].([]any)
	require.True(t, ok)
	require.Len(t, backfills, 1)
}

func TestHTTP_AssetReadEndpoints_PaginationAndEmpty(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithAssets: true})
	asset := createTestAsset(t, env.MetaDB, "analytics.read_pagination", domain.PartitionTypeDaily)
	emptyAsset := createTestAsset(t, env.MetaDB, "analytics.read_empty", domain.PartitionTypeDaily)

	partitionRepo := repository.NewAssetPartitionRepo(env.MetaDB)
	_, err := partitionRepo.Upsert(ctx, &domain.AssetPartition{ID: domain.NewID(), AssetID: asset.ID, PartitionKey: "2026-06-01", Status: "MATERIALIZED", MetadataJSON: map[string]any{}})
	require.NoError(t, err)
	_, err = partitionRepo.Upsert(ctx, &domain.AssetPartition{ID: domain.NewID(), AssetID: asset.ID, PartitionKey: "2026-06-02", Status: "MATERIALIZED", MetadataJSON: map[string]any{}})
	require.NoError(t, err)

	checkRepo := repository.NewAssetCheckRepo(env.MetaDB)
	check, err := checkRepo.CreateCheck(ctx, &domain.AssetCheck{ID: domain.NewID(), AssetID: asset.ID, Name: "non_null", CheckType: "NOT_NULL", Severity: "ERROR", Enabled: true, ConfigJSON: map[string]any{}})
	require.NoError(t, err)
	_, err = checkRepo.CreateCheckResult(ctx, &domain.AssetCheckResult{ID: domain.NewID(), CheckID: check.ID, Status: "PASS", MetricsJSON: map[string]any{"rows": float64(10)}})
	require.NoError(t, err)
	_, err = checkRepo.CreateCheckResult(ctx, &domain.AssetCheckResult{ID: domain.NewID(), CheckID: check.ID, Status: "FAIL", MetricsJSON: map[string]any{"rows": float64(3)}})
	require.NoError(t, err)

	partitionsPage1Resp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/partitions?max_results=1", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, partitionsPage1Resp.StatusCode)
	var partitionsPage1 map[string]any
	decodeJSON(t, partitionsPage1Resp, &partitionsPage1)
	page1Rows, ok := partitionsPage1["data"].([]any)
	require.True(t, ok)
	require.Len(t, page1Rows, 1)
	nextPartitionsToken, ok := partitionsPage1["next_page_token"].(string)
	require.True(t, ok)
	require.NotEmpty(t, nextPartitionsToken)

	partitionsPage2Resp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/partitions?max_results=1&page_token="+nextPartitionsToken, env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, partitionsPage2Resp.StatusCode)
	var partitionsPage2 map[string]any
	decodeJSON(t, partitionsPage2Resp, &partitionsPage2)
	page2Rows, ok := partitionsPage2["data"].([]any)
	require.True(t, ok)
	require.Len(t, page2Rows, 1)

	checkResultsPage1Resp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/checks/results?max_results=1", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, checkResultsPage1Resp.StatusCode)
	var checkResultsPage1 map[string]any
	decodeJSON(t, checkResultsPage1Resp, &checkResultsPage1)
	checkPage1Rows, ok := checkResultsPage1["data"].([]any)
	require.True(t, ok)
	require.Len(t, checkPage1Rows, 1)
	nextChecksToken, ok := checkResultsPage1["next_page_token"].(string)
	require.True(t, ok)
	require.NotEmpty(t, nextChecksToken)

	checkResultsPage2Resp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+asset.AssetKey+"/checks/results?max_results=1&page_token="+nextChecksToken, env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, checkResultsPage2Resp.StatusCode)
	var checkResultsPage2 map[string]any
	decodeJSON(t, checkResultsPage2Resp, &checkResultsPage2)
	checkPage2Rows, ok := checkResultsPage2["data"].([]any)
	require.True(t, ok)
	require.Len(t, checkPage2Rows, 1)

	emptyChecksResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+emptyAsset.AssetKey+"/checks", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, emptyChecksResp.StatusCode)
	var emptyChecks map[string]any
	decodeJSON(t, emptyChecksResp, &emptyChecks)
	emptyChecksRows, ok := emptyChecks["data"].([]any)
	require.True(t, ok)
	require.Empty(t, emptyChecksRows)

	emptyBackfillsResp := doRequest(t, "GET", env.Server.URL+"/v1/assets/"+emptyAsset.AssetKey+"/backfills", env.Keys.Admin, nil)
	require.Equal(t, httpStatusOK, emptyBackfillsResp.StatusCode)
	var emptyBackfills map[string]any
	decodeJSON(t, emptyBackfillsResp, &emptyBackfills)
	emptyBackfillRows, ok := emptyBackfills["data"].([]any)
	require.True(t, ok)
	require.Empty(t, emptyBackfillRows)
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
	resp := doRequest(t, "POST", env.Server.URL+"/v1/assets/"+downstream.AssetKey+"/materializations", env.Keys.Admin, map[string]any{
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

func hasAssetKey(rows []any, assetKey string) bool {
	for i := range rows {
		row, ok := rows[i].(map[string]any)
		if !ok {
			continue
		}
		if key, cast := row["asset_key"].(string); cast && key == assetKey {
			return true
		}
	}
	return false
}

const (
	httpStatusOK        = 200
	httpStatusCreated   = 201
	httpStatusAccepted  = 202
	httpStatusForbidden = 403
)
