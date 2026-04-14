package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/domain"
)

func setupAssetRepos(t *testing.T) (*DataAssetRepo, *AssetRunRepo, *AssetCheckRepo, *AssetPartitionRepo, *AssetDependencyRepo) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewDataAssetRepo(writeDB), NewAssetRunRepo(writeDB), NewAssetCheckRepo(writeDB), NewAssetPartitionRepo(writeDB), NewAssetDependencyRepo(writeDB)
}

func TestDataAssetRepo_CreateGetList(t *testing.T) {
	assetRepo, _, _, _, _ := setupAssetRepos(t)
	ctx := context.Background()

	asset, err := assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.sales.daily_orders",
		AssetType: domain.AssetTypeTable,
		Owner:     "data-platform",
		Tags:      []string{"finance", "daily"},
		SchemaJSON: map[string]any{
			"columns": []string{"order_id", "amount"},
		},
		PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily, Timezone: "UTC"},
		FreshnessPolicy:     &domain.AssetFreshnessPolicy{MaxLagSeconds: 3600},
		CreatedBy:           "admin",
		IsActive:            true,
	})
	require.NoError(t, err)
	require.NotNil(t, asset)

	got, err := assetRepo.GetByKey(ctx, "main.sales.daily_orders")
	require.NoError(t, err)
	assert.Equal(t, asset.ID, got.ID)
	assert.Equal(t, domain.PartitionTypeDaily, got.PartitionDefinition.Type)
	assert.Equal(t, int64(3600), got.FreshnessPolicy.MaxLagSeconds)

	active := true
	list, total, err := assetRepo.List(ctx, domain.AssetFilter{IsActive: &active, Page: domain.PageRequest{MaxResults: 10}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, list, 1)
	assert.Equal(t, "main.sales.daily_orders", list[0].AssetKey)
}

func TestAssetRunRepo_RunEventMaterialization(t *testing.T) {
	assetRepo, runRepo, _, partitionRepo, depRepo := setupAssetRepos(t)
	ctx := context.Background()

	upstream, err := assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.raw.orders",
		AssetType: domain.AssetTypeTable,
		Owner:     "data-platform",
		CreatedBy: "admin",
		IsActive:  true,
	})
	require.NoError(t, err)

	asset, err := assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.curated.daily_orders",
		AssetType: domain.AssetTypeTable,
		Owner:     "data-platform",
		CreatedBy: "admin",
		IsActive:  true,
	})
	require.NoError(t, err)

	_, err = depRepo.Create(ctx, &domain.AssetDependency{
		AssetID:         asset.ID,
		UpstreamAssetID: upstream.ID,
		DependencyType:  domain.DependencyTypeHard,
	})
	require.NoError(t, err)

	partitionKey := "2026-03-05"
	partitionFrom := "2026-03-01"
	partitionTo := "2026-03-07"
	_, err = partitionRepo.Upsert(ctx, &domain.AssetPartition{
		AssetID:      asset.ID,
		PartitionKey: partitionKey,
		Status:       "MISSING",
	})
	require.NoError(t, err)

	run, err := runRepo.CreateRun(ctx, &domain.AssetRun{
		AssetID:       asset.ID,
		PartitionKey:  &partitionKey,
		PartitionFrom: &partitionFrom,
		PartitionTo:   &partitionTo,
		Status:        domain.AssetRunStatusQueued,
		TriggerType:   domain.AssetTriggerTypeReconciler,
		TriggeredBy:   "system",
		MaxAttempts:   3,
	})
	require.NoError(t, err)

	err = runRepo.UpdateRunStarted(ctx, run.ID)
	require.NoError(t, err)

	msg := "row count validated"
	_, err = runRepo.CreateRunEvent(ctx, &domain.AssetRunEvent{
		RunID:     run.ID,
		EventType: "CHECK_PASS",
		EventAt:   time.Now().UTC(),
		Message:   &msg,
		StatsJSON: map[string]any{"row_count": 42},
	})
	require.NoError(t, err)

	rows := int64(42)
	_, err = runRepo.CreateMaterialization(ctx, &domain.AssetMaterialization{
		AssetID:        asset.ID,
		RunID:          &run.ID,
		PartitionKey:   &partitionKey,
		MetadataJSON:   map[string]any{"source": "integration-test"},
		RowCount:       &rows,
		MaterializedAt: time.Now().UTC(),
	})
	require.NoError(t, err)

	errMsg := "transient"
	err = runRepo.UpdateRunRetrying(ctx, run.ID, 1, &errMsg)
	require.NoError(t, err)
	err = runRepo.UpdateRunFinished(ctx, run.ID, domain.AssetRunStatusSuccess, nil)
	require.NoError(t, err)

	updatedRun, err := runRepo.GetRunByID(ctx, run.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.AssetRunStatusSuccess, updatedRun.Status)
	require.NotNil(t, updatedRun.PartitionFrom)
	require.NotNil(t, updatedRun.PartitionTo)
	assert.Equal(t, partitionFrom, *updatedRun.PartitionFrom)
	assert.Equal(t, partitionTo, *updatedRun.PartitionTo)

	events, totalEvents, err := runRepo.ListRunEvents(ctx, run.ID, domain.PageRequest{MaxResults: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), totalEvents)
	require.Len(t, events, 1)
	assert.Equal(t, "CHECK_PASS", events[0].EventType)

	mats, totalMats, err := runRepo.ListMaterializationsByAsset(ctx, asset.ID, domain.PageRequest{MaxResults: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), totalMats)
	require.Len(t, mats, 1)
	assert.Equal(t, rows, *mats[0].RowCount)
}

func TestAssetCheckRepo_CreateAndResult(t *testing.T) {
	assetRepo, runRepo, checkRepo, _, _ := setupAssetRepos(t)
	ctx := context.Background()

	asset, err := assetRepo.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.finance.monthly_revenue",
		AssetType: domain.AssetTypeTable,
		Owner:     "finance",
		CreatedBy: "admin",
		IsActive:  true,
	})
	require.NoError(t, err)

	run, err := runRepo.CreateRun(ctx, &domain.AssetRun{
		AssetID:     asset.ID,
		Status:      domain.AssetRunStatusQueued,
		TriggerType: domain.AssetTriggerTypeManual,
		TriggeredBy: "admin",
		MaxAttempts: 1,
	})
	require.NoError(t, err)

	check, err := checkRepo.CreateCheck(ctx, &domain.AssetCheck{
		AssetID:    asset.ID,
		Name:       "row_count_positive",
		CheckType:  "custom_sql",
		Severity:   "ERROR",
		ConfigJSON: map[string]any{"sql": "select 1 where 1=0"},
		Enabled:    true,
	})
	require.NoError(t, err)

	message := "zero failing rows"
	_, err = checkRepo.CreateCheckResult(ctx, &domain.AssetCheckResult{
		CheckID:     check.ID,
		RunID:       &run.ID,
		Status:      domain.TestResultPass,
		Message:     &message,
		MetricsJSON: map[string]any{"failing_rows": 0},
	})
	require.NoError(t, err)

	results, total, err := checkRepo.ListCheckResults(ctx, check.ID, domain.PageRequest{MaxResults: 10})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, results, 1)
	assert.Equal(t, domain.TestResultPass, results[0].Status)
}
