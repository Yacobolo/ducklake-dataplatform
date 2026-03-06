package repository

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func setupQueueRepos(t *testing.T) (*OrchestrationEventRepo, *BackfillRepo, *DataAssetRepo) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewOrchestrationEventRepo(writeDB), NewBackfillRepo(writeDB), NewDataAssetRepo(writeDB)
}

func TestOrchestrationEventRepo_EnqueueClaimAndProcess(t *testing.T) {
	events, _, assets := setupQueueRepos(t)
	ctx := context.Background()

	asset, err := assets.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.ops.events",
		AssetType: domain.AssetTypeTable,
		Owner:     "ops",
		CreatedBy: "admin",
		IsActive:  true,
	})
	require.NoError(t, err)

	assetID := asset.ID
	event, err := events.Enqueue(ctx, &domain.OrchestrationEvent{
		EventType:      "UPSTREAM_UPDATE",
		AssetID:        &assetID,
		Status:         domain.OrchestrationEventStatusPending,
		PayloadJSON:    map[string]any{"source": "test"},
		IdempotencyKey: ptr("event-1"),
	})
	require.NoError(t, err)

	claimed, err := events.ClaimNextPending(ctx, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, event.ID, claimed.ID)
	assert.Equal(t, domain.OrchestrationEventStatusProcessing, claimed.Status)
	assert.Equal(t, 1, claimed.AttemptCount)

	err = events.MarkProcessed(ctx, claimed.ID)
	require.NoError(t, err)

	status := domain.OrchestrationEventStatusProcessed
	list, total, err := events.List(ctx, domain.OrchestrationEventFilter{Status: &status, Page: domain.PageRequest{MaxResults: 10}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, list, 1)
	assert.Equal(t, domain.OrchestrationEventStatusProcessed, list[0].Status)
}

func TestOrchestrationEventRepo_Enqueue_IdempotentByKey(t *testing.T) {
	events, _, assets := setupQueueRepos(t)
	ctx := context.Background()

	asset, err := assets.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.ops.idem",
		AssetType: domain.AssetTypeTable,
		Owner:     "ops",
		CreatedBy: "admin",
		IsActive:  true,
	})
	require.NoError(t, err)

	assetID := asset.ID
	key := "event-idem-1"
	first, err := events.Enqueue(ctx, &domain.OrchestrationEvent{
		EventType:      "UPSTREAM_UPDATE",
		AssetID:        &assetID,
		Status:         domain.OrchestrationEventStatusPending,
		PayloadJSON:    map[string]any{"source": "first"},
		IdempotencyKey: &key,
	})
	require.NoError(t, err)

	second, err := events.Enqueue(ctx, &domain.OrchestrationEvent{
		EventType:      "UPSTREAM_UPDATE",
		AssetID:        &assetID,
		Status:         domain.OrchestrationEventStatusPending,
		PayloadJSON:    map[string]any{"source": "second"},
		IdempotencyKey: &key,
	})
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.Equal(t, first.ID, second.ID)

	all, total, err := events.List(ctx, domain.OrchestrationEventFilter{Page: domain.PageRequest{MaxResults: 10}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, all, 1)
	assert.Equal(t, key, derefStr(all[0].IdempotencyKey))
}

func TestBackfillRepo_CreateAndUpdate(t *testing.T) {
	_, backfills, assets := setupQueueRepos(t)
	ctx := context.Background()

	asset, err := assets.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.finance.daily_revenue",
		AssetType: domain.AssetTypeTable,
		Owner:     "finance",
		CreatedBy: "admin",
		IsActive:  true,
	})
	require.NoError(t, err)

	req, err := backfills.CreateRequest(ctx, &domain.BackfillRequest{
		AssetID:        asset.ID,
		PartitionFrom:  "2026-03-01",
		PartitionTo:    "2026-03-03",
		RequestedBy:    "admin",
		MaxParallelism: 0,
	})
	require.NoError(t, err)
	require.NotEmpty(t, req.ID)
	assert.Equal(t, 1, req.MaxParallelism)

	slice, err := backfills.CreateSlice(ctx, &domain.BackfillSlice{
		RequestID:    req.ID,
		AssetID:      asset.ID,
		PartitionKey: "2026-03-01",
	})
	require.NoError(t, err)

	err = backfills.UpdateSliceStatus(ctx, slice.ID, domain.BackfillStatusRunning, nil, nil)
	require.NoError(t, err)
	err = backfills.UpdateSliceStatus(ctx, slice.ID, domain.BackfillStatusSuccess, nil, nil)
	require.NoError(t, err)

	err = backfills.UpdateRequestStatus(ctx, req.ID, domain.BackfillStatusSuccess, nil)
	require.NoError(t, err)

	list, total, err := backfills.ListRequests(ctx, domain.BackfillFilter{Page: domain.PageRequest{MaxResults: 10}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, list, 1)
	assert.Equal(t, domain.BackfillStatusSuccess, list[0].Status)
	assert.Equal(t, 1, list[0].MaxParallelism)
}

func TestOrchestrationEventRepo_ClaimNextPending_ConcurrentSingleWinner(t *testing.T) {
	events, _, assets := setupQueueRepos(t)
	ctx := context.Background()

	asset, err := assets.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.ops.events_race",
		AssetType: domain.AssetTypeTable,
		Owner:     "ops",
		CreatedBy: "admin",
		IsActive:  true,
	})
	require.NoError(t, err)

	assetID := asset.ID
	_, err = events.Enqueue(ctx, &domain.OrchestrationEvent{
		EventType:   "UPSTREAM_UPDATE",
		AssetID:     &assetID,
		Status:      domain.OrchestrationEventStatusPending,
		PayloadJSON: map[string]any{"source": "race-test"},
	})
	require.NoError(t, err)

	type claimResult struct {
		event *domain.OrchestrationEvent
		err   error
	}

	const workers = 8
	start := make(chan struct{})
	results := make(chan claimResult, workers)
	var wg sync.WaitGroup
	claimAt := time.Now().UTC()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			event, claimErr := events.ClaimNextPending(ctx, claimAt)
			results <- claimResult{event: event, err: claimErr}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	successes := 0
	notFounds := 0
	for result := range results {
		if result.err == nil {
			require.NotNil(t, result.event)
			successes++
			continue
		}
		var notFoundErr *domain.NotFoundError
		if errors.As(result.err, &notFoundErr) {
			notFounds++
			continue
		}
		require.NoError(t, result.err)
	}

	assert.Equal(t, 1, successes)
	assert.Equal(t, workers-1, notFounds)
}

func TestOrchestrationEventRepo_ClaimNextPending_DoesNotReclaimFreshProcessing(t *testing.T) {
	events, _, assets := setupQueueRepos(t)
	ctx := context.Background()

	asset, err := assets.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.ops.events_fresh_processing",
		AssetType: domain.AssetTypeTable,
		Owner:     "ops",
		CreatedBy: "admin",
		IsActive:  true,
	})
	require.NoError(t, err)

	assetID := asset.ID
	_, err = events.Enqueue(ctx, &domain.OrchestrationEvent{
		EventType:   "UPSTREAM_UPDATE",
		AssetID:     &assetID,
		Status:      domain.OrchestrationEventStatusPending,
		PayloadJSON: map[string]any{"source": "fresh-processing"},
	})
	require.NoError(t, err)

	claimed, err := events.ClaimNextPending(ctx, time.Now().UTC())
	require.NoError(t, err)
	require.NotNil(t, claimed)

	_, err = events.ClaimNextPending(ctx, time.Now().UTC())
	require.Error(t, err)
	var notFoundErr *domain.NotFoundError
	require.ErrorAs(t, err, &notFoundErr)
}

func TestOrchestrationEventRepo_ClaimNextPending_ReclaimsStaleProcessing(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	events := NewOrchestrationEventRepo(writeDB)
	assets := NewDataAssetRepo(writeDB)
	ctx := context.Background()

	asset, err := assets.Create(ctx, &domain.DataAsset{
		AssetKey:  "main.ops.events_stale_processing",
		AssetType: domain.AssetTypeTable,
		Owner:     "ops",
		CreatedBy: "admin",
		IsActive:  true,
	})
	require.NoError(t, err)

	assetID := asset.ID
	enqueued, err := events.Enqueue(ctx, &domain.OrchestrationEvent{
		EventType:   "UPSTREAM_UPDATE",
		AssetID:     &assetID,
		Status:      domain.OrchestrationEventStatusPending,
		PayloadJSON: map[string]any{"source": "stale-processing"},
	})
	require.NoError(t, err)

	claimed, err := events.ClaimNextPending(ctx, time.Now().UTC())
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, enqueued.ID, claimed.ID)
	assert.Equal(t, 1, claimed.AttemptCount)

	staleAt := time.Now().UTC().Add(-(processingLeaseTTL + time.Minute))
	_, err = writeDB.ExecContext(ctx, `
		UPDATE orchestration_events
		SET updated_at = ?
		WHERE id = ?
	`, staleAt, enqueued.ID)
	require.NoError(t, err)

	reclaimed, err := events.ClaimNextPending(ctx, time.Now().UTC())
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	assert.Equal(t, enqueued.ID, reclaimed.ID)
	assert.Equal(t, domain.OrchestrationEventStatusProcessing, reclaimed.Status)
	assert.Equal(t, 2, reclaimed.AttemptCount)
}

func TestClaimRetryableLockError(t *testing.T) {
	t.Parallel()

	assert.True(t, claimRetryableLockError(errors.New("database is locked")))
	assert.True(t, claimRetryableLockError(errors.New("database table is locked: orchestration_events")))
	assert.True(t, claimRetryableLockError(errors.New("database schema is locked")))
	assert.True(t, claimRetryableLockError(errors.New("database is busy")))
	assert.False(t, claimRetryableLockError(errors.New("constraint failed")))
}

func ptr(s string) *string { return &s }

func derefStr(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
