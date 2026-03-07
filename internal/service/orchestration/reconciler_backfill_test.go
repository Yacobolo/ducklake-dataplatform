package orchestration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestReconciler_ShadowModeProcessesEvent(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-1"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:        "ev-1",
		EventType: domain.AssetTriggerTypeUpstreamUpdate,
		AssetID:   &assetID,
		Status:    domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{assetID: {ID: assetID}}}
	runRepo := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	scheduler := NewAssetScheduler(assetRepo, &fakeDeps{downstream: map[string][]domain.AssetDependency{}}, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, true)
	require.NoError(t, r.Tick(context.Background()))

	status := domain.OrchestrationEventStatusProcessed
	list, total, err := eventRepo.List(context.Background(), domain.OrchestrationEventFilter{Status: &status, Page: domain.PageRequest{MaxResults: 10}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, list, 1)
}

func TestReconciler_BlockedOnMissingUpstreamReadiness(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-target"
	upstreamID := "asset-upstream"
	partitionKey := "2026-03-01"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:           "ev-missing-upstream",
		EventType:    domain.AssetTriggerTypeUpstreamUpdate,
		AssetID:      &assetID,
		PartitionKey: &partitionKey,
		Status:       domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{
		assetID: {
			ID:                  assetID,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily},
		},
		upstreamID: {
			ID:                  upstreamID,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily},
		},
	}}
	runRepo := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	deps := &fakeDeps{
		upstream: map[string][]domain.AssetDependency{
			assetID: {{AssetID: assetID, UpstreamAssetID: upstreamID, DependencyType: domain.DependencyTypeHard}},
		},
		downstream: map[string][]domain.AssetDependency{},
	}
	scheduler := NewAssetScheduler(assetRepo, deps, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, false)
	require.NoError(t, r.Tick(context.Background()))

	assert.Empty(t, runRepo.runs)
	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusPending, eventRepo.events[0].Status)
	require.NotNil(t, eventRepo.events[0].LastError)
	assert.Contains(t, *eventRepo.events[0].LastError, "waiting for upstream")
	assert.Contains(t, *eventRepo.events[0].LastError, partitionKey)
	assert.True(t, eventRepo.events[0].AvailableAt.After(time.Now().UTC().Add(-1*time.Second)))
}

func TestReconciler_RetryableFailureStopsAfterMaxAttempts(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-target-max-attempts"
	upstreamID := "asset-upstream-max-attempts"
	partitionKey := "2026-03-01"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:           "ev-max-attempts",
		EventType:    domain.AssetTriggerTypeUpstreamUpdate,
		AssetID:      &assetID,
		PartitionKey: &partitionKey,
		Status:       domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{
		assetID: {
			ID:                  assetID,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily},
		},
		upstreamID: {
			ID:                  upstreamID,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily},
		},
	}}
	runRepo := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	deps := &fakeDeps{
		upstream: map[string][]domain.AssetDependency{
			assetID: {{AssetID: assetID, UpstreamAssetID: upstreamID, DependencyType: domain.DependencyTypeHard}},
		},
		downstream: map[string][]domain.AssetDependency{},
	}
	scheduler := NewAssetScheduler(assetRepo, deps, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})
	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, false)

	for attempt := 1; attempt <= reconcilerMaxAttempts; attempt++ {
		require.NoError(t, r.Tick(context.Background()))
		require.Len(t, eventRepo.events, 1)

		event := eventRepo.events[0]
		assert.Equal(t, attempt, event.AttemptCount)
		require.NotNil(t, event.LastError)
		assert.Contains(t, *event.LastError, "waiting for upstream")

		if attempt < reconcilerMaxAttempts {
			assert.Equal(t, domain.OrchestrationEventStatusPending, event.Status)
			eventRepo.events[0].AvailableAt = time.Now().UTC().Add(-time.Millisecond)
			continue
		}

		assert.Equal(t, domain.OrchestrationEventStatusFailed, event.Status)
	}

	require.NoError(t, r.Tick(context.Background()))
	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, reconcilerMaxAttempts, eventRepo.events[0].AttemptCount)
	assert.Equal(t, domain.OrchestrationEventStatusFailed, eventRepo.events[0].Status)
}

func TestReconciler_ReadyPathExecutes(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-target"
	upstreamID := "asset-upstream"
	partitionKey := "2026-03-01"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:           "ev-ready",
		EventType:    domain.AssetTriggerTypeUpstreamUpdate,
		AssetID:      &assetID,
		PartitionKey: &partitionKey,
		Status:       domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{
		assetID: {
			ID:                  assetID,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily},
		},
		upstreamID: {
			ID:                  upstreamID,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily},
		},
	}}
	now := time.Now().UTC()
	runRepo := &fakeRunRepo{
		runs: map[string]*domain.AssetRun{},
		materialize: map[string][]domain.AssetMaterialization{
			upstreamID: {{AssetID: upstreamID, PartitionKey: &partitionKey, MaterializedAt: now.Add(-2 * time.Minute)}},
		},
	}
	deps := &fakeDeps{
		upstream: map[string][]domain.AssetDependency{
			assetID: {{AssetID: assetID, UpstreamAssetID: upstreamID, DependencyType: domain.DependencyTypeHard}},
		},
		downstream: map[string][]domain.AssetDependency{},
	}
	scheduler := NewAssetScheduler(assetRepo, deps, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, false)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusProcessed, eventRepo.events[0].Status)
	require.Len(t, runRepo.runs, 1)
	for _, run := range runRepo.runs {
		assert.Equal(t, assetID, run.AssetID)
		require.NotNil(t, run.PartitionKey)
		assert.Equal(t, partitionKey, *run.PartitionKey)
		assert.Equal(t, domain.AssetRunStatusSuccess, run.Status)
	}
}

func TestReconciler_PolicyIntervalBlocksImmediateRetrigger(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-interval"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:        "ev-policy",
		EventType: domain.AssetTriggerTypeUpstreamUpdate,
		AssetID:   &assetID,
		Status:    domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{
		assetID: {
			ID: assetID,
			AutoMaterializePolicy: &domain.AssetAutoMaterializePolicy{
				MinIntervalSeconds:     600,
				OnUpstreamMaterialized: true,
			},
		},
	}}
	runRepo := &fakeRunRepo{
		runs: map[string]*domain.AssetRun{},
		materialize: map[string][]domain.AssetMaterialization{
			assetID: {{AssetID: assetID, MaterializedAt: time.Now().UTC().Add(-30 * time.Second)}},
		},
	}
	deps := &fakeDeps{downstream: map[string][]domain.AssetDependency{}}
	scheduler := NewAssetScheduler(assetRepo, deps, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, false)
	require.NoError(t, r.Tick(context.Background()))

	assert.Empty(t, runRepo.runs)
	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusPending, eventRepo.events[0].Status)
	require.NotNil(t, eventRepo.events[0].LastError)
	assert.Contains(t, *eventRepo.events[0].LastError, "min interval")
}

func TestReconciler_AutoMaterializeModeOffSkipsNonManualEvents(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-policy-off"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:        "ev-policy-off",
		EventType: domain.AssetTriggerTypeUpstreamUpdate,
		AssetID:   &assetID,
		Status:    domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{
		assetID: {
			ID: assetID,
			AutoMaterializePolicy: &domain.AssetAutoMaterializePolicy{
				Mode: "off",
			},
		},
	}}
	runRepo := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	scheduler := NewAssetScheduler(assetRepo, &fakeDeps{downstream: map[string][]domain.AssetDependency{}}, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, false)
	require.NoError(t, r.Tick(context.Background()))

	assert.Empty(t, runRepo.runs)
	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusProcessed, eventRepo.events[0].Status)
	require.NotNil(t, eventRepo.events[0].LastError)
	assert.Contains(t, *eventRepo.events[0].LastError, "suppresses reconciler auto-trigger")
}

func TestReconciler_AutoMaterializeModeManualAllowsManualEvent(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-policy-manual"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:        "ev-policy-manual",
		EventType: domain.AssetTriggerTypeManual,
		AssetID:   &assetID,
		Status:    domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{
		assetID: {
			ID: assetID,
			AutoMaterializePolicy: &domain.AssetAutoMaterializePolicy{
				Mode: "manual",
			},
		},
	}}
	runRepo := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	scheduler := NewAssetScheduler(assetRepo, &fakeDeps{downstream: map[string][]domain.AssetDependency{}}, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, false)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, runRepo.runs, 1)
	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusProcessed, eventRepo.events[0].Status)
	assert.Nil(t, eventRepo.events[0].LastError)
}

func TestReconciler_OnUpstreamMaterializedFalseSkipsUpstreamUpdateEvent(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-no-upstream-trigger"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:        "ev-no-upstream-trigger",
		EventType: domain.AssetTriggerTypeUpstreamUpdate,
		AssetID:   &assetID,
		Status:    domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{
		assetID: {
			ID: assetID,
			AutoMaterializePolicy: &domain.AssetAutoMaterializePolicy{
				OnUpstreamMaterialized: false,
			},
		},
	}}
	runRepo := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	scheduler := NewAssetScheduler(assetRepo, &fakeDeps{downstream: map[string][]domain.AssetDependency{}}, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, false)
	require.NoError(t, r.Tick(context.Background()))

	assert.Empty(t, runRepo.runs)
	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusProcessed, eventRepo.events[0].Status)
	require.NotNil(t, eventRepo.events[0].LastError)
	assert.Contains(t, *eventRepo.events[0].LastError, "disables upstream materialization triggers")
}

func TestReconciler_OnFreshnessBreachTrueAllowsFreshnessEvent(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-freshness-trigger"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:        "ev-freshness-trigger",
		EventType: domain.AssetTriggerTypeFreshnessBreach,
		AssetID:   &assetID,
		Status:    domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{
		assetID: {
			ID: assetID,
			AutoMaterializePolicy: &domain.AssetAutoMaterializePolicy{
				OnFreshnessBreach:      true,
				OnUpstreamMaterialized: false,
			},
		},
	}}
	runRepo := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	scheduler := NewAssetScheduler(assetRepo, &fakeDeps{downstream: map[string][]domain.AssetDependency{}}, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, false)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, runRepo.runs, 1)
	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusProcessed, eventRepo.events[0].Status)
	assert.Nil(t, eventRepo.events[0].LastError)
}

func TestReconciler_PartitionedFanInRequiresAllHardUpstreamsMatchingPartition(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-target-fanin"
	upstreamA := "asset-upstream-a"
	upstreamB := "asset-upstream-b"
	partitionKey := "2026-03-02"
	otherPartition := "2026-03-01"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:           "ev-fanin",
		EventType:    domain.AssetTriggerTypeUpstreamUpdate,
		AssetID:      &assetID,
		PartitionKey: &partitionKey,
		Status:       domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{
		assetID: {
			ID:                  assetID,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily},
			AutoMaterializePolicy: &domain.AssetAutoMaterializePolicy{
				RequireAllUpstreams:    true,
				OnUpstreamMaterialized: true,
			},
		},
		upstreamA: {
			ID:                  upstreamA,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily},
		},
		upstreamB: {
			ID:                  upstreamB,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily},
		},
	}}
	runRepo := &fakeRunRepo{
		runs: map[string]*domain.AssetRun{},
		materialize: map[string][]domain.AssetMaterialization{
			upstreamA: {{AssetID: upstreamA, PartitionKey: &partitionKey, MaterializedAt: time.Now().UTC().Add(-2 * time.Minute)}},
			upstreamB: {{AssetID: upstreamB, PartitionKey: &otherPartition, MaterializedAt: time.Now().UTC().Add(-2 * time.Minute)}},
		},
	}
	deps := &fakeDeps{
		upstream: map[string][]domain.AssetDependency{
			assetID: {
				{AssetID: assetID, UpstreamAssetID: upstreamA, DependencyType: domain.DependencyTypeHard},
				{AssetID: assetID, UpstreamAssetID: upstreamB, DependencyType: domain.DependencyTypeHard},
			},
		},
		downstream: map[string][]domain.AssetDependency{},
	}
	scheduler := NewAssetScheduler(assetRepo, deps, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, false)
	require.NoError(t, r.Tick(context.Background()))

	assert.Empty(t, runRepo.runs)
	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusPending, eventRepo.events[0].Status)
	require.NotNil(t, eventRepo.events[0].LastError)
	assert.Contains(t, *eventRepo.events[0].LastError, "required upstream")
	assert.Contains(t, *eventRepo.events[0].LastError, upstreamB)
	assert.Contains(t, *eventRepo.events[0].LastError, partitionKey)
}

func TestReconciler_PartitionedTargetAllowsUnpartitionedUpstreamMaterialization(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-target-partitioned"
	upstreamID := "asset-upstream-unpartitioned"
	partitionKey := "2026-03-02"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:           "ev-unpartitioned-upstream",
		EventType:    domain.AssetTriggerTypeUpstreamUpdate,
		AssetID:      &assetID,
		PartitionKey: &partitionKey,
		Status:       domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{
		assetID: {
			ID:                  assetID,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily},
		},
		upstreamID: {
			ID:                  upstreamID,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeUnpartitioned},
		},
	}}
	runRepo := &fakeRunRepo{
		runs: map[string]*domain.AssetRun{},
		materialize: map[string][]domain.AssetMaterialization{
			upstreamID: {{AssetID: upstreamID, MaterializedAt: time.Now().UTC().Add(-2 * time.Minute)}},
		},
	}
	deps := &fakeDeps{
		upstream: map[string][]domain.AssetDependency{
			assetID: {{AssetID: assetID, UpstreamAssetID: upstreamID, DependencyType: domain.DependencyTypeHard}},
		},
		downstream: map[string][]domain.AssetDependency{},
	}
	scheduler := NewAssetScheduler(assetRepo, deps, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, false)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, runRepo.runs, 1)
	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusProcessed, eventRepo.events[0].Status)
}

func TestBackfillService_CreateEmitsSingleEventPerRequest(t *testing.T) {
	eventRepo := &memEventRepo{}
	router := NewTriggerRouter(eventRepo)
	backfills := &memBackfillRepo{}
	svc := NewBackfillService(backfills, router, nil, nil)

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true, Type: "user"})
	req, slices, err := svc.Create(ctx, "asset-1", "admin", "2026-03-01", "2026-03-03", 2)
	require.NoError(t, err)
	require.NotNil(t, req)
	assert.Len(t, slices, 3)
	assert.Len(t, backfills.slices, 3)
	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.AssetTriggerTypeBackfill, eventRepo.events[0].EventType)
	assert.Nil(t, eventRepo.events[0].PartitionKey)
	require.NotNil(t, eventRepo.events[0].IdempotencyKey)
	assert.Equal(t, backfillRequestEventIdempotencyKey(req.ID), *eventRepo.events[0].IdempotencyKey)
}

func TestBackfillService_CreateFailsWhenEventEnqueueFails(t *testing.T) {
	eventRepo := &memEventRepo{failEnqueueAtCall: 1, enqueueErr: errors.New("queue unavailable")}
	router := NewTriggerRouter(eventRepo)
	backfills := &memBackfillRepo{}
	svc := NewBackfillService(backfills, router, nil, nil)

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true, Type: "user"})
	req, slices, err := svc.Create(ctx, "asset-1", "admin", "2026-03-01", "2026-03-03", 2)
	require.Error(t, err)
	require.Nil(t, req)
	require.Nil(t, slices)
	assert.Contains(t, err.Error(), "queue unavailable")

	require.Len(t, backfills.requests, 1)
	assert.Equal(t, domain.BackfillStatusFailed, backfills.requests[0].Status)
	require.NotNil(t, backfills.requests[0].ErrorMessage)
	assert.Contains(t, *backfills.requests[0].ErrorMessage, "queue unavailable")

	require.Len(t, backfills.slices, 3)
	for _, slice := range backfills.slices {
		assert.Equal(t, domain.BackfillStatusFailed, slice.Status)
		require.NotNil(t, slice.ErrorMessage)
		assert.Contains(t, *slice.ErrorMessage, "queue unavailable")
	}
	assert.Empty(t, eventRepo.events)
}

func TestBackfillService_CreateUsesIdempotentBackfillEventKey(t *testing.T) {
	eventRepo := &memEventRepo{}
	router := NewTriggerRouter(eventRepo)
	backfills := &memBackfillRepo{}
	svc := NewBackfillService(backfills, router, nil, nil)

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true, Type: "user"})
	req, _, err := svc.Create(ctx, "asset-1", "admin", "2026-03-01", "2026-03-01", 1)
	require.NoError(t, err)

	idemKey := backfillRequestEventIdempotencyKey(req.ID)
	assetID := req.AssetID
	_, err = router.Ingest(ctx, domain.AssetTriggerTypeBackfill, &assetID, nil, map[string]any{"backfill_request_id": req.ID}, &idemKey)
	require.NoError(t, err)

	require.Len(t, eventRepo.events, 1)
}

func TestBackfillService_CreateValidatesRangeBeforePersistingRequest(t *testing.T) {
	eventRepo := &memEventRepo{}
	router := NewTriggerRouter(eventRepo)
	backfills := &memBackfillRepo{}
	svc := NewBackfillService(backfills, router, nil, nil)

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "admin", IsAdmin: true, Type: "user"})
	req, slices, err := svc.Create(ctx, "asset-1", "admin", "2026-03-03", "2026-03-01", 2)
	require.Error(t, err)
	require.Nil(t, req)
	require.Nil(t, slices)
	assert.Contains(t, err.Error(), "partition_from must be <= partition_to")
	assert.Empty(t, backfills.requests)
	assert.Empty(t, backfills.slices)
	assert.Empty(t, eventRepo.events)
}

func TestReconciler_BackfillEventRunsBackfillRunner(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-1"
	requestID := "req-1"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:        "ev-backfill",
		EventType: domain.AssetTriggerTypeBackfill,
		AssetID:   &assetID,
		Status:    domain.OrchestrationEventStatusPending,
		PayloadJSON: map[string]any{
			"backfill_request_id": requestID,
		},
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{assetID: {ID: assetID}}}
	runRepo := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	deps := &fakeDeps{upstream: map[string][]domain.AssetDependency{}, downstream: map[string][]domain.AssetDependency{}}
	scheduler := NewAssetScheduler(assetRepo, deps, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})
	backfills := &memBackfillRepo{
		requests: []domain.BackfillRequest{{
			ID:            requestID,
			AssetID:       assetID,
			PartitionFrom: "2026-03-01",
			PartitionTo:   "2026-03-02",
			Status:        domain.BackfillStatusPending,
			RequestedBy:   "admin",
		}},
		slices: []domain.BackfillSlice{
			{ID: "s-2", RequestID: requestID, AssetID: assetID, PartitionKey: "2026-03-02", Status: domain.BackfillStatusPending},
			{ID: "s-1", RequestID: requestID, AssetID: assetID, PartitionKey: "2026-03-01", Status: domain.BackfillStatusPending},
		},
	}
	runner := NewBackfillRunner(backfills, deps, runRepo, scheduler, executor)

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, runner, false)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusProcessed, eventRepo.events[0].Status)
	req, err := backfills.GetRequestByID(context.Background(), requestID)
	require.NoError(t, err)
	assert.Equal(t, domain.BackfillStatusSuccess, req.Status)
	assert.Equal(t, []string{"2026-03-01", "2026-03-02"}, runRepo.createOrder)
	for _, run := range runRepo.runs {
		require.NotNil(t, run.PartitionFrom)
		require.NotNil(t, run.PartitionTo)
		assert.Equal(t, "2026-03-01", *run.PartitionFrom)
		assert.Equal(t, "2026-03-02", *run.PartitionTo)
	}
}

func TestReconciler_ShadowModeBackfillEventIsDeferred(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-1"
	requestID := "req-1"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:        "ev-backfill-shadow",
		EventType: domain.AssetTriggerTypeBackfill,
		AssetID:   &assetID,
		Status:    domain.OrchestrationEventStatusPending,
		PayloadJSON: map[string]any{
			"backfill_request_id": requestID,
		},
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{assetID: {ID: assetID}}}
	runRepo := &fakeRunRepo{runs: map[string]*domain.AssetRun{}}
	deps := &fakeDeps{upstream: map[string][]domain.AssetDependency{}, downstream: map[string][]domain.AssetDependency{}}
	scheduler := NewAssetScheduler(assetRepo, deps, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})
	backfills := &memBackfillRepo{
		requests: []domain.BackfillRequest{{
			ID:            requestID,
			AssetID:       assetID,
			PartitionFrom: "2026-03-01",
			PartitionTo:   "2026-03-02",
			Status:        domain.BackfillStatusPending,
			RequestedBy:   "admin",
		}},
		slices: []domain.BackfillSlice{
			{ID: "s-2", RequestID: requestID, AssetID: assetID, PartitionKey: "2026-03-02", Status: domain.BackfillStatusPending},
			{ID: "s-1", RequestID: requestID, AssetID: assetID, PartitionKey: "2026-03-01", Status: domain.BackfillStatusPending},
		},
	}
	runner := NewBackfillRunner(backfills, deps, runRepo, scheduler, executor)

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, runner, true)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusPending, eventRepo.events[0].Status)
	require.NotNil(t, eventRepo.events[0].LastError)
	assert.Contains(t, *eventRepo.events[0].LastError, "deferred while reconciler shadow mode")
	assert.Empty(t, runRepo.createOrder)
	req, getErr := backfills.GetRequestByID(context.Background(), requestID)
	require.NoError(t, getErr)
	assert.Equal(t, domain.BackfillStatusPending, req.Status)
}

func TestReconciler_DuplicateEventWithInFlightRunIsNoop(t *testing.T) {
	eventRepo := &memEventRepo{}
	assetID := "asset-dup"
	partitionKey := "2026-03-01"
	_, err := eventRepo.Enqueue(context.Background(), &domain.OrchestrationEvent{
		ID:           "ev-dup",
		EventType:    domain.AssetTriggerTypeUpstreamUpdate,
		AssetID:      &assetID,
		PartitionKey: &partitionKey,
		Status:       domain.OrchestrationEventStatusPending,
	})
	require.NoError(t, err)

	assetRepo := &fakeAssets{items: map[string]*domain.DataAsset{
		assetID: {
			ID:                  assetID,
			PartitionDefinition: &domain.PartitionDefinition{Type: domain.PartitionTypeDaily},
		},
	}}
	deps := &fakeDeps{downstream: map[string][]domain.AssetDependency{}, upstream: map[string][]domain.AssetDependency{}}
	runRepo := &fakeRunRepo{runs: map[string]*domain.AssetRun{
		"run-existing": {
			ID:           "run-existing",
			AssetID:      assetID,
			PartitionKey: &partitionKey,
			Status:       domain.AssetRunStatusRunning,
		},
	}}
	scheduler := NewAssetScheduler(assetRepo, deps, runRepo)
	executor := NewAssetExecutor(runRepo, NewAssetRunStateMachine(), NewInMemoryIOManager(), NewConcurrencyLimiter(1, 1), &fakeStepper{})

	r := NewReconciler(eventRepo, assetRepo, runRepo, scheduler, executor, nil, false)
	require.NoError(t, r.Tick(context.Background()))

	require.Len(t, runRepo.runs, 1)
	require.Len(t, eventRepo.events, 1)
	assert.Equal(t, domain.OrchestrationEventStatusProcessed, eventRepo.events[0].Status)
	require.NotNil(t, eventRepo.events[0].LastError)
	assert.Contains(t, *eventRepo.events[0].LastError, "matching run already in progress")
}

type memEventRepo struct {
	events            []domain.OrchestrationEvent
	enqueueCalls      int
	failEnqueueAtCall int
	enqueueErr        error
}

func (m *memEventRepo) Enqueue(_ context.Context, event *domain.OrchestrationEvent) (*domain.OrchestrationEvent, error) {
	m.enqueueCalls++
	if m.failEnqueueAtCall > 0 && m.enqueueCalls == m.failEnqueueAtCall {
		if m.enqueueErr != nil {
			return nil, m.enqueueErr
		}
		return nil, errors.New("enqueue failed")
	}
	if event.IdempotencyKey != nil {
		key := strings.TrimSpace(*event.IdempotencyKey)
		if key != "" {
			for i := range m.events {
				if m.events[i].IdempotencyKey != nil && *m.events[i].IdempotencyKey == key {
					existing := m.events[i]
					return &existing, nil
				}
			}
		}
	}

	eventCopy := *event
	if eventCopy.ID == "" {
		eventCopy.ID = domain.NewID()
	}
	if eventCopy.AvailableAt.IsZero() {
		eventCopy.AvailableAt = time.Now().UTC()
	}
	m.events = append(m.events, eventCopy)
	return &eventCopy, nil
}

func (m *memEventRepo) ClaimNextPending(_ context.Context, now time.Time) (*domain.OrchestrationEvent, error) {
	for i := range m.events {
		e := &m.events[i]
		if e.Status == domain.OrchestrationEventStatusPending && !e.AvailableAt.After(now) {
			e.Status = domain.OrchestrationEventStatusProcessing
			e.AttemptCount++
			return e, nil
		}
	}
	return nil, domain.ErrNotFound("no pending orchestration event")
}

func (m *memEventRepo) MarkProcessed(_ context.Context, id string) error {
	for i := range m.events {
		if m.events[i].ID == id {
			if m.events[i].Status != domain.OrchestrationEventStatusFailed {
				m.events[i].LastError = nil
			}
			m.events[i].Status = domain.OrchestrationEventStatusProcessed
			return nil
		}
	}
	return domain.ErrNotFound("event not found")
}

func (m *memEventRepo) MarkFailed(_ context.Context, id string, errMsg string, retryAt *time.Time) error {
	for i := range m.events {
		if m.events[i].ID == id {
			if retryAt != nil {
				m.events[i].Status = domain.OrchestrationEventStatusPending
				m.events[i].AvailableAt = *retryAt
			} else {
				m.events[i].Status = domain.OrchestrationEventStatusFailed
			}
			m.events[i].LastError = &errMsg
			return nil
		}
	}
	return domain.ErrNotFound("event not found")
}

func (m *memEventRepo) List(_ context.Context, filter domain.OrchestrationEventFilter) ([]domain.OrchestrationEvent, int64, error) {
	out := make([]domain.OrchestrationEvent, 0)
	for _, e := range m.events {
		if filter.Status == nil || e.Status == *filter.Status {
			out = append(out, e)
		}
	}
	return out, int64(len(out)), nil
}

type memBackfillRepo struct {
	requests []domain.BackfillRequest
	slices   []domain.BackfillSlice
}

func (m *memBackfillRepo) CreateRequest(_ context.Context, req *domain.BackfillRequest) (*domain.BackfillRequest, error) {
	copyReq := *req
	if copyReq.ID == "" {
		copyReq.ID = domain.NewID()
	}
	m.requests = append(m.requests, copyReq)
	return &copyReq, nil
}

func (m *memBackfillRepo) GetRequestByID(_ context.Context, id string) (*domain.BackfillRequest, error) {
	for i := range m.requests {
		if m.requests[i].ID == id {
			return &m.requests[i], nil
		}
	}
	return nil, domain.ErrNotFound("backfill request not found")
}

func (m *memBackfillRepo) ListRequests(_ context.Context, _ domain.BackfillFilter) ([]domain.BackfillRequest, int64, error) {
	return m.requests, int64(len(m.requests)), nil
}

func (m *memBackfillRepo) UpdateRequestStatus(_ context.Context, id string, status string, errMsg *string) error {
	for i := range m.requests {
		if m.requests[i].ID == id {
			m.requests[i].Status = status
			m.requests[i].ErrorMessage = errMsg
			return nil
		}
	}
	return nil
}

func (m *memBackfillRepo) CreateSlice(_ context.Context, slice *domain.BackfillSlice) (*domain.BackfillSlice, error) {
	copySlice := *slice
	if copySlice.ID == "" {
		copySlice.ID = domain.NewID()
	}
	m.slices = append(m.slices, copySlice)
	return &copySlice, nil
}

func (m *memBackfillRepo) ListSlicesByRequest(_ context.Context, requestID string) ([]domain.BackfillSlice, error) {
	out := make([]domain.BackfillSlice, 0)
	for _, s := range m.slices {
		if s.RequestID == requestID {
			out = append(out, s)
		}
	}
	return out, nil
}

func (m *memBackfillRepo) UpdateSliceStatus(_ context.Context, id string, status string, runID *string, errMsg *string) error {
	for i := range m.slices {
		if m.slices[i].ID == id {
			m.slices[i].Status = status
			if runID != nil {
				m.slices[i].RunID = runID
			}
			m.slices[i].ErrorMessage = errMsg
			return nil
		}
	}
	return nil
}
