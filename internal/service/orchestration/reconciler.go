//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"errors"
	"fmt"
	"time"

	"duck-demo/internal/domain"
)

type Reconciler struct {
	events     domain.OrchestrationEventRepository
	assets     domain.DataAssetRepository
	scheduler  *AssetScheduler
	executor   *AssetExecutor
	runs       domain.AssetRunRepository
	shadowMode bool
}

func NewReconciler(
	events domain.OrchestrationEventRepository,
	assets domain.DataAssetRepository,
	runs domain.AssetRunRepository,
	scheduler *AssetScheduler,
	executor *AssetExecutor,
	shadowMode bool,
) *Reconciler {
	return &Reconciler{
		events:     events,
		assets:     assets,
		scheduler:  scheduler,
		executor:   executor,
		runs:       runs,
		shadowMode: shadowMode,
	}
}

func (r *Reconciler) Tick(ctx context.Context) error {
	event, err := r.events.ClaimNextPending(ctx, time.Now().UTC())
	if err != nil {
		var nf *domain.NotFoundError
		if errors.As(err, &nf) {
			return nil
		}
		return err
	}

	if event.AssetID == nil || *event.AssetID == "" {
		_ = r.events.MarkProcessed(ctx, event.ID)
		return nil
	}

	assetID := *event.AssetID
	if _, err := r.assets.GetByID(ctx, assetID); err != nil {
		msg := fmt.Sprintf("asset not found: %v", err)
		_ = r.events.MarkFailed(ctx, event.ID, msg, nil)
		return err
	}

	plan, err := r.scheduler.BuildPlan(ctx, assetID)
	if err != nil {
		retryAt := time.Now().UTC().Add(2 * time.Second)
		_ = r.events.MarkFailed(ctx, event.ID, err.Error(), &retryAt)
		return err
	}

	if r.shadowMode {
		_ = r.events.MarkProcessed(ctx, event.ID)
		return nil
	}

	triggerType := domain.AssetTriggerTypeReconciler
	run := &domain.AssetRun{
		ID:          domain.NewID(),
		AssetID:     assetID,
		Status:      domain.AssetRunStatusQueued,
		TriggerType: triggerType,
		TriggeredBy: "reconciler",
		MaxAttempts: 1,
	}
	if event.PartitionKey != nil {
		run.PartitionKey = event.PartitionKey
	}
	created, err := r.runs.CreateRun(ctx, run)
	if err != nil {
		retryAt := time.Now().UTC().Add(2 * time.Second)
		_ = r.events.MarkFailed(ctx, event.ID, err.Error(), &retryAt)
		return err
	}

	if err := r.executor.ExecutePlan(ctx, created.ID, created.Status, plan); err != nil {
		retryAt := time.Now().UTC().Add(2 * time.Second)
		_ = r.events.MarkFailed(ctx, event.ID, err.Error(), &retryAt)
		return err
	}

	_ = r.events.MarkProcessed(ctx, event.ID)
	return nil
}
