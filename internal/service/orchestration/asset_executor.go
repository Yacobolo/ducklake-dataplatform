//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"fmt"
	"time"

	"duck-demo/internal/domain"
)

type AssetStepExecutor interface {
	Execute(ctx context.Context, assetID string, io IOManager) (map[string]any, error)
}

type AssetExecutor struct {
	runs    domain.AssetRunRepository
	state   *AssetRunStateMachine
	io      IOManager
	limiter *ConcurrencyLimiter
	stepper AssetStepExecutor
}

func NewAssetExecutor(
	runs domain.AssetRunRepository,
	state *AssetRunStateMachine,
	io IOManager,
	limiter *ConcurrencyLimiter,
	stepper AssetStepExecutor,
) *AssetExecutor {
	return &AssetExecutor{
		runs:    runs,
		state:   state,
		io:      io,
		limiter: limiter,
		stepper: stepper,
	}
}

func (e *AssetExecutor) ExecutePlan(ctx context.Context, runID string, currentStatus string, plan *AssetRunPlan) error {
	if plan == nil {
		return domain.ErrValidation("plan is required")
	}
	if err := e.state.Transition(ctx, e.runs, runID, currentStatus, domain.AssetRunStatusRunning, 0, nil); err != nil {
		return err
	}

	for _, level := range plan.Levels {
		for _, assetID := range level {
			if err := e.executeAsset(ctx, runID, assetID); err != nil {
				msg := err.Error()
				_, _ = e.runs.CreateRunEvent(ctx, &domain.AssetRunEvent{
					RunID:     runID,
					EventType: "ASSET_EXECUTION_FAILED",
					EventAt:   time.Now().UTC(),
					Message:   &msg,
				})
				_ = e.state.Transition(ctx, e.runs, runID, domain.AssetRunStatusRunning, domain.AssetRunStatusFailed, 0, &msg)
				return err
			}
		}
	}

	return e.state.Transition(ctx, e.runs, runID, domain.AssetRunStatusRunning, domain.AssetRunStatusSuccess, 0, nil)
}

func (e *AssetExecutor) executeAsset(ctx context.Context, runID, assetID string) error {
	if err := e.limiter.Acquire(ctx, assetID); err != nil {
		return fmt.Errorf("acquire concurrency slot: %w", err)
	}
	defer e.limiter.Release(assetID)

	result, err := e.stepper.Execute(ctx, assetID, e.io)
	if err != nil {
		return fmt.Errorf("execute asset %s: %w", assetID, err)
	}

	if err := e.io.StoreOutput(ctx, assetID, result); err != nil {
		return fmt.Errorf("store output for asset %s: %w", assetID, err)
	}

	_, _ = e.runs.CreateRunEvent(ctx, &domain.AssetRunEvent{
		RunID:     runID,
		EventType: "ASSET_EXECUTED",
		EventAt:   time.Now().UTC(),
		StatsJSON: result,
	})

	return nil
}
