//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"fmt"
	"time"

	"duck-demo/internal/domain"
)

const assetExecutionSkipMaterializationKey = "_skip_materialization"

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
	run, err := e.runs.GetRunByID(ctx, runID)
	if err != nil {
		return fmt.Errorf("get run by id: %w", err)
	}
	maxAttempts := normalizeMaxAttempts(run)

	if err := e.state.Transition(ctx, e.runs, runID, currentStatus, domain.AssetRunStatusRunning, 0, nil); err != nil {
		return err
	}

	for _, level := range plan.Levels {
		for _, assetID := range level {
			attempt, err := e.executeAsset(ctx, runID, assetID, maxAttempts)
			if err != nil {
				msg := err.Error()
				_, _ = e.runs.CreateRunEvent(ctx, &domain.AssetRunEvent{
					RunID:     runID,
					EventType: "ASSET_EXECUTION_FAILED",
					EventAt:   time.Now().UTC(),
					Message:   &msg,
					MetadataJSON: map[string]any{
						"asset_id": assetID,
						"attempt":  attempt,
					},
				})
				_ = e.state.Transition(ctx, e.runs, runID, domain.AssetRunStatusRunning, domain.AssetRunStatusFailed, 0, &msg)
				return err
			}
		}
	}

	return e.state.Transition(ctx, e.runs, runID, domain.AssetRunStatusRunning, domain.AssetRunStatusSuccess, 0, nil)
}

func (e *AssetExecutor) executeAsset(ctx context.Context, runID, assetID string, maxAttempts int) (int, error) {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := e.executeAssetOnce(ctx, runID, assetID)
		if err == nil {
			return attempt, nil
		}

		if attempt >= maxAttempts {
			return attempt, err
		}

		msg := err.Error()
		if transitionErr := e.state.Transition(ctx, e.runs, runID, domain.AssetRunStatusRunning, domain.AssetRunStatusRetrying, attempt, &msg); transitionErr != nil {
			return attempt, transitionErr
		}

		backoff := backoffForAttempt(attempt)
		_, _ = e.runs.CreateRunEvent(ctx, &domain.AssetRunEvent{
			RunID:     runID,
			EventType: "ASSET_EXECUTION_RETRY",
			EventAt:   time.Now().UTC(),
			Message:   &msg,
			MetadataJSON: map[string]any{
				"asset_id":        assetID,
				"attempt":         attempt,
				"max_attempts":    maxAttempts,
				"backoff_seconds": backoff.Seconds(),
			},
		})

		if err := sleepWithContext(ctx, backoff); err != nil {
			return attempt, fmt.Errorf("sleep before retry: %w", err)
		}

		if transitionErr := e.state.Transition(ctx, e.runs, runID, domain.AssetRunStatusRetrying, domain.AssetRunStatusRunning, 0, nil); transitionErr != nil {
			return attempt, transitionErr
		}
	}

	return maxAttempts, nil
}

func (e *AssetExecutor) executeAssetOnce(ctx context.Context, runID, assetID string) error {
	if err := e.limiter.Acquire(ctx, assetID); err != nil {
		return fmt.Errorf("acquire concurrency slot: %w", err)
	}
	defer e.limiter.Release(assetID)

	result, err := e.stepper.Execute(ctx, assetID, e.io)
	if err != nil {
		return fmt.Errorf("execute asset %s: %w", assetID, err)
	}

	if shouldSkipMaterialization(result) {
		_, _ = e.runs.CreateRunEvent(ctx, &domain.AssetRunEvent{
			RunID:     runID,
			EventType: "ASSET_EXECUTED",
			EventAt:   time.Now().UTC(),
			StatsJSON: sanitizeExecutionResult(result),
		})
		return nil
	}

	if err := e.io.StoreOutput(ctx, assetID, result); err != nil {
		return fmt.Errorf("store output for asset %s: %w", assetID, err)
	}

	run, err := e.runs.GetRunByID(ctx, runID)
	if err != nil {
		return fmt.Errorf("get run for materialization %s: %w", runID, err)
	}
	materializedAt := time.Now().UTC()
	if _, err := e.runs.CreateMaterialization(ctx, &domain.AssetMaterialization{
		ID:             domain.NewID(),
		AssetID:        assetID,
		RunID:          &runID,
		PartitionKey:   run.PartitionKey,
		MetadataJSON:   result,
		MaterializedAt: materializedAt,
	}); err != nil {
		return fmt.Errorf("record materialization for asset %s: %w", assetID, err)
	}

	_, _ = e.runs.CreateRunEvent(ctx, &domain.AssetRunEvent{
		RunID:     runID,
		EventType: "ASSET_EXECUTED",
		EventAt:   time.Now().UTC(),
		StatsJSON: result,
	})

	return nil
}

func shouldSkipMaterialization(result map[string]any) bool {
	if len(result) == 0 {
		return false
	}
	raw, ok := result[assetExecutionSkipMaterializationKey]
	if !ok {
		return false
	}
	flag, ok := raw.(bool)
	return ok && flag
}

func sanitizeExecutionResult(result map[string]any) map[string]any {
	if len(result) == 0 {
		return result
	}
	out := make(map[string]any, len(result))
	for k, v := range result {
		if k == assetExecutionSkipMaterializationKey {
			continue
		}
		out[k] = v
	}
	return out
}

func normalizeMaxAttempts(run *domain.AssetRun) int {
	if run == nil || run.MaxAttempts <= 0 {
		return 1
	}
	return run.MaxAttempts
}

func backoffForAttempt(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	backoff := time.Duration(attempt) * 100 * time.Millisecond
	if backoff > 3*time.Second {
		return 3 * time.Second
	}
	return backoff
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
