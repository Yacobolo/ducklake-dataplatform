//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"duck-demo/internal/domain"
	"golang.org/x/sync/errgroup"
)

type BackfillRunner struct {
	backfills    domain.BackfillRepository
	dependencies domain.AssetDependencyRepository
	runs         domain.AssetRunRepository
	scheduler    *AssetScheduler
	executor     *AssetExecutor
	requestLocks sync.Map
}

func NewBackfillRunner(
	backfills domain.BackfillRepository,
	dependencies domain.AssetDependencyRepository,
	runs domain.AssetRunRepository,
	scheduler *AssetScheduler,
	executor *AssetExecutor,
) *BackfillRunner {
	if dependencies == nil && scheduler != nil {
		dependencies = scheduler.dependencies
	}
	return &BackfillRunner{
		backfills:    backfills,
		dependencies: dependencies,
		runs:         runs,
		scheduler:    scheduler,
		executor:     executor,
	}
}

func (r *BackfillRunner) RunRequest(ctx context.Context, requestID string) (retErr error) {
	if r == nil || r.backfills == nil || r.scheduler == nil || r.executor == nil || r.runs == nil {
		return domain.ErrValidation("backfill runner is not fully configured")
	}
	if requestID == "" {
		return domain.ErrValidation("backfill request id is required")
	}

	unlock := r.lockRequest(requestID)
	defer unlock()

	req, err := r.backfills.GetRequestByID(ctx, requestID)
	if err != nil {
		return fmt.Errorf("get backfill request: %w", err)
	}
	if isTerminalBackfillStatus(req.Status) {
		return nil
	}
	if req.Status == domain.BackfillStatusRunning {
		return nil
	}

	requestFinalized := false
	defer func() {
		if retErr == nil || requestFinalized {
			return
		}
		errMsg := retErr.Error()
		finalizeErr := r.backfills.UpdateRequestStatus(context.WithoutCancel(ctx), requestID, domain.BackfillStatusFailed, &errMsg)
		if finalizeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("finalize backfill request %s as failed: %w", requestID, finalizeErr))
			return
		}
		requestFinalized = true
	}()

	if err := r.backfills.UpdateRequestStatus(ctx, requestID, domain.BackfillStatusRunning, nil); err != nil {
		return fmt.Errorf("set request running: %w", err)
	}

	plan, err := r.scheduler.BuildPlan(ctx, req.AssetID)
	if err != nil {
		return fmt.Errorf("build backfill plan: %w", err)
	}

	slices, err := r.backfills.ListSlicesByRequest(ctx, requestID)
	if err != nil {
		return fmt.Errorf("list backfill slices: %w", err)
	}
	if len(slices) == 0 {
		if err := r.backfills.UpdateRequestStatus(ctx, requestID, domain.BackfillStatusSuccess, nil); err != nil {
			return fmt.Errorf("set request success: %w", err)
		}
		return nil
	}

	sort.Slice(slices, func(i, j int) bool {
		if slices[i].PartitionKey == slices[j].PartitionKey {
			return slices[i].ID < slices[j].ID
		}
		return slices[i].PartitionKey < slices[j].PartitionKey
	})

	failedReasons := make([]string, 0)

	maxParallelism := normalizeMaxParallelism(req.MaxParallelism)
	failedBySliceID := make(map[string]string)
	var failedMu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxParallelism)

	for _, slice := range slices {
		slice := slice
		g.Go(func() error {
			errMsg, failed, err := r.runSlice(gctx, req, plan, slice)
			if err != nil {
				return err
			}
			if failed {
				failedMu.Lock()
				failedBySliceID[slice.ID] = errMsg
				failedMu.Unlock()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	for _, slice := range slices {
		if errMsg, ok := failedBySliceID[slice.ID]; ok {
			failedReasons = append(failedReasons, errMsg)
		}
	}

	if len(failedReasons) > 0 {
		errMsg := failedReasons[0]
		if err := r.backfills.UpdateRequestStatus(ctx, requestID, domain.BackfillStatusFailed, &errMsg); err != nil {
			return fmt.Errorf("set request failed: %w", err)
		}
		requestFinalized = true
		return fmt.Errorf("backfill request %s failed: %s", requestID, errMsg)
	}

	if err := r.backfills.UpdateRequestStatus(ctx, requestID, domain.BackfillStatusSuccess, nil); err != nil {
		return fmt.Errorf("set request success: %w", err)
	}
	requestFinalized = true
	return nil
}

func (r *BackfillRunner) lockRequest(requestID string) func() {
	v, _ := r.requestLocks.LoadOrStore(requestID, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func (r *BackfillRunner) runSlice(ctx context.Context, req *domain.BackfillRequest, plan *AssetRunPlan, slice domain.BackfillSlice) (string, bool, error) {
	if slice.Status == domain.BackfillStatusSuccess || slice.Status == domain.BackfillStatusCancelled {
		return "", false, nil
	}

	ready, reason, readyErr := r.upstreamReady(ctx, slice.AssetID, slice.PartitionKey)
	if readyErr != nil {
		return "", false, readyErr
	}
	if !ready {
		deferReason := fmt.Sprintf("deferred: %s", reason)
		if err := r.backfills.UpdateSliceStatus(ctx, slice.ID, domain.BackfillStatusFailed, slice.RunID, &deferReason); err != nil {
			return "", false, fmt.Errorf("set slice deferred: %w", err)
		}
		return deferReason, true, nil
	}

	if err := r.backfills.UpdateSliceStatus(ctx, slice.ID, domain.BackfillStatusRunning, slice.RunID, nil); err != nil {
		return "", false, fmt.Errorf("set slice running: %w", err)
	}

	run := &domain.AssetRun{
		ID:            domain.NewID(),
		AssetID:       req.AssetID,
		PartitionKey:  ptrSliceKey(slice.PartitionKey),
		PartitionFrom: ptrSliceKey(req.PartitionFrom),
		PartitionTo:   ptrSliceKey(req.PartitionTo),
		Status:        domain.AssetRunStatusQueued,
		TriggerType:   domain.AssetTriggerTypeBackfill,
		TriggeredBy:   req.RequestedBy,
		MaxAttempts:   1,
	}
	created, err := r.runs.CreateRun(ctx, run)
	if err != nil {
		errMsg := fmt.Sprintf("create run: %v", err)
		if updateErr := r.backfills.UpdateSliceStatus(ctx, slice.ID, domain.BackfillStatusFailed, slice.RunID, &errMsg); updateErr != nil {
			return "", false, fmt.Errorf("create run and update slice status: %w", errors.Join(err, updateErr))
		}
		return errMsg, true, nil
	}

	if err := r.backfills.UpdateSliceStatus(ctx, slice.ID, domain.BackfillStatusRunning, &created.ID, nil); err != nil {
		return "", false, fmt.Errorf("link run id to slice: %w", err)
	}

	if err := r.executor.ExecutePlan(ctx, created.ID, created.Status, plan); err != nil {
		errMsg := fmt.Sprintf("execute partition %s: %v", slice.PartitionKey, err)
		if updateErr := r.backfills.UpdateSliceStatus(ctx, slice.ID, domain.BackfillStatusFailed, &created.ID, &errMsg); updateErr != nil {
			return "", false, fmt.Errorf("execute slice and update status: %w", errors.Join(err, updateErr))
		}
		return errMsg, true, nil
	}

	if err := r.backfills.UpdateSliceStatus(ctx, slice.ID, domain.BackfillStatusSuccess, &created.ID, nil); err != nil {
		return "", false, fmt.Errorf("set slice success: %w", err)
	}

	return "", false, nil
}

func (r *BackfillRunner) upstreamReady(ctx context.Context, assetID, partitionKey string) (bool, string, error) {
	if r.dependencies == nil {
		return true, "", nil
	}

	upstream, err := r.dependencies.ListUpstream(ctx, assetID)
	if err != nil {
		return false, "", fmt.Errorf("list upstream dependencies: %w", err)
	}

	status := domain.AssetRunStatusSuccess
	for _, dep := range upstream {
		upstreamAssetID := dep.UpstreamAssetID
		runs, _, err := r.runs.ListRuns(ctx, domain.AssetRunFilter{
			AssetID: &upstreamAssetID,
			Status:  &status,
			Page:    domain.PageRequest{MaxResults: domain.MaxMaxResults},
		})
		if err != nil {
			return false, "", fmt.Errorf("list upstream runs for %s: %w", dep.UpstreamAssetID, err)
		}
		if !hasSuccessfulPartitionRun(runs, partitionKey) {
			return false, fmt.Sprintf("upstream %s not ready for partition %s", dep.UpstreamAssetID, partitionKey), nil
		}
	}

	return true, "", nil
}

func hasSuccessfulPartitionRun(runs []domain.AssetRun, partitionKey string) bool {
	for _, run := range runs {
		if run.PartitionKey == nil || *run.PartitionKey == partitionKey {
			return true
		}
	}
	return false
}

func isTerminalBackfillStatus(status string) bool {
	return status == domain.BackfillStatusSuccess || status == domain.BackfillStatusCancelled
}

func ptrSliceKey(key string) *string {
	k := key
	return &k
}

func normalizeMaxParallelism(maxParallelism int) int {
	if maxParallelism <= 0 {
		return 1
	}
	return maxParallelism
}
