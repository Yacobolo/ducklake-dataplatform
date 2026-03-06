//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"duck-demo/internal/domain"
)

const reconcilerRetryBackoff = 2 * time.Second

var dedupeInFlightRunStatuses = []string{
	domain.AssetRunStatusQueued,
	domain.AssetRunStatusPlanning,
	domain.AssetRunStatusRunning,
	domain.AssetRunStatusRetrying,
}

type Reconciler struct {
	events     domain.OrchestrationEventRepository
	assets     domain.DataAssetRepository
	deps       domain.AssetDependencyRepository
	scheduler  *AssetScheduler
	executor   *AssetExecutor
	backfills  *BackfillRunner
	runs       domain.AssetRunRepository
	shadowMode bool
}

func NewReconciler(
	events domain.OrchestrationEventRepository,
	assets domain.DataAssetRepository,
	runs domain.AssetRunRepository,
	scheduler *AssetScheduler,
	executor *AssetExecutor,
	backfills *BackfillRunner,
	shadowMode bool,
) *Reconciler {
	var deps domain.AssetDependencyRepository
	if scheduler != nil {
		deps = scheduler.dependencies
	}

	return &Reconciler{
		events:     events,
		assets:     assets,
		deps:       deps,
		scheduler:  scheduler,
		executor:   executor,
		backfills:  backfills,
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

	if event.EventType == domain.AssetTriggerTypeBackfill && r.backfills != nil && !r.shadowMode {
		requestID, err := backfillRequestIDFromPayload(event.PayloadJSON)
		if err != nil {
			_ = r.events.MarkFailed(ctx, event.ID, err.Error(), nil)
			return err
		}
		if err := r.backfills.RunRequest(ctx, requestID); err != nil {
			retryAt := time.Now().UTC().Add(reconcilerRetryBackoff)
			_ = r.events.MarkFailed(ctx, event.ID, err.Error(), &retryAt)
			return err
		}
		_ = r.events.MarkProcessed(ctx, event.ID)
		return nil
	}

	plan, err := r.scheduler.BuildPlan(ctx, assetID)
	if err != nil {
		retryAt := time.Now().UTC().Add(reconcilerRetryBackoff)
		_ = r.events.MarkFailed(ctx, event.ID, err.Error(), &retryAt)
		return err
	}

	if r.shadowMode {
		_ = r.events.MarkProcessed(ctx, event.ID)
		return nil
	}

	ready, reason, retriable, err := r.isReadyForRun(ctx, assetID, event.EventType, event.PartitionKey)
	if err != nil {
		retryAt := time.Now().UTC().Add(reconcilerRetryBackoff)
		_ = r.events.MarkFailed(ctx, event.ID, err.Error(), &retryAt)
		return err
	}
	if !ready {
		if retriable {
			retryAt := time.Now().UTC().Add(reconcilerRetryBackoff)
			_ = r.events.MarkFailed(ctx, event.ID, reason, &retryAt)
			return nil
		}
		r.markProcessedNoop(ctx, event.ID, reason)
		return nil
	}

	inFlight, err := r.hasInFlightRun(ctx, assetID, event.PartitionKey)
	if err != nil {
		retryAt := time.Now().UTC().Add(reconcilerRetryBackoff)
		_ = r.events.MarkFailed(ctx, event.ID, err.Error(), &retryAt)
		return err
	}
	if inFlight {
		r.markProcessedNoop(ctx, event.ID, "duplicate event ignored; matching run already in progress")
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
		retryAt := time.Now().UTC().Add(reconcilerRetryBackoff)
		_ = r.events.MarkFailed(ctx, event.ID, err.Error(), &retryAt)
		return err
	}

	if err := r.executor.ExecutePlan(ctx, created.ID, created.Status, plan); err != nil {
		retryAt := time.Now().UTC().Add(reconcilerRetryBackoff)
		_ = r.events.MarkFailed(ctx, event.ID, err.Error(), &retryAt)
		return err
	}

	_ = r.events.MarkProcessed(ctx, event.ID)
	return nil
}

func (r *Reconciler) isReadyForRun(
	ctx context.Context,
	assetID string,
	eventType string,
	partitionKey *string,
) (bool, string, bool, error) {
	asset, err := r.assets.GetByID(ctx, assetID)
	if err != nil {
		return false, "", true, fmt.Errorf("get target asset: %w", err)
	}

	if allowed, reason := autoMaterializeAllowed(asset, eventType); !allowed {
		return false, reason, false, nil
	}

	if ready, reason, err := r.isPolicyIntervalSatisfied(ctx, asset, partitionKey); err != nil || !ready {
		return ready, reason, true, err
	}

	if r.deps == nil {
		return true, "", false, nil
	}

	upstream, err := r.deps.ListUpstream(ctx, assetID)
	if err != nil {
		return false, "", true, fmt.Errorf("list upstream dependencies: %w", err)
	}

	targetIsPartitioned := isPartitioned(asset.PartitionDefinition)
	if targetIsPartitioned && partitionKey == nil {
		return false, "waiting for partition-aware trigger for partitioned asset", true, nil
	}
	policyRequiresAllUpstreams := asset.AutoMaterializePolicy != nil && asset.AutoMaterializePolicy.RequireAllUpstreams

	for _, dep := range upstream {
		if dep.DependencyType == domain.DependencyTypeSoft {
			continue
		}
		ready, waitReason, err := r.hasUpstreamMaterialization(ctx, dep.UpstreamAssetID, targetIsPartitioned, partitionKey)
		if err != nil {
			return false, "", true, err
		}
		if !ready {
			if waitReason != "" {
				if policyRequiresAllUpstreams {
					return false, strings.Replace(waitReason, "upstream ", "required upstream ", 1), true, nil
				}
				return false, waitReason, true, nil
			}
			upstreamLabel := "upstream"
			if policyRequiresAllUpstreams {
				upstreamLabel = "required upstream"
			}
			if targetIsPartitioned && partitionKey != nil {
				return false, fmt.Sprintf("waiting for %s %s partition %s", upstreamLabel, dep.UpstreamAssetID, *partitionKey), true, nil
			}
			return false, fmt.Sprintf("waiting for %s %s materialization", upstreamLabel, dep.UpstreamAssetID), true, nil
		}
	}

	return true, "", false, nil
}

func (r *Reconciler) isPolicyIntervalSatisfied(
	ctx context.Context,
	asset *domain.DataAsset,
	partitionKey *string,
) (bool, string, error) {
	if asset.AutoMaterializePolicy == nil || asset.AutoMaterializePolicy.MinIntervalSeconds <= 0 {
		return true, "", nil
	}

	materializedAt, found, err := r.latestMaterializationAt(ctx, asset.ID, partitionKey)
	if err != nil {
		return false, "", err
	}
	if !found {
		return true, "", nil
	}

	minInterval := time.Duration(asset.AutoMaterializePolicy.MinIntervalSeconds) * time.Second
	if time.Since(materializedAt) >= minInterval {
		return true, "", nil
	}

	nextAllowed := materializedAt.Add(minInterval).UTC().Format(time.RFC3339)
	if partitionKey != nil {
		return false, fmt.Sprintf("auto-materialize min interval not met for partition %s; next eligible at %s", *partitionKey, nextAllowed), nil
	}
	return false, fmt.Sprintf("auto-materialize min interval not met; next eligible at %s", nextAllowed), nil
}

func autoMaterializeAllowed(asset *domain.DataAsset, eventType string) (bool, string) {
	if asset == nil || asset.AutoMaterializePolicy == nil {
		return true, ""
	}

	policy := asset.AutoMaterializePolicy
	if eventType == domain.AssetTriggerTypeManual {
		return true, ""
	}

	switch strings.ToLower(strings.TrimSpace(policy.Mode)) {
	case "disabled", "disable", "off", "manual":
		return false, fmt.Sprintf(
			"auto-materialize policy mode %q suppresses reconciler auto-trigger for event type %s",
			policy.Mode,
			eventType,
		)
	}

	switch eventType {
	case domain.AssetTriggerTypeUpstreamUpdate:
		if !policy.OnUpstreamMaterialized {
			return false, fmt.Sprintf(
				"auto-materialize policy disables upstream materialization triggers for event type %s",
				eventType,
			)
		}
	case domain.AssetTriggerTypeFreshnessBreach:
		if !policy.OnFreshnessBreach {
			return false, fmt.Sprintf(
				"auto-materialize policy disables freshness breach triggers for event type %s",
				eventType,
			)
		}
	}

	return true, ""
}

func (r *Reconciler) hasUpstreamMaterialization(
	ctx context.Context,
	upstreamAssetID string,
	targetIsPartitioned bool,
	partitionKey *string,
) (bool, string, error) {
	upstreamAsset, err := r.assets.GetByID(ctx, upstreamAssetID)
	if err != nil {
		return false, "", fmt.Errorf("get upstream asset %s: %w", upstreamAssetID, err)
	}

	key := partitionKey
	if !targetIsPartitioned || !isPartitioned(upstreamAsset.PartitionDefinition) {
		key = nil
	}

	_, found, err := r.latestMaterializationAt(ctx, upstreamAssetID, key)
	if err != nil {
		return false, "", err
	}
	if !found && targetIsPartitioned && key != nil {
		return false, fmt.Sprintf("waiting for upstream %s partition %s", upstreamAssetID, *key), nil
	}
	return found, "", nil
}

func (r *Reconciler) markProcessedNoop(ctx context.Context, eventID string, reason string) {
	_ = r.events.MarkFailed(ctx, eventID, reason, nil)
	_ = r.events.MarkProcessed(ctx, eventID)
}

func (r *Reconciler) hasInFlightRun(ctx context.Context, assetID string, partitionKey *string) (bool, error) {
	for _, status := range dedupeInFlightRunStatuses {
		status := status
		runs, _, err := r.runs.ListRuns(ctx, domain.AssetRunFilter{
			AssetID: &assetID,
			Status:  &status,
			Page:    domain.PageRequest{MaxResults: domain.DefaultMaxResults},
		})
		if err != nil {
			return false, fmt.Errorf("list in-flight runs for asset %s and status %s: %w", assetID, status, err)
		}

		for _, run := range runs {
			if samePartition(partitionKey, run.PartitionKey) {
				return true, nil
			}
		}
	}

	return false, nil
}

func samePartition(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func (r *Reconciler) latestMaterializationAt(
	ctx context.Context,
	assetID string,
	partitionKey *string,
) (time.Time, bool, error) {
	page := domain.PageRequest{MaxResults: domain.DefaultMaxResults}

	for {
		materializations, total, err := r.runs.ListMaterializationsByAsset(ctx, assetID, page)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("list materializations for asset %s: %w", assetID, err)
		}

		for _, mat := range materializations {
			if partitionKey != nil {
				if mat.PartitionKey == nil || *mat.PartitionKey != *partitionKey {
					continue
				}
			}
			return mat.MaterializedAt, true, nil
		}

		offset := page.Offset() + page.Limit()
		if int64(offset) >= total {
			return time.Time{}, false, nil
		}
		page.PageToken = domain.EncodePageToken(offset)
	}
}

func isPartitioned(def *domain.PartitionDefinition) bool {
	if def == nil {
		return false
	}
	return def.Type != "" && def.Type != domain.PartitionTypeUnpartitioned
}

func backfillRequestIDFromPayload(payload map[string]any) (string, error) {
	if payload == nil {
		return "", domain.ErrValidation("backfill payload is required")
	}
	v, ok := payload["backfill_request_id"]
	if !ok {
		return "", domain.ErrValidation("backfill_request_id is required")
	}
	requestID, ok := v.(string)
	if !ok || requestID == "" {
		return "", domain.ErrValidation("backfill_request_id must be a non-empty string")
	}
	return requestID, nil
}
