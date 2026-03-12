package asset

import (
	"context"
	"fmt"
	"time"

	"duck-demo/internal/domain"
)

const defaultFreshnessEventBucketSeconds int64 = 60

// ReconcileFreshness enqueues freshness-breach events for the nearest stale executable assets required by assetKey.
func (s *Service) ReconcileFreshness(ctx context.Context, assetKey string) (*domain.AssetFreshnessReconcileResult, error) {
	asset, err := s.assets.GetByKey(ctx, assetKey)
	if err != nil {
		return nil, err
	}

	node, err := s.explainFreshnessByID(ctx, asset.ID, 0, map[string]struct{}{})
	if err != nil {
		return nil, err
	}

	targets, err := s.enqueueFreshnessTargets(ctx, *node)
	if err != nil {
		return nil, err
	}

	return &domain.AssetFreshnessReconcileResult{
		Asset: domain.AssetFreshnessStatus{
			AssetID:                node.AssetID,
			AssetKey:               node.AssetKey,
			AssetType:              node.AssetType,
			FreshnessStatus:        node.FreshnessStatus,
			EffectiveMaxLagSeconds: node.EffectiveMaxLagSeconds,
			LastMaterializedAt:     node.LastMaterializedAt,
			StaleSince:             node.StaleSince,
			Reason:                 node.Reason,
			Basis:                  node.Basis,
		},
		Targets: targets,
	}, nil
}

// ReconcileFreshnessPolicies scans all assets with freshness policies and enqueues refresh work for stale executable dependencies.
func (s *Service) ReconcileFreshnessPolicies(ctx context.Context) (int, error) {
	page := domain.PageRequest{MaxResults: domain.MaxMaxResults}
	totalTargets := 0

	for {
		assets, total, err := s.assets.List(ctx, domain.AssetFilter{Page: page})
		if err != nil {
			return totalTargets, err
		}

		for i := range assets {
			asset := assets[i]
			if asset.FreshnessPolicy == nil || asset.FreshnessPolicy.MaxLagSeconds <= 0 {
				continue
			}
			result, reconcileErr := s.ReconcileFreshness(ctx, asset.AssetKey)
			if reconcileErr != nil {
				return totalTargets, fmt.Errorf("reconcile freshness for %s: %w", asset.AssetKey, reconcileErr)
			}
			totalTargets += len(result.Targets)
		}

		nextOffset := page.Offset() + page.Limit()
		if int64(nextOffset) >= total || len(assets) == 0 {
			return totalTargets, nil
		}
		page.PageToken = domain.EncodePageToken(nextOffset)
	}
}

func (s *Service) enqueueFreshnessTargets(ctx context.Context, root domain.AssetFreshnessNode) ([]domain.AssetFreshnessReconcileTarget, error) {
	candidates := selectFreshnessTargets(root)
	if len(candidates) == 0 {
		return []domain.AssetFreshnessReconcileTarget{}, nil
	}

	out := make([]domain.AssetFreshnessReconcileTarget, 0, len(candidates))
	for _, candidate := range candidates {
		idemKey := freshnessEventIdempotencyKey(candidate.AssetID, candidate.EffectiveMaxLagSeconds, time.Now().UTC())
		event, err := s.events.Enqueue(ctx, &domain.OrchestrationEvent{
			ID:             domain.NewID(),
			EventType:      domain.AssetTriggerTypeFreshnessBreach,
			AssetID:        &candidate.AssetID,
			Status:         domain.OrchestrationEventStatusPending,
			IdempotencyKey: &idemKey,
			PayloadJSON: map[string]any{
				"source_asset_id":  root.AssetID,
				"source_asset_key": root.AssetKey,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("enqueue freshness event for %s: %w", candidate.AssetKey, err)
		}
		out = append(out, domain.AssetFreshnessReconcileTarget{
			AssetID:         candidate.AssetID,
			AssetKey:        candidate.AssetKey,
			AssetType:       candidate.AssetType,
			FreshnessStatus: candidate.FreshnessStatus,
			EventID:         event.ID,
		})
	}

	return out, nil
}

func selectFreshnessTargets(root domain.AssetFreshnessNode) []domain.AssetFreshnessNode {
	seen := map[string]struct{}{}
	out := make([]domain.AssetFreshnessNode, 0)
	selectFreshnessTargetsInto(root, seen, &out)
	return out
}

func selectFreshnessTargetsInto(node domain.AssetFreshnessNode, seen map[string]struct{}, out *[]domain.AssetFreshnessNode) {
	if out == nil {
		return
	}
	if node.FreshnessStatus == domain.AssetFreshnessStatusFresh || node.FreshnessStatus == domain.AssetFreshnessStatusRefreshing {
		return
	}

	if assetTypeSupportsExecution(node.AssetType) {
		if _, ok := seen[node.AssetID]; ok {
			return
		}
		seen[node.AssetID] = struct{}{}
		*out = append(*out, node)
		return
	}

	for _, child := range node.Upstream {
		selectFreshnessTargetsInto(child, seen, out)
	}
}

func freshnessEventIdempotencyKey(assetID string, effectiveMaxLagSeconds int64, now time.Time) string {
	bucketSeconds := effectiveMaxLagSeconds
	if bucketSeconds <= 0 {
		bucketSeconds = defaultFreshnessEventBucketSeconds
	}
	return fmt.Sprintf("freshness:%s:%d", assetID, now.Unix()/bucketSeconds)
}
