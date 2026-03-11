package asset

import (
	"context"
	"fmt"
	"sort"
	"time"

	"duck-demo/internal/domain"
)

var activeAssetRunStatuses = []string{
	domain.AssetRunStatusQueued,
	domain.AssetRunStatusPlanning,
	domain.AssetRunStatusRunning,
	domain.AssetRunStatusRetrying,
}

// CheckFreshness returns the current effective freshness status for an asset key.
func (s *Service) CheckFreshness(ctx context.Context, assetKey string) (*domain.AssetFreshnessStatus, error) {
	asset, err := s.assets.GetByKey(ctx, assetKey)
	if err != nil {
		return nil, err
	}

	node, err := s.explainFreshnessByID(ctx, asset.ID, 0, map[string]struct{}{})
	if err != nil {
		return nil, err
	}

	return &domain.AssetFreshnessStatus{
		AssetID:                node.AssetID,
		AssetKey:               node.AssetKey,
		AssetType:              node.AssetType,
		FreshnessStatus:        node.FreshnessStatus,
		EffectiveMaxLagSeconds: node.EffectiveMaxLagSeconds,
		LastMaterializedAt:     node.LastMaterializedAt,
		StaleSince:             node.StaleSince,
		Reason:                 node.Reason,
		Basis:                  node.Basis,
	}, nil
}

// ExplainFreshness returns the asset freshness explanation tree for an asset key.
func (s *Service) ExplainFreshness(ctx context.Context, assetKey string) (*domain.AssetFreshnessNode, error) {
	asset, err := s.assets.GetByKey(ctx, assetKey)
	if err != nil {
		return nil, err
	}
	return s.explainFreshnessByID(ctx, asset.ID, 0, map[string]struct{}{})
}

func (s *Service) explainFreshnessByID(ctx context.Context, assetID string, requiredMaxLagSeconds int64, stack map[string]struct{}) (*domain.AssetFreshnessNode, error) {
	if _, ok := stack[assetID]; ok {
		return nil, domain.ErrValidation("cycle detected while resolving asset freshness for %s", assetID)
	}
	stack[assetID] = struct{}{}
	defer delete(stack, assetID)

	asset, err := s.assets.GetByID(ctx, assetID)
	if err != nil {
		return nil, err
	}

	effectiveLag := strictestMaxLag(requiredMaxLagSeconds, assetDeclaredMaxLag(asset))
	node := &domain.AssetFreshnessNode{
		AssetID:                asset.ID,
		AssetKey:               asset.AssetKey,
		AssetType:              asset.AssetType,
		EffectiveMaxLagSeconds: effectiveLag,
	}

	upstreamDeps, err := s.deps.ListUpstream(ctx, asset.ID)
	if err != nil {
		return nil, fmt.Errorf("list upstream dependencies for %s: %w", asset.AssetKey, err)
	}

	for _, dep := range upstreamDeps {
		if dep.DependencyType == domain.DependencyTypeSoft {
			continue
		}
		child, childErr := s.explainFreshnessByID(ctx, dep.UpstreamAssetID, effectiveLag, stack)
		if childErr != nil {
			return nil, childErr
		}
		node.Upstream = append(node.Upstream, *child)
	}

	if assetTypeSupportsExecution(asset.AssetType) {
		s.populateExecutableFreshness(ctx, asset, node)
	} else {
		s.populateLogicalFreshness(node)
	}

	node.Basis = dedupeBasis(node.Basis)
	return node, nil
}

func (s *Service) populateExecutableFreshness(ctx context.Context, asset *domain.DataAsset, node *domain.AssetFreshnessNode) {
	if asset == nil || node == nil {
		return
	}

	for _, child := range node.Upstream {
		node.Basis = append(node.Basis, child.Basis...)
	}
	if len(node.Basis) == 0 {
		node.Basis = append(node.Basis, node.AssetKey)
	}

	if node.EffectiveMaxLagSeconds <= 0 {
		node.FreshnessStatus = domain.AssetFreshnessStatusUnknown
		node.Reason = "no freshness requirement configured"
		return
	}

	inFlight, err := s.hasActiveRun(ctx, asset.ID)
	if err == nil && inFlight {
		node.FreshnessStatus = domain.AssetFreshnessStatusRefreshing
		node.Reason = "asset refresh is currently in progress"
		return
	}

	lastMaterializedAt, found, err := s.latestMaterializationAt(ctx, asset.ID)
	if err != nil {
		node.FreshnessStatus = domain.AssetFreshnessStatusUnknown
		node.Reason = err.Error()
		return
	}
	if !found {
		node.FreshnessStatus = domain.AssetFreshnessStatusStale
		node.Reason = "no materialization found"
		return
	}

	node.LastMaterializedAt = &lastMaterializedAt
	deadline := lastMaterializedAt.Add(time.Duration(node.EffectiveMaxLagSeconds) * time.Second)
	if time.Now().After(deadline) {
		node.FreshnessStatus = domain.AssetFreshnessStatusStale
		node.Reason = fmt.Sprintf("last materialization is older than %ds", node.EffectiveMaxLagSeconds)
		node.StaleSince = &deadline
		return
	}

	node.FreshnessStatus = domain.AssetFreshnessStatusFresh
}

func (s *Service) populateLogicalFreshness(node *domain.AssetFreshnessNode) {
	if node == nil {
		return
	}

	if len(node.Upstream) == 0 {
		node.FreshnessStatus = domain.AssetFreshnessStatusUnknown
		node.Reason = "no upstream freshness basis found"
		node.Basis = append(node.Basis, node.AssetKey)
		return
	}

	for _, child := range node.Upstream {
		node.Basis = append(node.Basis, child.Basis...)
	}

	status := domain.AssetFreshnessStatusFresh
	reason := ""
	for _, child := range node.Upstream {
		switch child.FreshnessStatus {
		case domain.AssetFreshnessStatusRefreshing:
			status = domain.AssetFreshnessStatusRefreshing
			reason = fmt.Sprintf("upstream %s is refreshing", child.AssetKey)
			goto done
		case domain.AssetFreshnessStatusBlocked:
			status = domain.AssetFreshnessStatusBlocked
			reason = fmt.Sprintf("upstream %s is blocked", child.AssetKey)
			goto done
		case domain.AssetFreshnessStatusStale:
			status = domain.AssetFreshnessStatusStale
			reason = fmt.Sprintf("upstream %s is stale", child.AssetKey)
			goto done
		case domain.AssetFreshnessStatusUnknown:
			status = domain.AssetFreshnessStatusUnknown
			reason = fmt.Sprintf("upstream %s freshness is unknown", child.AssetKey)
		}
	}

done:
	node.FreshnessStatus = status
	node.Reason = reason
}

func (s *Service) latestMaterializationAt(ctx context.Context, assetID string) (time.Time, bool, error) {
	page := domain.PageRequest{MaxResults: domain.MaxMaxResults}
	for {
		materializations, total, err := s.runs.ListMaterializationsByAsset(ctx, assetID, page)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("list materializations for %s: %w", assetID, err)
		}
		for _, materialization := range materializations {
			return materialization.MaterializedAt, true, nil
		}
		nextOffset := page.Offset() + page.Limit()
		if int64(nextOffset) >= total || len(materializations) == 0 {
			return time.Time{}, false, nil
		}
		page.PageToken = domain.EncodePageToken(nextOffset)
	}
}

func (s *Service) hasActiveRun(ctx context.Context, assetID string) (bool, error) {
	filter := domain.AssetRunFilter{
		AssetID: &assetID,
		Page:    domain.PageRequest{MaxResults: domain.MaxMaxResults},
	}
	for _, status := range activeAssetRunStatuses {
		filter.Status = &status
		runs, _, err := s.runs.ListRuns(ctx, filter)
		if err != nil {
			return false, fmt.Errorf("list runs for %s: %w", assetID, err)
		}
		if len(runs) > 0 {
			return true, nil
		}
	}
	return false, nil
}

func strictestMaxLag(a int64, b int64) int64 {
	switch {
	case a > 0 && b > 0:
		if a < b {
			return a
		}
		return b
	case a > 0:
		return a
	default:
		return b
	}
}

func assetDeclaredMaxLag(asset *domain.DataAsset) int64 {
	if asset == nil || asset.FreshnessPolicy == nil {
		return 0
	}
	return asset.FreshnessPolicy.MaxLagSeconds
}

func assetTypeSupportsExecution(assetType string) bool {
	switch assetType {
	case domain.AssetTypeTable,
		domain.AssetTypeView,
		domain.AssetTypeModel,
		domain.AssetTypeOutput,
		domain.AssetTypeSemanticPreAggregation,
		domain.AssetTypeNotebookOutput:
		return true
	default:
		return false
	}
}

func dedupeBasis(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, item := range in {
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}
