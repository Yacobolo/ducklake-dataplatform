package asset

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"duck-demo/internal/domain"
	servicepolicy "duck-demo/internal/service/policy"
)

// CreateAsset creates an asset definition and reconciles dependencies and checks.
func (s *Service) CreateAsset(ctx context.Context, req domain.CreateAssetRequest) (*domain.DataAsset, error) {
	if err := domain.ValidateCreateAssetRequest(req); err != nil {
		return nil, err
	}
	if err := s.requirePrivilege(ctx, domain.PrivManageAssetDefinitions); err != nil {
		return nil, err
	}

	asset := &domain.DataAsset{
		AssetKey:              strings.TrimSpace(req.AssetKey),
		AssetType:             strings.TrimSpace(req.AssetType),
		ProductID:             "",
		Owner:                 strings.TrimSpace(req.Owner),
		Description:           strings.TrimSpace(req.Description),
		Tags:                  normalizedTags(req.Tags),
		FreshnessPolicy:       req.FreshnessPolicy,
		MaterializationPolicy: req.MaterializationPolicy,
		AutoMaterializePolicy: req.AutoMaterializePolicy,
		IOProfile:             strings.TrimSpace(req.IOProfile),
		IsActive:              req.IsActive,
		CreatedBy:             servicepolicy.CallerName(ctx),
		SchemaJSON:            map[string]any{},
	}
	if s.products == nil {
		return nil, domain.ErrValidation("product linkage is not configured")
	}
	product, err := s.products.GetBySlug(ctx, strings.TrimSpace(req.ProductSlug))
	if err != nil {
		return nil, fmt.Errorf("resolve product %q: %w", strings.TrimSpace(req.ProductSlug), err)
	}
	asset.ProductID = product.Product.ID

	created, err := s.assets.Create(ctx, asset)
	if err != nil {
		return nil, fmt.Errorf("create asset: %w", err)
	}
	if err := s.reconcileAssetDependencies(ctx, created.ID, created.AssetKey, req.UpstreamAssetKeys); err != nil {
		s.rollbackCreatedAsset(ctx, created.ID)
		return nil, err
	}
	if err := s.reconcileAssetChecks(ctx, created.ID, req.Checks); err != nil {
		s.rollbackCreatedAsset(ctx, created.ID)
		return nil, err
	}
	if s.audit != nil {
		_ = s.audit.Insert(ctx, &domain.AuditEntry{
			ID:            domain.NewID(),
			PrincipalName: servicepolicy.CallerName(ctx),
			Action:        "asset.create",
			Status:        "ALLOWED",
			CreatedAt:     created.CreatedAt,
		})
	}
	return s.assets.GetByID(ctx, created.ID)
}

// UpdateAsset replaces an asset definition and reconciles dependencies and checks.
func (s *Service) UpdateAsset(ctx context.Context, assetKey string, req domain.UpdateAssetRequest) (*domain.DataAsset, error) {
	assetKey = strings.TrimSpace(assetKey)
	if err := domain.ValidateUpdateAssetRequest(assetKey, req); err != nil {
		return nil, err
	}
	if err := s.requirePrivilege(ctx, domain.PrivManageAssetDefinitions); err != nil {
		return nil, err
	}

	existing, err := s.assets.GetByKey(ctx, assetKey)
	if err != nil {
		return nil, err
	}
	if s.products == nil {
		return nil, domain.ErrValidation("product linkage is not configured")
	}
	product, err := s.products.GetBySlug(ctx, strings.TrimSpace(req.ProductSlug))
	if err != nil {
		return nil, fmt.Errorf("resolve product %q: %w", strings.TrimSpace(req.ProductSlug), err)
	}

	updated, err := s.assets.Update(ctx, existing.ID, &domain.DataAsset{
		ID:                    existing.ID,
		AssetKey:              existing.AssetKey,
		AssetType:             strings.TrimSpace(req.AssetType),
		ProductID:             product.Product.ID,
		Owner:                 strings.TrimSpace(req.Owner),
		Description:           strings.TrimSpace(req.Description),
		Tags:                  normalizedTags(req.Tags),
		SchemaJSON:            existing.SchemaJSON,
		PartitionDefinition:   existing.PartitionDefinition,
		FreshnessPolicy:       req.FreshnessPolicy,
		MaterializationPolicy: req.MaterializationPolicy,
		AutoMaterializePolicy: req.AutoMaterializePolicy,
		IOProfile:             strings.TrimSpace(req.IOProfile),
		IsActive:              req.IsActive,
		CreatedBy:             existing.CreatedBy,
	})
	if err != nil {
		return nil, fmt.Errorf("update asset: %w", err)
	}
	if err := s.reconcileAssetDependencies(ctx, updated.ID, updated.AssetKey, req.UpstreamAssetKeys); err != nil {
		return nil, err
	}
	if err := s.reconcileAssetChecks(ctx, updated.ID, req.Checks); err != nil {
		return nil, err
	}
	if s.audit != nil {
		_ = s.audit.Insert(ctx, &domain.AuditEntry{
			ID:            domain.NewID(),
			PrincipalName: principalName(ctx),
			Action:        "asset.update",
			Status:        "ALLOWED",
			CreatedAt:     updated.UpdatedAt,
		})
	}
	return s.assets.GetByID(ctx, updated.ID)
}

// DeleteAsset deletes an asset definition and its init-managed metadata.
func (s *Service) DeleteAsset(ctx context.Context, assetKey string) error {
	assetKey = strings.TrimSpace(assetKey)
	if assetKey == "" {
		return domain.ErrValidation("asset_key is required")
	}
	if err := s.requirePrivilege(ctx, domain.PrivManageAssetDefinitions); err != nil {
		return err
	}

	asset, err := s.assets.GetByKey(ctx, assetKey)
	if err != nil {
		return err
	}
	checks, err := s.checks.ListChecksByAsset(ctx, asset.ID)
	if err != nil {
		return fmt.Errorf("list asset checks: %w", err)
	}
	for i := range checks {
		if err := s.checks.DeleteCheck(ctx, checks[i].ID); err != nil {
			return fmt.Errorf("delete asset check %q: %w", checks[i].Name, err)
		}
	}
	if err := s.deps.DeleteByAsset(ctx, asset.ID); err != nil {
		return fmt.Errorf("delete asset dependencies: %w", err)
	}
	if err := s.assets.Delete(ctx, asset.ID); err != nil {
		return fmt.Errorf("delete asset: %w", err)
	}
	if s.audit != nil {
		_ = s.audit.Insert(ctx, &domain.AuditEntry{
			ID:            domain.NewID(),
			PrincipalName: principalName(ctx),
			Action:        "asset.delete",
			Status:        "ALLOWED",
			CreatedAt:     time.Now().UTC(),
		})
	}
	return nil
}

func (s *Service) reconcileAssetDependencies(ctx context.Context, assetID, assetKey string, upstreamAssetKeys []string) error {
	if err := s.deps.DeleteByAsset(ctx, assetID); err != nil {
		return fmt.Errorf("clear asset dependencies: %w", err)
	}

	for _, upstreamKey := range upstreamAssetKeys {
		upstreamKey = strings.TrimSpace(upstreamKey)
		if upstreamKey == "" {
			continue
		}
		upstream, err := s.assets.GetByKey(ctx, upstreamKey)
		if err != nil {
			return fmt.Errorf("resolve upstream asset %q for %q: %w", upstreamKey, assetKey, err)
		}
		if _, err := s.deps.Create(ctx, &domain.AssetDependency{
			AssetID:         assetID,
			UpstreamAssetID: upstream.ID,
			DependencyType:  domain.DependencyTypeHard,
		}); err != nil {
			return fmt.Errorf("create asset dependency %q <- %q: %w", assetKey, upstreamKey, err)
		}
	}

	return nil
}

func (s *Service) reconcileAssetChecks(ctx context.Context, assetID string, desired []domain.AssetCheckInput) error {
	existing, err := s.checks.ListChecksByAsset(ctx, assetID)
	if err != nil {
		return fmt.Errorf("list asset checks: %w", err)
	}

	existingByName := make(map[string]domain.AssetCheck, len(existing))
	for i := range existing {
		existingByName[existing[i].Name] = existing[i]
	}
	desiredNames := make(map[string]struct{}, len(desired))

	for i := range desired {
		check := desired[i]
		name := strings.TrimSpace(check.Name)
		desiredNames[name] = struct{}{}
		candidate := &domain.AssetCheck{
			AssetID:    assetID,
			Name:       name,
			CheckType:  strings.TrimSpace(check.CheckType),
			Severity:   strings.TrimSpace(check.Severity),
			Enabled:    check.Enabled,
			ConfigJSON: cloneMap(check.ConfigJSON),
		}
		if candidate.Severity == "" {
			candidate.Severity = "ERROR"
		}

		if current, ok := existingByName[name]; ok {
			if _, err := s.checks.UpdateCheck(ctx, current.ID, candidate); err != nil {
				return fmt.Errorf("update asset check %q: %w", name, err)
			}
			continue
		}

		if _, err := s.checks.CreateCheck(ctx, candidate); err != nil {
			return fmt.Errorf("create asset check %q: %w", name, err)
		}
	}

	for i := range existing {
		if _, ok := desiredNames[existing[i].Name]; ok {
			continue
		}
		if err := s.checks.DeleteCheck(ctx, existing[i].ID); err != nil {
			return fmt.Errorf("delete asset check %q: %w", existing[i].Name, err)
		}
	}

	return nil
}

func normalizedTags(tags []string) []string {
	if len(tags) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(tags))
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}
	sort.Strings(out)
	return out
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func principalName(ctx context.Context) string { return servicepolicy.CallerName(ctx) }

func (s *Service) rollbackCreatedAsset(ctx context.Context, assetID string) {
	if s == nil {
		return
	}

	if s.checks != nil {
		checks, err := s.checks.ListChecksByAsset(ctx, assetID)
		if err == nil {
			for i := range checks {
				_ = s.checks.DeleteCheck(ctx, checks[i].ID)
			}
		}
	}
	if s.deps != nil {
		_ = s.deps.DeleteByAsset(ctx, assetID)
	}
	if s.assets != nil {
		_ = s.assets.Delete(ctx, assetID)
	}
}
