package domain

import (
	"strings"
)

// AssetCheckInput describes one asset check mutation payload.
type AssetCheckInput struct {
	Name       string
	CheckType  string
	Severity   string
	Enabled    bool
	ConfigJSON map[string]any
}

// CreateAssetRequest captures the writable asset definition fields.
type CreateAssetRequest struct {
	AssetKey          string
	AssetType         string
	Owner             string
	Description       string
	Tags              []string
	IOProfile         string
	IsActive          bool
	UpstreamAssetKeys []string
	Checks            []AssetCheckInput
}

// UpdateAssetRequest captures the full replacement body for an asset definition.
type UpdateAssetRequest struct {
	AssetType         string
	Owner             string
	Description       string
	Tags              []string
	IOProfile         string
	IsActive          bool
	UpstreamAssetKeys []string
	Checks            []AssetCheckInput
}

// ValidateCreateAssetRequest validates a create asset request.
func ValidateCreateAssetRequest(req CreateAssetRequest) error {
	if strings.TrimSpace(req.AssetKey) == "" {
		return ErrValidation("asset_key is required")
	}
	return validateAssetMutation(strings.TrimSpace(req.AssetKey), req.AssetType, req.Owner, req.UpstreamAssetKeys, req.Checks)
}

// ValidateUpdateAssetRequest validates an update asset request.
func ValidateUpdateAssetRequest(assetKey string, req UpdateAssetRequest) error {
	if strings.TrimSpace(assetKey) == "" {
		return ErrValidation("asset_key is required")
	}
	return validateAssetMutation(strings.TrimSpace(assetKey), req.AssetType, req.Owner, req.UpstreamAssetKeys, req.Checks)
}

func validateAssetMutation(assetKey, assetType, owner string, upstreamAssetKeys []string, checks []AssetCheckInput) error {
	if strings.TrimSpace(assetType) == "" {
		return ErrValidation("asset_type is required")
	}
	if strings.TrimSpace(owner) == "" {
		return ErrValidation("owner is required")
	}

	seenUpstream := make(map[string]struct{}, len(upstreamAssetKeys))
	for _, upstream := range upstreamAssetKeys {
		upstream = strings.TrimSpace(upstream)
		if upstream == "" {
			return ErrValidation("upstream_asset_keys cannot contain blanks")
		}
		if upstream == assetKey {
			return ErrValidation("asset %q cannot depend on itself", assetKey)
		}
		if _, exists := seenUpstream[upstream]; exists {
			return ErrValidation("duplicate upstream asset key %q", upstream)
		}
		seenUpstream[upstream] = struct{}{}
	}

	seenChecks := make(map[string]struct{}, len(checks))
	for _, check := range checks {
		name := strings.TrimSpace(check.Name)
		if name == "" {
			return ErrValidation("check name is required")
		}
		if strings.TrimSpace(check.CheckType) == "" {
			return ErrValidation("check_type is required for check %q", name)
		}
		if _, exists := seenChecks[name]; exists {
			return ErrValidation("duplicate check name %q", name)
		}
		seenChecks[name] = struct{}{}
	}

	return nil
}
