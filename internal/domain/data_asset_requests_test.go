package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateCreateAssetRequest_RejectsExecutionPoliciesForLogicalAssets(t *testing.T) {
	t.Parallel()

	err := ValidateCreateAssetRequest(CreateAssetRequest{
		AssetKey:  "dashboard.exec",
		AssetType: AssetTypeDashboard,
		Owner:     "analytics",
		MaterializationPolicy: &AssetMaterializationPolicy{
			Mode: "TABLE",
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support materialization_policy")
}

func TestValidateCreateAssetRequest_AcceptsFreshnessForLogicalAssets(t *testing.T) {
	t.Parallel()

	err := ValidateCreateAssetRequest(CreateAssetRequest{
		AssetKey:  "dashboard.kpi",
		AssetType: AssetTypeDashboard,
		Owner:     "analytics",
		FreshnessPolicy: &AssetFreshnessPolicy{
			MaxLagSeconds: 1800,
		},
	})
	require.NoError(t, err)
}

func TestValidateCreateAssetRequest_AcceptsExecutionPoliciesForExecutableAssets(t *testing.T) {
	t.Parallel()

	err := ValidateCreateAssetRequest(CreateAssetRequest{
		AssetKey:  "semantic.preagg.daily_orders",
		AssetType: AssetTypeSemanticPreAggregation,
		Owner:     "analytics",
		FreshnessPolicy: &AssetFreshnessPolicy{
			MaxLagSeconds: 900,
		},
		MaterializationPolicy: &AssetMaterializationPolicy{
			Mode: "TABLE",
		},
		AutoMaterializePolicy: &AssetAutoMaterializePolicy{
			Mode:               "AUTO",
			OnFreshnessBreach:  true,
			MinIntervalSeconds: 300,
		},
	})
	require.NoError(t, err)
}

func TestValidateCreateAssetRequest_RejectsUnknownAssetType(t *testing.T) {
	t.Parallel()

	err := ValidateCreateAssetRequest(CreateAssetRequest{
		AssetKey:  "bad.asset",
		AssetType: "BLOB",
		Owner:     "analytics",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}
