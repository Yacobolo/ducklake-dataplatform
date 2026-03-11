package domain

import "time"

const (
	// AssetFreshnessStatusFresh indicates the asset satisfies its effective freshness requirement.
	AssetFreshnessStatusFresh = "FRESH"
	// AssetFreshnessStatusStale indicates the asset is older than its effective freshness requirement.
	AssetFreshnessStatusStale = "STALE"
	// AssetFreshnessStatusRefreshing indicates the asset or one of its upstreams is actively refreshing.
	AssetFreshnessStatusRefreshing = "REFRESHING"
	// AssetFreshnessStatusBlocked indicates the asset cannot currently satisfy freshness because an upstream is blocked.
	AssetFreshnessStatusBlocked = "BLOCKED"
	// AssetFreshnessStatusUnknown indicates the platform cannot currently determine freshness.
	AssetFreshnessStatusUnknown = "UNKNOWN"
)

// AssetFreshnessStatus is the flattened freshness state returned for an asset.
type AssetFreshnessStatus struct {
	AssetID                string
	AssetKey               string
	AssetType              string
	FreshnessStatus        string
	EffectiveMaxLagSeconds int64
	LastMaterializedAt     *time.Time
	StaleSince             *time.Time
	Reason                 string
	Basis                  []string
}

// AssetFreshnessNode describes freshness state for an asset and its upstream explanation tree.
type AssetFreshnessNode struct {
	AssetID                string
	AssetKey               string
	AssetType              string
	UpstreamDependencyType string
	FreshnessStatus        string
	EffectiveMaxLagSeconds int64
	LastMaterializedAt     *time.Time
	StaleSince             *time.Time
	Reason                 string
	Basis                  []string
	Upstream               []AssetFreshnessNode
}

// AssetFreshnessReconcileTarget describes an executable asset selected for freshness-driven refresh.
type AssetFreshnessReconcileTarget struct {
	AssetID         string
	AssetKey        string
	AssetType       string
	FreshnessStatus string
	EventID         string
}

// AssetFreshnessReconcileResult captures the outcome of reconciling freshness for a logical or executable asset.
type AssetFreshnessReconcileResult struct {
	Asset   AssetFreshnessStatus
	Targets []AssetFreshnessReconcileTarget
}
