package ui

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssetLookupCandidates(t *testing.T) {
	assert.Equal(t,
		[]string{"orders", "analytics.orders", "duck.analytics.orders"},
		assetLookupCandidates("duck", "analytics", "orders"),
	)
}

func TestAssetLookupCandidates_DedupesAndTrims(t *testing.T) {
	assert.Equal(t,
		[]string{"orders"},
		assetLookupCandidates("", "", " orders "),
	)
}

func TestLinkedAssetResolver_ResolvePrefersMostSpecificMatch(t *testing.T) {
	resolver := linkedAssetResolver{byKey: map[string]linkedAssetRef{
		"orders":                {Key: "orders", URL: "/ui/assets/orders"},
		"analytics.orders":      {Key: "analytics.orders", URL: "/ui/assets/analytics.orders"},
		"duck.analytics.orders": {Key: "duck.analytics.orders", URL: "/ui/assets/duck.analytics.orders"},
		"duck.finance.orders":   {Key: "duck.finance.orders", URL: "/ui/assets/duck.finance.orders"},
	}}

	assert.Equal(t,
		linkedAssetRef{Key: "orders", URL: "/ui/assets/orders"},
		resolver.resolve("duck", "analytics", "orders"),
	)
}

func TestLinkedAssetResolver_ResolveReturnsEmptyWhenMissing(t *testing.T) {
	resolver := linkedAssetResolver{byKey: map[string]linkedAssetRef{
		"duck.analytics.orders": {Key: "duck.analytics.orders", URL: "/ui/assets/duck.analytics.orders"},
	}}

	assert.Equal(t, linkedAssetRef{}, resolver.resolve("duck", "analytics", "customers"))
}
