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
