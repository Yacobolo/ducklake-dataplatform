package ui

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomponents "maragu.dev/gomponents"

	"duck-demo/internal/domain"
)

func TestUIAssetDetailPage_ActionsVisibility(t *testing.T) {
	t.Run("authorized and configured", func(t *testing.T) {
		html := renderAssetDetailPageForTest(t, assetDetailPageData{
			AssetKey:           "orders.daily",
			CanMaterialize:     true,
			CanBackfill:        true,
			BackfillConfigured: true,
		})

		assert.Contains(t, html, `action="/ui/assets/orders.daily/materialize"`)
		assert.Contains(t, html, `action="/ui/assets/orders.daily/backfills"`)
		assert.NotContains(t, html, "Materialization unavailable")
		assert.NotContains(t, html, "Backfill unavailable")
		assert.NotContains(t, html, "Backfill service is not configured")
	})

	t.Run("unauthorized", func(t *testing.T) {
		html := renderAssetDetailPageForTest(t, assetDetailPageData{
			AssetKey:           "orders.daily",
			CanMaterialize:     false,
			CanBackfill:        false,
			BackfillConfigured: true,
		})

		assert.Contains(t, html, "Materialization unavailable")
		assert.Contains(t, html, "Backfill unavailable")
		assert.Contains(t, html, "Requires execute asset materialization on catalog")
		assert.Contains(t, html, "<fieldset disabled")
	})

	t.Run("backfill not configured", func(t *testing.T) {
		html := renderAssetDetailPageForTest(t, assetDetailPageData{
			AssetKey:           "orders.daily",
			CanMaterialize:     true,
			CanBackfill:        false,
			BackfillConfigured: false,
		})

		assert.Contains(t, html, `action="/ui/assets/orders.daily/materialize"`)
		assert.NotContains(t, html, `action="/ui/assets/orders.daily/backfills"`)
		assert.Contains(t, html, "Backfill service is not configured")
	})
}

func renderAssetDetailPageForTest(t *testing.T, data assetDetailPageData) string {
	t.Helper()

	if data.CSRFFieldFunc == nil {
		data.CSRFFieldFunc = func() gomponents.Node { return gomponents.Text("") }
	}
	data.Principal = domain.ContextPrincipal{Name: "tester", Type: "user"}
	data.AssetType = "table"
	data.Owner = "analytics"

	var buf strings.Builder
	err := assetDetailPage(data).Render(&buf)
	require.NoError(t, err)
	return buf.String()
}
