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

func TestUIAssetsListPage_EmptyStateMessaging(t *testing.T) {
	html := renderAssetsListPageForTest(t, nil, "", false, false)

	assert.Contains(t, html, "No assets found yet.")
	assert.Contains(t, html, "Open catalogs")
	assert.Contains(t, html, "requires execute asset materialization permission")
	assert.Contains(t, html, "Backfill is not configured in this environment")
}

func TestUIAssetsListPage_FilterValueHydratesFromQuery(t *testing.T) {
	rows := []assetsListRowData{{
		Filter:   "orders.daily table analytics",
		AssetKey: "orders.daily",
		URL:      "/ui/assets/orders.daily",
		Type:     "table",
		Owner:    "analytics",
		Active:   true,
		Updated:  "2026-03-01T00:00:00Z",
	}}
	html := renderAssetsListPageForTest(t, rows, "orders", true, true)

	assert.Contains(t, html, `data-signals="{&#34;q&#34;:&#34;orders&#34;}"`)
	assert.Contains(t, html, "data-quick-filter-input=\"true\"")
	assert.Contains(t, html, "history.replaceState")
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

func renderAssetsListPageForTest(t *testing.T, rows []assetsListRowData, filter string, canMaterialize bool, backfillConfigured bool) string {
	t.Helper()

	var buf strings.Builder
	err := assetsListPage(
		domain.ContextPrincipal{Name: "tester", Type: "user"},
		rows,
		domain.PageRequest{MaxResults: 30},
		int64(len(rows)),
		filter,
		canMaterialize,
		backfillConfigured,
	).Render(&buf)
	require.NoError(t, err)
	return buf.String()
}
