package legacy

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
			Runs: []domain.AssetRun{{
				ID:           "run-1",
				Status:       domain.AssetRunStatusSuccess,
				TriggerType:  domain.AssetTriggerTypeManual,
				AttemptCount: 1,
				MaxAttempts:  1,
			}},
		})

		assert.Contains(t, html, `action="/ui/assets/orders.daily/materialize"`)
		assert.Contains(t, html, `action="/ui/assets/orders.daily/backfills"`)
		assert.NotContains(t, html, "Materialization unavailable")
		assert.NotContains(t, html, "Backfill unavailable")
		assert.NotContains(t, html, "Backfill service is not configured")
		assert.Contains(t, html, "Asset command center")
		assert.Contains(t, html, "At a glance")
		assert.Contains(t, html, "asset-graph")
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
		Filter:              "orders.daily table analytics gold daily",
		AssetKey:            "orders.daily",
		URL:                 "/ui/assets/orders.daily",
		Type:                "table",
		Owner:               "analytics",
		Description:         "Daily order mart",
		Tags:                []string{"gold", "daily"},
		Active:              true,
		Updated:             "2026-03-01T00:00:00Z",
		FreshnessTracked:    true,
		PartitionType:       "Daily",
		AutoMaterialized:    true,
		MaterializationMode: "Eager",
	}}
	html := renderAssetsListPageForTest(t, rows, "orders", true, true)

	assert.Contains(t, html, `data-signals="{&#34;q&#34;:&#34;orders&#34;}"`)
	assert.Contains(t, html, "data-quick-filter-input=\"true\"")
	assert.Contains(t, html, "history.replaceState")
	assert.Contains(t, html, "Asset showcase")
	assert.Contains(t, html, "Daily order mart")
	assert.Contains(t, html, "Total assets")
}

func TestUIAssetsListPage_RendersOperationalSummary(t *testing.T) {
	html := renderAssetsListPageForTest(t, []assetsListRowData{{
		Filter:              "orders.daily table analytics",
		AssetKey:            "orders.daily",
		URL:                 "/ui/assets/orders.daily",
		Type:                "table",
		Owner:               "analytics",
		Description:         "Daily orders mart",
		Active:              true,
		Updated:             "2026-03-01T00:00:00Z",
		FreshnessTracked:    true,
		PartitionType:       "Daily",
		AutoMaterialized:    true,
		MaterializationMode: "Eager",
	}}, "", true, true)

	assert.Contains(t, html, "Assets are where metadata turns into runtime behavior")
	assert.Contains(t, html, "Total assets")
	assert.Contains(t, html, "Asset mix")
	assert.Contains(t, html, "Inventory")
	assert.Contains(t, html, "SLA")
	assert.Contains(t, html, "Auto")
}

func TestUIAssetDetailPage_RendersGraphHost(t *testing.T) {
	html := renderAssetDetailPageForTest(t, assetDetailPageData{
		AssetKey:            "mart.daily_revenue",
		UpstreamAssetKeys:   []string{"models.orders_enriched"},
		DownstreamAssetKeys: []string{"analytics.exec_summary"},
		DependencyEdges: []assetDependencyEdgeData{
			{FromKey: "models.orders_enriched", ToKey: "mart.daily_revenue"},
			{FromKey: "mart.daily_revenue", ToKey: "analytics.exec_summary"},
		},
	})

	assert.Contains(t, html, "asset-graph-view")
	assert.Contains(t, html, "Interactive dependency map")
	assert.Contains(t, html, "Adjacency list")
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
