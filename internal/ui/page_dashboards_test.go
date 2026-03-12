package ui

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomponents "maragu.dev/gomponents"

	"duck-demo/internal/domain"
	dashboardsvc "duck-demo/internal/service/dashboard"
)

func TestDashboardsDetailPage_RendersWidgetStates(t *testing.T) {
	chartType := domain.VisualChartBar
	page := dashboardsDetailPage(dashboardDetailPageData{
		Principal: domain.ContextPrincipal{Name: "alice", Type: "user"},
		Dashboard: &domain.Dashboard{
			ID:          "dash-1",
			Name:        "Revenue Dashboard",
			Description: "Executive view",
		},
		Freshness: &domain.AssetFreshnessStatus{
			AssetKey:               "dashboard.dash-1",
			FreshnessStatus:        domain.AssetFreshnessStatusStale,
			EffectiveMaxLagSeconds: 1800,
			Reason:                 "upstream metric.sales.orders.revenue is stale",
		},
		FreshnessExplain: &domain.AssetFreshnessNode{
			AssetKey:        "dashboard.dash-1",
			FreshnessStatus: domain.AssetFreshnessStatusStale,
			Upstream: []domain.AssetFreshnessNode{
				{
					AssetKey:        "metric.sales.orders.revenue",
					FreshnessStatus: domain.AssetFreshnessStatusStale,
					Reason:          "upstream model.sales.orders is stale",
				},
			},
		},
		BaseURL:         "/ui/dashboards/dash-1",
		EditURL:         "/ui/dashboards/dash-1/edit",
		DeleteURL:       "/ui/dashboards/dash-1/delete",
		CreateWidgetURL: "/ui/dashboards/dash-1/widgets",
		CSRFFieldProvider: func() gomponents.Node {
			return nil
		},
		Widgets: []dashboardsvc.ResolvedWidget{
			{
				Widget: domain.DashboardWidget{
					ID:          "widget-chart",
					Name:        "Revenue by Region",
					Description: "Chart widget",
					VisualSpec: &domain.VisualSpec{
						Kind:      domain.VisualOutputChart,
						ChartType: &chartType,
						Encodings: domain.VisualEncodings{
							X: &domain.VisualFieldBinding{Field: "region"},
							Y: &domain.VisualFieldBinding{Field: "revenue"},
						},
					},
				},
				Columns:      []string{"region", "revenue"},
				Rows:         [][]interface{}{{"APAC", 42}},
				RowCount:     1,
				GeneratedSQL: "select region, revenue from summary",
			},
			{
				Widget: domain.DashboardWidget{
					ID:   "widget-metric",
					Name: "Total Revenue",
					VisualSpec: &domain.VisualSpec{
						Kind: domain.VisualOutputMetric,
						Encodings: domain.VisualEncodings{
							Value: &domain.VisualFieldBinding{Field: "revenue"},
						},
					},
				},
				Columns:  []string{"revenue"},
				Rows:     [][]interface{}{{42}},
				RowCount: 1,
			},
		},
	})

	var buf bytes.Buffer
	require.NoError(t, page.Render(&buf))
	html := buf.String()
	assert.Contains(t, html, "Revenue Dashboard")
	assert.Contains(t, html, "Generated SQL")
	assert.Contains(t, html, "View data")
	assert.Contains(t, html, "Edit widget")
	assert.Contains(t, html, "data-chart-payload")
	assert.Contains(t, html, "Total Revenue")
	assert.Contains(t, html, "Freshness")
	assert.Contains(t, html, "metric.sales.orders.revenue")
}
