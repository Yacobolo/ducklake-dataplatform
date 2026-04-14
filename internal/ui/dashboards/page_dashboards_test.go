package dashboards

import (
	"bytes"
	"testing"

	"github.com/Yacobolo/quackstack/internal/domain"
	dashboardsvc "github.com/Yacobolo/quackstack/internal/service/dashboard"
	"github.com/Yacobolo/quackstack/internal/ui/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomponents "maragu.dev/gomponents"
)

func TestDashboardsDetailPage_ViewModeHidesAuthoringChrome(t *testing.T) {
	chartType := domain.VisualChartBar
	page := dashboardsDetailPage(dashboardDetailPageData{
		Principal: domain.ContextPrincipal{Name: "alice", Type: "user"},
		Dashboard: &domain.Dashboard{
			ID:                  "dash-1",
			Name:                "Revenue Dashboard",
			Description:         "Executive view",
			SemanticProjectName: "analytics",
			SemanticModelName:   "sales",
			Compute: domain.DashboardComputePolicy{
				Mode:          domain.ComputeModeSharedEndpoint,
				EndpointName:  "analytics-xl",
				FallbackLocal: true,
			},
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
		BaseURL:          "/ui/dashboards/dash-1",
		ViewURL:          "/ui/dashboards/dash-1?page=overview",
		StudioURL:        "/ui/dashboards/dash-1?page=overview&mode=edit",
		EditURL:          "/ui/dashboards/dash-1/edit",
		DeleteURL:        "/ui/dashboards/dash-1/delete",
		CreateWidgetURL:  "/ui/dashboards/dash-1/widgets",
		SurfaceURL:       "/ui/dashboards/dash-1/surface",
		UpdatesStreamURL: "/ui/dashboards/dash-1/updates/stream-1",
		UpdatesApplyURL:  "/ui/dashboards/dash-1/updates/stream-1",
		StreamID:         "stream-1",
		UpdateVersion:    "version-1",
		PendingWidgetIDs: []string{"widget-chart", "widget-metric", "widget-table"},
		PageTabs: []core.SectionTab{
			{Label: "Overview", Href: "/ui/dashboards/dash-1?page=overview", Active: true},
			{Label: "Geography", Href: "/ui/dashboards/dash-1?page=geography", Active: false},
		},
		CurrentPageName: "Overview",
		CurrentPageKey:  "overview",
		ActiveFilters: []dashboardsvc.InteractiveFilter{
			{Dimension: "borough", Values: []string{"Queens"}},
		},
		CSRFToken: "token-123",
		CSRFFieldProvider: func() gomponents.Node {
			return nil
		},
		Widgets: []dashboardsvc.ResolvedWidget{
			{
				Widget: domain.DashboardWidget{
					ID:          "widget-chart",
					PageName:    "Overview",
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
				Interaction: &dashboardsvc.ResolvedWidgetInteraction{
					Participates: true,
					CanInitiate:  true,
				},
			},
			{
				Widget: domain.DashboardWidget{
					ID:       "widget-metric",
					PageName: "Overview",
					Name:     "Total Revenue",
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
			{
				Widget: domain.DashboardWidget{
					ID:       "widget-sql",
					PageName: "Overview",
					Name:     "SQL Helper",
					Source: domain.DashboardWidgetSource{
						Kind:     domain.DashboardWidgetSourceSQLQuery,
						SQLQuery: &domain.DashboardSQLQuerySource{SQL: "select 1"},
					},
				},
				Columns: []string{"value"},
				Rows:    [][]interface{}{{1}},
				Interaction: &dashboardsvc.ResolvedWidgetInteraction{
					Participates:   false,
					DisabledReason: "Not interactive in this dashboard.",
				},
			},
			{
				Widget: domain.DashboardWidget{
					ID:       "widget-table",
					PageName: "Overview",
					Name:     "Zone Revenue Detail",
					VisualSpec: &domain.VisualSpec{
						Kind: domain.VisualOutputTable,
					},
				},
				Columns:  []string{"pickup_zone", "gross_revenue"},
				Rows:     [][]interface{}{{"JFK Airport", 1338981.25}},
				RowCount: 1,
			},
		},
	})

	var buf bytes.Buffer
	require.NoError(t, page.Render(&buf))
	html := buf.String()
	assert.Contains(t, html, "Revenue Dashboard")
	assert.NotContains(t, html, "Generated SQL")
	assert.NotContains(t, html, "View data")
	assert.NotContains(t, html, "Edit widget")
	assert.NotContains(t, html, "data-chart-payload")
	assert.Contains(t, html, "data-ignore-morph")
	assert.Contains(t, html, "data-widget-id=\"widget-chart\"")
	assert.Contains(t, html, "data-widget-id=\"widget-table\"")
	assert.Contains(t, html, "<quack-table")
	assert.Contains(t, html, "Total Revenue")
	assert.Contains(t, html, "Studio")
	assert.Contains(t, html, "Cross Filters")
	assert.Contains(t, html, "Geography")
	assert.Contains(t, html, "Queens")
	assert.Contains(t, html, "data-dashboard-clear-filters")
	assert.Contains(t, html, "data-dashboard-remove-filter")
	assert.Contains(t, html, "data-dashboard-updates-url")
	assert.Contains(t, html, "data-dashboard-apply-url")
	assert.Contains(t, html, "data-dashboard-update-version=\"version-1\"")
	assert.Contains(t, html, "data-dashboard-pending-widget-ids=\"widget-chart,widget-metric,widget-table\"")
	assert.Contains(t, html, "Not interactive in this dashboard.")
	assert.Contains(t, html, "Compute Policy")
	assert.Contains(t, html, "Shared endpoint: analytics-xl (fallback local)")
	assert.Contains(t, html, "Viewer routing is read-only")
	assert.NotContains(t, html, "Freshness")
	assert.NotContains(t, html, "metric.sales.orders.revenue")
	assert.NotContains(t, html, "View Mode")
	assert.NotContains(t, html, "Dashboard Canvas")
}

func TestDashboardsDetailPage_StudioModeShowsAuthoringChrome(t *testing.T) {
	chartType := domain.VisualChartBar
	page := dashboardsDetailPage(dashboardDetailPageData{
		Principal: domain.ContextPrincipal{Name: "alice", Type: "user"},
		Dashboard: &domain.Dashboard{
			ID:          "dash-1",
			Name:        "Revenue Dashboard",
			Description: "Executive view",
		},
		EditMode:         true,
		BaseURL:          "/ui/dashboards/dash-1",
		ViewURL:          "/ui/dashboards/dash-1?page=overview",
		StudioURL:        "/ui/dashboards/dash-1?page=overview&mode=edit",
		EditURL:          "/ui/dashboards/dash-1/edit",
		DeleteURL:        "/ui/dashboards/dash-1/delete",
		CreateWidgetURL:  "/ui/dashboards/dash-1/widgets",
		SurfaceURL:       "/ui/dashboards/dash-1/surface",
		UpdatesStreamURL: "/ui/dashboards/dash-1/updates/stream-1",
		UpdatesApplyURL:  "/ui/dashboards/dash-1/updates/stream-1",
		StreamID:         "stream-1",
		UpdateVersion:    "version-1",
		PageTabs: []core.SectionTab{
			{Label: "Overview", Href: "/ui/dashboards/dash-1?page=overview&mode=edit", Active: true},
			{Label: "Geography", Href: "/ui/dashboards/dash-1?page=geography&mode=edit", Active: false},
		},
		CurrentPageName: "Overview",
		CurrentPageKey:  "overview",
		ActiveFilters: []dashboardsvc.InteractiveFilter{
			{Dimension: "borough", Values: []string{"Queens"}},
		},
		CSRFToken:         "token-123",
		CSRFFieldProvider: func() gomponents.Node { return nil },
		Widgets: []dashboardsvc.ResolvedWidget{
			{
				Widget: domain.DashboardWidget{
					ID:          "widget-chart",
					PageName:    "Overview",
					Name:        "Revenue by Region",
					Description: "Chart widget",
					Layout:      domain.DashboardWidgetLayout{W: 6, H: 4},
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
					ID:          "widget-table",
					PageName:    "Overview",
					Name:        "Zone Revenue Detail",
					Description: "Table widget",
					Layout:      domain.DashboardWidgetLayout{W: 12, H: 4},
					VisualSpec: &domain.VisualSpec{
						Kind: domain.VisualOutputTable,
					},
				},
				Columns:      []string{"pickup_zone", "gross_revenue"},
				Rows:         [][]interface{}{{"JFK Airport", 1338981.25}},
				RowCount:     1,
				GeneratedSQL: "select pickup_zone, gross_revenue from summary",
			},
		},
	})

	var buf bytes.Buffer
	require.NoError(t, page.Render(&buf))
	html := buf.String()
	assert.Contains(t, html, "Studio Mode")
	assert.Contains(t, html, "Generated SQL")
	assert.Contains(t, html, "View data")
	assert.Contains(t, html, "Studio Rail")
	assert.Contains(t, html, "Edit widget")
	assert.Contains(t, html, "Delete widget")
	assert.Contains(t, html, "Page")
	assert.NotContains(t, html, "Cross Filters")
	assert.Contains(t, html, "data-chart-payload")
	assert.Contains(t, html, "data-table-payload")
}

func TestDashboardsNewPage_ShowsComputePolicyControls(t *testing.T) {
	page := dashboardsNewPage(domain.ContextPrincipal{Name: "alice", Type: "user"}, "", func() gomponents.Node { return nil })

	var buf bytes.Buffer
	require.NoError(t, page.Render(&buf))
	html := buf.String()
	assert.Contains(t, html, "Compute mode")
	assert.Contains(t, html, "name=\"compute_mode\"")
	assert.Contains(t, html, "name=\"compute_endpoint_name\"")
	assert.Contains(t, html, "name=\"compute_fallback_local\"")
	assert.Contains(t, html, "Fallback to local if shared endpoint is unavailable")
	assert.Contains(t, html, ">AUTO<")
	assert.Contains(t, html, ">BYOC_LOCAL<")
	assert.Contains(t, html, ">SHARED_ENDPOINT<")
}

func TestDashboardsEditPage_ShowsCurrentComputePolicy(t *testing.T) {
	page := dashboardsEditPage(domain.ContextPrincipal{Name: "alice", Type: "user"}, &domain.Dashboard{
		ID:   "dash-1",
		Name: "Revenue Dashboard",
		Compute: domain.DashboardComputePolicy{
			Mode:          domain.ComputeModeSharedEndpoint,
			EndpointName:  "analytics-xl",
			FallbackLocal: true,
		},
	}, func() gomponents.Node { return nil })

	var buf bytes.Buffer
	require.NoError(t, page.Render(&buf))
	html := buf.String()
	assert.Contains(t, html, "name=\"compute_mode\"")
	assert.Contains(t, html, "option value=\"SHARED_ENDPOINT\" selected")
	assert.Contains(t, html, "name=\"compute_endpoint_name\"")
	assert.Contains(t, html, "value=\"analytics-xl\"")
	assert.Contains(t, html, "name=\"compute_fallback_local\"")
	assert.Contains(t, html, "checked")
}
