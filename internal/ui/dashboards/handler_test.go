package dashboards

import (
	"net/http/httptest"
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDashboardActivePage_UsesRequestedPageKey(t *testing.T) {
	t.Parallel()

	widgets := []domain.DashboardWidget{
		{Name: "Revenue", PageName: "Overview"},
		{Name: "Zones", PageName: "Geography"},
	}
	pages := dashboardPagesFromWidgets(widgets)

	r := httptest.NewRequest("GET", "/ui/dashboards/dash-1?page=geography", nil)
	page := dashboardActivePage(r, pages)

	assert.Equal(t, "Geography", page.Name)
	assert.Equal(t, "geography", page.Key)
}

func TestDashboardWidgetsForPage_FiltersToSelectedPage(t *testing.T) {
	t.Parallel()

	widgets := []domain.DashboardWidget{
		{ID: "widget-overview", PageName: "Overview"},
		{ID: "widget-geo", PageName: "Geography"},
	}

	filtered := dashboardWidgetsForPage(widgets, "Geography")
	require.Len(t, filtered, 1)
	assert.Equal(t, "widget-geo", filtered[0].ID)
}
