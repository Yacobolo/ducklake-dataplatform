package dashboards

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"duck-demo/internal/domain"
	dashboardsvc "duck-demo/internal/service/dashboard"
)

type dashboardStateResponse struct {
	DashboardID   string                             `json:"dashboard_id"`
	Page          string                             `json:"page"`
	FilterKey     string                             `json:"filter_key,omitempty"`
	Compute       domain.DashboardComputePolicy      `json:"compute"`
	ActiveFilters []dashboardsvc.InteractiveFilter   `json:"active_filters,omitempty"`
	Widgets       []dashboardStateWidgetResponseItem `json:"widgets"`
}

type dashboardStateWidgetResponseItem struct {
	WidgetID        string                  `json:"widget_id"`
	FilterOriginKey string                  `json:"filter_origin_key"`
	Name            string                  `json:"name"`
	VisualKind      domain.VisualOutputKind `json:"visual_kind"`
	Payload         widgetRenderPayload     `json:"payload"`
}

func dashboardStateResponseFromResolved(dashboard *domain.Dashboard, pageKey string, filterKey string, filters []dashboardsvc.InteractiveFilter, widgets []dashboardsvc.ResolvedWidget) dashboardStateResponse {
	response := dashboardStateResponse{
		Compute:       domain.DashboardComputePolicy{}.Normalize(),
		Page:          pageKey,
		FilterKey:     filterKey,
		ActiveFilters: cloneInteractiveFilters(filters),
		Widgets:       make([]dashboardStateWidgetResponseItem, 0, len(widgets)),
	}
	if dashboard != nil {
		response.DashboardID = dashboard.ID
		response.Compute = dashboard.Compute.Normalize()
	}
	for _, widget := range widgets {
		if widget.Widget.VisualSpec == nil {
			continue
		}
		response.Widgets = append(response.Widgets, dashboardStateWidgetResponseItem{
			WidgetID:        widget.Widget.ID,
			FilterOriginKey: widget.Widget.FilterOriginKey,
			Name:            widget.Widget.Name,
			VisualKind:      widget.Widget.VisualSpec.Kind,
			Payload: widgetRenderPayload{
				Name:        widget.Widget.Name,
				Columns:     widget.Columns,
				Rows:        widget.Rows,
				RowCount:    widget.RowCount,
				Visual:      dashboardChartVisual(widget.Widget.VisualSpec),
				Interaction: widget.Interaction,
				Page:        widget.Page,
				Sort:        widget.Sort,
			},
		})
	}
	return response
}

func writeDashboardStateJSON(w http.ResponseWriter, response dashboardStateResponse) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(response)
}

func dashboardStateTablePageRequestsFromQuery(r *http.Request, pageWidgets []domain.DashboardWidget) (map[string]dashboardsvc.TablePageRequest, error) {
	tablePages := dashboardInitialTablePageRequests(pageWidgets)
	if r == nil {
		return tablePages, nil
	}

	widgetID := strings.TrimSpace(r.URL.Query().Get("table_widget_id"))
	if widgetID == "" {
		return tablePages, nil
	}

	var target *domain.DashboardWidget
	for index := range pageWidgets {
		widget := &pageWidgets[index]
		if strings.TrimSpace(widget.ID) != widgetID {
			continue
		}
		if widget.VisualSpec == nil || widget.VisualSpec.Kind != domain.VisualOutputTable {
			return nil, fmt.Errorf("table widget %q is not a table widget on this page", widgetID)
		}
		target = widget
		break
	}
	if target == nil {
		return nil, fmt.Errorf("table widget %q not found on this page", widgetID)
	}

	offset, err := dashboardStateIntQuery(r, "offset", 0)
	if err != nil {
		return nil, err
	}
	limit, err := dashboardStateIntQuery(r, "limit", 50)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	tablePages[target.ID] = dashboardsvc.TablePageRequest{
		Offset:        offset,
		Limit:         limit,
		Append:        false,
		SortColumn:    strings.TrimSpace(r.URL.Query().Get("sort_column")),
		SortDirection: strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort_direction"))),
	}
	return tablePages, nil
}

func dashboardStateIntQuery(r *http.Request, name string, fallback int) (int, error) {
	if r == nil {
		return fallback, nil
	}
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s query parameter", name)
	}
	return value, nil
}
