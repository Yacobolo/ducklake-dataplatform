package dashboards

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"duck-demo/internal/domain"
	dashboardsvc "duck-demo/internal/service/dashboard"
	"duck-demo/internal/ui/core"

	"github.com/go-chi/chi/v5"
	"github.com/starfederation/datastar-go/datastar"
)

type Handler struct {
	deps    *core.Dependencies
	updates *dashboardUpdateHub
}

func New(deps *core.Dependencies) *Handler {
	return &Handler{
		deps:    deps,
		updates: newDashboardUpdateHub(),
	}
}

func (h *Handler) DashboardsList(w http.ResponseWriter, r *http.Request) {
	principal, isAdmin := principalLabel(r)
	var owner *string
	if !isAdmin {
		owner = &principal
	}
	page := pageFromRequest(r, 25)
	items, total, err := h.deps.Dashboard.ListDashboards(r.Context(), owner, page)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	rows := make([]dashboardListRowData, 0, len(items))
	for _, item := range items {
		rows = append(rows, dashboardListRowData{
			Name:        item.Name,
			Description: item.Description,
			URL:         "/ui/dashboards/" + item.ID,
			Owner:       item.Owner,
			Updated:     formatTime(item.UpdatedAt),
		})
	}

	core.RenderHTML(w, http.StatusOK, dashboardsListPage(core.PrincipalFromContext(r.Context()), rows, page, total))
}

func (h *Handler) DashboardsNew(w http.ResponseWriter, r *http.Request) {
	selectedFolderID := strings.TrimSpace(r.URL.Query().Get("folder_id"))
	core.RenderHTML(w, http.StatusOK, dashboardsNewPage(core.PrincipalFromContext(r.Context()), selectedFolderID, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) DashboardsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	principal, _ := principalLabel(r)
	item, err := h.deps.Dashboard.CreateDashboard(r.Context(), principal, domain.CreateDashboardRequest{
		Name:                formString(r.Form, "name"),
		Description:         formString(r.Form, "description"),
		FolderID:            formOptionalString(r.Form, "folder_id"),
		SemanticProjectName: formString(r.Form, "semantic_project_name"),
		SemanticModelName:   formString(r.Form, "semantic_model_name"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/dashboards/"+item.ID, http.StatusSeeOther)
}

func (h *Handler) DashboardsDetail(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	editMode := r.URL.Query().Get("mode") == "edit"
	streamID := ""
	if !editMode {
		streamID = domain.NewID()
	}
	item, widgets, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	principal, _ := principalLabel(r)
	activeFilters := []dashboardsvc.InteractiveFilter(nil)
	dataStreamURL := "/ui/dashboards/" + dashboardID + "/updates/" + streamID + "/data"
	if !editMode {
		dataStreamURL = dashboardStreamURLWithFilters(dataStreamURL, activeDashboardFilterQuery(r))
	}
	var resolved []dashboardsvc.ResolvedWidget
	if editMode {
		resolved, err = h.deps.Dashboard.ResolveWidgets(r.Context(), principal, widgets)
		if err != nil {
			renderServiceError(w, err)
			return
		}
	} else {
		activeFilters = dashboardFiltersFromRequest(r)
		resolved, err = h.deps.Dashboard.ResolveWidgetsForDashboard(r.Context(), principal, item, widgets, activeFilters)
		if err != nil {
			renderServiceError(w, err)
			return
		}
	}

	var freshness *domain.AssetFreshnessStatus
	var freshnessExplain *domain.AssetFreshnessNode
	if h.deps.Asset != nil {
		assetKey := "dashboard." + dashboardID
		var notFoundErr *domain.NotFoundError
		if status, statusErr := h.deps.Asset.CheckFreshness(r.Context(), assetKey); statusErr == nil {
			freshness = status
		} else if !errors.As(statusErr, &notFoundErr) {
			renderServiceError(w, statusErr)
			return
		}
		notFoundErr = nil
		if explain, explainErr := h.deps.Asset.ExplainFreshness(r.Context(), assetKey); explainErr == nil {
			freshnessExplain = explain
		} else if !errors.As(explainErr, &notFoundErr) {
			renderServiceError(w, explainErr)
			return
		}
	}

	core.RenderHTML(w, http.StatusOK, dashboardsDetailPage(dashboardDetailPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		Dashboard:         item,
		Widgets:           resolved,
		EditMode:          editMode,
		Freshness:         freshness,
		FreshnessExplain:  freshnessExplain,
		BaseURL:           "/ui/dashboards/" + dashboardID,
		ViewURL:           "/ui/dashboards/" + dashboardID,
		StudioURL:         "/ui/dashboards/" + dashboardID + "?mode=edit",
		EditURL:           "/ui/dashboards/" + dashboardID + "/edit",
		DeleteURL:         "/ui/dashboards/" + dashboardID + "/delete",
		CreateWidgetURL:   "/ui/dashboards/" + dashboardID + "/widgets",
		SurfaceURL:        "/ui/dashboards/" + dashboardID + "/surface",
		UpdatesStreamURL:  "/ui/dashboards/" + dashboardID + "/updates/" + streamID,
		DataStreamURL:     dataStreamURL,
		UpdatesApplyURL:   "/ui/dashboards/" + dashboardID + "/updates/" + streamID,
		StreamID:          streamID,
		ActiveFilters:     activeFilters,
		CSRFToken:         h.deps.CSRFToken(r),
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}

func activeDashboardFilterQuery(r *http.Request) url.Values {
	values := url.Values{}
	if r == nil {
		return values
	}
	for _, filter := range r.URL.Query()["fo"] {
		filter = strings.TrimSpace(filter)
		if filter == "" {
			continue
		}
		values.Add("fo", filter)
	}
	return values
}

func dashboardStreamURLWithFilters(base string, values url.Values) string {
	if len(values) == 0 {
		return base
	}
	query := values.Encode()
	if query == "" {
		return base
	}
	return base + "?" + query
}

func (h *Handler) DashboardsSurface(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	item, widgets, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	principal, _ := principalLabel(r)
	activeFilters := dashboardFiltersFromRequest(r)
	resolved, err := h.deps.Dashboard.ResolveWidgetsForDashboard(r.Context(), principal, item, widgets, activeFilters)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	data := dashboardDetailPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		Dashboard:         item,
		Widgets:           resolved,
		BaseURL:           "/ui/dashboards/" + dashboardID,
		ViewURL:           "/ui/dashboards/" + dashboardID,
		StudioURL:         "/ui/dashboards/" + dashboardID + "?mode=edit",
		EditURL:           "/ui/dashboards/" + dashboardID + "/edit",
		DeleteURL:         "/ui/dashboards/" + dashboardID + "/delete",
		CreateWidgetURL:   "/ui/dashboards/" + dashboardID + "/widgets",
		SurfaceURL:        "/ui/dashboards/" + dashboardID + "/surface",
		ActiveFilters:     activeFilters,
		CSRFToken:         h.deps.CSRFToken(r),
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}

	html, err := renderHTMLString(dashboardViewSurface(data, dashboardWidgetNodes(data.Widgets, data.BaseURL, data.CSRFFieldProvider, false)))
	if err != nil {
		renderServiceError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(html))
}

func (h *Handler) DashboardsUpdatesStream(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	streamID := strings.TrimSpace(chi.URLParam(r, "streamID"))
	if streamID == "" {
		http.Error(w, "missing stream id", http.StatusBadRequest)
		return
	}

	ch, unsubscribe := h.updates.subscribe(streamID)
	defer unsubscribe()

	sse := datastar.NewSSE(w, r)
	principal, _ := principalLabel(r)
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := sse.Send(datastar.EventTypePatchSignals, []string{"signals {}"}); err != nil {
				return
			}
		case filters := <-ch:
			if err := h.writeDashboardSurfacePatch(r.Context(), principal, dashboardID, streamID, filters, r, sse); err != nil {
				_ = sse.ConsoleError(err)
				return
			}
		}
	}
}

func (h *Handler) DashboardsDataStream(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	streamID := strings.TrimSpace(chi.URLParam(r, "streamID"))
	if streamID == "" {
		http.Error(w, "missing stream id", http.StatusBadRequest)
		return
	}

	sse := datastar.NewSSE(w, r)

	principal, _ := principalLabel(r)
	if err := h.writeDashboardChartPayloadEvent(r.Context(), principal, dashboardID, dashboardFiltersFromRequest(r), sse); err != nil {
		renderServiceError(w, err)
		return
	}

	ch, unsubscribe := h.updates.subscribe(streamID)
	defer unsubscribe()

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := sse.Send(datastar.EventType("dashboard-ping"), []string{"{}"}); err != nil {
				return
			}
		case filters := <-ch:
			if err := h.writeDashboardChartPayloadEvent(r.Context(), principal, dashboardID, filters, sse); err != nil {
				return
			}
		}
	}
}

func (h *Handler) DashboardsUpdatesApply(w http.ResponseWriter, r *http.Request) {
	streamID := strings.TrimSpace(chi.URLParam(r, "streamID"))
	if streamID == "" {
		http.Error(w, "missing stream id", http.StatusBadRequest)
		return
	}

	filters, err := decodeDashboardUpdateRequest(r)
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to decode dashboard update request."))
		return
	}

	h.updates.publish(streamID, filters)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) DashboardsEdit(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	item, _, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	core.RenderHTML(w, http.StatusOK, dashboardsEditPage(core.PrincipalFromContext(r.Context()), item, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) DashboardsUpdate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	dashboardID := chi.URLParam(r, "dashboardID")
	principal, isAdmin := principalLabel(r)
	_, err := h.deps.Dashboard.UpdateDashboard(r.Context(), principal, isAdmin, dashboardID, domain.UpdateDashboardRequest{
		Name:                formOptionalString(r.Form, "name"),
		Description:         formOptionalString(r.Form, "description"),
		SemanticProjectName: formOptionalString(r.Form, "semantic_project_name"),
		SemanticModelName:   formOptionalString(r.Form, "semantic_model_name"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/dashboards/"+dashboardID, http.StatusSeeOther)
}

func (h *Handler) DashboardsDelete(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	principal, isAdmin := principalLabel(r)
	if err := h.deps.Dashboard.DeleteDashboard(r.Context(), principal, isAdmin, dashboardID); err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/dashboards", http.StatusSeeOther)
}

func (h *Handler) DashboardWidgetsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	dashboardID := chi.URLParam(r, "dashboardID")
	principal, isAdmin := principalLabel(r)
	spec, err := visualSpecFromForm(r.Form)
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", err.Error()))
		return
	}
	source, err := dashboardWidgetSourceFromForm(r.Form)
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", err.Error()))
		return
	}

	req := domain.CreateDashboardWidgetRequest{
		Name:        formString(r.Form, "name"),
		Description: formString(r.Form, "description"),
		Source:      source,
		VisualSpec:  spec,
		Layout: domain.DashboardWidgetLayout{
			X: parseIntWithDefault(formString(r.Form, "layout_x"), 0),
			Y: parseIntWithDefault(formString(r.Form, "layout_y"), 0),
			W: parseIntWithDefault(formString(r.Form, "layout_w"), 4),
			H: parseIntWithDefault(formString(r.Form, "layout_h"), 3),
		},
	}
	if _, err := h.deps.Dashboard.CreateWidget(r.Context(), principal, isAdmin, dashboardID, req); err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/dashboards/"+dashboardID, http.StatusSeeOther)
}

func (h *Handler) DashboardWidgetsEdit(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	widgetID := chi.URLParam(r, "widgetID")

	dashboard, widgets, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	var widget *domain.DashboardWidget
	for i := range widgets {
		if widgets[i].ID == widgetID {
			widget = &widgets[i]
			break
		}
	}
	if widget == nil {
		renderServiceError(w, domain.ErrNotFound("dashboard widget not found"))
		return
	}

	core.RenderHTML(w, http.StatusOK, dashboardWidgetEditPage(core.PrincipalFromContext(r.Context()), dashboard, widget, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) DashboardWidgetsUpdate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	dashboardID := chi.URLParam(r, "dashboardID")
	widgetID := chi.URLParam(r, "widgetID")
	principal, isAdmin := principalLabel(r)

	spec, err := visualSpecFromForm(r.Form)
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", err.Error()))
		return
	}
	source, err := dashboardWidgetSourceFromForm(r.Form)
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", err.Error()))
		return
	}
	name := formString(r.Form, "name")
	description := formString(r.Form, "description")

	req := domain.UpdateDashboardWidgetRequest{
		Name:        &name,
		Description: &description,
		Source:      &source,
		VisualSpec:  spec,
		Layout: &domain.DashboardWidgetLayout{
			X: parseIntWithDefault(formString(r.Form, "layout_x"), 0),
			Y: parseIntWithDefault(formString(r.Form, "layout_y"), 0),
			W: parseIntWithDefault(formString(r.Form, "layout_w"), 4),
			H: parseIntWithDefault(formString(r.Form, "layout_h"), 3),
		},
	}
	if _, err := h.deps.Dashboard.UpdateWidget(r.Context(), principal, isAdmin, widgetID, req); err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/dashboards/"+dashboardID, http.StatusSeeOther)
}

func (h *Handler) DashboardWidgetsDelete(w http.ResponseWriter, r *http.Request) {
	widgetID := chi.URLParam(r, "widgetID")
	dashboardID := chi.URLParam(r, "dashboardID")
	principal, isAdmin := principalLabel(r)
	if err := h.deps.Dashboard.DeleteWidget(r.Context(), principal, isAdmin, widgetID); err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/dashboards/"+dashboardID, http.StatusSeeOther)
}

func renderServiceError(w http.ResponseWriter, err error) {
	status, message := core.ServiceErrorStatus(err)
	title := "Unexpected Error"
	switch status {
	case http.StatusNotFound:
		title = "Not Found"
	case http.StatusForbidden:
		title = "Access Denied"
	case http.StatusBadRequest:
		title = "Invalid Request"
	case http.StatusConflict:
		title = "Conflict"
	}
	core.RenderHTML(w, status, core.ErrorPage(title, message))
}

func principalLabel(r *http.Request) (string, bool) {
	p := core.PrincipalFromContext(r.Context())
	if p.Name == "" {
		return "unknown", p.IsAdmin
	}
	return p.Name, p.IsAdmin
}

func pageFromRequest(r *http.Request, defaultPageSize int) domain.PageRequest {
	maxResults := defaultPageSize
	if maxResults <= 0 {
		maxResults = 25
	}
	if raw := r.URL.Query().Get("max_results"); raw != "" {
		if parsed, err := parseInt(raw); err == nil {
			maxResults = parsed
		}
	}
	if maxResults < 1 {
		maxResults = 1
	}
	if maxResults > 200 {
		maxResults = 200
	}
	return domain.PageRequest{
		MaxResults: maxResults,
		PageToken:  r.URL.Query().Get("page_token"),
	}
}

func dashboardFiltersFromRequest(r *http.Request) []dashboardsvc.InteractiveFilter {
	return interactiveFiltersFromOriginRaw(r.URL.Query()["fo"])
}

var _ dashboardsvc.ResolvedWidget

func (h *Handler) resolveDashboardViewData(ctx context.Context, principal, dashboardID, streamID string, filters []dashboardsvc.InteractiveFilter, r *http.Request) (dashboardDetailPageData, error) {
	item, widgets, err := h.deps.Dashboard.GetDashboard(ctx, dashboardID)
	if err != nil {
		return dashboardDetailPageData{}, err
	}

	resolved, err := h.deps.Dashboard.ResolveWidgetsForDashboard(ctx, principal, item, widgets, filters)
	if err != nil {
		return dashboardDetailPageData{}, err
	}

	return dashboardDetailPageData{
		Principal:         core.PrincipalFromContext(ctx),
		Dashboard:         item,
		Widgets:           resolved,
		BaseURL:           "/ui/dashboards/" + dashboardID,
		ViewURL:           "/ui/dashboards/" + dashboardID,
		StudioURL:         "/ui/dashboards/" + dashboardID + "?mode=edit",
		EditURL:           "/ui/dashboards/" + dashboardID + "/edit",
		DeleteURL:         "/ui/dashboards/" + dashboardID + "/delete",
		CreateWidgetURL:   "/ui/dashboards/" + dashboardID + "/widgets",
		SurfaceURL:        "/ui/dashboards/" + dashboardID + "/surface",
		UpdatesStreamURL:  "/ui/dashboards/" + dashboardID + "/updates/" + streamID,
		DataStreamURL:     "/ui/dashboards/" + dashboardID + "/updates/" + streamID + "/data",
		UpdatesApplyURL:   "/ui/dashboards/" + dashboardID + "/updates/" + streamID,
		StreamID:          streamID,
		ActiveFilters:     cloneInteractiveFilters(filters),
		CSRFToken:         h.deps.CSRFToken(r),
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}, nil
}

func (h *Handler) writeDashboardSurfacePatch(ctx context.Context, principal, dashboardID, streamID string, filters []dashboardsvc.InteractiveFilter, r *http.Request, sse *datastar.ServerSentEventGenerator) error {
	data, err := h.resolveDashboardViewData(ctx, principal, dashboardID, streamID, filters, r)
	if err != nil {
		return err
	}

	return sse.PatchElementGostar(
		dashboardViewSurface(data, dashboardWidgetNodes(data.Widgets, data.BaseURL, data.CSRFFieldProvider, false)),
		datastar.WithSelectorID("dashboard-view-surface"),
	)
}

func (h *Handler) writeDashboardChartPayloadEvent(ctx context.Context, principal, dashboardID string, filters []dashboardsvc.InteractiveFilter, sse *datastar.ServerSentEventGenerator) error {
	item, widgets, err := h.deps.Dashboard.GetDashboard(ctx, dashboardID)
	if err != nil {
		return err
	}

	resolved, err := h.deps.Dashboard.ResolveWidgetsForDashboard(ctx, principal, item, widgets, filters)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(dashboardChartPayloadEnvelope(resolved, filters))
	if err != nil {
		return fmt.Errorf("marshal dashboard chart payloads: %w", err)
	}

	return sse.Send(datastar.EventType("dashboard-chart-payloads"), []string{string(payload)})
}
