package dashboards

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"duck-demo/internal/domain"
	dashboardsvc "duck-demo/internal/service/dashboard"
	"duck-demo/internal/ui/core"

	"github.com/go-chi/chi/v5"
	"github.com/starfederation/datastar-go/datastar"
)

type dashboardPageRef struct {
	Name string
	Key  string
}

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
		Compute:             dashboardComputePolicyFromForm(r.Form),
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
	updateVersion := ""
	if !editMode {
		updateVersion = dashboardUpdateVersionFromRequest(r)
	}
	item, widgets, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	pages := dashboardPagesFromWidgets(widgets)
	activePage := dashboardActivePage(r, pages)
	pageWidgets := dashboardWidgetsForPage(widgets, activePage.Name)
	updatesValues := url.Values{"page": []string{activePage.Key}}
	if updateVersion != "" {
		updatesValues.Set("version", updateVersion)
	}

	principal, _ := principalLabel(r)
	activeFilters := []dashboardsvc.InteractiveFilter(nil)
	filterKey := ""
	rawOriginFilters := []string(nil)
	if !editMode {
		rawOriginFilters = dashboardOriginFiltersFromRequest(r)
		activeFilters = interactiveFiltersFromOriginRaw(rawOriginFilters, widgets)
		filterKey = dashboardFilterKeyFromOriginRaw(rawOriginFilters)
	}
	for _, rawFilter := range rawOriginFilters {
		updatesValues.Add("fo", rawFilter)
	}
	var resolved []dashboardsvc.ResolvedWidget
	if editMode {
		resolved, err = h.deps.Dashboard.ResolveWidgets(r.Context(), principal, pageWidgets)
		if err != nil {
			renderServiceError(w, err)
			return
		}
	} else {
		resolved = dashboardShellWidgets(pageWidgets)
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
		PageTabs:          dashboardPageTabs("/ui/dashboards/"+dashboardID, pages, activePage, editMode),
		CurrentPageName:   activePage.Name,
		CurrentPageKey:    activePage.Key,
		EditMode:          editMode,
		Freshness:         freshness,
		FreshnessExplain:  freshnessExplain,
		BaseURL:           "/ui/dashboards/" + dashboardID,
		ViewURL:           dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID, url.Values{"page": []string{activePage.Key}}),
		StudioURL:         dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID, url.Values{"page": []string{activePage.Key}, "mode": []string{"edit"}}),
		EditURL:           "/ui/dashboards/" + dashboardID + "/edit",
		DeleteURL:         "/ui/dashboards/" + dashboardID + "/delete",
		CreateWidgetURL:   "/ui/dashboards/" + dashboardID + "/widgets",
		SurfaceURL:        dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID+"/surface", url.Values{"page": []string{activePage.Key}}),
		UpdatesStreamURL:  dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID+"/updates/"+streamID, updatesValues),
		UpdatesApplyURL:   dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID+"/updates/"+streamID, url.Values{"page": []string{activePage.Key}}),
		TablePageURL:      dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID+"/updates/"+streamID+"/table-page", url.Values{"page": []string{activePage.Key}}),
		StreamID:          streamID,
		ActiveFilters:     activeFilters,
		FilterKey:         filterKey,
		UpdateVersion:     updateVersion,
		PendingWidgetIDs:  dashboardPayloadWidgetIDs(pageWidgets),
		CSRFToken:         h.deps.CSRFToken(r),
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) DashboardsState(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	item, widgets, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	pages := dashboardPagesFromWidgets(widgets)
	activePage := dashboardActivePage(r, pages)
	pageWidgets := dashboardWidgetsForPage(widgets, activePage.Name)
	tablePages, err := dashboardStateTablePageRequestsFromQuery(r, pageWidgets)
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", err.Error()))
		return
	}

	principal, _ := principalLabel(r)
	rawOriginFilters := dashboardOriginFiltersFromRequest(r)
	activeFilters := interactiveFiltersFromOriginRaw(rawOriginFilters, widgets)
	state, err := h.deps.Dashboard.BuildDashboardPageState(r.Context(), principal, item, widgets, activePage.Name, activeFilters, tablePages)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	if err := writeDashboardStateJSON(w, dashboardStateResponseFromResolved(item, activePage.Key, dashboardFilterKeyFromOriginRaw(rawOriginFilters), state.ActiveFilters, state.Widgets)); err != nil {
		renderServiceError(w, err)
		return
	}
}

func activeDashboardFilterQuery(r *http.Request) url.Values {
	values := url.Values{}
	if r == nil {
		return values
	}
	if pageKey := strings.TrimSpace(r.URL.Query().Get("page")); pageKey != "" {
		values.Set("page", pageKey)
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

func dashboardComputePolicyFromForm(values url.Values) *domain.DashboardComputePolicy {
	if values == nil {
		return nil
	}
	return &domain.DashboardComputePolicy{
		Mode:          strings.TrimSpace(values.Get("compute_mode")),
		EndpointName:  strings.TrimSpace(values.Get("compute_endpoint_name")),
		FallbackLocal: strings.EqualFold(strings.TrimSpace(values.Get("compute_fallback_local")), "true"),
	}
}

func dashboardUpdateVersionFromRequest(r *http.Request) string {
	if r != nil {
		if version := strings.TrimSpace(r.URL.Query().Get("version")); version != "" {
			return version
		}
	}
	return domain.NewID()
}

func dashboardShellWidgets(widgets []domain.DashboardWidget) []dashboardsvc.ResolvedWidget {
	if len(widgets) == 0 {
		return nil
	}
	out := make([]dashboardsvc.ResolvedWidget, 0, len(widgets))
	for _, widget := range widgets {
		out = append(out, dashboardsvc.ResolvedWidget{Widget: widget})
	}
	return out
}

func dashboardPayloadWidgetIDs(widgets []domain.DashboardWidget) []string {
	if len(widgets) == 0 {
		return nil
	}
	ids := make([]string, 0, len(widgets))
	for _, widget := range widgets {
		if widget.ID == "" || widget.VisualSpec == nil {
			continue
		}
		switch widget.VisualSpec.Kind {
		case domain.VisualOutputChart, domain.VisualOutputTable, domain.VisualOutputMetric:
			ids = append(ids, widget.ID)
		}
	}
	return ids
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

func dashboardPagesFromWidgets(widgets []domain.DashboardWidget) []dashboardPageRef {
	pages := make([]dashboardPageRef, 0)
	seen := make(map[string]struct{}, len(widgets))
	for _, widget := range widgets {
		name := domain.NormalizeDashboardPageName(widget.PageName)
		key := dashboardPageKey(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		pages = append(pages, dashboardPageRef{Name: name, Key: key})
	}
	if len(pages) == 0 {
		pages = append(pages, dashboardPageRef{Name: domain.DefaultDashboardPageName, Key: dashboardPageKey(domain.DefaultDashboardPageName)})
	}
	return pages
}

func dashboardPageKey(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return "overview"
	}
	var builder strings.Builder
	lastDash := false
	for _, r := range name {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			builder.WriteRune(r)
			lastDash = false
		case !lastDash:
			builder.WriteByte('-')
			lastDash = true
		}
	}
	key := strings.Trim(builder.String(), "-")
	if key == "" {
		return "overview"
	}
	return key
}

func dashboardActivePage(r *http.Request, pages []dashboardPageRef) dashboardPageRef {
	if len(pages) == 0 {
		return dashboardPageRef{Name: domain.DefaultDashboardPageName, Key: dashboardPageKey(domain.DefaultDashboardPageName)}
	}
	requested := ""
	if r != nil {
		requested = strings.TrimSpace(r.URL.Query().Get("page"))
	}
	for _, page := range pages {
		if page.Key == requested {
			return page
		}
	}
	return pages[0]
}

func dashboardWidgetsForPage(widgets []domain.DashboardWidget, pageName string) []domain.DashboardWidget {
	filtered := make([]domain.DashboardWidget, 0, len(widgets))
	pageName = domain.NormalizeDashboardPageName(pageName)
	for _, widget := range widgets {
		if domain.NormalizeDashboardPageName(widget.PageName) == pageName {
			filtered = append(filtered, widget)
		}
	}
	return filtered
}

func dashboardPageTabs(basePath string, pages []dashboardPageRef, active dashboardPageRef, editMode bool) []core.SectionTab {
	if len(pages) <= 1 {
		return nil
	}
	tabs := make([]core.SectionTab, 0, len(pages))
	for _, page := range pages {
		values := url.Values{}
		values.Set("page", page.Key)
		if editMode {
			values.Set("mode", "edit")
		}
		tabs = append(tabs, core.SectionTab{
			Label:  page.Name,
			Href:   dashboardStreamURLWithFilters(basePath, values),
			Active: page.Key == active.Key,
		})
	}
	return tabs
}

func (h *Handler) DashboardsSurface(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	item, widgets, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	pages := dashboardPagesFromWidgets(widgets)
	activePage := dashboardActivePage(r, pages)
	pageWidgets := dashboardWidgetsForPage(widgets, activePage.Name)

	principal, _ := principalLabel(r)
	rawOriginFilters := dashboardOriginFiltersFromRequest(r)
	activeFilters := interactiveFiltersFromOriginRaw(rawOriginFilters, widgets)
	resolved, err := h.deps.Dashboard.ResolveWidgetsForDashboard(r.Context(), principal, item, pageWidgets, activeFilters)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	data := dashboardDetailPageData{
		Principal:         core.PrincipalFromContext(r.Context()),
		Dashboard:         item,
		Widgets:           resolved,
		PageTabs:          dashboardPageTabs("/ui/dashboards/"+dashboardID, pages, activePage, false),
		CurrentPageName:   activePage.Name,
		CurrentPageKey:    activePage.Key,
		BaseURL:           "/ui/dashboards/" + dashboardID,
		ViewURL:           dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID, url.Values{"page": []string{activePage.Key}}),
		StudioURL:         dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID, url.Values{"page": []string{activePage.Key}, "mode": []string{"edit"}}),
		EditURL:           "/ui/dashboards/" + dashboardID + "/edit",
		DeleteURL:         "/ui/dashboards/" + dashboardID + "/delete",
		CreateWidgetURL:   "/ui/dashboards/" + dashboardID + "/widgets",
		SurfaceURL:        dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID+"/surface", url.Values{"page": []string{activePage.Key}}),
		ActiveFilters:     activeFilters,
		FilterKey:         dashboardFilterKeyFromOriginRaw(rawOriginFilters),
		CSRFToken:         h.deps.CSRFToken(r),
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
		TablePageURL:      "",
	}

	html, err := renderHTMLString(dashboardViewContent(data, dashboardWidgetNodes(data.Widgets, data.BaseURL, data.CSRFFieldProvider, false)))
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

	initialVersion := dashboardUpdateVersionFromRequest(r)
	rawOriginFilters := dashboardOriginFiltersFromRequest(r)
	_, widgets, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	initialFilters := interactiveFiltersFromOriginRaw(rawOriginFilters, widgets)
	initialFilterKey := dashboardFilterKeyFromOriginRaw(rawOriginFilters)
	if err := h.writeDashboardSurfacePatch(r.Context(), dashboardID, streamID, initialFilterKey, initialVersion, rawOriginFilters, initialFilters, r, sse); err != nil {
		renderServiceError(w, err)
		return
	}
	if err := h.writeDashboardWidgetPayloadEvents(r.Context(), principal, dashboardID, initialVersion, initialFilters, r, sse); err != nil {
		renderServiceError(w, err)
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := sse.Send(datastar.EventTypePatchSignals, []string{"signals {}"}); err != nil {
				return
			}
		case update := <-ch:
			if update.TablePage != nil {
				if err := h.writeDashboardWidgetPagePayloadEvent(r.Context(), principal, dashboardID, *update.TablePage, sse); err != nil {
					_ = sse.ConsoleError(err)
					return
				}
				continue
			}
			if err := h.writeDashboardSurfacePatch(r.Context(), dashboardID, streamID, update.FilterKey, update.Version, update.RawOriginFilters, update.Filters, r, sse); err != nil {
				_ = sse.ConsoleError(err)
				return
			}
			if err := h.writeDashboardWidgetPayloadEvents(r.Context(), principal, dashboardID, update.Version, update.Filters, r, sse); err != nil {
				_ = sse.ConsoleError(err)
				return
			}
		}
	}
}

func (h *Handler) DashboardsUpdatesApply(w http.ResponseWriter, r *http.Request) {
	streamID := strings.TrimSpace(chi.URLParam(r, "streamID"))
	dashboardID := chi.URLParam(r, "dashboardID")
	if streamID == "" {
		http.Error(w, "missing stream id", http.StatusBadRequest)
		return
	}

	rawOriginFilters, version, err := decodeDashboardUpdateRequest(r)
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to decode dashboard update request."))
		return
	}

	_, widgets, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	if version == "" {
		version = domain.NewID()
	}
	h.updates.publishFilters(streamID, dashboardFilterKeyFromOriginRaw(rawOriginFilters), version, rawOriginFilters, interactiveFiltersFromOriginRaw(rawOriginFilters, widgets))
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) DashboardsTablePageApply(w http.ResponseWriter, r *http.Request) {
	streamID := strings.TrimSpace(chi.URLParam(r, "streamID"))
	dashboardID := chi.URLParam(r, "dashboardID")
	if streamID == "" {
		http.Error(w, "missing stream id", http.StatusBadRequest)
		return
	}

	req, err := decodeDashboardTablePageRequest(r)
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to decode dashboard table page request."))
		return
	}
	if req == nil || strings.TrimSpace(req.WidgetID) == "" {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Missing table widget id."))
		return
	}

	_, widgets, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	req.Filters = interactiveFiltersFromOriginRaw(req.RawOriginFilters, widgets)
	req.FilterKey = dashboardFilterKeyFromOriginRaw(req.RawOriginFilters)
	if req.Version == "" {
		req.Version = domain.NewID()
	}
	h.updates.publishTablePage(streamID, *req)
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
		Compute:             dashboardComputePolicyFromForm(r.Form),
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
		PageName:    formString(r.Form, "page_name"),
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

	http.Redirect(w, r, dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID, url.Values{
		"mode": []string{"edit"},
		"page": []string{dashboardPageKey(req.PageName)},
	}), http.StatusSeeOther)
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
	pageName := formString(r.Form, "page_name")

	req := domain.UpdateDashboardWidgetRequest{
		PageName:    &pageName,
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

	http.Redirect(w, r, dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID, url.Values{
		"mode": []string{"edit"},
		"page": []string{dashboardPageKey(pageName)},
	}), http.StatusSeeOther)
}

func (h *Handler) DashboardWidgetsDelete(w http.ResponseWriter, r *http.Request) {
	widgetID := chi.URLParam(r, "widgetID")
	dashboardID := chi.URLParam(r, "dashboardID")
	principal, isAdmin := principalLabel(r)
	widgetPageKey := dashboardPageKey(domain.DefaultDashboardPageName)
	if _, widgets, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID); err == nil {
		for _, widget := range widgets {
			if widget.ID == widgetID {
				widgetPageKey = dashboardPageKey(widget.PageName)
				break
			}
		}
	}
	if err := h.deps.Dashboard.DeleteWidget(r.Context(), principal, isAdmin, widgetID); err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID, url.Values{
		"mode": []string{"edit"},
		"page": []string{widgetPageKey},
	}), http.StatusSeeOther)
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

func dashboardOriginFiltersFromRequest(r *http.Request) []string {
	if r == nil {
		return nil
	}
	return sanitizeOriginFilterRaw(r.URL.Query()["fo"])
}

var _ dashboardsvc.ResolvedWidget

func (h *Handler) resolveDashboardViewData(ctx context.Context, dashboardID, streamID, filterKey, version string, rawOriginFilters []string, filters []dashboardsvc.InteractiveFilter, r *http.Request) (dashboardDetailPageData, error) {
	item, widgets, err := h.deps.Dashboard.GetDashboard(ctx, dashboardID)
	if err != nil {
		return dashboardDetailPageData{}, err
	}
	pages := dashboardPagesFromWidgets(widgets)
	activePage := dashboardActivePage(r, pages)
	pageWidgets := dashboardWidgetsForPage(widgets, activePage.Name)
	updatesValues := url.Values{"page": []string{activePage.Key}, "version": []string{version}}
	for _, rawFilter := range sanitizeOriginFilterRaw(rawOriginFilters) {
		updatesValues.Add("fo", rawFilter)
	}

	return dashboardDetailPageData{
		Principal:         core.PrincipalFromContext(ctx),
		Dashboard:         item,
		Widgets:           dashboardShellWidgets(pageWidgets),
		PageTabs:          dashboardPageTabs("/ui/dashboards/"+dashboardID, pages, activePage, false),
		CurrentPageName:   activePage.Name,
		CurrentPageKey:    activePage.Key,
		BaseURL:           "/ui/dashboards/" + dashboardID,
		ViewURL:           dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID, url.Values{"page": []string{activePage.Key}}),
		StudioURL:         dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID, url.Values{"page": []string{activePage.Key}, "mode": []string{"edit"}}),
		EditURL:           "/ui/dashboards/" + dashboardID + "/edit",
		DeleteURL:         "/ui/dashboards/" + dashboardID + "/delete",
		CreateWidgetURL:   "/ui/dashboards/" + dashboardID + "/widgets",
		SurfaceURL:        dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID+"/surface", url.Values{"page": []string{activePage.Key}}),
		UpdatesStreamURL:  dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID+"/updates/"+streamID, updatesValues),
		UpdatesApplyURL:   dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID+"/updates/"+streamID, url.Values{"page": []string{activePage.Key}}),
		TablePageURL:      dashboardStreamURLWithFilters("/ui/dashboards/"+dashboardID+"/updates/"+streamID+"/table-page", url.Values{"page": []string{activePage.Key}}),
		StreamID:          streamID,
		ActiveFilters:     cloneInteractiveFilters(filters),
		FilterKey:         filterKey,
		UpdateVersion:     version,
		PendingWidgetIDs:  dashboardPayloadWidgetIDs(pageWidgets),
		CSRFToken:         h.deps.CSRFToken(r),
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}, nil
}

func (h *Handler) writeDashboardSurfacePatch(ctx context.Context, dashboardID, streamID, filterKey, version string, rawOriginFilters []string, filters []dashboardsvc.InteractiveFilter, r *http.Request, sse *datastar.ServerSentEventGenerator) error {
	data, err := h.resolveDashboardViewData(ctx, dashboardID, streamID, filterKey, version, rawOriginFilters, filters, r)
	if err != nil {
		return err
	}
	return newDashboardStream(sse).patchSurface(
		dashboardViewContent(data, dashboardWidgetNodes(data.Widgets, data.BaseURL, data.CSRFFieldProvider, false)),
	)
}

func (h *Handler) writeDashboardWidgetPayloadEvents(ctx context.Context, principal, dashboardID, version string, filters []dashboardsvc.InteractiveFilter, r *http.Request, sse *datastar.ServerSentEventGenerator) error {
	item, widgets, err := h.deps.Dashboard.GetDashboard(ctx, dashboardID)
	if err != nil {
		return err
	}
	activePage := dashboardActivePage(r, dashboardPagesFromWidgets(widgets))
	state, err := h.deps.Dashboard.BuildDashboardPageState(ctx, principal, item, widgets, activePage.Name, filters, dashboardInitialTablePageRequests(dashboardWidgetsForPage(widgets, activePage.Name)))
	if err != nil {
		return err
	}
	for _, widget := range state.Widgets {
		payload, ok := dashboardWidgetPayload(widget, version)
		if !ok {
			continue
		}
		if err := newDashboardStream(sse).dispatchWidgetPayload(payload); err != nil {
			return fmt.Errorf("dispatch dashboard widget payload: %w", err)
		}
	}
	return nil
}

func (h *Handler) writeDashboardWidgetPagePayloadEvent(ctx context.Context, principal, dashboardID string, req dashboardTablePageRequest, sse *datastar.ServerSentEventGenerator) error {
	item, widgets, err := h.deps.Dashboard.GetDashboard(ctx, dashboardID)
	if err != nil {
		return err
	}

	resolved, err := h.deps.Dashboard.ResolveWidgetForDashboardPage(ctx, principal, item, widgets, req.WidgetID, req.Filters, dashboardsvc.TablePageRequest{
		Offset:        req.Offset,
		Limit:         req.Limit,
		Append:        req.Append,
		SortColumn:    req.SortColumn,
		SortDirection: req.SortDirection,
	})
	if err != nil {
		return err
	}

	payload, ok := dashboardWidgetPayload(*resolved, req.Version)
	if !ok {
		return nil
	}
	if err := newDashboardStream(sse).dispatchWidgetPayload(payload); err != nil {
		return fmt.Errorf("dispatch dashboard widget page payload: %w", err)
	}
	return nil
}

func dashboardInitialTablePageRequests(widgets []domain.DashboardWidget) map[string]dashboardsvc.TablePageRequest {
	out := make(map[string]dashboardsvc.TablePageRequest)
	for _, widget := range widgets {
		if widget.ID == "" || widget.VisualSpec == nil || widget.VisualSpec.Kind != domain.VisualOutputTable {
			continue
		}
		out[widget.ID] = dashboardsvc.TablePageRequest{
			Offset: 0,
			Limit:  50,
			Append: false,
		}
	}
	return out
}
