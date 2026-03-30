package dashboards

import (
	"errors"
	"net/http"
	"strings"

	"duck-demo/internal/domain"
	dashboardsvc "duck-demo/internal/service/dashboard"
	"duck-demo/internal/ui/core"

	"github.com/go-chi/chi/v5"
)

type Handler struct {
	deps *core.Dependencies
}

func New(deps *core.Dependencies) *Handler {
	return &Handler{deps: deps}
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
		Name:        formString(r.Form, "name"),
		Description: formString(r.Form, "description"),
		FolderID:    formOptionalString(r.Form, "folder_id"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/dashboards/"+item.ID, http.StatusSeeOther)
}

func (h *Handler) DashboardsDetail(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	item, widgets, err := h.deps.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	principal, _ := principalLabel(r)
	resolved, err := h.deps.Dashboard.ResolveWidgets(r.Context(), principal, widgets)
	if err != nil {
		renderServiceError(w, err)
		return
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
		Freshness:         freshness,
		FreshnessExplain:  freshnessExplain,
		BaseURL:           "/ui/dashboards/" + dashboardID,
		EditURL:           "/ui/dashboards/" + dashboardID + "/edit",
		DeleteURL:         "/ui/dashboards/" + dashboardID + "/delete",
		CreateWidgetURL:   "/ui/dashboards/" + dashboardID + "/widgets",
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
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
		Name:        formOptionalString(r.Form, "name"),
		Description: formOptionalString(r.Form, "description"),
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

var _ dashboardsvc.ResolvedWidget
