package ui

import (
	"errors"
	"net/http"

	"duck-demo/internal/domain"

	"github.com/go-chi/chi/v5"
)

func (h *Handler) DashboardsList(w http.ResponseWriter, r *http.Request) {
	principal, isAdmin := principalLabel(r.Context())
	var owner *string
	if !isAdmin {
		owner = &principal
	}
	items, total, err := h.Dashboard.ListDashboards(r.Context(), owner, pageFromRequest(r, 25))
	if err != nil {
		h.renderServiceError(w, r, err)
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
	renderHTML(w, http.StatusOK, dashboardsListPage(principalFromContext(r.Context()), rows, pageFromRequest(r, 25), total))
}

func (h *Handler) DashboardsNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, dashboardsNewPage(principalFromContext(r.Context()), csrfFieldProvider(r)))
}

func (h *Handler) DashboardsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	principal, _ := principalLabel(r.Context())
	item, err := h.Dashboard.CreateDashboard(r.Context(), principal, domain.CreateDashboardRequest{
		Name:        formString(r.Form, "name"),
		Description: formString(r.Form, "description"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/dashboards/"+item.ID, http.StatusSeeOther)
}

func (h *Handler) DashboardsDetail(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	item, widgets, err := h.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	principal, _ := principalLabel(r.Context())
	resolved, err := h.Dashboard.ResolveWidgets(r.Context(), principal, widgets)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	var freshness *domain.AssetFreshnessStatus
	var freshnessExplain *domain.AssetFreshnessNode
	if h.Asset != nil {
		assetKey := "dashboard." + dashboardID
		var notFoundErr *domain.NotFoundError
		if status, statusErr := h.Asset.CheckFreshness(r.Context(), assetKey); statusErr == nil {
			freshness = status
		} else if !errors.As(statusErr, &notFoundErr) {
			h.renderServiceError(w, r, statusErr)
			return
		}
		notFoundErr = nil
		if explain, explainErr := h.Asset.ExplainFreshness(r.Context(), assetKey); explainErr == nil {
			freshnessExplain = explain
		} else if !errors.As(explainErr, &notFoundErr) {
			h.renderServiceError(w, r, explainErr)
			return
		}
	}
	renderHTML(w, http.StatusOK, dashboardsDetailPage(dashboardDetailPageData{
		Principal:         principalFromContext(r.Context()),
		Dashboard:         item,
		Widgets:           resolved,
		Freshness:         freshness,
		FreshnessExplain:  freshnessExplain,
		BaseURL:           "/ui/dashboards/" + dashboardID,
		EditURL:           "/ui/dashboards/" + dashboardID + "/edit",
		DeleteURL:         "/ui/dashboards/" + dashboardID + "/delete",
		CreateWidgetURL:   "/ui/dashboards/" + dashboardID + "/widgets",
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func (h *Handler) DashboardsEdit(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	item, _, err := h.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, dashboardsEditPage(principalFromContext(r.Context()), item, csrfFieldProvider(r)))
}

func (h *Handler) DashboardsUpdate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	dashboardID := chi.URLParam(r, "dashboardID")
	principal, isAdmin := principalLabel(r.Context())
	_, err := h.Dashboard.UpdateDashboard(r.Context(), principal, isAdmin, dashboardID, domain.UpdateDashboardRequest{
		Name:        formOptionalString(r.Form, "name"),
		Description: formOptionalString(r.Form, "description"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/dashboards/"+dashboardID, http.StatusSeeOther)
}

func (h *Handler) DashboardsDelete(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	principal, isAdmin := principalLabel(r.Context())
	if err := h.Dashboard.DeleteDashboard(r.Context(), principal, isAdmin, dashboardID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/dashboards", http.StatusSeeOther)
}

func (h *Handler) DashboardWidgetsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	dashboardID := chi.URLParam(r, "dashboardID")
	principal, isAdmin := principalLabel(r.Context())
	spec, err := visualSpecFromForm(r.Form)
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", err.Error()))
		return
	}
	source, err := dashboardWidgetSourceFromForm(r.Form)
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", err.Error()))
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
	if _, err := h.Dashboard.CreateWidget(r.Context(), principal, isAdmin, dashboardID, req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/dashboards/"+dashboardID, http.StatusSeeOther)
}

func (h *Handler) DashboardWidgetsEdit(w http.ResponseWriter, r *http.Request) {
	dashboardID := chi.URLParam(r, "dashboardID")
	widgetID := chi.URLParam(r, "widgetID")

	dashboard, widgets, err := h.Dashboard.GetDashboard(r.Context(), dashboardID)
	if err != nil {
		h.renderServiceError(w, r, err)
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
		h.renderServiceError(w, r, domain.ErrNotFound("dashboard widget not found"))
		return
	}

	renderHTML(w, http.StatusOK, dashboardWidgetEditPage(principalFromContext(r.Context()), dashboard, widget, csrfFieldProvider(r)))
}

func (h *Handler) DashboardWidgetsUpdate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	dashboardID := chi.URLParam(r, "dashboardID")
	widgetID := chi.URLParam(r, "widgetID")
	principal, isAdmin := principalLabel(r.Context())

	spec, err := visualSpecFromForm(r.Form)
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", err.Error()))
		return
	}
	source, err := dashboardWidgetSourceFromForm(r.Form)
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", err.Error()))
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
	if _, err := h.Dashboard.UpdateWidget(r.Context(), principal, isAdmin, widgetID, req); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/dashboards/"+dashboardID, http.StatusSeeOther)
}

func (h *Handler) DashboardWidgetsDelete(w http.ResponseWriter, r *http.Request) {
	widgetID := chi.URLParam(r, "widgetID")
	dashboardID := chi.URLParam(r, "dashboardID")
	principal, isAdmin := principalLabel(r.Context())
	if err := h.Dashboard.DeleteWidget(r.Context(), principal, isAdmin, widgetID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/dashboards/"+dashboardID, http.StatusSeeOther)
}
