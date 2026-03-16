package dashboards

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	DashboardsList(http.ResponseWriter, *http.Request)
	DashboardsNew(http.ResponseWriter, *http.Request)
	DashboardsCreate(http.ResponseWriter, *http.Request)
	DashboardsDetail(http.ResponseWriter, *http.Request)
	DashboardsEdit(http.ResponseWriter, *http.Request)
	DashboardsUpdate(http.ResponseWriter, *http.Request)
	DashboardsDelete(http.ResponseWriter, *http.Request)
	DashboardWidgetsCreate(http.ResponseWriter, *http.Request)
	DashboardWidgetsEdit(http.ResponseWriter, *http.Request)
	DashboardWidgetsUpdate(http.ResponseWriter, *http.Request)
	DashboardWidgetsDelete(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/dashboards", h.DashboardsList)
	r.Get("/dashboards/new", h.DashboardsNew)
	r.Post("/dashboards", h.DashboardsCreate)
	r.Get("/dashboards/{dashboardID}", h.DashboardsDetail)
	r.Get("/dashboards/{dashboardID}/edit", h.DashboardsEdit)
	r.Post("/dashboards/{dashboardID}/update", h.DashboardsUpdate)
	r.Post("/dashboards/{dashboardID}/delete", h.DashboardsDelete)
	r.Post("/dashboards/{dashboardID}/widgets", h.DashboardWidgetsCreate)
	r.Get("/dashboards/{dashboardID}/widgets/{widgetID}/edit", h.DashboardWidgetsEdit)
	r.Post("/dashboards/{dashboardID}/widgets/{widgetID}/update", h.DashboardWidgetsUpdate)
	r.Post("/dashboards/{dashboardID}/widgets/{widgetID}/delete", h.DashboardWidgetsDelete)
}
