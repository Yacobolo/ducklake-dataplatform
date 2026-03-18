package compute

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	ComputeHome(http.ResponseWriter, *http.Request)
	ComputeEndpointsList(http.ResponseWriter, *http.Request)
	ComputeEndpointsNew(http.ResponseWriter, *http.Request)
	ComputeEndpointsCreate(http.ResponseWriter, *http.Request)
	ComputeEndpointsDetail(http.ResponseWriter, *http.Request)
	ComputeEndpointsEdit(http.ResponseWriter, *http.Request)
	ComputeEndpointsUpdate(http.ResponseWriter, *http.Request)
	ComputeEndpointsDelete(http.ResponseWriter, *http.Request)
	ComputeAssignmentsCreate(http.ResponseWriter, *http.Request)
	ComputeAssignmentsDelete(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/compute", h.ComputeHome)
	r.Get("/compute/endpoints", h.ComputeEndpointsList)
	r.Get("/compute/endpoints/new", h.ComputeEndpointsNew)
	r.Post("/compute/endpoints", h.ComputeEndpointsCreate)
	r.Get("/compute/endpoints/{endpointName}", h.ComputeEndpointsDetail)
	r.Get("/compute/endpoints/{endpointName}/edit", h.ComputeEndpointsEdit)
	r.Post("/compute/endpoints/{endpointName}/update", h.ComputeEndpointsUpdate)
	r.Post("/compute/endpoints/{endpointName}/delete", h.ComputeEndpointsDelete)
	r.Post("/compute/endpoints/{endpointName}/assignments", h.ComputeAssignmentsCreate)
	r.Post("/compute/endpoints/{endpointName}/assignments/{assignmentID}/delete", h.ComputeAssignmentsDelete)
}
