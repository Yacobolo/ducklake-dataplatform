package compute

import (
	"net/http"

	"duck-demo/internal/ui/legacy"
)

type Handler struct{ legacy *legacy.Handler }

func New(h *legacy.Handler) *Handler { return &Handler{legacy: h} }

func (h *Handler) ComputeHome(w http.ResponseWriter, r *http.Request) { h.legacy.ComputeHome(w, r) }
func (h *Handler) ComputeEndpointsList(w http.ResponseWriter, r *http.Request) {
	h.legacy.ComputeEndpointsList(w, r)
}
func (h *Handler) ComputeEndpointsNew(w http.ResponseWriter, r *http.Request) {
	h.legacy.ComputeEndpointsNew(w, r)
}
func (h *Handler) ComputeEndpointsCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.ComputeEndpointsCreate(w, r)
}
func (h *Handler) ComputeEndpointsDetail(w http.ResponseWriter, r *http.Request) {
	h.legacy.ComputeEndpointsDetail(w, r)
}
func (h *Handler) ComputeEndpointsEdit(w http.ResponseWriter, r *http.Request) {
	h.legacy.ComputeEndpointsEdit(w, r)
}
func (h *Handler) ComputeEndpointsUpdate(w http.ResponseWriter, r *http.Request) {
	h.legacy.ComputeEndpointsUpdate(w, r)
}
func (h *Handler) ComputeEndpointsDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.ComputeEndpointsDelete(w, r)
}
func (h *Handler) ComputeAssignmentsCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.ComputeAssignmentsCreate(w, r)
}
func (h *Handler) ComputeAssignmentsDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.ComputeAssignmentsDelete(w, r)
}
