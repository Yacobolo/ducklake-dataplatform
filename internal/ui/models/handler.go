package models

import (
	"net/http"

	"duck-demo/internal/ui/legacy"
)

type Handler struct{ legacy *legacy.Handler }

func New(h *legacy.Handler) *Handler { return &Handler{legacy: h} }

func (h *Handler) ModelsList(w http.ResponseWriter, r *http.Request)    { h.legacy.ModelsList(w, r) }
func (h *Handler) ModelsDAG(w http.ResponseWriter, r *http.Request)     { h.legacy.ModelsDAG(w, r) }
func (h *Handler) ModelRunsList(w http.ResponseWriter, r *http.Request) { h.legacy.ModelRunsList(w, r) }
func (h *Handler) ModelRunsDetail(w http.ResponseWriter, r *http.Request) {
	h.legacy.ModelRunsDetail(w, r)
}
func (h *Handler) ModelSourceFreshnessPage(w http.ResponseWriter, r *http.Request) {
	h.legacy.ModelSourceFreshnessPage(w, r)
}
func (h *Handler) ModelSourceFreshnessCheck(w http.ResponseWriter, r *http.Request) {
	h.legacy.ModelSourceFreshnessCheck(w, r)
}
func (h *Handler) ModelPromoteNotebook(w http.ResponseWriter, r *http.Request) {
	h.legacy.ModelPromoteNotebook(w, r)
}
func (h *Handler) ModelsDetail(w http.ResponseWriter, r *http.Request)  { h.legacy.ModelsDetail(w, r) }
func (h *Handler) ModelsNew(w http.ResponseWriter, r *http.Request)     { h.legacy.ModelsNew(w, r) }
func (h *Handler) ModelsCreate(w http.ResponseWriter, r *http.Request)  { h.legacy.ModelsCreate(w, r) }
func (h *Handler) ModelsEdit(w http.ResponseWriter, r *http.Request)    { h.legacy.ModelsEdit(w, r) }
func (h *Handler) ModelsUpdate(w http.ResponseWriter, r *http.Request)  { h.legacy.ModelsUpdate(w, r) }
func (h *Handler) ModelsDelete(w http.ResponseWriter, r *http.Request)  { h.legacy.ModelsDelete(w, r) }
func (h *Handler) ModelTestsNew(w http.ResponseWriter, r *http.Request) { h.legacy.ModelTestsNew(w, r) }
func (h *Handler) ModelTestsCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.ModelTestsCreate(w, r)
}
func (h *Handler) ModelTestsDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.ModelTestsDelete(w, r)
}
func (h *Handler) ModelFreshnessCheck(w http.ResponseWriter, r *http.Request) {
	h.legacy.ModelFreshnessCheck(w, r)
}
func (h *Handler) ModelRunsTrigger(w http.ResponseWriter, r *http.Request) {
	h.legacy.ModelRunsTrigger(w, r)
}
func (h *Handler) ModelRunsCancel(w http.ResponseWriter, r *http.Request) {
	h.legacy.ModelRunsCancel(w, r)
}
func (h *Handler) ModelRunsManualCancel(w http.ResponseWriter, r *http.Request) {
	h.legacy.ModelRunsManualCancel(w, r)
}
