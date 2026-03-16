package semantic

import (
	"net/http"

	"duck-demo/internal/ui/legacy"
)

type Handler struct{ legacy *legacy.Handler }

func New(h *legacy.Handler) *Handler { return &Handler{legacy: h} }

func (h *Handler) SemanticHome(w http.ResponseWriter, r *http.Request) { h.legacy.SemanticHome(w, r) }
func (h *Handler) SemanticModelsList(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticModelsList(w, r)
}
func (h *Handler) SemanticModelsNew(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticModelsNew(w, r)
}
func (h *Handler) SemanticModelsCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticModelsCreate(w, r)
}
func (h *Handler) SemanticModelsDetail(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticModelsDetail(w, r)
}
func (h *Handler) SemanticModelsEdit(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticModelsEdit(w, r)
}
func (h *Handler) SemanticModelsUpdate(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticModelsUpdate(w, r)
}
func (h *Handler) SemanticModelsDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticModelsDelete(w, r)
}
func (h *Handler) SemanticMetricsEdit(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticMetricsEdit(w, r)
}
func (h *Handler) SemanticMetricsUpdate(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticMetricsUpdate(w, r)
}
func (h *Handler) SemanticMetricsCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticMetricsCreate(w, r)
}
func (h *Handler) SemanticMetricsDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticMetricsDelete(w, r)
}
func (h *Handler) SemanticPreAggregationsEdit(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticPreAggregationsEdit(w, r)
}
func (h *Handler) SemanticPreAggregationsUpdate(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticPreAggregationsUpdate(w, r)
}
func (h *Handler) SemanticPreAggregationsCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticPreAggregationsCreate(w, r)
}
func (h *Handler) SemanticPreAggregationsDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticPreAggregationsDelete(w, r)
}
func (h *Handler) SemanticRelationshipsList(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticRelationshipsList(w, r)
}
func (h *Handler) SemanticRelationshipsEdit(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticRelationshipsEdit(w, r)
}
func (h *Handler) SemanticRelationshipsUpdate(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticRelationshipsUpdate(w, r)
}
func (h *Handler) SemanticRelationshipsCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticRelationshipsCreate(w, r)
}
func (h *Handler) SemanticRelationshipsDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticRelationshipsDelete(w, r)
}
func (h *Handler) SemanticQueryExplain(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticQueryExplain(w, r)
}
func (h *Handler) SemanticQueryRun(w http.ResponseWriter, r *http.Request) {
	h.legacy.SemanticQueryRun(w, r)
}
