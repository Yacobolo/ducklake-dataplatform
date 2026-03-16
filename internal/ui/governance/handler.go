package governance

import (
	"net/http"

	"duck-demo/internal/ui/legacy"
)

type Handler struct{ legacy *legacy.Handler }

func New(h *legacy.Handler) *Handler { return &Handler{legacy: h} }

func (h *Handler) GovernanceHome(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceHome(w, r)
}
func (h *Handler) GovernanceSearch(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceSearch(w, r)
}
func (h *Handler) GovernanceTagsList(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceTagsList(w, r)
}
func (h *Handler) GovernanceTagsCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceTagsCreate(w, r)
}
func (h *Handler) GovernanceTagsDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceTagsDelete(w, r)
}
func (h *Handler) GovernanceTagAssignmentsCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceTagAssignmentsCreate(w, r)
}
func (h *Handler) GovernanceTagAssignmentsDelete(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceTagAssignmentsDelete(w, r)
}
func (h *Handler) GovernanceAuditLogs(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceAuditLogs(w, r)
}
func (h *Handler) GovernanceQueryHistory(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceQueryHistory(w, r)
}
func (h *Handler) GovernanceManifestPage(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceManifestPage(w, r)
}
func (h *Handler) GovernanceManifestCreate(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceManifestCreate(w, r)
}
func (h *Handler) GovernanceLineage(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceLineage(w, r)
}
func (h *Handler) GovernanceLineageDeleteEdge(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceLineageDeleteEdge(w, r)
}
func (h *Handler) GovernanceLineagePurge(w http.ResponseWriter, r *http.Request) {
	h.legacy.GovernanceLineagePurge(w, r)
}
