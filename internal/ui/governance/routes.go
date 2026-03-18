package governance

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Routes interface {
	GovernanceHome(http.ResponseWriter, *http.Request)
	GovernanceSearch(http.ResponseWriter, *http.Request)
	GovernanceTagsList(http.ResponseWriter, *http.Request)
	GovernanceTagsCreate(http.ResponseWriter, *http.Request)
	GovernanceTagsDelete(http.ResponseWriter, *http.Request)
	GovernanceTagAssignmentsCreate(http.ResponseWriter, *http.Request)
	GovernanceTagAssignmentsDelete(http.ResponseWriter, *http.Request)
	GovernanceAuditLogs(http.ResponseWriter, *http.Request)
	GovernanceQueryHistory(http.ResponseWriter, *http.Request)
	GovernanceManifestPage(http.ResponseWriter, *http.Request)
	GovernanceManifestCreate(http.ResponseWriter, *http.Request)
	GovernanceLineage(http.ResponseWriter, *http.Request)
	GovernanceLineageDeleteEdge(http.ResponseWriter, *http.Request)
	GovernanceLineagePurge(http.ResponseWriter, *http.Request)
}

func MountRoutes(r chi.Router, h Routes) {
	r.Get("/governance", h.GovernanceHome)
	r.Get("/governance/search", h.GovernanceSearch)
	r.Get("/governance/tags", h.GovernanceTagsList)
	r.Post("/governance/tags", h.GovernanceTagsCreate)
	r.Post("/governance/tags/{tagID}/delete", h.GovernanceTagsDelete)
	r.Post("/governance/tag-assignments", h.GovernanceTagAssignmentsCreate)
	r.Post("/governance/tag-assignments/delete", h.GovernanceTagAssignmentsDelete)
	r.Get("/governance/audit-logs", h.GovernanceAuditLogs)
	r.Get("/governance/query-history", h.GovernanceQueryHistory)
	r.Get("/governance/manifest", h.GovernanceManifestPage)
	r.Post("/governance/manifest", h.GovernanceManifestCreate)
	r.Get("/governance/lineage", h.GovernanceLineage)
	r.Post("/governance/lineage/edges/{edgeID}/delete", h.GovernanceLineageDeleteEdge)
	r.Post("/governance/lineage/purge", h.GovernanceLineagePurge)
}
