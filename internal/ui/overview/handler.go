package overview

import (
	"net/http"

	"duck-demo/internal/ui/core"
)

type Handler struct{}

func New() *Handler { return &Handler{} }

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, overviewPage(core.PrincipalFromContext(r.Context()), []overviewCardData{
		{Title: "Products", Description: "Browse product contracts, ownership, linked outputs, and trust signals.", Href: "/ui/products", LinkLabel: "Open products ->"},
		{Title: "Components", Description: "Browse the shared component library and design-token patterns.", Href: "/ui/components", LinkLabel: "Open components ->"},
		{Title: "Catalogs", Description: "Browse registered catalogs and metastore summary.", Href: "/ui/catalogs", LinkLabel: "Open catalogs ->"},
		{Title: "Security", Description: "Manage principals, groups, grants, and API keys.", Href: "/ui/security", LinkLabel: "Open security ->"},
		{Title: "Storage", Description: "Manage credentials, external locations, and volumes.", Href: "/ui/storage", LinkLabel: "Open storage ->"},
		{Title: "Compute", Description: "Manage compute endpoints, assignments, and health.", Href: "/ui/compute", LinkLabel: "Open compute ->"},
		{Title: "Governance", Description: "Search catalog objects, inspect tags, lineage, audit logs, and query history.", Href: "/ui/governance", LinkLabel: "Open governance ->"},
		{Title: "Runtime Assets", Description: "Inspect asset graph, runs, materializations, and backfills behind published products.", Href: "/ui/assets", LinkLabel: "Open runtime assets ->"},
		{Title: "Notebooks", Description: "Read notebook metadata and cell snapshots.", Href: "/ui/notebooks", LinkLabel: "Open notebooks ->"},
		{Title: "Macros", Description: "Inspect macro definitions and revisions.", Href: "/ui/macros", LinkLabel: "Open macros ->"},
		{Title: "Models", Description: "Read model SQL, dependencies, and config.", Href: "/ui/models", LinkLabel: "Open models ->"},
		{Title: "Semantic", Description: "Manage semantic models, metrics, pre-aggregations, relationships, and metric queries.", Href: "/ui/semantic", LinkLabel: "Open semantic ->"},
	}))
}
