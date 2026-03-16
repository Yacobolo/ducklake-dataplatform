package legacy

import (
	"net/http"

	"duck-demo/internal/ui/core"
)

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, overviewPage(principalFromContext(r.Context()), []overviewCardData{
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

func (h *Handler) ComponentsPage(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, componentsPage(principalFromContext(r.Context())))
}

func stringsJoin(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	out := values[0]
	for i := 1; i < len(values); i++ {
		out += ", " + values[i]
	}
	return out
}

func strOrDash(v *string) string {
	if v == nil || *v == "" {
		return "-"
	}
	return *v
}

func (h *Handler) renderServiceError(w http.ResponseWriter, r *http.Request, err error) {
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

	_ = r
	renderHTML(w, status, core.ErrorPage(title, message))
}

func serviceErrorStatus(err error) (int, string) {
	return core.ServiceErrorStatus(err)
}
