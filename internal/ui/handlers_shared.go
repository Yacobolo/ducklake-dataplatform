package ui

import (
	"errors"
	"net/http"

	"duck-demo/internal/domain"
)

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, overviewPage(principalFromContext(r.Context()), []overviewCardData{
		{Title: "Components", Description: "Browse the shared component library and design-token patterns.", Href: "/ui/components", LinkLabel: "Open components ->"},
		{Title: "SQL Editor", Description: "Run ad-hoc SQL with current principal permissions.", Href: "/ui/sql", LinkLabel: "Open SQL editor ->"},
		{Title: "Catalogs", Description: "Browse registered catalogs and metastore summary.", Href: "/ui/catalogs", LinkLabel: "Open catalogs ->"},
		{Title: "Security", Description: "Manage principals, groups, grants, and API keys.", Href: "/ui/security", LinkLabel: "Open security ->"},
		{Title: "Storage", Description: "Manage credentials, external locations, and volumes.", Href: "/ui/storage", LinkLabel: "Open storage ->"},
		{Title: "Compute", Description: "Manage compute endpoints, assignments, and health.", Href: "/ui/compute", LinkLabel: "Open compute ->"},
		{Title: "Governance", Description: "Search catalog objects, inspect tags, lineage, audit logs, and query history.", Href: "/ui/governance", LinkLabel: "Open governance ->"},
		{Title: "Assets", Description: "Inspect asset graph, runs, materializations, and backfills.", Href: "/ui/assets", LinkLabel: "Open assets ->"},
		{Title: "Notebooks", Description: "Read notebook metadata and cell snapshots.", Href: "/ui/notebooks", LinkLabel: "Open notebooks ->"},
		{Title: "Macros", Description: "Inspect macro definitions and revisions.", Href: "/ui/macros", LinkLabel: "Open macros ->"},
		{Title: "Models", Description: "Read model SQL, dependencies, and config.", Href: "/ui/models", LinkLabel: "Open models ->"},
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
	status := http.StatusInternalServerError
	title := "Unexpected Error"
	message := "An unexpected error occurred while loading this page."

	var notFound *domain.NotFoundError
	var accessDenied *domain.AccessDeniedError
	var validation *domain.ValidationError
	var conflict *domain.ConflictError
	if errors.As(err, &notFound) {
		status = http.StatusNotFound
		title = "Not Found"
		message = notFound.Error()
	} else if errors.As(err, &accessDenied) {
		status = http.StatusForbidden
		title = "Access Denied"
		message = accessDenied.Error()
	} else if errors.As(err, &validation) {
		status = http.StatusBadRequest
		title = "Invalid Request"
		message = validation.Error()
	} else if errors.As(err, &conflict) {
		status = http.StatusConflict
		title = "Conflict"
		message = conflict.Error()
	}

	_ = r
	renderHTML(w, status, errorPage(title, message))
}
