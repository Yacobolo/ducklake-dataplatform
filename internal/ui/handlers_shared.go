package ui

import (
	"errors"
	"net/http"

	"duck-demo/internal/domain"
)

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, overviewPage(principalFromContext(r.Context()), []overviewCardData{
		{Title: "SQL Editor", Description: "Run ad-hoc SQL with current principal permissions.", Href: "/ui/sql", LinkLabel: "Open SQL editor ->"},
		{Title: "Catalogs", Description: "Browse registered catalogs and metastore summary.", Href: "/ui/catalogs", LinkLabel: "Open catalogs ->"},
		{Title: "Pipelines", Description: "Inspect pipeline definitions, jobs, and recent runs.", Href: "/ui/pipelines", LinkLabel: "Open pipelines ->"},
		{Title: "Notebooks", Description: "Read notebook metadata and cell snapshots.", Href: "/ui/notebooks", LinkLabel: "Open notebooks ->"},
		{Title: "Macros", Description: "Inspect macro definitions and revisions.", Href: "/ui/macros", LinkLabel: "Open macros ->"},
		{Title: "Models", Description: "Read model SQL, dependencies, and config.", Href: "/ui/models", LinkLabel: "Open models ->"},
	}))
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
