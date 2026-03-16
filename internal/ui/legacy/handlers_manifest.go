package legacy

import (
	"net/http"
)

func (h *Handler) GovernanceManifestPage(w http.ResponseWriter, r *http.Request) {
	if h.Manifest == nil {
		renderHTML(w, http.StatusInternalServerError, errorPage("Manifest Unavailable", "Manifest service is not configured."))
		return
	}
	renderHTML(w, http.StatusOK, governanceManifestPage(governanceManifestPageData{
		Principal:         principalFromContext(r.Context()),
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}

func (h *Handler) GovernanceManifestCreate(w http.ResponseWriter, r *http.Request) {
	if h.Manifest == nil {
		renderHTML(w, http.StatusInternalServerError, errorPage("Manifest Unavailable", "Manifest service is not configured."))
		return
	}
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	principal, _ := principalLabel(r.Context())
	result, err := h.Manifest.GetManifest(
		r.Context(),
		principal,
		formString(r.Form, "catalog_name"),
		formString(r.Form, "schema_name"),
		formString(r.Form, "table_name"),
	)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, governanceManifestPage(governanceManifestPageData{
		Principal:         principalFromContext(r.Context()),
		CatalogName:       formString(r.Form, "catalog_name"),
		SchemaName:        formString(r.Form, "schema_name"),
		TableName:         formString(r.Form, "table_name"),
		Result:            result,
		CSRFFieldProvider: csrfFieldProvider(r),
	}))
}
