package overview

import (
	"net/http"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"
)

const overviewListLimit = 6

type Handler struct {
	deps *core.Dependencies
}

func New(deps *core.Dependencies) *Handler { return &Handler{deps: deps} }

func (h *Handler) Home(w http.ResponseWriter, r *http.Request) {
	principal := core.PrincipalFromContext(r.Context())
	var recent []domain.ResourceAccessEvent
	var saved []domain.SavedResource
	if h.deps != nil {
		var err error
		if h.deps.ResourceAccess != nil {
			recent, err = h.deps.ResourceAccess.ListRecent(r.Context(), principal, overviewListLimit)
			if err != nil {
				core.RenderHTML(w, http.StatusInternalServerError, core.ErrorPage("Unable to load home", "Recent resources could not be loaded."))
				return
			}
		}
		if h.deps.SavedResource != nil {
			saved, err = h.deps.SavedResource.ListSaved(r.Context(), principal, overviewListLimit)
			if err != nil {
				core.RenderHTML(w, http.StatusInternalServerError, core.ErrorPage("Unable to load home", "Saved resources could not be loaded."))
				return
			}
		}
	}

	core.RenderHTML(w, http.StatusOK, overviewPage(overviewPageData{
		Principal: principal,
		Recent:    recent,
		Saved:     saved,
		CSRFField: h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) SaveResource(w http.ResponseWriter, r *http.Request) {
	if h.deps == nil || h.deps.SavedResource == nil {
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Saved resources are not configured."))
		return
	}
	if err := r.ParseForm(); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to parse resource form."))
		return
	}

	resource := domain.ResourceRef{
		ResourceType: r.FormValue("resource_type"),
		ResourceKey:  r.FormValue("resource_key"),
		DisplayName:  r.FormValue("display_name"),
		ResourcePath: r.FormValue("resource_path"),
		Section:      r.FormValue("section"),
	}
	if err := h.deps.SavedResource.Save(r.Context(), core.PrincipalFromContext(r.Context()), resource); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", err.Error()))
		return
	}
	http.Redirect(w, r, core.SafeUIReturnPath(r.FormValue("return_to")), http.StatusSeeOther)
}

func (h *Handler) UnsaveResource(w http.ResponseWriter, r *http.Request) {
	if h.deps == nil || h.deps.SavedResource == nil {
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Saved resources are not configured."))
		return
	}
	if err := r.ParseForm(); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Unable to parse resource form."))
		return
	}
	if err := h.deps.SavedResource.Unsave(
		r.Context(),
		core.PrincipalFromContext(r.Context()),
		r.FormValue("resource_type"),
		r.FormValue("resource_key"),
	); err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", err.Error()))
		return
	}
	http.Redirect(w, r, core.SafeUIReturnPath(r.FormValue("return_to")), http.StatusSeeOther)
}
