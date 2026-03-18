package compute

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"
)

type Handler struct{ deps *core.Dependencies }

func New(deps *core.Dependencies) *Handler { return &Handler{deps: deps} }

func (h *Handler) ComputeHome(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, computeHomePage(core.PrincipalFromContext(r.Context())))
}

func (h *Handler) ComputeEndpointsList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.deps.ComputeEndpoint.List(r.Context(), principalName(r), pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	rows := make([]computeEndpointRowData, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, computeEndpointRowData{
			Name:    item.Name,
			URL:     "/ui/compute/endpoints/" + url.PathEscape(item.Name),
			Type:    item.Type,
			Status:  item.Status,
			URLText: item.URL,
		})
	}
	core.RenderHTML(w, http.StatusOK, computeEndpointsListPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) ComputeEndpointsNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, computeEndpointFormPage(core.PrincipalFromContext(r.Context()), "New Compute Endpoint", "/ui/compute/endpoints", nil, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) ComputeEndpointsCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	maxMemoryGB, err := formOptionalInt64(r.Form, "max_memory_gb")
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "max_memory_gb must be numeric."))
		return
	}
	item, err := h.deps.ComputeEndpoint.Create(r.Context(), principalName(r), domain.CreateComputeEndpointRequest{
		Name:        formString(r.Form, "name"),
		URL:         formString(r.Form, "url"),
		Type:        formString(r.Form, "type"),
		Size:        formString(r.Form, "size"),
		MaxMemoryGB: maxMemoryGB,
		AuthToken:   formString(r.Form, "auth_token"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/compute/endpoints/"+url.PathEscape(item.Name), http.StatusSeeOther)
}

func (h *Handler) ComputeEndpointsDetail(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	item, err := h.deps.ComputeEndpoint.GetByName(r.Context(), principalName(r), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	assignments, _, err := h.deps.ComputeEndpoint.ListAssignments(r.Context(), principalName(r), name, domain.PageRequest{MaxResults: 100})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	healthText := "Unavailable"
	if health, healthErr := h.deps.ComputeEndpoint.HealthCheck(r.Context(), principalName(r), name); healthErr == nil && health != nil {
		parts := []string{}
		if health.Status != nil {
			parts = append(parts, "status="+*health.Status)
		}
		if health.DuckdbVersion != nil {
			parts = append(parts, "duckdb="+*health.DuckdbVersion)
		}
		healthText = strings.Join(parts, ", ")
	}
	rows := make([]computeAssignmentRowData, 0, len(assignments))
	for i := range assignments {
		a := assignments[i]
		rows = append(rows, computeAssignmentRowData{
			ID:            a.ID,
			PrincipalID:   a.PrincipalID,
			PrincipalType: a.PrincipalType,
			IsDefault:     a.IsDefault,
			FallbackLocal: a.FallbackLocal,
		})
	}
	core.RenderHTML(w, http.StatusOK, computeEndpointDetailPage(core.PrincipalFromContext(r.Context()), item, healthText, rows, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) ComputeEndpointsEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	item, err := h.deps.ComputeEndpoint.GetByName(r.Context(), principalName(r), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, computeEndpointFormPage(core.PrincipalFromContext(r.Context()), "Edit Compute Endpoint", "/ui/compute/endpoints/"+url.PathEscape(name)+"/update", item, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) ComputeEndpointsUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	maxMemoryGB, err := formOptionalInt64(r.Form, "max_memory_gb")
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "max_memory_gb must be numeric."))
		return
	}
	req := domain.UpdateComputeEndpointRequest{
		URL:         formOptionalString(r.Form, "url"),
		Size:        formOptionalString(r.Form, "size"),
		MaxMemoryGB: maxMemoryGB,
		AuthToken:   formOptionalString(r.Form, "auth_token"),
		Status:      formOptionalString(r.Form, "status"),
	}
	if _, err := h.deps.ComputeEndpoint.Update(r.Context(), principalName(r), name, req); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/compute/endpoints/"+url.PathEscape(name), http.StatusSeeOther)
}

func (h *Handler) ComputeEndpointsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	if err := h.deps.ComputeEndpoint.Delete(r.Context(), principalName(r), name); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/compute/endpoints", http.StatusSeeOther)
}

func (h *Handler) ComputeAssignmentsCreate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	req := domain.CreateComputeAssignmentRequest{
		PrincipalID:   formString(r.Form, "principal_id"),
		PrincipalType: formString(r.Form, "principal_type"),
		IsDefault:     formBool(r.Form, "is_default"),
		FallbackLocal: formBool(r.Form, "fallback_local"),
	}
	if _, err := h.deps.ComputeEndpoint.Assign(r.Context(), principalName(r), name, req); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/compute/endpoints/"+url.PathEscape(name), http.StatusSeeOther)
}

func (h *Handler) ComputeAssignmentsDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "endpointName")
	assignmentID := chi.URLParam(r, "assignmentID")
	if err := h.deps.ComputeEndpoint.Unassign(r.Context(), principalName(r), assignmentID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/compute/endpoints/"+url.PathEscape(name), http.StatusSeeOther)
}
