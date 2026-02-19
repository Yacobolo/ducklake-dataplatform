package ui

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
)

func (h *Handler) MacrosList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.Macro.List(r.Context(), pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]macrosListRowData, 0, len(items))
	for i := range items {
		m := items[i]
		rows = append(rows, macrosListRowData{Filter: m.Name + " " + m.Visibility, Name: m.Name, URL: "/ui/macros/" + m.Name, Type: m.MacroType, Visibility: m.Visibility, Status: m.Status})
	}
	renderHTML(w, http.StatusOK, macrosListPage(principalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) MacrosDetail(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	m, err := h.Macro.Get(r.Context(), name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	revisions, _ := h.Macro.ListRevisions(r.Context(), name)

	revRows := make([]macroRevisionRowData, 0, len(revisions))
	for i := range revisions {
		rev := revisions[i]
		revRows = append(revRows, macroRevisionRowData{Version: strconv.Itoa(rev.Version), Status: rev.Status, CreatedBy: rev.CreatedBy, Created: formatTime(rev.CreatedAt)})
	}
	renderHTML(w, http.StatusOK, macroDetailPage(macroDetailPageData{
		Principal:     principalFromContext(r.Context()),
		Name:          m.Name,
		Type:          m.MacroType,
		Visibility:    m.Visibility,
		Status:        m.Status,
		Owner:         m.Owner,
		EditURL:       "/ui/macros/" + name + "/edit",
		DeleteURL:     "/ui/macros/" + name + "/delete",
		Definition:    m.Body,
		Revisions:     revRows,
		CSRFFieldFunc: csrfFieldProvider(r),
	}))
}

func (h *Handler) MacrosNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, macrosNewPage(principalFromContext(r.Context()), csrfFieldProvider(r)))
}

func (h *Handler) MacrosCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.Macro.Create(r.Context(), principal, domain.CreateMacroRequest{
		Name:        formString(r.Form, "name"),
		MacroType:   formString(r.Form, "macro_type"),
		Visibility:  formString(r.Form, "visibility"),
		Description: formString(r.Form, "description"),
		Parameters:  formCSV(r.Form, "parameters"),
		Body:        formString(r.Form, "body"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/macros", http.StatusSeeOther)
}

func (h *Handler) MacrosEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	m, err := h.Macro.Get(r.Context(), name)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, macrosEditPage(principalFromContext(r.Context()), name, m, csrfFieldProvider(r)))
}

func (h *Handler) MacrosUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	body := formString(r.Form, "body")
	description := formString(r.Form, "description")
	visibility := formString(r.Form, "visibility")
	status := formString(r.Form, "status")
	_, err := h.Macro.Update(r.Context(), principal, name, domain.UpdateMacroRequest{
		Body:        &body,
		Description: &description,
		Visibility:  &visibility,
		Status:      &status,
		Parameters:  formCSV(r.Form, "parameters"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/macros/"+name, http.StatusSeeOther)
}

func (h *Handler) MacrosDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	principal, _ := principalLabel(r.Context())
	if err := h.Macro.Delete(r.Context(), principal, name); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/macros", http.StatusSeeOther)
}
