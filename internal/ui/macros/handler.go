package macros

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/ui/core"
)

type Handler struct{ deps *core.Dependencies }

func New(deps *core.Dependencies) *Handler { return &Handler{deps: deps} }

func (h *Handler) MacrosList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.deps.Macro.List(r.Context(), pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	rows := make([]macrosListRowData, 0, len(items))
	for i := range items {
		m := items[i]
		rows = append(rows, macrosListRowData{
			Name:       m.Name,
			URL:        "/ui/macros/" + m.Name,
			Type:       m.MacroType,
			Visibility: m.Visibility,
			Status:     m.Status,
		})
	}
	core.RenderHTML(w, http.StatusOK, macrosListPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) MacrosDiff(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	revisions, err := h.deps.Macro.ListRevisions(r.Context(), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	if len(revisions) < 2 {
		core.RenderHTML(w, http.StatusOK, macroDiffPage(macroDiffPageData{
			Principal: core.PrincipalFromContext(r.Context()),
			Name:      name,
		}))
		return
	}

	fromVersion := revisions[len(revisions)-1].Version
	toVersion := revisions[0].Version
	if raw := r.URL.Query().Get("from"); raw != "" {
		if parsed, convErr := strconv.Atoi(raw); convErr == nil {
			fromVersion = parsed
		}
	}
	if raw := r.URL.Query().Get("to"); raw != "" {
		if parsed, convErr := strconv.Atoi(raw); convErr == nil {
			toVersion = parsed
		}
	}

	diff, err := h.deps.Macro.DiffRevisions(r.Context(), name, fromVersion, toVersion)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	fromRevision, err := h.deps.Macro.GetRevisionByVersion(r.Context(), name, fromVersion)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	toRevision, err := h.deps.Macro.GetRevisionByVersion(r.Context(), name, toVersion)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	addedImpact, removedImpact, unchangedImpact, err := macroImpactDelta(r.Context(), h, name, &fromRevision.CreatedAt, &toRevision.CreatedAt)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	revisionOptions := make([]macroRevisionOptionData, 0, len(revisions))
	for i := range revisions {
		revisionOptions = append(revisionOptions, macroRevisionOptionData{
			Value: strconv.Itoa(revisions[i].Version),
			Label: "v" + strconv.Itoa(revisions[i].Version) + " • " + formatTime(revisions[i].CreatedAt),
		})
	}

	core.RenderHTML(w, http.StatusOK, macroDiffPage(macroDiffPageData{
		Principal:       core.PrincipalFromContext(r.Context()),
		Name:            name,
		FromVersion:     fromVersion,
		ToVersion:       toVersion,
		RevisionOptions: revisionOptions,
		Diff:            diff,
		ImpactAdded:     addedImpact,
		ImpactRemoved:   removedImpact,
		ImpactUnchanged: unchangedImpact,
	}))
}

func (h *Handler) MacrosImpact(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	if _, err := h.deps.Macro.Get(r.Context(), name); err != nil {
		renderServiceError(w, err)
		return
	}
	rows, err := listMacroImpactAsOf(r.Context(), h, name, nil)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, macroImpactPage(macroImpactPageData{
		Principal: core.PrincipalFromContext(r.Context()),
		Name:      name,
		Rows:      rows,
	}))
}

func (h *Handler) MacrosDetail(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	m, err := h.deps.Macro.Get(r.Context(), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	revisions, _ := h.deps.Macro.ListRevisions(r.Context(), name)

	revRows := make([]macroRevisionRowData, 0, len(revisions))
	for i := range revisions {
		rev := revisions[i]
		revRows = append(revRows, macroRevisionRowData{
			Version:   strconv.Itoa(rev.Version),
			Status:    rev.Status,
			CreatedBy: rev.CreatedBy,
			Created:   formatTime(rev.CreatedAt),
		})
	}

	core.RenderHTML(w, http.StatusOK, macroDetailPage(macroDetailPageData{
		Principal:     core.PrincipalFromContext(r.Context()),
		Name:          m.Name,
		Type:          m.MacroType,
		Visibility:    m.Visibility,
		Status:        m.Status,
		Owner:         m.Owner,
		EditURL:       "/ui/macros/" + name + "/edit",
		DiffURL:       "/ui/macros/" + name + "/diff",
		ImpactURL:     "/ui/macros/" + name + "/impact",
		DeleteURL:     "/ui/macros/" + name + "/delete",
		Definition:    m.Body,
		Revisions:     revRows,
		CSRFFieldFunc: h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) MacrosNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, macrosNewPage(core.PrincipalFromContext(r.Context()), h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) MacrosCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.Macro.Create(r.Context(), principalName(r), domain.CreateMacroRequest{
		Name:        formString(r.Form, "name"),
		MacroType:   formString(r.Form, "macro_type"),
		Visibility:  formString(r.Form, "visibility"),
		Description: formString(r.Form, "description"),
		Parameters:  formCSV(r.Form, "parameters"),
		Body:        formString(r.Form, "body"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/macros", http.StatusSeeOther)
}

func (h *Handler) MacrosEdit(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	m, err := h.deps.Macro.Get(r.Context(), name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, macrosEditPage(core.PrincipalFromContext(r.Context()), name, m, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) MacrosUpdate(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	body := formString(r.Form, "body")
	description := formString(r.Form, "description")
	visibility := formString(r.Form, "visibility")
	status := formString(r.Form, "status")
	_, err := h.deps.Macro.Update(r.Context(), principalName(r), name, domain.UpdateMacroRequest{
		Body:        &body,
		Description: &description,
		Visibility:  &visibility,
		Status:      &status,
		Parameters:  formCSV(r.Form, "parameters"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/macros/"+name, http.StatusSeeOther)
}

func (h *Handler) MacrosDelete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "macroName")
	if err := h.deps.Macro.Delete(r.Context(), principalName(r), name); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/macros", http.StatusSeeOther)
}
