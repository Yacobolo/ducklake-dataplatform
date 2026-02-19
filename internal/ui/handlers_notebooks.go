package ui

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"duck-demo/internal/domain"
)

func (h *Handler) NotebooksList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.Notebook.ListNotebooks(r.Context(), nil, pageReq)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	rows := make([]notebooksListRowData, 0, len(items))
	for i := range items {
		n := items[i]
		rows = append(rows, notebooksListRowData{Filter: n.Name + " " + n.Owner, Name: n.Name, URL: "/ui/notebooks/" + n.ID, Owner: n.Owner, Updated: formatTime(n.UpdatedAt)})
	}
	renderHTML(w, http.StatusOK, notebooksListPage(principalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) NotebooksDetail(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	nb, cells, err := h.Notebook.GetNotebook(r.Context(), id)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	jobs, _, _ := h.SessionManager.ListJobs(r.Context(), id, domain.PageRequest{MaxResults: 20})

	cellNodes := make([]notebookCellRowData, 0, len(cells))
	for i := range cells {
		cell := cells[i]
		cellNodes = append(cellNodes, notebookCellRowData{Title: fmt.Sprintf("Cell %d (%s)", cell.Position, cell.CellType), Content: cell.Content, EditURL: "/ui/notebooks/" + id + "/cells/" + cell.ID + "/edit", DeleteURL: "/ui/notebooks/" + id + "/cells/" + cell.ID + "/delete"})
	}

	jobRows := make([]notebookJobRowData, 0, len(jobs))
	for i := range jobs {
		job := jobs[i]
		jobRows = append(jobRows, notebookJobRowData{ID: job.ID, State: string(job.State), Updated: formatTime(job.UpdatedAt)})
	}
	renderHTML(w, http.StatusOK, notebookDetailPage(notebookDetailPageData{
		Principal:     principalFromContext(r.Context()),
		Name:          nb.Name,
		Owner:         nb.Owner,
		Description:   stringPtr(nb.Description),
		EditURL:       "/ui/notebooks/" + id + "/edit",
		DeleteURL:     "/ui/notebooks/" + id + "/delete",
		NewCellURL:    "/ui/notebooks/" + id + "/cells/new",
		Jobs:          jobRows,
		Cells:         cellNodes,
		CSRFFieldFunc: csrfFieldProvider(r),
	}))
}

func (h *Handler) NotebooksNew(w http.ResponseWriter, r *http.Request) {
	renderHTML(w, http.StatusOK, notebooksNewPage(principalFromContext(r.Context()), csrfFieldProvider(r)))
}

func (h *Handler) NotebooksCreate(w http.ResponseWriter, r *http.Request) {
	principal, _ := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.Notebook.CreateNotebook(r.Context(), principal, domain.CreateNotebookRequest{
		Name:        formString(r.Form, "name"),
		Description: formOptionalString(r.Form, "description"),
		Source:      formOptionalString(r.Form, "source"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks", http.StatusSeeOther)
}

func (h *Handler) NotebooksEdit(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	nb, _, err := h.Notebook.GetNotebook(r.Context(), id)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	renderHTML(w, http.StatusOK, notebooksEditPage(principalFromContext(r.Context()), id, nb, csrfFieldProvider(r)))
}

func (h *Handler) NotebooksUpdate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal, isAdmin := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.Notebook.UpdateNotebook(r.Context(), principal, isAdmin, id, domain.UpdateNotebookRequest{
		Name:        formOptionalString(r.Form, "name"),
		Description: formOptionalString(r.Form, "description"),
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+id, http.StatusSeeOther)
}

func (h *Handler) NotebooksDelete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal, isAdmin := principalLabel(r.Context())
	if err := h.Notebook.DeleteNotebook(r.Context(), principal, isAdmin, id); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks", http.StatusSeeOther)
}

func (h *Handler) NotebookCellsNew(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	renderHTML(w, http.StatusOK, notebookCellsNewPage(principalFromContext(r.Context()), id, csrfFieldProvider(r)))
}

func (h *Handler) NotebookCellsCreate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal, isAdmin := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	pos, err := formOptionalInt(r.Form, "position")
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "position must be an integer."))
		return
	}
	_, err = h.Notebook.CreateCell(r.Context(), principal, isAdmin, id, domain.CreateCellRequest{
		CellType: domain.CellType(formString(r.Form, "cell_type")),
		Content:  formString(r.Form, "content"),
		Position: pos,
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+id, http.StatusSeeOther)
}

func (h *Handler) NotebookCellsEdit(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	_, cells, err := h.Notebook.GetNotebook(r.Context(), notebookID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	var found *domain.Cell
	for i := range cells {
		if cells[i].ID == cellID {
			found = &cells[i]
			break
		}
	}
	if found == nil {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Cell not found in notebook."))
		return
	}
	renderHTML(w, http.StatusOK, notebookCellsEditPage(principalFromContext(r.Context()), notebookID, cellID, found, csrfFieldProvider(r)))
}

func (h *Handler) NotebookCellsUpdate(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	principal, isAdmin := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	pos, err := formOptionalInt(r.Form, "position")
	if err != nil {
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "position must be an integer."))
		return
	}
	_, err = h.Notebook.UpdateCell(r.Context(), principal, isAdmin, cellID, domain.UpdateCellRequest{
		Content:  formOptionalString(r.Form, "content"),
		Position: pos,
	})
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}

func (h *Handler) NotebookCellsDelete(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	principal, isAdmin := principalLabel(r.Context())
	if err := h.Notebook.DeleteCell(r.Context(), principal, isAdmin, cellID); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}

func (h *Handler) NotebookCellsReorder(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	principal, isAdmin := principalLabel(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	cellIDs := r.Form["cell_ids"]
	if len(cellIDs) == 1 {
		cellIDs = formCSV(r.Form, "cell_ids")
	}
	if _, err := h.Notebook.ReorderCells(r.Context(), principal, isAdmin, notebookID, domain.ReorderCellsRequest{CellIDs: cellIDs}); err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}
