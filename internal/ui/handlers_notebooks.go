package ui

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

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
		cellNodes = append(cellNodes, notebookCellRowData{
			ID:           cell.ID,
			Title:        fmt.Sprintf("Cell %d", cell.Position),
			CellType:     string(cell.CellType),
			Content:      cell.Content,
			Position:     cell.Position,
			EditURL:      "/ui/notebooks/" + id + "/cells/" + cell.ID + "/edit",
			UpdateURL:    "/ui/notebooks/" + id + "/cells/" + cell.ID + "/update",
			DeleteURL:    "/ui/notebooks/" + id + "/cells/" + cell.ID + "/delete",
			RunURL:       "/ui/notebooks/" + id + "/cells/" + cell.ID + "/run",
			MoveURL:      "/ui/notebooks/" + id + "/cells/" + cell.ID + "/move",
			DownloadURL:  "/ui/notebooks/" + id + "/cells/" + cell.ID + "/download.csv",
			OpenInSQLURL: "/ui/sql?sql=" + url.QueryEscape(cell.Content),
			LastResult:   parseNotebookCellResult(cell.LastResult),
		})
	}

	jobRows := make([]notebookJobRowData, 0, len(jobs))
	for i := range jobs {
		job := jobs[i]
		jobRows = append(jobRows, notebookJobRowData{ID: job.ID, State: string(job.State), Updated: formatTime(job.UpdatedAt)})
	}
	renderHTML(w, http.StatusOK, notebookDetailPage(notebookDetailPageData{
		Principal:     principalFromContext(r.Context()),
		NotebookID:    id,
		Name:          nb.Name,
		Owner:         nb.Owner,
		Description:   stringPtr(nb.Description),
		EditURL:       "/ui/notebooks/" + id + "/edit",
		DeleteURL:     "/ui/notebooks/" + id + "/delete",
		NewCellURL:    "/ui/notebooks/" + id + "/cells/new",
		RunAllURL:     "/ui/notebooks/" + id + "/run-all",
		ReorderURL:    "/ui/notebooks/" + id + "/cells/reorder",
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

func (h *Handler) NotebookCellsRun(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	principal, isAdmin := principalLabel(r.Context())

	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	if content := formOptionalString(r.Form, "content"); content != nil {
		if _, err := h.Notebook.UpdateCell(r.Context(), principal, isAdmin, cellID, domain.UpdateCellRequest{Content: content}); err != nil {
			h.renderServiceError(w, r, err)
			return
		}
	}

	session, err := h.SessionManager.CreateSession(r.Context(), notebookID, principal)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	defer func() { _ = h.SessionManager.CloseSession(r.Context(), session.ID, principal) }()

	if _, err := h.SessionManager.ExecuteCell(r.Context(), session.ID, cellID, principal); err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	http.Redirect(w, r, "/ui/notebooks/"+notebookID+"#cell-"+cellID, http.StatusSeeOther)
}

func (h *Handler) NotebookRunAll(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	principal, _ := principalLabel(r.Context())

	session, err := h.SessionManager.CreateSession(r.Context(), notebookID, principal)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}
	defer func() { _ = h.SessionManager.CloseSession(r.Context(), session.ID, principal) }()

	if _, err := h.SessionManager.RunAll(r.Context(), session.ID, principal); err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}

func (h *Handler) NotebookCellsMove(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	principal, isAdmin := principalLabel(r.Context())

	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	_, cells, err := h.Notebook.GetNotebook(r.Context(), notebookID)
	if err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	direction := formString(r.Form, "direction")
	idx := -1
	for i := range cells {
		if cells[i].ID == cellID {
			idx = i
			break
		}
	}
	if idx == -1 {
		renderHTML(w, http.StatusNotFound, errorPage("Not Found", "Cell not found in notebook."))
		return
	}

	swapWith := idx
	switch direction {
	case "up":
		swapWith = idx - 1
	case "down":
		swapWith = idx + 1
	default:
		renderHTML(w, http.StatusBadRequest, errorPage("Invalid Request", "direction must be up or down."))
		return
	}

	if swapWith < 0 || swapWith >= len(cells) {
		http.Redirect(w, r, "/ui/notebooks/"+notebookID+"#cell-"+cellID, http.StatusSeeOther)
		return
	}

	cells[idx], cells[swapWith] = cells[swapWith], cells[idx]
	ids := make([]string, 0, len(cells))
	for i := range cells {
		ids = append(ids, cells[i].ID)
	}

	if _, err := h.Notebook.ReorderCells(r.Context(), principal, isAdmin, notebookID, domain.ReorderCellsRequest{CellIDs: ids}); err != nil {
		h.renderServiceError(w, r, err)
		return
	}

	http.Redirect(w, r, "/ui/notebooks/"+notebookID+"#cell-"+cellID, http.StatusSeeOther)
}

func (h *Handler) NotebookCellsDownloadCSV(w http.ResponseWriter, r *http.Request) {
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

	parsed := parseNotebookCellResult(found.LastResult)
	if parsed == nil || parsed.Error != "" || len(parsed.Columns) == 0 {
		renderHTML(w, http.StatusBadRequest, errorPage("Export Failed", "No tabular result available for this cell."))
		return
	}

	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	if err := writer.Write(parsed.Columns); err != nil {
		renderHTML(w, http.StatusInternalServerError, errorPage("Export Failed", "Failed writing CSV header."))
		return
	}

	for i := range parsed.Rows {
		if err := writer.Write(parsed.Rows[i]); err != nil {
			renderHTML(w, http.StatusInternalServerError, errorPage("Export Failed", "Failed writing CSV rows."))
			return
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		renderHTML(w, http.StatusInternalServerError, errorPage("Export Failed", "Failed finalizing CSV."))
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", "notebook-"+notebookID+"-cell-"+cellID+".csv"))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
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

type persistedNotebookCellResult struct {
	Columns  []string        `json:"Columns"`
	Rows     [][]interface{} `json:"Rows"`
	RowCount int             `json:"RowCount"`
	Error    *string         `json:"Error"`
	Duration time.Duration   `json:"Duration"`
}

func parseNotebookCellResult(raw *string) *notebookCellResultData {
	if raw == nil || *raw == "" {
		return nil
	}

	var parsed persistedNotebookCellResult
	if err := json.Unmarshal([]byte(*raw), &parsed); err != nil {
		return &notebookCellResultData{Error: "Unable to parse cached result."}
	}

	rows := make([][]string, 0, len(parsed.Rows))
	for i := range parsed.Rows {
		cells := make([]string, 0, len(parsed.Rows[i]))
		for j := range parsed.Rows[i] {
			cells = append(cells, sqlCellString(parsed.Rows[i][j]))
		}
		rows = append(rows, cells)
	}

	out := &notebookCellResultData{
		Columns:  parsed.Columns,
		Rows:     rows,
		RowCount: parsed.RowCount,
		Duration: parsed.Duration,
	}
	if parsed.Error != nil {
		out.Error = *parsed.Error
	}
	return out
}
