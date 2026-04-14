package notebooks

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/service/query"
	"github.com/Yacobolo/quackstack/internal/ui/core"

	"github.com/go-chi/chi/v5"
)

type Handler struct {
	deps *core.Dependencies
}

func New(deps *core.Dependencies) *Handler {
	return &Handler{deps: deps}
}

func (h *Handler) NotebooksList(w http.ResponseWriter, r *http.Request) {
	query := url.Values{}
	query.Add("kind", domain.ExploreKindNotebook)
	if folderID := strings.TrimSpace(r.URL.Query().Get("folder_id")); folderID != "" {
		query.Set("folder_id", folderID)
	}
	http.Redirect(w, r, withQuery("/ui/explore", query), http.StatusSeeOther)
}

func withQuery(path string, values url.Values) string {
	if values == nil {
		return path
	}
	encoded := values.Encode()
	if encoded == "" {
		return path
	}
	return path + "?" + encoded
}

func redirectFormAlias(w http.ResponseWriter, r *http.Request, target string) {
	http.Redirect(w, r, withQuery(target, r.URL.Query()), http.StatusTemporaryRedirect)
}

func (h *Handler) NotebookFoldersList(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, withQuery("/ui/explore", r.URL.Query()), http.StatusSeeOther)
}

func (h *Handler) NotebookFoldersNew(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, withQuery("/ui/explore/folders/new", r.URL.Query()), http.StatusSeeOther)
}

func (h *Handler) NotebookFoldersCreate(w http.ResponseWriter, r *http.Request) {
	redirectFormAlias(w, r, "/ui/explore/folders")
}

func (h *Handler) NotebookFoldersEdit(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, withQuery("/ui/explore/folders/"+chi.URLParam(r, "folderID")+"/edit", r.URL.Query()), http.StatusSeeOther)
}

func (h *Handler) NotebookFoldersUpdate(w http.ResponseWriter, r *http.Request) {
	redirectFormAlias(w, r, "/ui/explore/folders/"+chi.URLParam(r, "folderID")+"/update")
}

func (h *Handler) NotebookFoldersMove(w http.ResponseWriter, r *http.Request) {
	redirectFormAlias(w, r, "/ui/explore/folders/"+chi.URLParam(r, "folderID")+"/move")
}

func (h *Handler) NotebookFoldersDelete(w http.ResponseWriter, r *http.Request) {
	redirectFormAlias(w, r, "/ui/explore/folders/"+chi.URLParam(r, "folderID")+"/delete")
}

func (h *Handler) NotebookFoldersShare(w http.ResponseWriter, r *http.Request) {
	redirectFormAlias(w, r, "/ui/explore/folders/"+chi.URLParam(r, "folderID")+"/share")
}

func (h *Handler) NotebookFoldersUnshare(w http.ResponseWriter, r *http.Request) {
	redirectFormAlias(w, r, "/ui/explore/folders/"+chi.URLParam(r, "folderID")+"/shares/"+chi.URLParam(r, "principalName")+"/delete")
}

func (h *Handler) NotebookGitReposList(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, withQuery("/ui/explore/git-repos", r.URL.Query()), http.StatusSeeOther)
}

func (h *Handler) NotebookGitReposNew(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, withQuery("/ui/explore/git-repos/new", r.URL.Query()), http.StatusSeeOther)
}

func (h *Handler) NotebookGitReposCreate(w http.ResponseWriter, r *http.Request) {
	redirectFormAlias(w, r, "/ui/explore/git-repos")
}

func (h *Handler) NotebookGitReposDetail(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, withQuery("/ui/explore/git-repos/"+chi.URLParam(r, "gitRepoID"), r.URL.Query()), http.StatusSeeOther)
}

func (h *Handler) NotebookGitReposDelete(w http.ResponseWriter, r *http.Request) {
	redirectFormAlias(w, r, "/ui/explore/git-repos/"+chi.URLParam(r, "gitRepoID")+"/delete")
}

func (h *Handler) NotebookGitReposSync(w http.ResponseWriter, r *http.Request) {
	redirectFormAlias(w, r, "/ui/explore/git-repos/"+chi.URLParam(r, "gitRepoID")+"/sync")
}

func (h *Handler) NotebooksDetail(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())
	nb, cells, err := h.deps.Notebook.GetNotebookForPrincipal(r.Context(), principal.Name, principal.IsAdmin, id)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	notebookContext, err := h.deps.Notebook.GetNotebookContext(r.Context(), principal.Name, principal.IsAdmin, id)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	shares, err := h.deps.Notebook.ListNotebookShares(r.Context(), principal.Name, principal.IsAdmin, id)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	jobs, _, _ := h.deps.SessionManager.ListJobs(r.Context(), id, domain.PageRequest{MaxResults: 20})

	cellNodes := make([]notebookCellRow, 0, len(cells))
	for i := range cells {
		cell := cells[i]
		lastResult := parseNotebookCellResult(cell.LastResult)
		var lastRunAt *time.Time
		if lastResult != nil && lastResult.ExecutedAt != nil && !lastResult.ExecutedAt.IsZero() {
			lastRunAt = lastResult.ExecutedAt
		}
		cellNodes = append(cellNodes, notebookCellRow{
			ID:          cell.ID,
			Title:       fmt.Sprintf("Cell %d", cell.Position),
			CellType:    string(cell.CellType),
			VisualSpec:  cell.VisualSpec,
			Content:     cell.Content,
			Position:    cell.Position,
			LastRunAt:   lastRunAt,
			EditURL:     "/ui/notebooks/" + id + "/cells/" + cell.ID + "/edit",
			UpdateURL:   "/ui/notebooks/" + id + "/cells/" + cell.ID + "/update",
			DeleteURL:   "/ui/notebooks/" + id + "/cells/" + cell.ID + "/delete",
			RunURL:      "/ui/notebooks/" + id + "/cells/" + cell.ID + "/run",
			MoveURL:     "/ui/notebooks/" + id + "/cells/" + cell.ID + "/move",
			DownloadURL: "/ui/notebooks/" + id + "/cells/" + cell.ID + "/download.csv",
			LastResult:  lastResult,
		})
	}

	jobRows := make([]notebookJobRow, 0, len(jobs))
	for i := range jobs {
		job := jobs[i]
		jobRows = append(jobRows, notebookJobRow{
			ID:      job.ID,
			URL:     "/ui/notebooks/" + id + "/jobs/" + job.ID,
			State:   string(job.State),
			Updated: formatTime(job.UpdatedAt),
		})
	}

	gitRepoURL := "/ui/explore/git-repos"
	if notebookContext != nil && notebookContext.EffectiveGitRepoID != nil && *notebookContext.EffectiveGitRepoID != "" {
		gitRepoURL = "/ui/explore/git-repos/" + *notebookContext.EffectiveGitRepoID
	} else if nb.GitRepoID != nil && *nb.GitRepoID != "" {
		gitRepoURL = "/ui/explore/git-repos/" + *nb.GitRepoID
	}

	selectedCatalog := strings.TrimSpace(r.URL.Query().Get("catalog"))
	selectedSchema := strings.TrimSpace(r.URL.Query().Get("schema"))
	catalogs, _, err := h.deps.CatalogRegistration.List(r.Context(), domain.PageRequest{MaxResults: 100})
	if err != nil {
		catalogs = nil
	}
	if selectedCatalog == "" && len(catalogs) > 0 {
		selectedCatalog = catalogs[0].Name
	}

	var schemas []domain.SchemaDetail
	if selectedCatalog != "" {
		s, _, err := h.deps.Catalog.ListSchemas(r.Context(), selectedCatalog, domain.PageRequest{MaxResults: 200})
		if err == nil {
			schemas = s
		}
	}
	if selectedSchema == "" && len(schemas) > 0 {
		selectedSchema = schemas[0].Name
	}

	computeReq := domain.ComputeExecutionRequest{WorkloadType: domain.ComputeWorkloadInteractive}
	computeTargets := []sqlComputeTarget{}
	if h.deps.ComputeEndpoint != nil {
		principal := principalName(r)
		if targets, err := h.deps.ComputeEndpoint.ListAvailableTargets(r.Context(), principal, domain.ComputeWorkloadInteractive); err == nil {
			computeTargets = sqlComputeTargetsFromDomain(targets)
		}
	}
	computeReq = sqlApplyDefaultComputeTarget(computeReq, computeTargets).Normalize()

	explorerCatalogs := make([]core.CatalogExplorerCatalogItem, 0, len(catalogs))
	for i := range catalogs {
		catalog := catalogs[i]
		catalogItem := core.CatalogExplorerCatalogItem{
			Name:      catalog.Name,
			URL:       notebookExplorerURL(id, catalog.Name, ""),
			Active:    catalog.Name == selectedCatalog,
			Open:      catalog.Name == selectedCatalog,
			EmptyText: "No schemas in this catalog.",
		}
		if catalog.Name == selectedCatalog {
			schemaItems := make([]core.CatalogExplorerSchemaItem, 0, len(schemas))
			for j := range schemas {
				schema := schemas[j]
				schemaItems = append(schemaItems, core.CatalogExplorerSchemaItem{
					Name:   schema.Name,
					URL:    notebookExplorerURL(id, catalog.Name, schema.Name),
					Active: schema.Name == selectedSchema,
				})
			}
			catalogItem.Schemas = schemaItems
		}
		explorerCatalogs = append(explorerCatalogs, catalogItem)
	}

	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "notebook",
		ResourceKey:  id,
		DisplayName:  nb.Name,
		ResourcePath: core.ResourceFolderPath(r.Context(), h.deps, principal, nb.Owner, nb.FolderID),
		Section:      "Build",
	})
	core.RenderHTML(w, http.StatusOK, notebookDetailPage(notebookDetailPageData{
		Principal:       principal,
		NotebookID:      id,
		Name:            nb.Name,
		Owner:           nb.Owner,
		Description:     stringPtr(nb.Description),
		Context:         notebookContext,
		SelectedCatalog: selectedCatalog,
		SelectedSchema:  selectedSchema,
		BrowserRuntime:  query.DefaultManifestBrowserRuntimeSpec(),
		ComputeTargets:  computeTargets,
		ComputeRequest:  computeReq,
		EditURL:         "/ui/notebooks/" + id + "/edit",
		MoveURL:         "/ui/notebooks/" + id + "/move",
		DuplicateURL:    "/ui/notebooks/" + id + "/duplicate",
		DeleteURL:       "/ui/notebooks/" + id + "/delete",
		ShareURL:        "/ui/notebooks/" + id + "/share",
		NewCellURL:      "/ui/notebooks/" + id + "/cells/new",
		RunAllURL:       "/ui/notebooks/" + id + "/run-all",
		RunAllAsyncURL:  "/ui/notebooks/" + id + "/run-all-async",
		ReorderURL:      "/ui/notebooks/" + id + "/cells/reorder",
		JobsURL:         "/ui/notebooks/" + id + "/jobs",
		GitRepoURL:      gitRepoURL,
		PromoteURL:      "/ui/models/promote",
		Shares:          notebookShareRows(id, shares),
		Jobs:            jobRows,
		Cells:           cellNodes,
		Explorer:        explorerCatalogs,
		CSRFFieldFunc:   h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) NotebooksShare(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.Notebook.ShareNotebook(r.Context(), principal.Name, principal.IsAdmin, notebookID, domain.NotebookShare{
		PrincipalName: formString(r.Form, "principal_name"),
		Role:          formString(r.Form, "role"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}

func (h *Handler) NotebooksUnshare(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	targetPrincipal := chi.URLParam(r, "principalName")
	principal := core.PrincipalFromContext(r.Context())
	if err := h.deps.Notebook.UnshareNotebook(r.Context(), principal.Name, principal.IsAdmin, notebookID, targetPrincipal); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}

func (h *Handler) NotebookJobsList(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	pageReq := pageFromRequest(r, defaultExplorePageSize)
	jobs, total, err := h.deps.SessionManager.ListJobs(r.Context(), notebookID, pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	rows := make([]notebookJobRow, 0, len(jobs))
	for i := range jobs {
		job := jobs[i]
		rows = append(rows, notebookJobRow{
			ID:      job.ID,
			URL:     "/ui/notebooks/" + notebookID + "/jobs/" + job.ID,
			State:   string(job.State),
			Updated: formatTime(job.UpdatedAt),
		})
	}
	core.RenderHTML(w, http.StatusOK, notebookJobsListPage(notebookJobsListPageData{
		Principal:  core.PrincipalFromContext(r.Context()),
		NotebookID: notebookID,
		Rows:       rows,
		Page:       pageReq,
		Total:      total,
	}))
}

func (h *Handler) NotebookJobsDetail(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	jobID := chi.URLParam(r, "jobID")
	job, err := h.deps.SessionManager.GetJob(r.Context(), jobID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, notebookJobDetailPage(notebookJobDetailPageData{
		Principal:  core.PrincipalFromContext(r.Context()),
		NotebookID: notebookID,
		JobID:      job.ID,
		State:      string(job.State),
		Result:     strOrDash(job.Result),
		ErrorText:  strOrDash(job.Error),
		CreatedAt:  formatTime(job.CreatedAt),
		UpdatedAt:  formatTime(job.UpdatedAt),
	}))
}

func (h *Handler) NotebooksNew(w http.ResponseWriter, r *http.Request) {
	principal := core.PrincipalFromContext(r.Context())
	selectedFolderID := strings.TrimSpace(r.URL.Query().Get("folder_id"))
	folders, err := h.folderOptions(r.Context(), principal.Name, principal.IsAdmin, selectedFolderID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, notebooksNewPage(principal, folders, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) NotebooksCreate(w http.ResponseWriter, r *http.Request) {
	principal := principalName(r)
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.Notebook.CreateNotebook(r.Context(), principal, domain.CreateNotebookRequest{
		Name:        formString(r.Form, "name"),
		Description: formOptionalString(r.Form, "description"),
		Source:      formOptionalString(r.Form, "source"),
		FolderID:    formOptionalString(r.Form, "folder_id"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks", http.StatusSeeOther)
}

func (h *Handler) NotebooksEdit(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	nb, _, err := h.deps.Notebook.GetNotebook(r.Context(), id)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, notebooksEditPage(core.PrincipalFromContext(r.Context()), id, nb, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) NotebooksUpdate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.Notebook.UpdateNotebook(r.Context(), principal.Name, principal.IsAdmin, id, domain.UpdateNotebookRequest{
		Name:        formOptionalString(r.Form, "name"),
		Description: formOptionalString(r.Form, "description"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+id, http.StatusSeeOther)
}

func (h *Handler) NotebooksMove(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())
	nb, _, err := h.deps.Notebook.GetNotebookForPrincipal(r.Context(), principal.Name, principal.IsAdmin, id)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	folders, err := h.folderOptions(r.Context(), principal.Name, principal.IsAdmin, nb.FolderID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, notebooksMovePage(principal, nb, folders, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) NotebooksMoveSubmit(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.Notebook.MoveNotebook(r.Context(), principal.Name, principal.IsAdmin, id, domain.MoveNotebookRequest{
		FolderID:             formString(r.Form, "folder_id"),
		GitPath:              formOptionalString(r.Form, "git_path"),
		ConfirmLeaveGit:      formBool(r.Form, "confirm_leave_git"),
		ConfirmContextChange: formBool(r.Form, "confirm_context_change"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+id, http.StatusSeeOther)
}

func (h *Handler) NotebooksDuplicate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())
	nb, _, err := h.deps.Notebook.GetNotebookForPrincipal(r.Context(), principal.Name, principal.IsAdmin, id)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	folders, err := h.folderOptions(r.Context(), principal.Name, principal.IsAdmin, "")
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, notebooksDuplicatePage(principal, nb, folders, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) NotebooksDuplicateSubmit(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	duplicated, err := h.deps.Notebook.DuplicateNotebook(r.Context(), principal.Name, principal.IsAdmin, id, domain.DuplicateNotebookRequest{
		FolderID: formString(r.Form, "folder_id"),
		Name:     formOptionalString(r.Form, "name"),
		GitPath:  formOptionalString(r.Form, "git_path"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+duplicated.ID, http.StatusSeeOther)
}

func (h *Handler) NotebooksDelete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())
	if err := h.deps.Notebook.DeleteNotebook(r.Context(), principal.Name, principal.IsAdmin, id); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks", http.StatusSeeOther)
}

func (h *Handler) folderOptions(ctx context.Context, principal string, isAdmin bool, selectedID string) ([]folderSelectOption, error) {
	items, err := h.deps.NotebookFolders.ListFoldersForPrincipal(ctx, principal, isAdmin, nil)
	if err != nil {
		return nil, err
	}
	options := make([]folderSelectOption, 0, len(items))
	for i := range items {
		item := items[i]
		options = append(options, folderSelectOption{
			ID:          item.ID,
			Label:       item.Name,
			Description: item.Path,
			Selected:    item.ID == selectedID,
		})
	}
	return options, nil
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func (h *Handler) NotebookCellsNew(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	core.RenderHTML(w, http.StatusOK, notebookCellsNewPage(core.PrincipalFromContext(r.Context()), id, h.deps.CSRFFieldProvider(r)))
}
func (h *Handler) NotebookCellsCreate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	pos, err := formOptionalInt(r.Form, "position")
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "position must be an integer."))
		return
	}
	_, err = h.deps.Notebook.CreateCell(r.Context(), principal.Name, principal.IsAdmin, id, domain.CreateCellRequest{
		CellType: domain.CellType(formString(r.Form, "cell_type")),
		Content:  formString(r.Form, "content"),
		Position: pos,
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+id, http.StatusSeeOther)
}
func (h *Handler) NotebookCellsEdit(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	_, cells, err := h.deps.Notebook.GetNotebook(r.Context(), notebookID)
	if err != nil {
		renderServiceError(w, err)
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
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Cell not found in notebook."))
		return
	}
	core.RenderHTML(w, http.StatusOK, notebookCellsEditPage(core.PrincipalFromContext(r.Context()), notebookID, cellID, found, h.deps.CSRFFieldProvider(r)))
}
func (h *Handler) NotebookCellsUpdate(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	principal := core.PrincipalFromContext(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	pos, err := formOptionalInt(r.Form, "position")
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "position must be an integer."))
		return
	}
	visualSpec, err := visualSpecFromForm(r.Form)
	if err != nil {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", err.Error()))
		return
	}
	_, err = h.deps.Notebook.UpdateCell(r.Context(), principal.Name, principal.IsAdmin, cellID, domain.UpdateCellRequest{
		Content:    formOptionalString(r.Form, "content"),
		VisualSpec: visualSpec,
		Position:   pos,
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}
func (h *Handler) NotebookCellsRun(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	principal := core.PrincipalFromContext(r.Context())

	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	if content := formOptionalString(r.Form, "content"); content != nil {
		if _, err := h.deps.Notebook.UpdateCell(r.Context(), principal.Name, principal.IsAdmin, cellID, domain.UpdateCellRequest{Content: content}); err != nil {
			renderServiceError(w, err)
			return
		}
	}

	ctx, err := sqlComputeContext(r.Context(), sqlComputeExecutionRequestFromForm(r.Form))
	if err != nil {
		renderServiceError(w, err)
		return
	}

	session, err := h.deps.SessionManager.CreateSession(ctx, notebookID, principal.Name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	defer func() { _ = h.deps.SessionManager.CloseSession(ctx, session.ID, principal.Name) }()

	if _, err := h.deps.SessionManager.ExecuteCell(ctx, session.ID, cellID, principal.Name); err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/notebooks/"+notebookID+"#cell-"+cellID, http.StatusSeeOther)
}
func (h *Handler) NotebookCellsMove(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	principal := core.PrincipalFromContext(r.Context())

	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	_, cells, err := h.deps.Notebook.GetNotebook(r.Context(), notebookID)
	if err != nil {
		renderServiceError(w, err)
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
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Cell not found in notebook."))
		return
	}

	swapWith := idx
	switch direction {
	case "up":
		swapWith = idx - 1
	case "down":
		swapWith = idx + 1
	default:
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "direction must be up or down."))
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

	if _, err := h.deps.Notebook.ReorderCells(r.Context(), principal.Name, principal.IsAdmin, notebookID, domain.ReorderCellsRequest{CellIDs: ids}); err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/notebooks/"+notebookID+"#cell-"+cellID, http.StatusSeeOther)
}
func (h *Handler) NotebookCellsDownloadCSV(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")

	_, cells, err := h.deps.Notebook.GetNotebook(r.Context(), notebookID)
	if err != nil {
		renderServiceError(w, err)
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
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Cell not found in notebook."))
		return
	}

	parsed := parseNotebookCellResult(found.LastResult)
	if parsed == nil || parsed.Error != "" || len(parsed.Columns) == 0 {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Export Failed", "No tabular result available for this cell."))
		return
	}

	csvBytes, err := writeCSV(parsed.Columns, parsed.Rows)
	if err != nil {
		core.RenderHTML(w, http.StatusInternalServerError, core.ErrorPage("Export Failed", "Failed generating CSV."))
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=\"notebook-"+notebookID+"-cell-"+cellID+".csv\"")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(csvBytes)
}
func (h *Handler) NotebookCellsDelete(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	cellID := chi.URLParam(r, "cellID")
	principal := core.PrincipalFromContext(r.Context())
	if err := h.deps.Notebook.DeleteCell(r.Context(), principal.Name, principal.IsAdmin, cellID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}
func (h *Handler) NotebookCellsReorder(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	cellIDs := r.Form["cell_ids"]
	if len(cellIDs) == 1 {
		cellIDs = formCSV(r.Form, "cell_ids")
	}
	if _, err := h.deps.Notebook.ReorderCells(r.Context(), principal.Name, principal.IsAdmin, notebookID, domain.ReorderCellsRequest{CellIDs: cellIDs}); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}
func (h *Handler) NotebookRunAll(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())

	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	ctx, err := sqlComputeContext(r.Context(), sqlComputeExecutionRequestFromForm(r.Form))
	if err != nil {
		renderServiceError(w, err)
		return
	}
	execReq, _ := domain.ComputeExecutionRequestFromContext(ctx)
	if strings.EqualFold(execReq.Mode, domain.ComputeModeByocLocal) {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Browser-local BYOC is only supported for single interactive SQL cell runs. Use Shared Endpoint or Auto for Run all."))
		return
	}

	session, err := h.deps.SessionManager.CreateSession(ctx, notebookID, principal.Name)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	defer func() { _ = h.deps.SessionManager.CloseSession(ctx, session.ID, principal.Name) }()

	if _, err := h.deps.SessionManager.RunAll(ctx, session.ID, principal.Name); err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/notebooks/"+notebookID, http.StatusSeeOther)
}
func (h *Handler) NotebookRunAllAsync(w http.ResponseWriter, r *http.Request) {
	notebookID := chi.URLParam(r, "notebookID")
	principal := core.PrincipalFromContext(r.Context())

	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	ctx, err := sqlComputeContext(r.Context(), sqlComputeExecutionRequestFromForm(r.Form))
	if err != nil {
		renderServiceError(w, err)
		return
	}
	execReq, _ := domain.ComputeExecutionRequestFromContext(ctx)
	if strings.EqualFold(execReq.Mode, domain.ComputeModeByocLocal) {
		core.RenderHTML(w, http.StatusBadRequest, core.ErrorPage("Invalid Request", "Browser-local BYOC is only supported for single interactive SQL cell runs. Use Shared Endpoint or Auto for async notebook runs."))
		return
	}

	session, err := h.deps.SessionManager.CreateSession(ctx, notebookID, principal.Name)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	job, err := h.deps.SessionManager.RunAllAsync(ctx, session.ID, principal.Name)
	if err != nil {
		_ = h.deps.SessionManager.CloseSession(ctx, session.ID, principal.Name)
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, "/ui/notebooks/"+notebookID+"/jobs/"+job.ID, http.StatusSeeOther)
}

func (h *Handler) NotebookRuntimeManifest(w http.ResponseWriter, r *http.Request) {
	if h.deps.Manifest == nil {
		writeJSONError(w, http.StatusInternalServerError, "manifest service is not configured")
		return
	}

	catalogName := strings.TrimSpace(r.URL.Query().Get("catalog"))
	schemaName := strings.TrimSpace(r.URL.Query().Get("schema"))
	tableName := strings.TrimSpace(r.URL.Query().Get("table"))
	if schemaName == "" {
		schemaName = "main"
	}
	if tableName == "" {
		writeJSONError(w, http.StatusBadRequest, "table is required")
		return
	}

	principal := principalName(r)
	result, err := h.deps.Manifest.GetManifest(r.Context(), principal, catalogName, schemaName, tableName)
	if err != nil {
		status, message := core.ServiceErrorStatus(err)
		writeJSONError(w, status, message)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(result)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func principalName(r *http.Request) string {
	principal := core.PrincipalFromContext(r.Context())
	if principal.Name == "" {
		return "unknown"
	}
	return principal.Name
}
