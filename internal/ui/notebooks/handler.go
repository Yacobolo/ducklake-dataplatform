package notebooks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"
	"duck-demo/internal/ui/core"

	"github.com/go-chi/chi/v5"
)

type Handler struct{ deps *core.Dependencies }

func New(deps *core.Dependencies) *Handler { return &Handler{deps: deps} }

func (h *Handler) ExploreList(w http.ResponseWriter, r *http.Request) {
	principal := core.PrincipalFromContext(r.Context())
	if h.deps.NotebookExplore == nil {
		renderServiceError(w, domain.ErrNotImplemented("explore service is not configured"))
		return
	}

	pageReq := pageFromRequest(r, 30)
	selectedFolderID := strings.TrimSpace(r.URL.Query().Get("folder_id"))
	selectedKind := strings.TrimSpace(r.URL.Query().Get("kind"))
	allItems, err := h.deps.NotebookExplore.List(r.Context(), principal.Name, principal.IsAdmin, domain.ExploreFilter{
		FolderID: selectedFolderID,
		Kind:     selectedKind,
		Page:     domain.PageRequest{MaxResults: domain.MaxMaxResults},
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	folderPaths, folderItems, err := h.folderPathMap(r.Context(), principal.Name, principal.IsAdmin)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	offset := pageReq.Offset()
	if offset > len(allItems) {
		offset = len(allItems)
	}
	end := offset + pageReq.Limit()
	if end > len(allItems) {
		end = len(allItems)
	}
	items := allItems[offset:end]
	rows := make([]exploreListRow, 0, len(items)+8)
	if pageReq.Offset() == 0 && normalizeExploreKind(selectedKind) == domain.ExploreKindAll {
		for _, folderRow := range h.exploreFolderRows(folderItems, folderPaths, principal.Name, selectedFolderID) {
			rows = append(rows, folderRow)
		}
	}
	for i := range items {
		item := items[i]
		rows = append(rows, exploreListRow{
			Name:         item.Name,
			URL:          exploreItemURL(item),
			Kind:         item.Kind,
			Owner:        item.Owner,
			Scope:        item.Scope,
			Folder:       valueOrDash(folderPaths[stringValue(item.FolderID)]),
			Project:      stringValue(item.ProjectName),
			Updated:      formatTime(item.UpdatedAt),
			Shared:       item.Shared,
			ProjectBound: item.ProjectBound,
		})
	}

	core.RenderHTML(w, http.StatusOK, exploreListPage(principal, rows, h.exploreBreadcrumbItems(folderItems, selectedFolderID, pageReq, selectedKind), selectedFolderID, selectedKind, pageReq, int64(len(allItems))))
}

func (h *Handler) NotebooksList(w http.ResponseWriter, r *http.Request) {
	target := explorePageURL(pageFromRequest(r, 30), selectedExploreKind(r, domain.ExploreKindNotebook), strings.TrimSpace(r.URL.Query().Get("folder_id")))
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (h *Handler) NotebookFoldersList(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/ui/explore", http.StatusSeeOther)
}

func (h *Handler) NotebookFoldersNew(w http.ResponseWriter, r *http.Request) {
	principal := core.PrincipalFromContext(r.Context())
	selectedParentID := strings.TrimSpace(r.URL.Query().Get("parent_folder_id"))
	folders, err := h.folderOptions(r.Context(), principal.Name, principal.IsAdmin, selectedParentID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	repos, err := h.gitRepoOptions(r.Context(), principal.Name, principal.IsAdmin, "")
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, notebookFoldersNewPage(principal, folders, repos, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) NotebookFoldersCreate(w http.ResponseWriter, r *http.Request) {
	principal := core.PrincipalFromContext(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.NotebookFolders.CreateFolder(r.Context(), principal.Name, domain.CreateFolderRequest{
		Name:                 formString(r.Form, "name"),
		ParentFolderID:       formOptionalString(r.Form, "parent_folder_id"),
		GitRepoID:            formOptionalString(r.Form, "git_repo_id"),
		GitRootPath:          formOptionalString(r.Form, "git_root_path"),
		DefaultProjectID:     formOptionalString(r.Form, "default_project_id"),
		DefaultEnvironmentID: formOptionalString(r.Form, "default_environment_id"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/explore/folders", http.StatusSeeOther)
}

func (h *Handler) NotebookFoldersEdit(w http.ResponseWriter, r *http.Request) {
	folderID := chi.URLParam(r, "folderID")
	principal := core.PrincipalFromContext(r.Context())
	folder, err := h.deps.NotebookFolders.GetFolderForPrincipal(r.Context(), principal.Name, principal.IsAdmin, folderID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	selectedGitRepoID := ""
	if folder.GitRepoID != nil {
		selectedGitRepoID = *folder.GitRepoID
	}
	repos, err := h.gitRepoOptions(r.Context(), principal.Name, principal.IsAdmin, selectedGitRepoID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	parentOptions, err := h.folderOptions(r.Context(), principal.Name, principal.IsAdmin, stringValue(folder.ParentFolderID))
	if err != nil {
		renderServiceError(w, err)
		return
	}
	parentOptions = filterFolderOptions(parentOptions, folder.ID)
	shares, err := h.deps.NotebookFolders.ListFolderShares(r.Context(), principal.Name, principal.IsAdmin, folderID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, notebookFoldersEditPage(principal, folder, parentOptions, repos, folderShareRows(folderID, shares), h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) NotebookFoldersUpdate(w http.ResponseWriter, r *http.Request) {
	folderID := chi.URLParam(r, "folderID")
	principal := core.PrincipalFromContext(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.NotebookFolders.UpdateFolder(r.Context(), principal.Name, principal.IsAdmin, folderID, domain.UpdateFolderRequest{
		Name:                 formOptionalString(r.Form, "name"),
		GitRepoID:            formOptionalString(r.Form, "git_repo_id"),
		GitRootPath:          formOptionalString(r.Form, "git_root_path"),
		DefaultProjectID:     formOptionalString(r.Form, "default_project_id"),
		DefaultEnvironmentID: formOptionalString(r.Form, "default_environment_id"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/explore/folders", http.StatusSeeOther)
}

func (h *Handler) NotebookFoldersMove(w http.ResponseWriter, r *http.Request) {
	folderID := chi.URLParam(r, "folderID")
	principal := core.PrincipalFromContext(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.NotebookFolders.MoveFolder(r.Context(), principal.Name, principal.IsAdmin, folderID, domain.MoveFolderRequest{
		ParentFolderID:       formOptionalString(r.Form, "parent_folder_id"),
		ConfirmLeaveGit:      formBool(r.Form, "confirm_leave_git"),
		ConfirmContextChange: formBool(r.Form, "confirm_context_change"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/explore/folders", http.StatusSeeOther)
}

func (h *Handler) NotebookFoldersDelete(w http.ResponseWriter, r *http.Request) {
	folderID := chi.URLParam(r, "folderID")
	principal := core.PrincipalFromContext(r.Context())
	if err := h.deps.NotebookFolders.DeleteFolder(r.Context(), principal.Name, principal.IsAdmin, folderID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/explore/folders", http.StatusSeeOther)
}

func (h *Handler) NotebookFoldersShare(w http.ResponseWriter, r *http.Request) {
	folderID := chi.URLParam(r, "folderID")
	principal := core.PrincipalFromContext(r.Context())
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	_, err := h.deps.NotebookFolders.ShareFolder(r.Context(), principal.Name, principal.IsAdmin, folderID, domain.FolderShare{
		PrincipalName: formString(r.Form, "principal_name"),
		Role:          formString(r.Form, "role"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/explore/folders/"+folderID+"/edit", http.StatusSeeOther)
}

func (h *Handler) NotebookFoldersUnshare(w http.ResponseWriter, r *http.Request) {
	folderID := chi.URLParam(r, "folderID")
	targetPrincipal := chi.URLParam(r, "principalName")
	principal := core.PrincipalFromContext(r.Context())
	if err := h.deps.NotebookFolders.UnshareFolder(r.Context(), principal.Name, principal.IsAdmin, folderID, targetPrincipal); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/explore/folders/"+folderID+"/edit", http.StatusSeeOther)
}

func (h *Handler) NotebookGitReposList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, 30)
	items, total, err := h.deps.GitService.ListGitRepos(r.Context(), pageReq)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	rows := make([]gitRepoRow, 0, len(items))
	for i := range items {
		item := items[i]
		rows = append(rows, gitRepoRow{
			ID:         item.ID,
			URL:        "/ui/explore/git-repos/" + item.ID,
			Repository: item.URL,
			Branch:     item.Branch,
			Path:       valueOrDash(item.Path),
			Owner:      item.Owner,
			LastSync:   formatTimePtr(item.LastSyncAt),
		})
	}
	core.RenderHTML(w, http.StatusOK, notebookGitReposListPage(notebookGitReposListPageData{
		Principal: core.PrincipalFromContext(r.Context()),
		Rows:      rows,
		Page:      pageReq,
		Total:     total,
	}))
}

func (h *Handler) NotebookGitReposNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, notebookGitReposNewPage(core.PrincipalFromContext(r.Context()), h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) NotebookGitReposCreate(w http.ResponseWriter, r *http.Request) {
	principal := principalName(r)
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}
	repo, err := h.deps.GitService.CreateGitRepo(r.Context(), principal, domain.CreateGitRepoRequest{
		URL:       formString(r.Form, "url"),
		Branch:    formString(r.Form, "branch"),
		Path:      formString(r.Form, "path"),
		AuthToken: formString(r.Form, "auth_token"),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/explore/git-repos/"+repo.ID, http.StatusSeeOther)
}

func (h *Handler) NotebookGitReposDetail(w http.ResponseWriter, r *http.Request) {
	gitRepoID := chi.URLParam(r, "gitRepoID")
	item, err := h.deps.GitService.GetGitRepo(r.Context(), gitRepoID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, notebookGitRepoDetailPage(notebookGitRepoDetailPageData{
		Principal:     core.PrincipalFromContext(r.Context()),
		ID:            item.ID,
		URL:           item.URL,
		Branch:        item.Branch,
		Path:          valueOrDash(item.Path),
		Owner:         item.Owner,
		LastSync:      formatTimePtr(item.LastSyncAt),
		LastCommit:    strOrDash(item.LastCommit),
		DeleteURL:     "/ui/explore/git-repos/" + item.ID + "/delete",
		SyncURL:       "/ui/explore/git-repos/" + item.ID + "/sync",
		CSRFFieldFunc: h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) NotebookGitReposDelete(w http.ResponseWriter, r *http.Request) {
	gitRepoID := chi.URLParam(r, "gitRepoID")
	principal := core.PrincipalFromContext(r.Context())
	if err := h.deps.GitService.DeleteGitRepo(r.Context(), principal.Name, principal.IsAdmin, gitRepoID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/explore/git-repos", http.StatusSeeOther)
}

func (h *Handler) NotebookGitReposSync(w http.ResponseWriter, r *http.Request) {
	gitRepoID := chi.URLParam(r, "gitRepoID")
	principal := core.PrincipalFromContext(r.Context())
	result, err := h.deps.GitService.SyncGitRepo(r.Context(), principal.Name, principal.IsAdmin, gitRepoID)
	if err != nil {
		var notImplemented *domain.NotImplementedError
		if errors.As(err, &notImplemented) {
			repo, repoErr := h.deps.GitService.GetGitRepo(r.Context(), gitRepoID)
			if repoErr != nil {
				renderServiceError(w, repoErr)
				return
			}
			core.RenderHTML(w, http.StatusOK, notebookGitRepoSyncUnavailablePage(notebookGitRepoSyncUnavailablePageData{
				Principal: core.PrincipalFromContext(r.Context()),
				GitRepoID: gitRepoID,
				RepoURL:   repo.URL,
				Branch:    repo.Branch,
				Path:      valueOrDash(repo.Path),
				Message:   notImplemented.Error(),
			}))
			return
		}
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, notebookGitRepoSyncResultPage(notebookGitRepoSyncResultPageData{
		Principal: core.PrincipalFromContext(r.Context()),
		GitRepoID: gitRepoID,
		Result:    result,
	}))
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
	pageReq := pageFromRequest(r, 30)
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

func (h *Handler) folderPathMap(ctx context.Context, principal string, isAdmin bool) (map[string]string, []domain.Folder, error) {
	owners := []string{principal}
	if isAdmin {
		notebooks, _, err := h.deps.Notebook.ListNotebooks(ctx, nil, domain.PageRequest{MaxResults: domain.MaxMaxResults})
		if err != nil {
			return nil, nil, err
		}
		ownerSet := map[string]struct{}{}
		for i := range notebooks {
			if strings.TrimSpace(notebooks[i].Owner) == "" {
				continue
			}
			ownerSet[notebooks[i].Owner] = struct{}{}
		}
		owners = owners[:0]
		for owner := range ownerSet {
			owners = append(owners, owner)
		}
	}

	paths := map[string]string{}
	ordered := []domain.Folder{}
	for _, owner := range owners {
		items, err := h.deps.NotebookFolders.ListFoldersForPrincipal(ctx, principal, isAdmin, &owner)
		if err != nil {
			return nil, nil, err
		}
		ordered = append(ordered, items...)
	}
	for id, label := range folderDisplayPathMap(ordered) {
		paths[id] = label
	}
	return paths, ordered, nil
}

func folderDisplayPathMap(folders []domain.Folder) map[string]string {
	byID := make(map[string]domain.Folder, len(folders))
	for i := range folders {
		byID[folders[i].ID] = folders[i]
	}

	paths := make(map[string]string, len(folders))
	var build func(string) string
	build = func(id string) string {
		if path, ok := paths[id]; ok {
			return path
		}
		folder, ok := byID[id]
		if !ok {
			return ""
		}
		label := strings.TrimSpace(folder.Name)
		if label == "" {
			label = id
		}
		parentID := stringValue(folder.ParentFolderID)
		if parentID == "" {
			paths[id] = label
			return label
		}
		parent, ok := byID[parentID]
		if !ok {
			paths[id] = label
			return label
		}
		parentPath := build(parentID)
		if parent.SystemRole != nil && *parent.SystemRole == domain.FolderSystemRolePersonalRoot {
			paths[id] = label
			return label
		}
		if parentPath == "" {
			paths[id] = label
			return label
		}
		paths[id] = parentPath + " / " + label
		return paths[id]
	}

	for id := range byID {
		build(id)
	}
	return paths
}

func (h *Handler) gitRepoOptions(ctx context.Context, principal string, isAdmin bool, selectedID string) ([]gitRepoSelectOption, error) {
	items, _, err := h.deps.GitService.ListGitReposForPrincipal(ctx, principal, isAdmin, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		return nil, err
	}
	options := make([]gitRepoSelectOption, 0, len(items))
	for i := range items {
		item := items[i]
		label := item.URL
		if strings.TrimSpace(item.Branch) != "" {
			label += " (" + item.Branch + ")"
		}
		options = append(options, gitRepoSelectOption{
			ID:       item.ID,
			Label:    label,
			Selected: item.ID == selectedID,
		})
	}
	return options, nil
}

func (h *Handler) folderNavItems(folders []domain.Folder, folderPaths map[string]string, notebooks []domain.Notebook, selectedFolderID string) []notebookFolderNavItem {
	items := make([]notebookFolderNavItem, 0, len(folders)+1)
	allCount := len(notebooks)
	items = append(items, notebookFolderNavItem{
		Label:  "All notebooks",
		URL:    "/ui/notebooks",
		Depth:  0,
		Count:  fmt.Sprintf("%d", allCount),
		Active: selectedFolderID == "",
	})
	for i := range folders {
		folder := folders[i]
		count := 0
		folderPath := strings.TrimSpace(folderPaths[folder.ID])
		for j := range notebooks {
			notebookPath := strings.TrimSpace(folderPaths[notebooks[j].FolderID])
			if notebookPath == folderPath || strings.HasPrefix(notebookPath, folderPath+"/") {
				count++
			}
		}
		items = append(items, notebookFolderNavItem{
			Label:  folder.Name,
			URL:    notebookListPageURL(domain.DefaultMaxResults, "", folder.ID),
			Depth:  folder.Depth,
			Count:  fmt.Sprintf("%d", count),
			Active: folder.ID == selectedFolderID,
		})
	}
	return items
}

func (h *Handler) exploreFolderNavItems(folders []domain.Folder, folderPaths map[string]string, items []domain.ExploreItem, selectedFolderID string, selectedKind string) []notebookFolderNavItem {
	kind := normalizeExploreKind(selectedKind)
	nav := make([]notebookFolderNavItem, 0, len(folders)+1)
	nav = append(nav, notebookFolderNavItem{
		Label:  "All assets",
		URL:    explorePageURL(domain.PageRequest{MaxResults: domain.DefaultMaxResults}, kind, ""),
		Depth:  0,
		Count:  fmt.Sprintf("%d", len(items)),
		Active: selectedFolderID == "",
	})
	for i := range folders {
		folder := folders[i]
		count := 0
		folderPath := strings.TrimSpace(folderPaths[folder.ID])
		for j := range items {
			itemFolderPath := strings.TrimSpace(folderPaths[stringValue(items[j].FolderID)])
			if itemFolderPath == "" {
				continue
			}
			if itemFolderPath == folderPath || strings.HasPrefix(itemFolderPath, folderPath+"/") {
				count++
			}
		}
		nav = append(nav, notebookFolderNavItem{
			Label:  folder.Name,
			URL:    explorePageURL(domain.PageRequest{MaxResults: domain.DefaultMaxResults}, kind, folder.ID),
			Depth:  folder.Depth,
			Count:  fmt.Sprintf("%d", count),
			Active: folder.ID == selectedFolderID,
		})
	}
	return nav
}

func (h *Handler) exploreFolderRows(folders []domain.Folder, folderPaths map[string]string, principal, selectedFolderID string) []exploreListRow {
	visible := make([]domain.Folder, 0, len(folders))
	for i := range folders {
		folder := folders[i]
		if folder.SystemRole != nil && *folder.SystemRole == domain.FolderSystemRolePersonalRoot {
			continue
		}
		if selectedFolderID == "" {
			if folder.ParentFolderID != nil && strings.TrimSpace(*folder.ParentFolderID) != "" {
				continue
			}
		} else if stringValue(folder.ParentFolderID) != selectedFolderID {
			continue
		}
		visible = append(visible, folder)
	}

	sort.Slice(visible, func(i, j int) bool {
		return strings.ToLower(visible[i].Name) < strings.ToLower(visible[j].Name)
	})

	rows := make([]exploreListRow, 0, len(visible))
	for _, folder := range visible {
		contextParts := make([]string, 0, 3)
		if folder.DefaultProjectID != nil && strings.TrimSpace(*folder.DefaultProjectID) != "" {
			contextParts = append(contextParts, "Project "+strings.TrimSpace(*folder.DefaultProjectID))
		}
		if folder.DefaultEnvironmentID != nil && strings.TrimSpace(*folder.DefaultEnvironmentID) != "" {
			contextParts = append(contextParts, "Env "+strings.TrimSpace(*folder.DefaultEnvironmentID))
		}
		if folder.GitRepoID != nil && strings.TrimSpace(*folder.GitRepoID) != "" {
			contextParts = append(contextParts, "Git-backed")
		}
		scope := "Folder"
		if len(contextParts) > 0 {
			scope = strings.Join(contextParts, " · ")
		}
		location := "Top level"
		if parentID := stringValue(folder.ParentFolderID); parentID != "" {
			location = valueOrDash(folderPaths[parentID])
		}
		rows = append(rows, exploreListRow{
			Name:         folder.Name,
			URL:          explorePageURL(domain.PageRequest{MaxResults: domain.DefaultMaxResults}, domain.ExploreKindAll, folder.ID),
			MetaURL:      "/ui/explore/folders/" + folder.ID + "/edit",
			MetaLabel:    "Settings",
			Kind:         "folder",
			Owner:        folder.Owner,
			Scope:        scope,
			Folder:       location,
			Updated:      formatTime(folder.UpdatedAt),
			Shared:       strings.TrimSpace(folder.Owner) != strings.TrimSpace(principal),
			ProjectBound: folder.DefaultProjectID != nil && strings.TrimSpace(*folder.DefaultProjectID) != "",
		})
	}
	return rows
}

func (h *Handler) exploreBreadcrumbItems(folders []domain.Folder, selectedFolderID string, page domain.PageRequest, selectedKind string) []exploreBreadcrumbItem {
	kind := normalizeExploreKind(selectedKind)
	breadcrumbs := []exploreBreadcrumbItem{{
		Label:   "All assets",
		URL:     explorePageURL(domain.PageRequest{MaxResults: page.Limit()}, kind, ""),
		Current: selectedFolderID == "",
	}}
	if strings.TrimSpace(selectedFolderID) == "" {
		return breadcrumbs
	}

	byID := make(map[string]domain.Folder, len(folders))
	for i := range folders {
		byID[folders[i].ID] = folders[i]
	}

	chain := make([]domain.Folder, 0, 4)
	currentID := strings.TrimSpace(selectedFolderID)
	for currentID != "" {
		folder, ok := byID[currentID]
		if !ok {
			break
		}
		chain = append(chain, folder)
		currentID = stringValue(folder.ParentFolderID)
	}
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	for i := range chain {
		folder := chain[i]
		breadcrumbs = append(breadcrumbs, exploreBreadcrumbItem{
			Label:   folder.Name,
			URL:     explorePageURL(domain.PageRequest{MaxResults: page.Limit()}, kind, folder.ID),
			Current: i == len(chain)-1,
		})
	}
	return breadcrumbs
}

func selectedExploreKind(r *http.Request, fallback string) string {
	kind := strings.TrimSpace(r.URL.Query().Get("kind"))
	if kind == "" {
		return fallback
	}
	return kind
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func filterFolderOptions(options []folderSelectOption, excludeIDs ...string) []folderSelectOption {
	exclude := make(map[string]struct{}, len(excludeIDs))
	for _, id := range excludeIDs {
		if strings.TrimSpace(id) != "" {
			exclude[id] = struct{}{}
		}
	}
	filtered := make([]folderSelectOption, 0, len(options))
	for _, option := range options {
		if _, skip := exclude[option.ID]; skip {
			continue
		}
		filtered = append(filtered, option)
	}
	return filtered
}

func exploreItemURL(item domain.ExploreItem) string {
	switch item.Kind {
	case domain.ExploreKindNotebook:
		return "/ui/notebooks/" + item.ID
	case domain.ExploreKindDashboard:
		return "/ui/dashboards/" + item.ID
	case domain.ExploreKindPipeline:
		return "/ui/pipelines/" + item.Name
	case domain.ExploreKindModel:
		if item.ProjectName != nil && strings.TrimSpace(*item.ProjectName) != "" {
			return "/ui/models/" + *item.ProjectName + "/" + item.Name
		}
	case domain.ExploreKindMacro:
		return "/ui/macros/" + item.Name
	case domain.ExploreKindSemanticModel:
		if item.ProjectName != nil && strings.TrimSpace(*item.ProjectName) != "" {
			return "/ui/semantic/models/" + *item.ProjectName + "/" + item.Name
		}
	}
	return "#"
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
