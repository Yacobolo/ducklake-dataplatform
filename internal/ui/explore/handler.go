package explore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	"github.com/go-chi/chi/v5"
	"github.com/starfederation/datastar-go/datastar"
	g "maragu.dev/gomponents"
)

type Handler struct {
	deps           *core.Dependencies
	exploreUpdates *exploreUpdateHub
}

func New(deps *core.Dependencies) *Handler {
	return &Handler{
		deps:           deps,
		exploreUpdates: newExploreUpdateHub(),
	}
}

type viewData struct {
	Principal        domain.ContextPrincipal
	Rows             []listRow
	Breadcrumbs      []breadcrumbItem
	AsideItems       []core.ExploreNavigatorItem
	SelectedFolderID string
	SelectedKinds    []string
	SelectedOwners   []string
	SearchQuery      string
	OwnerOptions     []string
	StreamID         string
	CSRFToken        string
	Page             domain.PageRequest
	Total            int64
}

func (h *Handler) ExploreList(w http.ResponseWriter, r *http.Request) {
	view, err := h.buildViewData(r, domain.NewID())
	if err != nil {
		renderServiceError(w, err)
		return
	}
	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "explore",
		DisplayName:  "Explore",
		Section:      "Discover",
	})
	core.RenderHTML(w, http.StatusOK, listPage(
		view.Principal,
		view.Rows,
		view.Breadcrumbs,
		view.AsideItems,
		view.SelectedFolderID,
		view.SelectedKinds,
		view.SelectedOwners,
		view.SearchQuery,
		view.OwnerOptions,
		view.StreamID,
		view.CSRFToken,
		view.Page,
		view.Total,
	))
}

func (h *Handler) ExploreFragment(w http.ResponseWriter, r *http.Request) {
	view, err := h.buildViewData(r, "")
	if err != nil {
		renderServiceError(w, err)
		return
	}
	if err := writeDatastarElementPatch(w, r, mainContent(
		view.Rows,
		view.Breadcrumbs,
		view.AsideItems,
		view.SelectedFolderID,
		view.SelectedKinds,
		view.SelectedOwners,
		view.SearchQuery,
		view.OwnerOptions,
		view.StreamID,
		view.CSRFToken,
		view.Page,
		view.Total,
	)); err != nil {
		renderServiceError(w, fmt.Errorf("render explore fragment: %w", err))
		return
	}
}

func (h *Handler) ExploreUpdatesStream(w http.ResponseWriter, r *http.Request) {
	streamID := strings.TrimSpace(chi.URLParam(r, "streamID"))
	if streamID == "" {
		http.Error(w, "missing stream id", http.StatusBadRequest)
		return
	}

	ch, unsubscribe := h.exploreUpdates.subscribe(streamID)
	defer unsubscribe()

	sse := datastar.NewSSE(w, r)
	principal := core.PrincipalFromContext(r.Context())

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := sse.Send(datastar.EventTypePatchSignals, []string{"signals {}"}); err != nil {
				return
			}
		case update := <-ch:
			view, err := h.buildViewDataForFilter(r.Context(), principal, domain.PageRequest{MaxResults: defaultPageSize}, update.FolderID, update.Kinds, update.Owners, update.Query, streamID)
			if err != nil {
				_ = sse.ConsoleError(err)
				return
			}
			if err := sse.PatchElementGostar(
				mainContent(
					view.Rows,
					view.Breadcrumbs,
					view.AsideItems,
					view.SelectedFolderID,
					view.SelectedKinds,
					view.SelectedOwners,
					view.SearchQuery,
					view.OwnerOptions,
					streamID,
					h.deps.CSRFToken(r),
					view.Page,
					view.Total,
				),
				datastar.WithSelectorID("main-content"),
			); err != nil {
				return
			}
		}
	}
}

func (h *Handler) ExploreUpdatesApply(w http.ResponseWriter, r *http.Request) {
	streamID := strings.TrimSpace(chi.URLParam(r, "streamID"))
	if streamID == "" {
		http.Error(w, "missing stream id", http.StatusBadRequest)
		return
	}
	params, err := decodeUpdateParams(r)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	h.exploreUpdates.publish(streamID, params)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) buildViewData(r *http.Request, streamID string) (viewData, error) {
	principal := core.PrincipalFromContext(r.Context())
	if h.deps.Explore == nil {
		return viewData{}, domain.ErrNotImplemented("explore service is not configured")
	}
	pageReq := pageFromRequest(r, defaultPageSize)
	selectedFolderID := strings.TrimSpace(r.URL.Query().Get("folder_id"))
	selectedKinds := selectedKindsFromRequest(r)
	selectedOwners := selectedOwnersFromRequest(r)
	searchQuery := selectedQueryFromRequest(r)
	view, err := h.buildViewDataForFilter(r.Context(), principal, pageReq, selectedFolderID, selectedKinds, selectedOwners, searchQuery, streamID)
	if err != nil {
		return viewData{}, err
	}
	view.CSRFToken = h.deps.CSRFToken(r)
	return view, nil
}

func (h *Handler) buildViewDataForFilter(ctx context.Context, principal domain.ContextPrincipal, pageReq domain.PageRequest, selectedFolderID string, selectedKinds []string, selectedOwners []string, searchQuery string, streamID string) (viewData, error) {
	allItems, err := h.deps.Explore.List(ctx, principal.Name, principal.IsAdmin, domain.ExploreFilter{
		FolderID: selectedFolderID,
		Kinds:    selectedKinds,
		Owners:   selectedOwners,
		Query:    searchQuery,
		Page:     domain.PageRequest{MaxResults: domain.MaxMaxResults},
	})
	if err != nil {
		return viewData{}, err
	}
	folderPaths, folderItems, err := h.folderPathMap(ctx, principal.Name, principal.IsAdmin)
	if err != nil {
		return viewData{}, err
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
	rows := make([]listRow, 0, len(items)+8)
	if pageReq.Offset() == 0 && folderKindAllowed(selectedKinds) {
		folderRows, err := h.folderRows(ctx, folderItems, folderPaths, principal.Name, principal.IsAdmin, selectedFolderID, selectedOwners, searchQuery)
		if err != nil {
			return viewData{}, err
		}
		rows = append(rows, folderRows...)
	}
	for i := range items {
		item := items[i]
		rows = append(rows, listRow{
			Name:         item.Name,
			URL:          itemURL(item),
			Kind:         item.Kind,
			Owner:        item.Owner,
			Scope:        item.Scope,
			Folder:       valueOrDash(folderPaths[stringValue(item.FolderID)]),
			Project:      stringValue(item.ProjectName),
			Updated:      formatTime(item.UpdatedAt),
			Shared:       item.Shared,
			ProjectBound: item.ProjectBound,
			GitBacked:    item.GitRepoID != nil && strings.TrimSpace(*item.GitRepoID) != "",
		})
	}

	asideItems, err := h.buildAsideItems(ctx, principal, folderItems, allItems, selectedFolderID, pageReq, selectedKinds, selectedOwners, searchQuery)
	if err != nil {
		return viewData{}, err
	}

	return viewData{
		Principal:        principal,
		Rows:             rows,
		Breadcrumbs:      breadcrumbItems(folderItems, selectedFolderID, pageReq, selectedKinds, selectedOwners, searchQuery),
		AsideItems:       asideItems,
		SelectedFolderID: selectedFolderID,
		SelectedKinds:    selectedKinds,
		SelectedOwners:   selectedOwners,
		SearchQuery:      searchQuery,
		OwnerOptions:     filterOwners(folderItems, allItems),
		StreamID:         streamID,
		Page:             pageReq,
		Total:            int64(len(allItems)),
	}, nil
}

func (h *Handler) buildAsideItems(ctx context.Context, principal domain.ContextPrincipal, folders []domain.Folder, currentItems []domain.ExploreItem, selectedFolderID string, page domain.PageRequest, selectedKinds []string, selectedOwners []string, searchQuery string) ([]core.ExploreNavigatorItem, error) {
	folderScopedItems, err := h.listDirectFolderAssets(ctx, principal, folders)
	if err != nil {
		return nil, err
	}
	mergedItems := make([]domain.ExploreItem, 0, len(folderScopedItems)+len(currentItems))
	mergedItems = append(mergedItems, folderScopedItems...)
	mergedItems = append(mergedItems, currentItems...)
	return buildExploreAsideItems(folders, mergedItems, selectedFolderID, page, selectedKinds, selectedOwners, searchQuery), nil
}

func (h *Handler) listDirectFolderAssets(ctx context.Context, principal domain.ContextPrincipal, folders []domain.Folder) ([]domain.ExploreItem, error) {
	seen := map[string]struct{}{}
	items := make([]domain.ExploreItem, 0, len(folders)*2)

	for i := range folders {
		folder := folders[i]
		folderItems, err := h.deps.Explore.List(ctx, principal.Name, principal.IsAdmin, domain.ExploreFilter{
			FolderID: folder.ID,
			Page:     domain.PageRequest{MaxResults: domain.MaxMaxResults},
		})
		if err != nil {
			return nil, fmt.Errorf("list direct folder assets for %s: %w", folder.ID, err)
		}

		for j := range folderItems {
			item := folderItems[j]
			folderID := stringValue(item.FolderID)
			if folderID == "" || folderID != folder.ID {
				continue
			}

			key := item.Kind + "\x00" + item.ID + "\x00" + folderID
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			items = append(items, item)
		}
	}

	return items, nil
}

func buildExploreAsideItems(folders []domain.Folder, items []domain.ExploreItem, selectedFolderID string, page domain.PageRequest, selectedKinds []string, selectedOwners []string, searchQuery string) []core.ExploreNavigatorItem {
	childrenByParent := make(map[string][]domain.Folder)
	for i := range folders {
		parentID := stringValue(folders[i].ParentFolderID)
		childrenByParent[parentID] = append(childrenByParent[parentID], folders[i])
	}
	for parentID := range childrenByParent {
		sort.Slice(childrenByParent[parentID], func(i, j int) bool {
			return strings.ToLower(folderDisplayName(childrenByParent[parentID][i])) < strings.ToLower(folderDisplayName(childrenByParent[parentID][j]))
		})
	}

	resourcesByFolder := make(map[string][]domain.ExploreItem)
	seenResources := make(map[string]map[string]struct{})
	for i := range items {
		item := items[i]
		folderID := stringValue(item.FolderID)
		if folderID == "" {
			folderID = strings.TrimSpace(selectedFolderID)
		}
		if folderID == "" {
			continue
		}
		if _, ok := seenResources[folderID]; !ok {
			seenResources[folderID] = map[string]struct{}{}
		}
		key := item.Kind + "\x00" + item.ID + "\x00" + folderID
		if _, ok := seenResources[folderID][key]; ok {
			continue
		}
		seenResources[folderID][key] = struct{}{}
		resourcesByFolder[folderID] = append(resourcesByFolder[folderID], item)
	}
	for folderID := range resourcesByFolder {
		sort.Slice(resourcesByFolder[folderID], func(i, j int) bool {
			left := strings.ToLower(resourcesByFolder[folderID][i].Name)
			right := strings.ToLower(resourcesByFolder[folderID][j].Name)
			if left == right {
				return strings.ToLower(resourcesByFolder[folderID][i].Kind) < strings.ToLower(resourcesByFolder[folderID][j].Kind)
			}
			return left < right
		})
	}

	var build func(parentID string) ([]core.ExploreNavigatorItem, bool)
	build = func(parentID string) ([]core.ExploreNavigatorItem, bool) {
		folderNodes := make([]core.ExploreNavigatorItem, 0, len(childrenByParent[parentID]))
		branchHasSelection := false
		for i := range childrenByParent[parentID] {
			folder := childrenByParent[parentID][i]
			children, childHasSelection := build(folder.ID)
			for j := range resourcesByFolder[folder.ID] {
				item := resourcesByFolder[folder.ID][j]
				children = append(children, core.ExploreNavigatorItem{
					Name: item.Name,
					URL:  itemURL(item),
					Icon: kindIcon(item.Kind, item.GitRepoID != nil && strings.TrimSpace(*item.GitRepoID) != ""),
				})
			}
			isActive := folder.ID == strings.TrimSpace(selectedFolderID)
			isOpen := isActive || childHasSelection
			folderNodes = append(folderNodes, core.ExploreNavigatorItem{
				Name:     folderDisplayName(folder),
				URL:      pageURL(domain.PageRequest{MaxResults: page.Limit()}, selectedKinds, selectedOwners, searchQuery, folder.ID),
				Icon:     folderNavIcon(folder, folders),
				Active:   isActive,
				Open:     isOpen,
				Children: children,
			})
			branchHasSelection = branchHasSelection || isOpen
		}
		return folderNodes, branchHasSelection
	}

	nodes, _ := build("")
	return nodes
}

func (h *Handler) folderPathMap(ctx context.Context, principal string, isAdmin bool) (map[string]string, []domain.Folder, error) {
	owners := []string{principal}
	if isAdmin {
		notebookItems, _, err := h.deps.Notebook.ListNotebooks(ctx, nil, domain.PageRequest{MaxResults: domain.MaxMaxResults})
		if err != nil {
			return nil, nil, err
		}
		ownerSet := map[string]struct{}{}
		for i := range notebookItems {
			owner := strings.TrimSpace(notebookItems[i].Owner)
			if owner == "" {
				continue
			}
			ownerSet[owner] = struct{}{}
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

func (h *Handler) folderRows(ctx context.Context, folders []domain.Folder, folderPaths map[string]string, principal string, isAdmin bool, selectedFolderID string, selectedOwners []string, searchQuery string) ([]listRow, error) {
	visible := make([]domain.Folder, 0, len(folders))
	query := strings.ToLower(strings.TrimSpace(searchQuery))
	for i := range folders {
		folder := folders[i]
		if selectedFolderID == "" {
			if folder.ParentFolderID != nil && strings.TrimSpace(*folder.ParentFolderID) != "" {
				continue
			}
		} else if stringValue(folder.ParentFolderID) != selectedFolderID {
			continue
		}
		if len(selectedOwners) > 0 && !containsString(selectedOwners, folder.Owner) {
			continue
		}
		if query != "" && !textMatch(query, folderDisplayName(folder), folder.Owner, valueOrDash(folderPaths[stringValue(folder.ParentFolderID)])) {
			continue
		}
		visible = append(visible, folder)
	}

	sort.Slice(visible, func(i, j int) bool {
		return strings.ToLower(visible[i].Name) < strings.ToLower(visible[j].Name)
	})

	updatedByFolder, err := h.folderLatestUpdates(ctx, principal, isAdmin, visible)
	if err != nil {
		return nil, err
	}

	rows := make([]listRow, 0, len(visible))
	for _, folder := range visible {
		location := "Top level"
		if parentID := stringValue(folder.ParentFolderID); parentID != "" {
			location = valueOrDash(folderPaths[parentID])
		}
		projectID := effectiveFolderProjectID(folder, folders)
		rows = append(rows, listRow{
			Name:         folderDisplayName(folder),
			URL:          pageURL(domain.PageRequest{MaxResults: defaultPageSize}, nil, selectedOwners, searchQuery, folder.ID),
			MetaURL:      "/ui/explore/folders/" + folder.ID + "/edit",
			MetaLabel:    "Configure folder",
			Kind:         "folder",
			Owner:        folder.Owner,
			Folder:       location,
			Project:      projectID,
			Updated:      formatTime(updatedByFolder[folder.ID]),
			Shared:       strings.TrimSpace(folder.Owner) != strings.TrimSpace(principal),
			ProjectBound: projectID != "",
			GitBacked:    effectiveFolderGitBacked(folder, folders),
			PersonalRoot: folder.SystemRole != nil && *folder.SystemRole == domain.FolderSystemRoleWorkspaceRoot,
		})
	}
	return rows, nil
}

func (h *Handler) folderLatestUpdates(ctx context.Context, principal string, isAdmin bool, folders []domain.Folder) (map[string]time.Time, error) {
	updatedByFolder := make(map[string]time.Time, len(folders))
	for _, folder := range folders {
		items, err := h.deps.Explore.List(ctx, principal, isAdmin, domain.ExploreFilter{
			FolderID: folder.ID,
			Page:     domain.PageRequest{MaxResults: domain.MaxMaxResults},
		})
		if err != nil {
			return nil, fmt.Errorf("list explore items for folder %s: %w", folder.ID, err)
		}
		var latest time.Time
		for _, item := range items {
			if item.UpdatedAt.After(latest) {
				latest = item.UpdatedAt
			}
		}
		updatedByFolder[folder.ID] = latest
	}
	return updatedByFolder, nil
}

func effectiveFolderProjectID(folder domain.Folder, allFolders []domain.Folder) string {
	ancestors := ancestorFolders(folder, allFolders)
	for _, ancestor := range ancestors {
		if ancestor.DefaultProjectID != nil && strings.TrimSpace(*ancestor.DefaultProjectID) != "" {
			return strings.TrimSpace(*ancestor.DefaultProjectID)
		}
	}
	return ""
}

func effectiveFolderGitBacked(folder domain.Folder, allFolders []domain.Folder) bool {
	ancestors := ancestorFolders(folder, allFolders)
	for _, ancestor := range ancestors {
		if ancestor.GitRepoID != nil && strings.TrimSpace(*ancestor.GitRepoID) != "" {
			return true
		}
	}
	return false
}

func ancestorFolders(folder domain.Folder, allFolders []domain.Folder) []domain.Folder {
	ancestors := make([]domain.Folder, 0, folder.Depth+1)
	for _, candidate := range allFolders {
		if candidate.Path == folder.Path || strings.HasPrefix(folder.Path, candidate.Path+"/") {
			ancestors = append(ancestors, candidate)
		}
	}
	sort.Slice(ancestors, func(i, j int) bool {
		return ancestors[i].Depth > ancestors[j].Depth
	})
	return ancestors
}

func breadcrumbItems(folders []domain.Folder, selectedFolderID string, page domain.PageRequest, selectedKinds []string, selectedOwners []string, searchQuery string) []breadcrumbItem {
	breadcrumbs := []breadcrumbItem{{
		Label:   "Explore",
		URL:     pageURL(domain.PageRequest{MaxResults: page.Limit()}, selectedKinds, selectedOwners, searchQuery, ""),
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
		breadcrumbs = append(breadcrumbs, breadcrumbItem{
			Label:   folderDisplayName(folder),
			URL:     pageURL(domain.PageRequest{MaxResults: page.Limit()}, selectedKinds, selectedOwners, searchQuery, folder.ID),
			Current: i == len(chain)-1,
		})
	}
	return breadcrumbs
}

func (h *Handler) FoldersList(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/ui/explore", http.StatusSeeOther)
}
func (h *Handler) FoldersNew(w http.ResponseWriter, r *http.Request) {
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
	core.RenderHTML(w, http.StatusOK, folderNewPage(principal, folders, repos, h.deps.CSRFFieldProvider(r)))
}
func (h *Handler) FoldersCreate(w http.ResponseWriter, r *http.Request) {
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
func (h *Handler) FoldersEdit(w http.ResponseWriter, r *http.Request) {
	folderID := chi.URLParam(r, "folderID")
	principal := core.PrincipalFromContext(r.Context())
	folder, err := h.deps.NotebookFolders.GetFolderForPrincipal(r.Context(), principal.Name, principal.IsAdmin, folderID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	repos, err := h.gitRepoOptions(r.Context(), principal.Name, principal.IsAdmin, stringValue(folder.GitRepoID))
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
	core.RenderHTML(w, http.StatusOK, folderEditPage(principal, folder, parentOptions, repos, folderShareRows(folderID, shares), h.deps.CSRFFieldProvider(r)))
}
func (h *Handler) FoldersUpdate(w http.ResponseWriter, r *http.Request) {
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
func (h *Handler) FoldersMove(w http.ResponseWriter, r *http.Request) {
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
func (h *Handler) FoldersDelete(w http.ResponseWriter, r *http.Request) {
	folderID := chi.URLParam(r, "folderID")
	principal := core.PrincipalFromContext(r.Context())
	if err := h.deps.NotebookFolders.DeleteFolder(r.Context(), principal.Name, principal.IsAdmin, folderID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/explore/folders", http.StatusSeeOther)
}
func (h *Handler) FoldersShare(w http.ResponseWriter, r *http.Request) {
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
func (h *Handler) FoldersUnshare(w http.ResponseWriter, r *http.Request) {
	folderID := chi.URLParam(r, "folderID")
	targetPrincipal := chi.URLParam(r, "principalName")
	principal := core.PrincipalFromContext(r.Context())
	if err := h.deps.NotebookFolders.UnshareFolder(r.Context(), principal.Name, principal.IsAdmin, folderID, targetPrincipal); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/explore/folders/"+folderID+"/edit", http.StatusSeeOther)
}
func (h *Handler) GitReposList(w http.ResponseWriter, r *http.Request) {
	pageReq := pageFromRequest(r, defaultPageSize)
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
	core.RenderHTML(w, http.StatusOK, gitReposListPage(gitReposListPageData{
		Principal: core.PrincipalFromContext(r.Context()),
		Rows:      rows,
		Page:      pageReq,
		Total:     total,
	}))
}
func (h *Handler) GitReposNew(w http.ResponseWriter, r *http.Request) {
	core.RenderHTML(w, http.StatusOK, gitReposNewPage(core.PrincipalFromContext(r.Context()), h.deps.CSRFFieldProvider(r)))
}
func (h *Handler) GitReposCreate(w http.ResponseWriter, r *http.Request) {
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
func (h *Handler) GitReposDetail(w http.ResponseWriter, r *http.Request) {
	gitRepoID := chi.URLParam(r, "gitRepoID")
	item, err := h.deps.GitService.GetGitRepo(r.Context(), gitRepoID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	core.RenderHTML(w, http.StatusOK, gitRepoDetailPage(gitRepoDetailPageData{
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
func (h *Handler) GitReposDelete(w http.ResponseWriter, r *http.Request) {
	gitRepoID := chi.URLParam(r, "gitRepoID")
	principal := core.PrincipalFromContext(r.Context())
	if err := h.deps.GitService.DeleteGitRepo(r.Context(), principal.Name, principal.IsAdmin, gitRepoID); err != nil {
		renderServiceError(w, err)
		return
	}
	http.Redirect(w, r, "/ui/explore/git-repos", http.StatusSeeOther)
}
func (h *Handler) GitReposSync(w http.ResponseWriter, r *http.Request) {
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
			core.RenderHTML(w, http.StatusOK, gitRepoSyncUnavailablePage(gitRepoSyncUnavailablePageData{
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
	core.RenderHTML(w, http.StatusOK, gitRepoSyncResultPage(gitRepoSyncResultPageData{
		Principal: core.PrincipalFromContext(r.Context()),
		GitRepoID: gitRepoID,
		Result:    result,
	}))
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

func writeDatastarElementPatch(w http.ResponseWriter, r *http.Request, node g.Node) error {
	sse := datastar.NewSSE(w, r)
	return sse.PatchElementGostar(node, datastar.WithSelectorID("main-content"))
}
