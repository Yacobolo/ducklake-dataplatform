package projects

import (
	"net/http"
	"sort"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	"github.com/go-chi/chi/v5"
)

type Handler struct{ deps *core.Dependencies }

func New(deps *core.Dependencies) *Handler { return &Handler{deps: deps} }

func (h *Handler) ProjectsList(w http.ResponseWriter, r *http.Request) {
	if h.deps.Project == nil || h.deps.Workspace == nil {
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Projects UI is not configured."))
		return
	}

	cp := core.PrincipalFromContext(r.Context())
	pageReq := pageFromRequest(r, 24)
	workspaces, _, err := h.deps.Workspace.ListWorkspacesForPrincipal(r.Context(), cp.Name, cp.IsAdmin, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	workspaceNames := make(map[string]string, len(workspaces))
	items := make([]domain.Project, 0, len(workspaces)*2)
	for i := range workspaces {
		workspace := workspaces[i]
		workspaceNames[workspace.ID] = workspace.Name
		projects, _, listErr := h.deps.Project.ListProjectsForPrincipal(r.Context(), cp.Name, cp.IsAdmin, workspace.ID, domain.PageRequest{MaxResults: domain.MaxMaxResults})
		if listErr != nil {
			renderServiceError(w, listErr)
			return
		}
		items = append(items, projects...)
	}

	sort.Slice(items, func(i, j int) bool {
		leftWorkspace := strings.ToLower(workspaceNames[items[i].WorkspaceID])
		rightWorkspace := strings.ToLower(workspaceNames[items[j].WorkspaceID])
		if leftWorkspace == rightWorkspace {
			return strings.ToLower(items[i].Name) < strings.ToLower(items[j].Name)
		}
		return leftWorkspace < rightWorkspace
	})

	total := int64(len(items))
	start := pageReq.Offset()
	if start > len(items) {
		start = len(items)
	}
	end := start + pageReq.Limit()
	if end > len(items) {
		end = len(items)
	}

	rows := make([]projectListRowData, 0, end-start)
	for _, item := range items[start:end] {
		workspaceName := workspaceNames[item.WorkspaceID]
		rows = append(rows, projectListRowData{
			ID:             item.ID,
			Name:           item.Name,
			Kind:           projectKindLabel(item.Kind),
			WorkspaceName:  valueOrDash(workspaceName),
			DefaultBranch:  valueOrDash(item.DefaultBranch),
			OwnerSummary:   ownerSummary(item),
			ProductSummary: productSummary(item),
			CreatedAt:      formatTime(item.CreatedAt),
			URL:            "/ui/projects/" + item.ID,
		})
	}

	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "projects",
		DisplayName:  "Projects",
		Section:      "Build",
	})
	core.RenderHTML(w, http.StatusOK, projectsListPage(core.PrincipalFromContext(r.Context()), rows, pageReq, total))
}

func (h *Handler) ProjectsDetail(w http.ResponseWriter, r *http.Request) {
	if h.deps.Project == nil {
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Projects UI is not configured."))
		return
	}

	cp := core.PrincipalFromContext(r.Context())
	projectID := chi.URLParam(r, "projectID")
	project, err := h.deps.Project.GetProjectForPrincipal(r.Context(), cp.Name, cp.IsAdmin, projectID)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	workspaceName := project.WorkspaceID
	if h.deps.Workspace != nil {
		workspace, workspaceErr := h.deps.Workspace.GetWorkspaceForPrincipal(r.Context(), cp.Name, cp.IsAdmin, project.WorkspaceID)
		if workspaceErr == nil && strings.TrimSpace(workspace.Name) != "" {
			workspaceName = workspace.Name
		}
	}

	models, modelTotal, err := h.deps.Model.ListModels(r.Context(), ptrString(project.Name), domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	macroRows, macroTotal, err := h.deps.Macro.ListFiltered(r.Context(), ptrString(project.Name), domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	semanticRows, semanticTotal, err := h.deps.Semantic.ListSemanticModels(r.Context(), ptrString(project.Name), domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	environments, environmentTotal, err := h.deps.Project.ListEnvironmentsForProject(r.Context(), cp.Name, cp.IsAdmin, project.ID, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		renderServiceError(w, err)
		return
	}
	builds, buildTotal, err := h.deps.Project.ListBuildsForProject(r.Context(), cp.Name, cp.IsAdmin, project.ID, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	tab := normalizedProjectTab(r.URL.Query().Get("tab"))
	baseURL := "/ui/projects/" + project.ID
	tabs := []core.SectionTab{
		{Label: "Overview", Href: projectTab(baseURL, projectTabOverview), Active: tab == projectTabOverview},
		{Label: "Assets", Href: projectTab(baseURL, projectTabAssets), Active: tab == projectTabAssets},
		{Label: "Environments", Href: projectTab(baseURL, projectTabEnvironments), Active: tab == projectTabEnvironments},
		{Label: "Builds", Href: projectTab(baseURL, projectTabBuilds), Active: tab == projectTabBuilds},
	}

	modelItems := make([]projectAssetRowData, 0, len(models))
	for i := range models {
		item := models[i]
		modelItems = append(modelItems, projectAssetRowData{
			Name:   item.Name,
			URL:    "/ui/models/" + item.ProjectName + "/" + item.Name,
			Meta1:  item.Materialization,
			Meta2:  formatTime(item.UpdatedAt),
			Detail: strings.Join(item.DependsOn, ", "),
		})
	}

	macroItems := make([]projectAssetRowData, 0, len(macroRows))
	for i := range macroRows {
		item := macroRows[i]
		macroItems = append(macroItems, projectAssetRowData{
			Name:   item.Name,
			URL:    "/ui/macros/" + item.Name,
			Meta1:  item.MacroType,
			Meta2:  valueOrDash(item.Visibility),
			Detail: formatTime(item.UpdatedAt),
		})
	}

	semanticItems := make([]projectAssetRowData, 0, len(semanticRows))
	for i := range semanticRows {
		item := semanticRows[i]
		semanticItems = append(semanticItems, projectAssetRowData{
			Name:   item.Name,
			URL:    "/ui/semantic/models/" + item.ID,
			Meta1:  valueOrDash(item.BaseModelRef),
			Meta2:  formatTime(item.UpdatedAt),
			Detail: valueOrDash(item.DefaultTimeDimension),
		})
	}

	environmentItems := make([]projectEnvironmentRowData, 0, len(environments))
	for i := range environments {
		item := environments[i]
		environmentItems = append(environmentItems, projectEnvironmentRowData{
			Name:           item.Name,
			Kind:           environmentKindLabel(item.Kind),
			TargetLocation: item.TargetCatalog + "." + item.TargetSchema,
			Compute:        valueOrDash(valueOrPtr(item.ComputeEndpoint)),
			UpdatedAt:      formatTime(item.UpdatedAt),
		})
	}

	buildItems := make([]projectBuildRowData, 0, len(builds))
	for i := range builds {
		item := builds[i]
		buildItems = append(buildItems, projectBuildRowData{
			ID:          item.ID,
			State:       buildStateLabel(item.State),
			Environment: valueOrDash(item.EnvironmentName),
			GitRef:      valueOrDash(item.GitRef),
			Target:      item.TargetCatalog + "." + item.TargetSchema,
			CreatedAt:   formatTime(item.CreatedAt),
		})
	}

	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "project/" + project.ID,
		DisplayName:  project.Name,
		Section:      "Build",
	})
	core.RenderHTML(w, http.StatusOK, projectHubPage(projectHubPageData{
		Principal:        cp,
		Project:          *project,
		WorkspaceName:    valueOrDash(workspaceName),
		OwnerSummary:     ownerSummary(*project),
		ProductSummary:   productSummary(*project),
		ActiveTab:        tab,
		Tabs:             tabs,
		ModelsURL:        modelsListURL(project.Name),
		MacrosURL:        macrosListURL(project.Name),
		SemanticURL:      semanticListURL(project.Name),
		NewModelURL:      newModelURL(project.Name),
		NewMacroURL:      newMacroURL(project.Name),
		NewSemanticURL:   newSemanticURL(project.Name),
		ModelCount:       modelTotal,
		MacroCount:       macroTotal,
		SemanticCount:    semanticTotal,
		EnvironmentCount: environmentTotal,
		BuildCount:       buildTotal,
		Models:           modelItems,
		Macros:           macroItems,
		SemanticModels:   semanticItems,
		Environments:     environmentItems,
		Builds:           buildItems,
	}))
}

func valueOrPtr(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
