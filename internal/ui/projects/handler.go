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

	workspaceName := h.projectWorkspaceName(r, cp, project)

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
	environmentNames := make(map[string]string, len(environments))
	environmentURLs := make(map[string]string, len(environments))
	for i := range environments {
		item := environments[i]
		environmentNames[item.ID] = item.Name
		environmentURLs[item.ID] = projectEnvironmentURL(project.ID, item.ID)
		environmentItems = append(environmentItems, projectEnvironmentRowData{
			ID:             item.ID,
			Name:           item.Name,
			URL:            projectEnvironmentURL(project.ID, item.ID),
			Description:    item.Description,
			Kind:           environmentKindLabel(item.Kind),
			TargetLocation: item.TargetCatalog + "." + item.TargetSchema,
			Compute:        valueOrDash(valueOrPtr(item.ComputeEndpoint)),
			UpdatedAt:      formatTime(item.UpdatedAt),
		})
	}

	buildItems := make([]projectBuildRowData, 0, len(builds))
	for i := range builds {
		item := builds[i]
		environmentName := valueOrDash(item.EnvironmentName)
		if strings.TrimSpace(item.EnvironmentID) != "" && strings.TrimSpace(item.EnvironmentName) == "" {
			if resolved := strings.TrimSpace(environmentNames[item.EnvironmentID]); resolved != "" {
				environmentName = resolved
			}
		}
		buildItems = append(buildItems, projectBuildRowData{
			ID:             item.ID,
			URL:            projectBuildURL(project.ID, item.ID),
			State:          buildStateLabel(item.State),
			Environment:    environmentName,
			EnvironmentURL: environmentURLs[item.EnvironmentID],
			GitRef:         valueOrDash(item.GitRef),
			Target:         item.TargetCatalog + "." + item.TargetSchema,
			CreatedAt:      formatTime(item.CreatedAt),
		})
	}

	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "project/" + project.ID,
		DisplayName:  project.Name,
		Section:      "Build",
	})
	core.RenderHTML(w, http.StatusOK, projectHubPage(projectHubPageData{
		Principal:         cp,
		Project:           *project,
		WorkspaceName:     valueOrDash(workspaceName),
		OwnerSummary:      ownerSummary(*project),
		ProductSummary:    productSummary(*project),
		ActiveTab:         tab,
		ModelsURL:         modelsListURL(project.Name),
		MacrosURL:         macrosListURL(project.Name),
		SemanticURL:       semanticListURL(project.Name),
		NewModelURL:       newModelURL(project.Name),
		NewMacroURL:       newMacroURL(project.Name),
		NewSemanticURL:    newSemanticURL(project.Name),
		NewEnvironmentURL: projectEnvironmentNewURL(project.ID),
		ModelCount:        modelTotal,
		MacroCount:        macroTotal,
		SemanticCount:     semanticTotal,
		EnvironmentCount:  environmentTotal,
		BuildCount:        buildTotal,
		Models:            modelItems,
		Macros:            macroItems,
		SemanticModels:    semanticItems,
		Environments:      environmentItems,
		Builds:            buildItems,
	}))
}

func (h *Handler) ProjectsEnvironmentNew(w http.ResponseWriter, r *http.Request) {
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

	core.RenderHTML(w, http.StatusOK, projectEnvironmentFormPage(cp, "New Environment", projectDetailURL(project.ID)+"/environments", *project, nil, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) ProjectsEnvironmentCreate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	cp := core.PrincipalFromContext(r.Context())
	projectID := chi.URLParam(r, "projectID")
	environment, err := h.deps.Project.CreateEnvironmentForProject(r.Context(), principalName(r), cp.IsAdmin, projectID, domain.CreateEnvironmentRequest{
		Name:               formString(r.Form, "name"),
		Kind:               formString(r.Form, "kind"),
		Description:        formString(r.Form, "description"),
		TargetCatalog:      formString(r.Form, "target_catalog"),
		TargetSchema:       formString(r.Form, "target_schema"),
		ComputeEndpoint:    formOptionalString(r.Form, "compute_endpoint"),
		DeferToEnvironment: formOptionalString(r.Form, "defer_to_environment"),
		Variables:          parseStringMapEditor(formString(r.Form, "variables")),
		SourceOverrides:    parseStringMapEditor(formString(r.Form, "source_overrides")),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, projectEnvironmentURL(projectID, environment.ID), http.StatusSeeOther)
}

func (h *Handler) ProjectsEnvironmentDetail(w http.ResponseWriter, r *http.Request) {
	if h.deps.Project == nil {
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Projects UI is not configured."))
		return
	}

	cp := core.PrincipalFromContext(r.Context())
	projectID := chi.URLParam(r, "projectID")
	environmentID := chi.URLParam(r, "environmentID")
	project, err := h.deps.Project.GetProjectForPrincipal(r.Context(), cp.Name, cp.IsAdmin, projectID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	environment, err := h.deps.Project.GetEnvironmentForProject(r.Context(), cp.Name, cp.IsAdmin, projectID, environmentID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	builds, _, err := h.deps.Project.ListBuildsForProject(r.Context(), cp.Name, cp.IsAdmin, projectID, domain.PageRequest{MaxResults: domain.MaxMaxResults})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	relatedBuilds := make([]projectBuildRowData, 0, len(builds))
	for i := range builds {
		item := builds[i]
		if item.EnvironmentID != environment.ID {
			continue
		}
		relatedBuilds = append(relatedBuilds, projectBuildRowData{
			ID:             item.ID,
			URL:            projectBuildURL(project.ID, item.ID),
			State:          buildStateLabel(item.State),
			Environment:    environment.Name,
			EnvironmentURL: projectEnvironmentURL(project.ID, environment.ID),
			GitRef:         valueOrDash(item.GitRef),
			Target:         item.TargetCatalog + "." + item.TargetSchema,
			CreatedAt:      formatTime(item.CreatedAt),
		})
	}

	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "project/" + project.ID + "/environment/" + environment.ID,
		DisplayName:  environment.Name,
		Section:      "Build",
	})
	core.RenderHTML(w, http.StatusOK, projectEnvironmentDetailPage(projectEnvironmentDetailPageData{
		Principal:         cp,
		Project:           *project,
		Environment:       *environment,
		WorkspaceName:     h.projectWorkspaceName(r, cp, project),
		RelatedBuilds:     relatedBuilds,
		NewEnvironmentURL: projectEnvironmentNewURL(project.ID),
		EditURL:           projectEnvironmentEditURL(project.ID, environment.ID),
		DeleteURL:         projectEnvironmentDeleteURL(project.ID, environment.ID),
		CSRFFieldProvider: h.deps.CSRFFieldProvider(r),
	}))
}

func (h *Handler) ProjectsEnvironmentEdit(w http.ResponseWriter, r *http.Request) {
	if h.deps.Project == nil {
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Projects UI is not configured."))
		return
	}

	cp := core.PrincipalFromContext(r.Context())
	projectID := chi.URLParam(r, "projectID")
	environmentID := chi.URLParam(r, "environmentID")
	project, err := h.deps.Project.GetProjectForPrincipal(r.Context(), cp.Name, cp.IsAdmin, projectID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	environment, err := h.deps.Project.GetEnvironmentForProject(r.Context(), cp.Name, cp.IsAdmin, projectID, environmentID)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	core.RenderHTML(w, http.StatusOK, projectEnvironmentFormPage(cp, "Edit Environment", projectEnvironmentUpdateURL(project.ID, environment.ID), *project, environment, h.deps.CSRFFieldProvider(r)))
}

func (h *Handler) ProjectsEnvironmentUpdate(w http.ResponseWriter, r *http.Request) {
	if !parseFormOrRenderBadRequest(w, r) {
		return
	}

	cp := core.PrincipalFromContext(r.Context())
	projectID := chi.URLParam(r, "projectID")
	environmentID := chi.URLParam(r, "environmentID")
	description := formString(r.Form, "description")
	targetCatalog := formString(r.Form, "target_catalog")
	targetSchema := formString(r.Form, "target_schema")
	computeEndpoint := formString(r.Form, "compute_endpoint")
	deferToEnvironment := formString(r.Form, "defer_to_environment")
	_, err := h.deps.Project.UpdateEnvironmentForProject(r.Context(), principalName(r), cp.IsAdmin, projectID, environmentID, domain.UpdateEnvironmentRequest{
		Description:        &description,
		TargetCatalog:      &targetCatalog,
		TargetSchema:       &targetSchema,
		ComputeEndpoint:    &computeEndpoint,
		DeferToEnvironment: &deferToEnvironment,
		Variables:          ptrStringMapOrEmpty(formString(r.Form, "variables")),
		SourceOverrides:    ptrStringMapOrEmpty(formString(r.Form, "source_overrides")),
	})
	if err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, projectEnvironmentURL(projectID, environmentID), http.StatusSeeOther)
}

func (h *Handler) ProjectsEnvironmentDelete(w http.ResponseWriter, r *http.Request) {
	cp := core.PrincipalFromContext(r.Context())
	projectID := chi.URLParam(r, "projectID")
	environmentID := chi.URLParam(r, "environmentID")
	if err := h.deps.Project.DeleteEnvironmentForProject(r.Context(), principalName(r), cp.IsAdmin, projectID, environmentID); err != nil {
		renderServiceError(w, err)
		return
	}

	http.Redirect(w, r, projectTab(projectDetailURL(projectID), projectTabEnvironments), http.StatusSeeOther)
}

func (h *Handler) ProjectsBuildDetail(w http.ResponseWriter, r *http.Request) {
	if h.deps.Project == nil {
		core.RenderHTML(w, http.StatusNotFound, core.ErrorPage("Not Found", "Projects UI is not configured."))
		return
	}

	cp := core.PrincipalFromContext(r.Context())
	projectID := chi.URLParam(r, "projectID")
	buildID := chi.URLParam(r, "buildID")
	project, err := h.deps.Project.GetProjectForPrincipal(r.Context(), cp.Name, cp.IsAdmin, projectID)
	if err != nil {
		renderServiceError(w, err)
		return
	}
	build, err := h.deps.Project.GetBuildForProject(r.Context(), cp.Name, cp.IsAdmin, projectID, buildID)
	if err != nil {
		renderServiceError(w, err)
		return
	}

	environmentName := valueOrDash(build.EnvironmentName)
	environmentURL := ""
	if strings.TrimSpace(build.EnvironmentID) != "" {
		environment, environmentErr := h.deps.Project.GetEnvironmentForProject(r.Context(), cp.Name, cp.IsAdmin, projectID, build.EnvironmentID)
		if environmentErr == nil {
			environmentName = environment.Name
			environmentURL = projectEnvironmentURL(project.ID, environment.ID)
		}
	}

	_ = core.TrackResourceVisit(r, h.deps, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "project/" + project.ID + "/build/" + build.ID,
		DisplayName:  build.ID,
		Section:      "Build",
	})
	core.RenderHTML(w, http.StatusOK, projectBuildDetailPage(projectBuildDetailPageData{
		Principal:       cp,
		Project:         *project,
		Build:           *build,
		WorkspaceName:   h.projectWorkspaceName(r, cp, project),
		EnvironmentName: environmentName,
		EnvironmentURL:  environmentURL,
	}))
}

func valueOrPtr(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func (h *Handler) projectWorkspaceName(r *http.Request, cp domain.ContextPrincipal, project *domain.Project) string {
	workspaceName := project.WorkspaceID
	if h.deps.Workspace != nil {
		workspace, workspaceErr := h.deps.Workspace.GetWorkspaceForPrincipal(r.Context(), cp.Name, cp.IsAdmin, project.WorkspaceID)
		if workspaceErr == nil && strings.TrimSpace(workspace.Name) != "" {
			workspaceName = workspace.Name
		}
	}
	return workspaceName
}

func ptrStringMapOrEmpty(raw string) *map[string]string {
	parsed := parseStringMapEditor(raw)
	if parsed == nil {
		empty := map[string]string{}
		return &empty
	}
	copyValue := make(map[string]string, len(parsed))
	for key, value := range parsed {
		copyValue[key] = value
	}
	return &copyValue
}
