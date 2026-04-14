package api

import (
	"context"

	"github.com/Yacobolo/quackstack/internal/domain"
)

type projectControlService interface {
	CreateProject(ctx context.Context, principal string, req domain.CreateProjectRequest) (*domain.Project, error)
	ListProjectsForPrincipal(ctx context.Context, principal string, isAdmin bool, workspaceID string, page domain.PageRequest) ([]domain.Project, int64, error)
	GetProjectForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Project, error)
	UpdateProjectForPrincipal(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateProjectRequest) (*domain.Project, error)
	DeleteProjectForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) error
	CreateEnvironmentForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateEnvironmentRequest) (*domain.Environment, error)
	ListEnvironmentsForProject(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.Environment, int64, error)
	UpdateEnvironmentForProject(ctx context.Context, principal string, isAdmin bool, projectID string, environmentID string, req domain.UpdateEnvironmentRequest) (*domain.Environment, error)
	DeleteEnvironmentForProject(ctx context.Context, principal string, isAdmin bool, projectID string, environmentID string) error
	CreateBuildForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateBuildRequest) (*domain.Build, error)
	ListBuildsForProject(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.Build, int64, error)
}

// ListWorkspaceProjects implements the endpoint for listing projects within a workspace.
func (h *APIHandler) ListWorkspaceProjects(ctx context.Context, req GenListWorkspaceProjectsRequest) (GenListWorkspaceProjectsResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.projectsCtl.ListProjectsForPrincipal(ctx, cp.Name, cp.IsAdmin, req.WorkspaceId, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListWorkspaceProjectsResponse]("listWorkspaceProjects", err, domainErrorResponder[GenListWorkspaceProjectsResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListWorkspaceProjectsResponse {
				return ListWorkspaceProjects400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenListWorkspaceProjectsResponse {
				return ListWorkspaceProjects403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListWorkspaceProjectsResponse {
				return ListWorkspaceProjects404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]Project, len(items))
	for i := range items {
		data[i] = projectToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListWorkspaceProjects200JSONResponse{
		Body:    PaginatedProjects{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListWorkspaceProjects200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateWorkspaceProject implements the endpoint for creating a project inside a workspace.
func (h *APIHandler) CreateWorkspaceProject(ctx context.Context, req GenCreateWorkspaceProjectRequest) (GenCreateWorkspaceProjectResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	if req.Body == nil {
		return CreateWorkspaceProject400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.CreateProject(ctx, cp.Name, domain.CreateProjectRequest{
		WorkspaceID:   req.WorkspaceId,
		Name:          req.Body.Name,
		Kind:          derefStringEnum(req.Body.Kind),
		Description:   derefString(req.Body.Description),
		ProductID:     req.Body.ProductId,
		DefaultBranch: derefString(req.Body.DefaultBranch),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateWorkspaceProjectResponse]("createWorkspaceProject", err, domainErrorResponder[GenCreateWorkspaceProjectResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateWorkspaceProjectResponse {
				return CreateWorkspaceProject400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateWorkspaceProjectResponse {
				return CreateWorkspaceProject403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateWorkspaceProjectResponse {
				return CreateWorkspaceProject404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateWorkspaceProjectResponse {
				return CreateWorkspaceProject409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return CreateWorkspaceProject201JSONResponse{
		Body:    projectToAPI(*item),
		Headers: CreateWorkspaceProject201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProject implements the endpoint for loading one project.
func (h *APIHandler) GetProject(ctx context.Context, req GenGetProjectRequest) (GenGetProjectResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.GetProjectForPrincipal(ctx, cp.Name, cp.IsAdmin, req.ProjectId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetProjectResponse]("getProject", err, domainErrorResponder[GenGetProjectResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenGetProjectResponse {
				return GetProject400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenGetProjectResponse {
				return GetProject403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenGetProjectResponse {
				return GetProject404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return GetProject200JSONResponse{
		Body:    projectToAPI(*item),
		Headers: GetProject200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateProject implements the endpoint for updating one project.
func (h *APIHandler) UpdateProject(ctx context.Context, req GenUpdateProjectRequest) (GenUpdateProjectResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	if req.Body == nil {
		return UpdateProject400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.UpdateProjectForPrincipal(ctx, cp.Name, cp.IsAdmin, req.ProjectId, domain.UpdateProjectRequest{
		Description:   req.Body.Description,
		DefaultBranch: req.Body.DefaultBranch,
		ProductID:     req.Body.ProductId,
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateProjectResponse]("updateProject", err, domainErrorResponder[GenUpdateProjectResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateProjectResponse {
				return UpdateProject400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateProjectResponse {
				return UpdateProject403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateProjectResponse {
				return UpdateProject404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUpdateProjectResponse {
				return UpdateProject409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return UpdateProject200JSONResponse{
		Body:    projectToAPI(*item),
		Headers: UpdateProject200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteProject implements the endpoint for deleting one project.
func (h *APIHandler) DeleteProject(ctx context.Context, req GenDeleteProjectRequest) (GenDeleteProjectResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.projectsCtl.DeleteProjectForPrincipal(ctx, cp.Name, cp.IsAdmin, req.ProjectId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteProjectResponse]("deleteProject", err, domainErrorResponder[GenDeleteProjectResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteProjectResponse {
				return DeleteProject400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteProjectResponse {
				return DeleteProject403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteProjectResponse {
				return DeleteProject404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDeleteProjectResponse {
				return DeleteProject409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return DeleteProject204Response{
		Headers: DeleteProject204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectEnvironments implements the endpoint for listing environments owned by a project.
func (h *APIHandler) ListProjectEnvironments(ctx context.Context, req GenListProjectEnvironmentsRequest) (GenListProjectEnvironmentsResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.projectsCtl.ListEnvironmentsForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListProjectEnvironmentsResponse]("listProjectEnvironments", err, domainErrorResponder[GenListProjectEnvironmentsResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListProjectEnvironmentsResponse {
				return ListProjectEnvironments400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenListProjectEnvironmentsResponse {
				return ListProjectEnvironments403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListProjectEnvironmentsResponse {
				return ListProjectEnvironments404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]Environment, len(items))
	for i := range items {
		data[i] = environmentToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListProjectEnvironments200JSONResponse{
		Body:    PaginatedEnvironments{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListProjectEnvironments200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectEnvironment implements the endpoint for creating an environment under a project.
func (h *APIHandler) CreateProjectEnvironment(ctx context.Context, req GenCreateProjectEnvironmentRequest) (GenCreateProjectEnvironmentResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	if req.Body == nil {
		return CreateProjectEnvironment400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.CreateEnvironmentForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, domain.CreateEnvironmentRequest{
		Name:               req.Body.Name,
		Kind:               derefStringEnum(req.Body.Kind),
		Description:        derefString(req.Body.Description),
		TargetCatalog:      req.Body.TargetCatalog,
		TargetSchema:       req.Body.TargetSchema,
		ComputeEndpoint:    req.Body.ComputeEndpoint,
		DeferToEnvironment: req.Body.DeferToEnvironment,
		Variables:          anyMapToStringMap(req.Body.Variables),
		SourceOverrides:    anyMapToStringMap(req.Body.SourceOverrides),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateProjectEnvironmentResponse]("createProjectEnvironment", err, domainErrorResponder[GenCreateProjectEnvironmentResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateProjectEnvironmentResponse {
				return CreateProjectEnvironment400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateProjectEnvironmentResponse {
				return CreateProjectEnvironment403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateProjectEnvironmentResponse {
				return CreateProjectEnvironment404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateProjectEnvironmentResponse {
				return CreateProjectEnvironment409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return CreateProjectEnvironment201JSONResponse{
		Body:    environmentToAPI(*item),
		Headers: CreateProjectEnvironment201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateProjectEnvironment implements the endpoint for updating an environment under a project.
func (h *APIHandler) UpdateProjectEnvironment(ctx context.Context, req GenUpdateProjectEnvironmentRequest) (GenUpdateProjectEnvironmentResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	if req.Body == nil {
		return UpdateProjectEnvironment400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.UpdateEnvironmentForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.EnvironmentId, domain.UpdateEnvironmentRequest{
		Description:        req.Body.Description,
		TargetCatalog:      req.Body.TargetCatalog,
		TargetSchema:       req.Body.TargetSchema,
		ComputeEndpoint:    req.Body.ComputeEndpoint,
		DeferToEnvironment: req.Body.DeferToEnvironment,
		Variables:          anyMapPtrToStringMap(req.Body.Variables),
		SourceOverrides:    anyMapPtrToStringMap(req.Body.SourceOverrides),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateProjectEnvironmentResponse]("updateProjectEnvironment", err, domainErrorResponder[GenUpdateProjectEnvironmentResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateProjectEnvironmentResponse {
				return UpdateProjectEnvironment400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateProjectEnvironmentResponse {
				return UpdateProjectEnvironment403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateProjectEnvironmentResponse {
				return UpdateProjectEnvironment404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUpdateProjectEnvironmentResponse {
				return UpdateProjectEnvironment409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return UpdateProjectEnvironment200JSONResponse{
		Body:    environmentToAPI(*item),
		Headers: UpdateProjectEnvironment200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteProjectEnvironment implements the endpoint for deleting an environment under a project.
func (h *APIHandler) DeleteProjectEnvironment(ctx context.Context, req GenDeleteProjectEnvironmentRequest) (GenDeleteProjectEnvironmentResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.projectsCtl.DeleteEnvironmentForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.EnvironmentId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteProjectEnvironmentResponse]("deleteProjectEnvironment", err, domainErrorResponder[GenDeleteProjectEnvironmentResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteProjectEnvironmentResponse {
				return DeleteProjectEnvironment400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteProjectEnvironmentResponse {
				return DeleteProjectEnvironment403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteProjectEnvironmentResponse {
				return DeleteProjectEnvironment404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDeleteProjectEnvironmentResponse {
				return DeleteProjectEnvironment409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return DeleteProjectEnvironment204Response{
		Headers: DeleteProjectEnvironment204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectBuilds implements the endpoint for listing project builds.
func (h *APIHandler) ListProjectBuilds(ctx context.Context, req GenListProjectBuildsRequest) (GenListProjectBuildsResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.projectsCtl.ListBuildsForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListProjectBuildsResponse]("listProjectBuilds", err, domainErrorResponder[GenListProjectBuildsResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListProjectBuildsResponse {
				return ListProjectBuilds400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenListProjectBuildsResponse {
				return ListProjectBuilds403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListProjectBuildsResponse {
				return ListProjectBuilds404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]Build, len(items))
	for i := range items {
		data[i] = buildToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListProjectBuilds200JSONResponse{
		Body:    PaginatedBuilds{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListProjectBuilds200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectBuild implements the endpoint for creating a build under a project.
func (h *APIHandler) CreateProjectBuild(ctx context.Context, req GenCreateProjectBuildRequest) (GenCreateProjectBuildResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	if req.Body == nil {
		return CreateProjectBuild400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.CreateBuildForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, domain.CreateBuildRequest{
		EnvironmentName:    req.Body.EnvironmentName,
		GitRef:             req.Body.GitRef,
		CommitSHA:          req.Body.CommitSha,
		Selector:           derefString(req.Body.Selector),
		TargetCatalog:      req.Body.TargetCatalog,
		TargetSchema:       req.Body.TargetSchema,
		SourceModelRunID:   req.Body.SourceModelRunId,
		CompileManifest:    req.Body.CompileManifest,
		CompileDiagnostics: req.Body.CompileDiagnostics,
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateProjectBuildResponse]("createProjectBuild", err, domainErrorResponder[GenCreateProjectBuildResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateProjectBuildResponse {
				return CreateProjectBuild400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateProjectBuildResponse {
				return CreateProjectBuild403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateProjectBuildResponse {
				return CreateProjectBuild404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateProjectBuildResponse {
				return CreateProjectBuild409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return CreateProjectBuild201JSONResponse{
		Body:    buildToAPI(*item),
		Headers: CreateProjectBuild201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func projectToAPI(item domain.Project) Project {
	return Project{
		Id:             optStr(item.ID),
		WorkspaceId:    item.WorkspaceID,
		Name:           item.Name,
		Kind:           ProjectKind(item.Kind),
		Description:    optStr(item.Description),
		OwnerTeamId:    item.OwnerTeamID,
		OwnerPrincipal: item.OwnerPrincipal,
		ProductId:      item.ProductID,
		DefaultBranch:  optStr(item.DefaultBranch),
		CreatedAt:      formatTimePtr(&item.CreatedAt),
		UpdatedAt:      formatTimePtr(&item.UpdatedAt),
	}
}

func environmentToAPI(item domain.Environment) Environment {
	return Environment{
		Id:                 optStr(item.ID),
		ProjectId:          optStr(item.ProjectID),
		ProjectName:        optStr(item.ProjectName),
		Name:               item.Name,
		Kind:               EnvironmentKind(item.Kind),
		Description:        optStr(item.Description),
		TargetCatalog:      item.TargetCatalog,
		TargetSchema:       item.TargetSchema,
		ComputeEndpoint:    item.ComputeEndpoint,
		DeferToEnvironment: item.DeferToEnvironment,
		Variables:          stringMapToAnyMap(item.Variables),
		SourceOverrides:    stringMapToAnyMap(item.SourceOverrides),
		CreatedAt:          formatTimePtr(&item.CreatedAt),
		UpdatedAt:          formatTimePtr(&item.UpdatedAt),
	}
}

func buildToAPI(item domain.Build) Build {
	return Build{
		Id:                 optStr(item.ID),
		ProjectId:          optStr(item.ProjectID),
		ProjectName:        optStr(item.ProjectName),
		ProductId:          item.ProductID,
		EnvironmentId:      optStr(item.EnvironmentID),
		EnvironmentName:    optStr(item.EnvironmentName),
		State:              buildStateToAPI(item.State),
		GitRef:             item.GitRef,
		CommitSha:          item.CommitSHA,
		Selector:           optStr(item.Selector),
		TargetCatalog:      item.TargetCatalog,
		TargetSchema:       item.TargetSchema,
		SourceModelRunId:   item.SourceModelRunID,
		CompileManifest:    item.CompileManifest,
		CompileDiagnostics: item.CompileDiagnostics,
		CreatedAt:          formatTimePtr(&item.CreatedAt),
	}
}

func buildStateToAPI(state string) *BuildState {
	if state == "" {
		return nil
	}
	value := BuildState(state)
	return &value
}
