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
	CreateDependencyForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateProjectDependencyRequest) (*domain.ProjectDependency, error)
	ListDependenciesForProject(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.ProjectDependency, int64, error)
	DeleteDependencyForProject(ctx context.Context, principal string, isAdmin bool, projectID string, dependencyProject string) error
	CreateSourceForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateSourceDefinitionRequest) (*domain.SourceDefinition, error)
	ListSourcesForProject(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.SourceDefinition, int64, error)
	GetSourceForProject(ctx context.Context, principal string, isAdmin bool, projectID string, sourceName string, tableName string) (*domain.SourceDefinition, error)
	UpdateSourceForProject(ctx context.Context, principal string, isAdmin bool, projectID string, sourceName string, tableName string, req domain.UpdateSourceDefinitionRequest) (*domain.SourceDefinition, error)
	DeleteSourceForProject(ctx context.Context, principal string, isAdmin bool, projectID string, sourceName string, tableName string) error
	CreateSeedForProject(ctx context.Context, principal string, isAdmin bool, projectID string, req domain.CreateSeedRequest) (*domain.Seed, error)
	ListSeedsForProject(ctx context.Context, principal string, isAdmin bool, projectID string, page domain.PageRequest) ([]domain.Seed, int64, error)
	GetSeedForProject(ctx context.Context, principal string, isAdmin bool, projectID string, seedName string) (*domain.Seed, error)
	UpdateSeedForProject(ctx context.Context, principal string, isAdmin bool, projectID string, seedName string, req domain.UpdateSeedRequest) (*domain.Seed, error)
	DeleteSeedForProject(ctx context.Context, principal string, isAdmin bool, projectID string, seedName string) error
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

// ListProjectDependencies implements the endpoint for listing project dependency declarations.
func (h *APIHandler) ListProjectDependencies(ctx context.Context, req GenListProjectDependenciesRequest) (GenListProjectDependenciesResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.projectsCtl.ListDependenciesForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListProjectDependenciesResponse]("listProjectDependencies", err, domainErrorResponder[GenListProjectDependenciesResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListProjectDependenciesResponse {
				return ListProjectDependencies400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenListProjectDependenciesResponse {
				return ListProjectDependencies403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListProjectDependenciesResponse {
				return ListProjectDependencies404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]ProjectDependency, len(items))
	for i := range items {
		data[i] = projectDependencyToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListProjectDependencies200JSONResponse{
		Body:    PaginatedProjectDependencies{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListProjectDependencies200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectDependency implements the endpoint for creating a project dependency.
func (h *APIHandler) CreateProjectDependency(ctx context.Context, req GenCreateProjectDependencyRequest) (GenCreateProjectDependencyResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	if req.Body == nil {
		return CreateProjectDependency400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.CreateDependencyForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, domain.CreateProjectDependencyRequest{
		DependencyProject: req.Body.DependencyProject,
		DependencyKind:    derefString(req.Body.DependencyKind),
		Position:          derefInt32(req.Body.Position),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateProjectDependencyResponse]("createProjectDependency", err, domainErrorResponder[GenCreateProjectDependencyResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateProjectDependencyResponse {
				return CreateProjectDependency400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateProjectDependencyResponse {
				return CreateProjectDependency403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateProjectDependencyResponse {
				return CreateProjectDependency404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateProjectDependencyResponse {
				return CreateProjectDependency409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return CreateProjectDependency201JSONResponse{
		Body:    projectDependencyToAPI(*item),
		Headers: CreateProjectDependency201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteProjectDependency implements the endpoint for deleting a project dependency.
func (h *APIHandler) DeleteProjectDependency(ctx context.Context, req GenDeleteProjectDependencyRequest) (GenDeleteProjectDependencyResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.projectsCtl.DeleteDependencyForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.DependencyProject); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteProjectDependencyResponse]("deleteProjectDependency", err, domainErrorResponder[GenDeleteProjectDependencyResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteProjectDependencyResponse {
				return DeleteProjectDependency400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteProjectDependencyResponse {
				return DeleteProjectDependency403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteProjectDependencyResponse {
				return DeleteProjectDependency404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDeleteProjectDependencyResponse {
				return DeleteProjectDependency409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return DeleteProjectDependency204Response{
		Headers: DeleteProjectDependency204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectSources implements the endpoint for listing project sources.
func (h *APIHandler) ListProjectSources(ctx context.Context, req GenListProjectSourcesRequest) (GenListProjectSourcesResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.projectsCtl.ListSourcesForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListProjectSourcesResponse]("listProjectSources", err, domainErrorResponder[GenListProjectSourcesResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListProjectSourcesResponse {
				return ListProjectSources400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenListProjectSourcesResponse {
				return ListProjectSources403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListProjectSourcesResponse {
				return ListProjectSources404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]SourceDefinition, len(items))
	for i := range items {
		data[i] = sourceDefinitionToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListProjectSources200JSONResponse{
		Body:    PaginatedProjectSources{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListProjectSources200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectSource implements the endpoint for creating a project source.
func (h *APIHandler) CreateProjectSource(ctx context.Context, req GenCreateProjectSourceRequest) (GenCreateProjectSourceResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	if req.Body == nil {
		return CreateProjectSource400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.CreateSourceForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, domain.CreateSourceDefinitionRequest{
		SourceName:  req.Body.SourceName,
		TableName:   req.Body.TableName,
		RelationRef: req.Body.RelationRef,
		Description: derefString(req.Body.Description),
		Freshness:   domainSourceFreshnessPolicy(req.Body.FreshnessPolicy),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateProjectSourceResponse]("createProjectSource", err, domainErrorResponder[GenCreateProjectSourceResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateProjectSourceResponse {
				return CreateProjectSource400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateProjectSourceResponse {
				return CreateProjectSource403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateProjectSourceResponse {
				return CreateProjectSource404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateProjectSourceResponse {
				return CreateProjectSource409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return CreateProjectSource201JSONResponse{
		Body:    sourceDefinitionToAPI(*item),
		Headers: CreateProjectSource201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectSource implements the endpoint for loading a project source.
func (h *APIHandler) GetProjectSource(ctx context.Context, req GenGetProjectSourceRequest) (GenGetProjectSourceResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.GetSourceForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.SourceName, req.TableName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetProjectSourceResponse]("getProjectSource", err, domainErrorResponder[GenGetProjectSourceResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenGetProjectSourceResponse {
				return GetProjectSource400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenGetProjectSourceResponse {
				return GetProjectSource403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenGetProjectSourceResponse {
				return GetProjectSource404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GetProjectSource200JSONResponse{
		Body:    sourceDefinitionToAPI(*item),
		Headers: GetProjectSource200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateProjectSource implements the endpoint for updating a project source.
func (h *APIHandler) UpdateProjectSource(ctx context.Context, req GenUpdateProjectSourceRequest) (GenUpdateProjectSourceResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	if req.Body == nil {
		return UpdateProjectSource400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.UpdateSourceForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.SourceName, req.TableName, domain.UpdateSourceDefinitionRequest{
		RelationRef: req.Body.RelationRef,
		Description: req.Body.Description,
		Freshness:   domainSourceFreshnessPolicy(req.Body.FreshnessPolicy),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateProjectSourceResponse]("updateProjectSource", err, domainErrorResponder[GenUpdateProjectSourceResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateProjectSourceResponse {
				return UpdateProjectSource400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateProjectSourceResponse {
				return UpdateProjectSource403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateProjectSourceResponse {
				return UpdateProjectSource404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUpdateProjectSourceResponse {
				return UpdateProjectSource409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return UpdateProjectSource200JSONResponse{
		Body:    sourceDefinitionToAPI(*item),
		Headers: UpdateProjectSource200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteProjectSource implements the endpoint for deleting a project source.
func (h *APIHandler) DeleteProjectSource(ctx context.Context, req GenDeleteProjectSourceRequest) (GenDeleteProjectSourceResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.projectsCtl.DeleteSourceForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.SourceName, req.TableName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteProjectSourceResponse]("deleteProjectSource", err, domainErrorResponder[GenDeleteProjectSourceResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteProjectSourceResponse {
				return DeleteProjectSource400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteProjectSourceResponse {
				return DeleteProjectSource403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteProjectSourceResponse {
				return DeleteProjectSource404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDeleteProjectSourceResponse {
				return DeleteProjectSource409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return DeleteProjectSource204Response{
		Headers: DeleteProjectSource204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListProjectSeeds implements the endpoint for listing project seeds.
func (h *APIHandler) ListProjectSeeds(ctx context.Context, req GenListProjectSeedsRequest) (GenListProjectSeedsResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.projectsCtl.ListSeedsForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListProjectSeedsResponse]("listProjectSeeds", err, domainErrorResponder[GenListProjectSeedsResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListProjectSeedsResponse {
				return ListProjectSeeds400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenListProjectSeedsResponse {
				return ListProjectSeeds403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListProjectSeedsResponse {
				return ListProjectSeeds404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]ProjectSeed, len(items))
	for i := range items {
		data[i] = projectSeedToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListProjectSeeds200JSONResponse{
		Body:    PaginatedProjectSeeds{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListProjectSeeds200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateProjectSeed implements the endpoint for creating a project seed.
func (h *APIHandler) CreateProjectSeed(ctx context.Context, req GenCreateProjectSeedRequest) (GenCreateProjectSeedResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	if req.Body == nil {
		return CreateProjectSeed400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.CreateSeedForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, domain.CreateSeedRequest{
		Name:        req.Body.Name,
		Description: derefString(req.Body.Description),
		InputRef:    req.Body.InputRef,
		Format:      derefStringEnum(req.Body.Format),
		Delimiter:   req.Body.Delimiter,
		HasHeader:   req.Body.HasHeader,
		ColumnTypes: anyMapToStringMap(req.Body.ColumnTypes),
		Tags:        derefStringSlice(req.Body.Tags),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateProjectSeedResponse]("createProjectSeed", err, domainErrorResponder[GenCreateProjectSeedResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateProjectSeedResponse {
				return CreateProjectSeed400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateProjectSeedResponse {
				return CreateProjectSeed403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateProjectSeedResponse {
				return CreateProjectSeed404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateProjectSeedResponse {
				return CreateProjectSeed409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return CreateProjectSeed201JSONResponse{
		Body:    projectSeedToAPI(*item),
		Headers: CreateProjectSeed201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetProjectSeed implements the endpoint for loading a project seed.
func (h *APIHandler) GetProjectSeed(ctx context.Context, req GenGetProjectSeedRequest) (GenGetProjectSeedResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.GetSeedForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.SeedName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetProjectSeedResponse]("getProjectSeed", err, domainErrorResponder[GenGetProjectSeedResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenGetProjectSeedResponse {
				return GetProjectSeed400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenGetProjectSeedResponse { return GetProjectSeed403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenGetProjectSeedResponse { return GetProjectSeed404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GetProjectSeed200JSONResponse{
		Body:    projectSeedToAPI(*item),
		Headers: GetProjectSeed200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateProjectSeed implements the endpoint for updating a project seed.
func (h *APIHandler) UpdateProjectSeed(ctx context.Context, req GenUpdateProjectSeedRequest) (GenUpdateProjectSeedResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	if req.Body == nil {
		return UpdateProjectSeed400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.projectsCtl.UpdateSeedForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.SeedName, domain.UpdateSeedRequest{
		Description: req.Body.Description,
		InputRef:    req.Body.InputRef,
		Format:      stringPtrFromEnum(req.Body.Format),
		Delimiter:   req.Body.Delimiter,
		HasHeader:   req.Body.HasHeader,
		ColumnTypes: anyMapPtrToStringMap(req.Body.ColumnTypes),
		Tags:        derefStringSlice(req.Body.Tags),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateProjectSeedResponse]("updateProjectSeed", err, domainErrorResponder[GenUpdateProjectSeedResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateProjectSeedResponse {
				return UpdateProjectSeed400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateProjectSeedResponse {
				return UpdateProjectSeed403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateProjectSeedResponse {
				return UpdateProjectSeed404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUpdateProjectSeedResponse {
				return UpdateProjectSeed409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return UpdateProjectSeed200JSONResponse{
		Body:    projectSeedToAPI(*item),
		Headers: UpdateProjectSeed200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteProjectSeed implements the endpoint for deleting a project seed.
func (h *APIHandler) DeleteProjectSeed(ctx context.Context, req GenDeleteProjectSeedRequest) (GenDeleteProjectSeedResponse, error) {
	if isNilService(h.projectsCtl) {
		return nil, domain.ErrNotImplemented("projects are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.projectsCtl.DeleteSeedForProject(ctx, cp.Name, cp.IsAdmin, req.ProjectId, req.SeedName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteProjectSeedResponse]("deleteProjectSeed", err, domainErrorResponder[GenDeleteProjectSeedResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteProjectSeedResponse {
				return DeleteProjectSeed400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteProjectSeedResponse {
				return DeleteProjectSeed403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteProjectSeedResponse {
				return DeleteProjectSeed404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDeleteProjectSeedResponse {
				return DeleteProjectSeed409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return DeleteProjectSeed204Response{
		Headers: DeleteProjectSeed204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
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

func projectDependencyToAPI(item domain.ProjectDependency) ProjectDependency {
	return ProjectDependency{
		Id:                optStr(item.ID),
		ProjectId:         item.ProjectID,
		ProjectName:       optStr(item.ProjectName),
		DependencyProject: item.DependencyProject,
		DependencyKind:    optStr(item.DependencyKind),
		Position:          safeIntPtr(item.Position),
		CreatedAt:         formatTimePtr(&item.CreatedAt),
		UpdatedAt:         formatTimePtr(&item.UpdatedAt),
	}
}

func sourceDefinitionToAPI(item domain.SourceDefinition) SourceDefinition {
	resp := SourceDefinition{
		Id:          item.ID,
		ProjectName: item.ProjectName,
		SourceName:  item.SourceName,
		TableName:   item.TableName,
		RelationRef: item.RelationRef,
		Description: optStr(item.Description),
		CreatedBy:   optStr(item.CreatedBy),
		CreatedAt:   formatTimePtr(&item.CreatedAt),
		UpdatedAt:   formatTimePtr(&item.UpdatedAt),
	}
	if item.Freshness != nil {
		freshness := apiSourceFreshnessPolicy(*item.Freshness)
		resp.FreshnessPolicy = &freshness
	}
	return resp
}

func projectSeedToAPI(item domain.Seed) ProjectSeed {
	format := SeedFormat(domain.NormalizeSeedFormat(item.Format))
	resp := ProjectSeed{
		Id:          optStr(item.ID),
		ProjectName: item.ProjectName,
		Name:        item.Name,
		Description: optStr(item.Description),
		InputRef:    item.InputRef,
		Format:      &format,
		Delimiter:   optStr(item.Delimiter),
		HasHeader:   &item.HasHeader,
		ColumnTypes: stringMapToAnyMap(item.ColumnTypes),
		CreatedBy:   optStr(item.CreatedBy),
		CreatedAt:   formatTimePtr(&item.CreatedAt),
		UpdatedAt:   formatTimePtr(&item.UpdatedAt),
	}
	if len(item.Tags) > 0 {
		resp.Tags = &item.Tags
	}
	return resp
}

func domainSourceFreshnessPolicy(value *SourceFreshnessPolicy) *domain.SourceFreshnessPolicy {
	if value == nil {
		return nil
	}
	return &domain.SourceFreshnessPolicy{
		TimestampColumn: derefString(value.TimestampColumn),
		MaxLagSeconds:   derefInt64(value.MaxLagSeconds),
	}
}

func apiSourceFreshnessPolicy(value domain.SourceFreshnessPolicy) SourceFreshnessPolicy {
	return SourceFreshnessPolicy{
		TimestampColumn: optStr(value.TimestampColumn),
		MaxLagSeconds:   safeInt64ToInt32Ptr(&value.MaxLagSeconds),
	}
}

func derefInt32(value *int32) int {
	if value == nil {
		return 0
	}
	return int(*value)
}

func derefInt64(value *int32) int64 {
	if value == nil {
		return 0
	}
	return int64(*value)
}

func safeIntPtr(value int) *int32 {
	if value > 1<<31-1 {
		result := int32(1<<31 - 1)
		return &result
	}
	if value < -1<<31 {
		result := int32(-1 << 31)
		return &result
	}
	result := int32(value)
	return &result
}

func stringPtrFromEnum[T ~string](value *T) *string {
	if value == nil {
		return nil
	}
	s := string(*value)
	return &s
}

func buildStateToAPI(state string) *BuildState {
	if state == "" {
		return nil
	}
	value := BuildState(state)
	return &value
}
