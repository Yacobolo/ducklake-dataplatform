package api

import (
	"context"

	"duck-demo/internal/domain"
)

type workspaceService interface {
	CreateWorkspace(ctx context.Context, principal string, isAdmin bool, req domain.CreateWorkspaceRequest) (*domain.Workspace, error)
	GetWorkspaceForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Workspace, error)
	ListWorkspacesForPrincipal(ctx context.Context, principal string, isAdmin bool, page domain.PageRequest) ([]domain.Workspace, int64, error)
	UpdateWorkspace(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateWorkspaceRequest) (*domain.Workspace, error)
	DeleteWorkspace(ctx context.Context, principal string, isAdmin bool, id string) error
	ListMembers(ctx context.Context, principal string, isAdmin bool, workspaceID string) ([]domain.WorkspaceMember, error)
	AddMember(ctx context.Context, principal string, isAdmin bool, workspaceID string, req domain.AddWorkspaceMemberRequest) (*domain.WorkspaceMember, error)
	RemoveMember(ctx context.Context, principal string, isAdmin bool, workspaceID string, principalName string) error
}

// ListWorkspaces implements the endpoint for listing visible workspaces.
func (h *APIHandler) ListWorkspaces(ctx context.Context, req GenListWorkspacesRequest) (GenListWorkspacesResponse, error) {
	if isNilService(h.workspaces) {
		empty := []Workspace{}
		return ListWorkspaces200JSONResponse{
			Body:    PaginatedWorkspaces{Data: empty, NextPageToken: nil},
			Headers: ListWorkspaces200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
		}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := h.workspaces.ListWorkspacesForPrincipal(ctx, cp.Name, cp.IsAdmin, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListWorkspacesResponse]("listWorkspaces", err, domainErrorResponder[GenListWorkspacesResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListWorkspacesResponse {
				return ListWorkspaces400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenListWorkspacesResponse {
				return ListWorkspaces403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]Workspace, len(items))
	for i := range items {
		data[i] = workspaceToAPI(items[i])
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListWorkspaces200JSONResponse{
		Body:    PaginatedWorkspaces{Data: data, NextPageToken: optStr(nextToken)},
		Headers: ListWorkspaces200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateWorkspace implements the endpoint for creating a workspace.
func (h *APIHandler) CreateWorkspace(ctx context.Context, req GenCreateWorkspaceRequest) (GenCreateWorkspaceResponse, error) {
	if isNilService(h.workspaces) {
		return nil, domain.ErrNotImplemented("workspaces are not configured")
	}
	if req.Body == nil {
		return CreateWorkspace400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.workspaces.CreateWorkspace(ctx, cp.Name, cp.IsAdmin, domain.CreateWorkspaceRequest{
		Name:                 req.Body.Name,
		Kind:                 derefStringEnum(req.Body.Kind),
		OwnerTeamID:          req.Body.OwnerTeamId,
		OwnerPrincipal:       req.Body.OwnerPrincipal,
		DefaultProjectID:     req.Body.DefaultProjectId,
		DefaultEnvironmentID: req.Body.DefaultEnvironmentId,
		GitRepoID:            req.Body.GitRepoId,
		GitRootPath:          req.Body.GitRootPath,
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateWorkspaceResponse]("createWorkspace", err, domainErrorResponder[GenCreateWorkspaceResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateWorkspaceResponse {
				return CreateWorkspace400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateWorkspaceResponse {
				return CreateWorkspace403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateWorkspaceResponse {
				return CreateWorkspace404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateWorkspaceResponse {
				return CreateWorkspace409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return CreateWorkspace201JSONResponse{
		Body:    workspaceToAPI(*item),
		Headers: CreateWorkspace201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetWorkspace implements the endpoint for loading one workspace.
func (h *APIHandler) GetWorkspace(ctx context.Context, req GenGetWorkspaceRequest) (GenGetWorkspaceResponse, error) {
	if isNilService(h.workspaces) {
		return nil, domain.ErrNotImplemented("workspaces are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.workspaces.GetWorkspaceForPrincipal(ctx, cp.Name, cp.IsAdmin, req.WorkspaceId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetWorkspaceResponse]("getWorkspace", err, domainErrorResponder[GenGetWorkspaceResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenGetWorkspaceResponse {
				return GetWorkspace400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenGetWorkspaceResponse {
				return GetWorkspace403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenGetWorkspaceResponse {
				return GetWorkspace404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return GetWorkspace200JSONResponse{
		Body:    workspaceToAPI(*item),
		Headers: GetWorkspace200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateWorkspace implements the endpoint for updating workspace defaults and metadata.
func (h *APIHandler) UpdateWorkspace(ctx context.Context, req GenUpdateWorkspaceRequest) (GenUpdateWorkspaceResponse, error) {
	if isNilService(h.workspaces) {
		return nil, domain.ErrNotImplemented("workspaces are not configured")
	}
	if req.Body == nil {
		return UpdateWorkspace400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.workspaces.UpdateWorkspace(ctx, cp.Name, cp.IsAdmin, req.WorkspaceId, domain.UpdateWorkspaceRequest{
		Name:                 req.Body.Name,
		DefaultProjectID:     req.Body.DefaultProjectId,
		DefaultEnvironmentID: req.Body.DefaultEnvironmentId,
		GitRepoID:            req.Body.GitRepoId,
		GitRootPath:          req.Body.GitRootPath,
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateWorkspaceResponse]("updateWorkspace", err, domainErrorResponder[GenUpdateWorkspaceResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateWorkspaceResponse {
				return UpdateWorkspace400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateWorkspaceResponse {
				return UpdateWorkspace403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateWorkspaceResponse {
				return UpdateWorkspace404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUpdateWorkspaceResponse {
				return UpdateWorkspace409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return UpdateWorkspace200JSONResponse{
		Body:    workspaceToAPI(*item),
		Headers: UpdateWorkspace200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteWorkspace implements the endpoint for deleting a workspace.
func (h *APIHandler) DeleteWorkspace(ctx context.Context, req GenDeleteWorkspaceRequest) (GenDeleteWorkspaceResponse, error) {
	if isNilService(h.workspaces) {
		return nil, domain.ErrNotImplemented("workspaces are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.workspaces.DeleteWorkspace(ctx, cp.Name, cp.IsAdmin, req.WorkspaceId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteWorkspaceResponse]("deleteWorkspace", err, domainErrorResponder[GenDeleteWorkspaceResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteWorkspaceResponse {
				return DeleteWorkspace400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteWorkspaceResponse {
				return DeleteWorkspace403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteWorkspaceResponse {
				return DeleteWorkspace404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDeleteWorkspaceResponse {
				return DeleteWorkspace409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return DeleteWorkspace204Response{
		Headers: DeleteWorkspace204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListWorkspaceMembers implements the endpoint for listing workspace memberships.
func (h *APIHandler) ListWorkspaceMembers(ctx context.Context, req GenListWorkspaceMembersRequest) (GenListWorkspaceMembersResponse, error) {
	if isNilService(h.workspaces) {
		return nil, domain.ErrNotImplemented("workspaces are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	items, err := h.workspaces.ListMembers(ctx, cp.Name, cp.IsAdmin, req.WorkspaceId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListWorkspaceMembersResponse]("listWorkspaceMembers", err, domainErrorResponder[GenListWorkspaceMembersResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListWorkspaceMembersResponse {
				return ListWorkspaceMembers400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenListWorkspaceMembersResponse {
				return ListWorkspaceMembers403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListWorkspaceMembersResponse {
				return ListWorkspaceMembers404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]WorkspaceMember, len(items))
	for i := range items {
		data[i] = workspaceMemberToAPI(items[i])
	}
	return ListWorkspaceMembers200JSONResponse{
		Body:    data,
		Headers: ListWorkspaceMembers200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// AddWorkspaceMember implements the endpoint for adding or updating a workspace member.
func (h *APIHandler) AddWorkspaceMember(ctx context.Context, req GenAddWorkspaceMemberRequest) (GenAddWorkspaceMemberResponse, error) {
	if isNilService(h.workspaces) {
		return nil, domain.ErrNotImplemented("workspaces are not configured")
	}
	if req.Body == nil {
		return AddWorkspaceMember400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	item, err := h.workspaces.AddMember(ctx, cp.Name, cp.IsAdmin, req.WorkspaceId, domain.AddWorkspaceMemberRequest{
		PrincipalName: req.Body.PrincipalName,
		Role:          derefStringEnum(req.Body.Role),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenAddWorkspaceMemberResponse]("addWorkspaceMember", err, domainErrorResponder[GenAddWorkspaceMemberResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenAddWorkspaceMemberResponse {
				return AddWorkspaceMember400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenAddWorkspaceMemberResponse {
				return AddWorkspaceMember403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenAddWorkspaceMemberResponse {
				return AddWorkspaceMember404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenAddWorkspaceMemberResponse {
				return AddWorkspaceMember409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return AddWorkspaceMember200JSONResponse{
		Body:    workspaceMemberToAPI(*item),
		Headers: AddWorkspaceMember200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// RemoveWorkspaceMember implements the endpoint for deleting a workspace member.
func (h *APIHandler) RemoveWorkspaceMember(ctx context.Context, req GenRemoveWorkspaceMemberRequest) (GenRemoveWorkspaceMemberResponse, error) {
	if isNilService(h.workspaces) {
		return nil, domain.ErrNotImplemented("workspaces are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.workspaces.RemoveMember(ctx, cp.Name, cp.IsAdmin, req.WorkspaceId, req.PrincipalName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenRemoveWorkspaceMemberResponse]("removeWorkspaceMember", err, domainErrorResponder[GenRemoveWorkspaceMemberResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenRemoveWorkspaceMemberResponse {
				return RemoveWorkspaceMember400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenRemoveWorkspaceMemberResponse {
				return RemoveWorkspaceMember403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenRemoveWorkspaceMemberResponse {
				return RemoveWorkspaceMember404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenRemoveWorkspaceMemberResponse {
				return RemoveWorkspaceMember409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	return RemoveWorkspaceMember204Response{
		Headers: RemoveWorkspaceMember204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

func workspaceToAPI(item domain.Workspace) Workspace {
	return Workspace{
		Id:                   optStr(item.ID),
		Name:                 item.Name,
		Kind:                 WorkspaceKind(item.Kind),
		OwnerTeamId:          item.OwnerTeamID,
		OwnerPrincipal:       item.OwnerPrincipal,
		DefaultProjectId:     item.DefaultProjectID,
		DefaultEnvironmentId: item.DefaultEnvironmentID,
		GitRepoId:            item.GitRepoID,
		GitRootPath:          item.GitRootPath,
		CreatedAt:            formatTimePtr(&item.CreatedAt),
		UpdatedAt:            formatTimePtr(&item.UpdatedAt),
	}
}

func workspaceMemberToAPI(item domain.WorkspaceMember) WorkspaceMember {
	role := NotebookShareRoleViewer
	if mapped := notebookShareRoleToAPI(item.Role); mapped != nil {
		role = *mapped
	}
	return WorkspaceMember{
		WorkspaceId:   item.WorkspaceID,
		PrincipalName: item.PrincipalName,
		Role:          role,
		CreatedAt:     formatTimePtr(&item.CreatedAt),
		UpdatedAt:     formatTimePtr(&item.UpdatedAt),
	}
}
