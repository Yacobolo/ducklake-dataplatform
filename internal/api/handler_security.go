package api

import (
	"context"

	"duck-demo/internal/domain"
)

// principalService defines the principal operations used by the API handler.
type principalService interface {
	List(ctx context.Context, page domain.PageRequest) ([]domain.Principal, int64, error)
	Create(ctx context.Context, req domain.CreatePrincipalRequest) (*domain.Principal, error)
	GetByID(ctx context.Context, id string) (*domain.Principal, error)
	Delete(ctx context.Context, id string) error
	SetAdmin(ctx context.Context, id string, isAdmin bool) error
}

// groupService defines the group operations used by the API handler.
type groupService interface {
	List(ctx context.Context, page domain.PageRequest) ([]domain.Group, int64, error)
	Create(ctx context.Context, req domain.CreateGroupRequest) (*domain.Group, error)
	GetByID(ctx context.Context, id string) (*domain.Group, error)
	Update(ctx context.Context, id string, req domain.UpdateGroupRequest) (*domain.Group, error)
	Delete(ctx context.Context, id string) error
	ListMembers(ctx context.Context, groupID string, page domain.PageRequest) ([]domain.GroupMember, int64, error)
	AddMember(ctx context.Context, req domain.AddGroupMemberRequest) error
	RemoveMember(ctx context.Context, req domain.RemoveGroupMemberRequest) error
}

// grantService defines the grant operations used by the API handler.
type grantService interface {
	ListAll(ctx context.Context, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error)
	ListForPrincipal(ctx context.Context, principalID string, principalType string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error)
	ListForSecurable(ctx context.Context, securableType string, securableID string, page domain.PageRequest) ([]domain.PrivilegeGrant, int64, error)
	Grant(ctx context.Context, req domain.CreateGrantRequest) (*domain.PrivilegeGrant, error)
	Revoke(ctx context.Context, principal string, grantID string) error
}

// rowFilterService defines the row filter operations used by the API handler.
type rowFilterService interface {
	GetForTable(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.RowFilter, int64, error)
	Create(ctx context.Context, req domain.CreateRowFilterRequest) (*domain.RowFilter, error)
	Delete(ctx context.Context, id string) error
	Bind(ctx context.Context, req domain.BindRowFilterRequest) error
	Unbind(ctx context.Context, req domain.BindRowFilterRequest) error
	ListBindings(ctx context.Context, filterID string) ([]domain.RowFilterBinding, error)
}

// columnMaskService defines the column mask operations used by the API handler.
type columnMaskService interface {
	GetForTable(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.ColumnMask, int64, error)
	Create(ctx context.Context, req domain.CreateColumnMaskRequest) (*domain.ColumnMask, error)
	Delete(ctx context.Context, id string) error
	Bind(ctx context.Context, req domain.BindColumnMaskRequest) error
	Unbind(ctx context.Context, req domain.BindColumnMaskRequest) error
	ListBindings(ctx context.Context, maskID string) ([]domain.ColumnMaskBinding, error)
}

// === Principals ===

// ListPrincipals implements the endpoint for listing all principals. Requires admin privileges.
func (h *APIHandler) ListPrincipals(ctx context.Context, req GenListPrincipalsRequest) (GenListPrincipalsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	ps, total, err := h.principals.List(ctx, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListPrincipalsResponse]("listPrincipals", err, domainErrorResponder[GenListPrincipalsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListPrincipalsResponse { return ListPrincipals403JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	out := make([]Principal, len(ps))
	for i, p := range ps {
		out[i] = principalToAPI(p)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListPrincipals200JSONResponse{
		Body:    PaginatedPrincipals{Data: out, NextPageToken: optStr(npt)},
		Headers: GenListPrincipals200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreatePrincipal implements the endpoint for creating a new principal.
func (h *APIHandler) CreatePrincipal(ctx context.Context, req GenCreatePrincipalRequest) (GenCreatePrincipalResponse, error) {
	domReq := domain.CreatePrincipalRequest{
		Name: req.Body.Name,
	}
	if req.Body.Type != nil {
		domReq.Type = string(*req.Body.Type)
	}
	if req.Body.IsAdmin != nil {
		domReq.IsAdmin = *req.Body.IsAdmin
	}
	result, err := h.principals.Create(ctx, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreatePrincipalResponse]("createPrincipal", err, domainErrorResponder[GenCreatePrincipalResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreatePrincipalResponse {
				return CreatePrincipal400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreatePrincipalResponse {
				return CreatePrincipal403JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreatePrincipalResponse {
				return CreatePrincipal409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreatePrincipal201JSONResponse{
		Body:    principalToAPI(*result),
		Headers: GenCreatePrincipal201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetPrincipal implements the endpoint for retrieving a principal by ID.
func (h *APIHandler) GetPrincipal(ctx context.Context, req GenGetPrincipalRequest) (GenGetPrincipalResponse, error) {
	p, err := h.principals.GetByID(ctx, req.PrincipalId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetPrincipalResponse]("getPrincipal", err, domainErrorResponder[GenGetPrincipalResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetPrincipalResponse {
				return GenGetPrincipal404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetPrincipal200JSONResponse{
		Body:    principalToAPI(*p),
		Headers: GenGetPrincipal200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeletePrincipal implements the endpoint for deleting a principal by ID.
func (h *APIHandler) DeletePrincipal(ctx context.Context, req GenDeletePrincipalRequest) (GenDeletePrincipalResponse, error) {
	if err := h.principals.Delete(ctx, req.PrincipalId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeletePrincipalResponse]("deletePrincipal", err, domainErrorResponder[GenDeletePrincipalResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeletePrincipalResponse {
				return DeletePrincipal403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeletePrincipalResponse {
				return DeletePrincipal404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeletePrincipal204Response{}, nil
}

// UpdatePrincipalAdmin implements the endpoint for updating a principal's admin status.
func (h *APIHandler) UpdatePrincipalAdmin(ctx context.Context, req GenUpdatePrincipalAdminRequest) (GenUpdatePrincipalAdminResponse, error) {
	if err := h.principals.SetAdmin(ctx, req.PrincipalId, req.Body.IsAdmin); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdatePrincipalAdminResponse]("updatePrincipalAdmin", err, domainErrorResponder[GenUpdatePrincipalAdminResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdatePrincipalAdminResponse {
				return UpdatePrincipalAdmin403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdatePrincipalAdminResponse {
				return UpdatePrincipalAdmin404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdatePrincipalAdmin204Response{}, nil
}

// === Groups ===

// ListGroups implements the endpoint for listing all groups.
func (h *APIHandler) ListGroups(ctx context.Context, req GenListGroupsRequest) (GenListGroupsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	gs, total, err := h.groups.List(ctx, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListGroupsResponse]("listGroups", err, domainErrorResponder[GenListGroupsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListGroupsResponse { return ListGroups403JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	out := make([]Group, len(gs))
	for i, g := range gs {
		out[i] = groupToAPI(g)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListGroups200JSONResponse{
		Body:    PaginatedGroups{Data: out, NextPageToken: optStr(npt)},
		Headers: GenListGroups200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateGroup implements the endpoint for creating a new group.
func (h *APIHandler) CreateGroup(ctx context.Context, req GenCreateGroupRequest) (GenCreateGroupResponse, error) {
	domReq := domain.CreateGroupRequest{Name: req.Body.Name}
	if req.Body.Description != nil {
		domReq.Description = *req.Body.Description
	}
	result, err := h.groups.Create(ctx, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateGroupResponse]("createGroup", err, domainErrorResponder[GenCreateGroupResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateGroupResponse { return CreateGroup400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenCreateGroupResponse { return CreateGroup403JSONResponse{resp} },
			Conflict:   func(resp ConflictJSONResponse) GenCreateGroupResponse { return CreateGroup409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreateGroup201JSONResponse{
		Body:    groupToAPI(*result),
		Headers: GenCreateGroup201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetGroup implements the endpoint for retrieving a group by ID.
func (h *APIHandler) GetGroup(ctx context.Context, req GenGetGroupRequest) (GenGetGroupResponse, error) {
	g, err := h.groups.GetByID(ctx, req.GroupId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetGroupResponse]("getGroup", err, domainErrorResponder[GenGetGroupResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenGetGroupResponse {
				return GenGetGroup404JSONResponse{GenNotFoundJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetGroup200JSONResponse{
		Body:    groupToAPI(*g),
		Headers: GenGetGroup200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateGroup implements the endpoint for updating a group by ID.
func (h *APIHandler) UpdateGroup(ctx context.Context, req GenUpdateGroupRequest) (GenUpdateGroupResponse, error) {
	if req.Body == nil {
		return UpdateGroup400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	domReq := domain.UpdateGroupRequest{}
	if req.Body.Description != nil {
		domReq.Description = req.Body.Description
	}
	group, err := h.groups.Update(ctx, req.GroupId, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateGroupResponse]("updateGroup", err, domainErrorResponder[GenUpdateGroupResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateGroupResponse { return UpdateGroup400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenUpdateGroupResponse { return UpdateGroup403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenUpdateGroupResponse { return UpdateGroup404JSONResponse{resp} },
			Conflict:   func(resp ConflictJSONResponse) GenUpdateGroupResponse { return UpdateGroup409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateGroup200JSONResponse{
		Body:    groupToAPI(*group),
		Headers: GenUpdateGroup200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteGroup implements the endpoint for deleting a group by ID.
func (h *APIHandler) DeleteGroup(ctx context.Context, req GenDeleteGroupRequest) (GenDeleteGroupResponse, error) {
	if err := h.groups.Delete(ctx, req.GroupId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteGroupResponse]("deleteGroup", err, domainErrorResponder[GenDeleteGroupResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteGroupResponse { return DeleteGroup403JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteGroup204Response{}, nil
}

// ListGroupMembers implements the endpoint for listing members of a group.
func (h *APIHandler) ListGroupMembers(ctx context.Context, req GenListGroupMembersRequest) (GenListGroupMembersResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	ms, total, err := h.groups.ListMembers(ctx, req.GroupId, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListGroupMembersResponse]("listGroupMembers", err, domainErrorResponder[GenListGroupMembersResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenListGroupMembersResponse {
				return ListGroupMembers400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenListGroupMembersResponse {
				return ListGroupMembers403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListGroupMembersResponse {
				return ListGroupMembers404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	out := make([]GroupMember, len(ms))
	for i, m := range ms {
		out[i] = groupMemberToAPI(m, req.GroupId)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListGroupMembers200JSONResponse{
		Body:    PaginatedGroupMembers{Data: out, NextPageToken: optStr(npt)},
		Headers: GenListGroupMembers200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateGroupMember implements the endpoint for adding a member to a group.
func (h *APIHandler) CreateGroupMember(ctx context.Context, req GenCreateGroupMemberRequest) (GenCreateGroupMemberResponse, error) {
	if err := h.groups.AddMember(ctx, domain.AddGroupMemberRequest{
		GroupID:    req.GroupId,
		MemberType: string(req.Body.MemberType),
		MemberID:   req.Body.MemberId,
	}); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateGroupMemberResponse]("createGroupMember", err, domainErrorResponder[GenCreateGroupMemberResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateGroupMemberResponse {
				return CreateGroupMember400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateGroupMemberResponse {
				return CreateGroupMember403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateGroupMemberResponse {
				return CreateGroupMember404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return CreateGroupMember204Response{}, nil
}

// DeleteGroupMember implements the endpoint for removing a member from a group.
func (h *APIHandler) DeleteGroupMember(ctx context.Context, req GenDeleteGroupMemberRequest) (GenDeleteGroupMemberResponse, error) {
	if err := h.groups.RemoveMember(ctx, domain.RemoveGroupMemberRequest{
		GroupID:    req.GroupId,
		MemberType: string(req.MemberType),
		MemberID:   req.MemberId,
	}); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteGroupMemberResponse]("deleteGroupMember", err, domainErrorResponder[GenDeleteGroupMemberResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteGroupMemberResponse {
				return DeleteGroupMember400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteGroupMemberResponse {
				return DeleteGroupMember403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteGroupMemberResponse {
				return DeleteGroupMember404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteGroupMember204Response{}, nil
}

// === Grants ===

// ListGrants implements the endpoint for listing privilege grants filtered by principal or securable.
func (h *APIHandler) ListGrants(ctx context.Context, req GenListGrantsRequest) (GenListGrantsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	var grants []domain.PrivilegeGrant
	var total int64
	var err error

	switch {
	case req.Params.PrincipalId != nil && req.Params.PrincipalType != nil:
		grants, total, err = h.grants.ListForPrincipal(ctx, *req.Params.PrincipalId, string(*req.Params.PrincipalType), page)
	case req.Params.SecurableType != nil && req.Params.SecurableId != nil:
		grants, total, err = h.grants.ListForSecurable(ctx, *req.Params.SecurableType, *req.Params.SecurableId, page)
	default:
		grants, total, err = h.grants.ListAll(ctx, page)
	}
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListGrantsResponse]("listGrants", err, domainErrorResponder[GenListGrantsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListGrantsResponse { return ListGrants403JSONResponse{resp} },
			Internal: func(resp InternalErrorJSONResponse) GenListGrantsResponse {
				return GenListGrants500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	out := make([]PrivilegeGrant, len(grants))
	for i, g := range grants {
		out[i] = grantToAPI(g)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListGrants200JSONResponse{
		Body:    PaginatedGrants{Data: out, NextPageToken: optStr(npt)},
		Headers: GenListGrants200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateGrant implements the endpoint for granting a privilege to a principal.
func (h *APIHandler) CreateGrant(ctx context.Context, req GenCreateGrantRequest) (GenCreateGrantResponse, error) {
	domReq := domain.CreateGrantRequest{
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: string(req.Body.PrincipalType),
		SecurableType: req.Body.SecurableType,
		SecurableID:   req.Body.SecurableId,
		Privilege:     string(req.Body.Privilege),
	}
	result, err := h.grants.Grant(ctx, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateGrantResponse]("createGrant", err, domainErrorResponder[GenCreateGrantResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateGrantResponse { return CreateGrant400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenCreateGrantResponse { return CreateGrant403JSONResponse{resp} },
			Conflict:   func(resp ConflictJSONResponse) GenCreateGrantResponse { return CreateGrant409JSONResponse{resp} },
			Internal: func(resp InternalErrorJSONResponse) GenCreateGrantResponse {
				return GenCreateGrant500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreateGrant201JSONResponse{
		Body:    grantToAPI(*result),
		Headers: GenCreateGrant201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteGrant implements the endpoint for revoking a privilege from a principal.
func (h *APIHandler) DeleteGrant(ctx context.Context, req GenDeleteGrantRequest) (GenDeleteGrantResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.grants.Revoke(ctx, principal, req.GrantId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteGrantResponse]("deleteGrant", err, domainErrorResponder[GenDeleteGrantResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteGrantResponse { return DeleteGrant403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenDeleteGrantResponse { return DeleteGrant404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteGrant204Response{}, nil
}

// === Row Filters ===

// ListRowFilters implements the endpoint for listing row filters for a table.
func (h *APIHandler) ListRowFilters(ctx context.Context, req GenListRowFiltersRequest) (GenListRowFiltersResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	fs, total, err := h.rowFilters.GetForTable(ctx, req.TableId, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListRowFiltersResponse]("listRowFilters", err, domainErrorResponder[GenListRowFiltersResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListRowFiltersResponse { return ListRowFilters403JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	out := make([]RowFilter, len(fs))
	for i, f := range fs {
		out[i] = rowFilterToAPI(f)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListRowFilters200JSONResponse{
		Body:    PaginatedRowFilters{Data: out, NextPageToken: optStr(npt)},
		Headers: GenListRowFilters200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateRowFilter implements the endpoint for creating a row filter on a table.
func (h *APIHandler) CreateRowFilter(ctx context.Context, req GenCreateRowFilterRequest) (GenCreateRowFilterResponse, error) {
	domReq := domain.CreateRowFilterRequest{
		TableID:   req.TableId,
		Name:      req.Body.Name,
		FilterSQL: req.Body.FilterSql,
	}
	if req.Body.Description != nil {
		domReq.Description = *req.Body.Description
	}
	result, err := h.rowFilters.Create(ctx, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateRowFilterResponse]("createRowFilter", err, domainErrorResponder[GenCreateRowFilterResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateRowFilterResponse {
				return CreateRowFilter400JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateRowFilterResponse {
				return CreateRowFilter409JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateRowFilterResponse {
				return CreateRowFilter403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreateRowFilter201JSONResponse{
		Body:    rowFilterToAPI(*result),
		Headers: GenCreateRowFilter201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteRowFilter implements the endpoint for deleting a row filter.
func (h *APIHandler) DeleteRowFilter(ctx context.Context, req GenDeleteRowFilterRequest) (GenDeleteRowFilterResponse, error) {
	if err := h.rowFilters.Delete(ctx, req.RowFilterId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteRowFilterResponse]("deleteRowFilter", err, domainErrorResponder[GenDeleteRowFilterResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteRowFilterResponse {
				return DeleteRowFilter403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteRowFilterResponse {
				return DeleteRowFilter404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteRowFilter204Response{}, nil
}

// BindRowFilter implements the endpoint for binding a row filter to a principal.
func (h *APIHandler) BindRowFilter(ctx context.Context, req GenBindRowFilterRequest) (GenBindRowFilterResponse, error) {
	if err := h.rowFilters.Bind(ctx, domain.BindRowFilterRequest{
		RowFilterID:   req.RowFilterId,
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: string(req.Body.PrincipalType),
	}); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenBindRowFilterResponse]("bindRowFilter", err, domainErrorResponder[GenBindRowFilterResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenBindRowFilterResponse { return BindRowFilter400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenBindRowFilterResponse { return BindRowFilter403JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return BindRowFilter204Response{}, nil
}

// ListRowFilterBindings implements the endpoint for listing bindings for a row filter.
func (h *APIHandler) ListRowFilterBindings(ctx context.Context, req GenListRowFilterBindingsRequest) (GenListRowFilterBindingsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	bindings, err := h.rowFilters.ListBindings(ctx, req.RowFilterId)
	if err != nil {
		return nil, err
	}
	start := page.Offset()
	if start > len(bindings) {
		start = len(bindings)
	}
	end := start + page.Limit()
	if end > len(bindings) {
		end = len(bindings)
	}
	data := make([]RowFilterBinding, 0, end-start)
	for _, binding := range bindings[start:end] {
		data = append(data, RowFilterBinding{
			Id:            &binding.ID,
			RowFilterId:   &binding.RowFilterID,
			PrincipalId:   &binding.PrincipalID,
			PrincipalType: ptrPrincipalType(binding.PrincipalType),
		})
	}
	next := domain.NextPageToken(start, page.Limit(), int64(len(bindings)))
	return ListRowFilterBindings200JSONResponse{
		Body:    PaginatedRowFilterBindings{Data: data, NextPageToken: optStr(next)},
		Headers: ListRowFilterBindings200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UnbindRowFilter implements the endpoint for unbinding a row filter from a principal.
func (h *APIHandler) UnbindRowFilter(ctx context.Context, req GenUnbindRowFilterRequest) (GenUnbindRowFilterResponse, error) {
	if err := h.rowFilters.Unbind(ctx, domain.BindRowFilterRequest{
		RowFilterID:   req.RowFilterId,
		PrincipalID:   req.PrincipalId,
		PrincipalType: string(req.PrincipalType),
	}); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUnbindRowFilterResponse]("unbindRowFilter", err, domainErrorResponder[GenUnbindRowFilterResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUnbindRowFilterResponse {
				return UnbindRowFilter403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUnbindRowFilterResponse {
				return UnbindRowFilter404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUnbindRowFilter204Response{}, nil
}

// === Column Masks ===

// ListColumnMasks implements the endpoint for listing column masks for a table.
func (h *APIHandler) ListColumnMasks(ctx context.Context, req GenListColumnMasksRequest) (GenListColumnMasksResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	ms, total, err := h.columnMasks.GetForTable(ctx, req.TableId, page)
	if err != nil {
		return nil, err
	}
	out := make([]ColumnMask, len(ms))
	for i, m := range ms {
		out[i] = columnMaskToAPI(m)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListColumnMasks200JSONResponse{
		Body:    PaginatedColumnMasks{Data: out, NextPageToken: optStr(npt)},
		Headers: GenListColumnMasks200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateColumnMask implements the endpoint for creating a column mask on a table.
func (h *APIHandler) CreateColumnMask(ctx context.Context, req GenCreateColumnMaskRequest) (GenCreateColumnMaskResponse, error) {
	domReq := domain.CreateColumnMaskRequest{
		Name:           req.Body.Name,
		TableID:        req.TableId,
		ColumnName:     req.Body.ColumnName,
		MaskExpression: req.Body.MaskExpression,
	}
	if req.Body.Description != nil {
		domReq.Description = *req.Body.Description
	}
	result, err := h.columnMasks.Create(ctx, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateColumnMaskResponse]("createColumnMask", err, domainErrorResponder[GenCreateColumnMaskResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateColumnMaskResponse {
				return CreateColumnMask400JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateColumnMaskResponse {
				return CreateColumnMask409JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateColumnMaskResponse {
				return CreateColumnMask403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreateColumnMask201JSONResponse{
		Body:    columnMaskToAPI(*result),
		Headers: GenCreateColumnMask201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteColumnMask implements the endpoint for deleting a column mask.
func (h *APIHandler) DeleteColumnMask(ctx context.Context, req GenDeleteColumnMaskRequest) (GenDeleteColumnMaskResponse, error) {
	if err := h.columnMasks.Delete(ctx, req.ColumnMaskId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteColumnMaskResponse]("deleteColumnMask", err, domainErrorResponder[GenDeleteColumnMaskResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteColumnMaskResponse {
				return DeleteColumnMask403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteColumnMaskResponse {
				return DeleteColumnMask404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteColumnMask204Response{}, nil
}

// BindColumnMask implements the endpoint for binding a column mask to a principal.
func (h *APIHandler) BindColumnMask(ctx context.Context, req GenBindColumnMaskRequest) (GenBindColumnMaskResponse, error) {
	seeOriginal := false
	if req.Body.SeeOriginal != nil {
		seeOriginal = *req.Body.SeeOriginal
	}
	if err := h.columnMasks.Bind(ctx, domain.BindColumnMaskRequest{
		ColumnMaskID:  req.ColumnMaskId,
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: string(req.Body.PrincipalType),
		SeeOriginal:   seeOriginal,
	}); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenBindColumnMaskResponse]("bindColumnMask", err, domainErrorResponder[GenBindColumnMaskResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenBindColumnMaskResponse {
				return BindColumnMask400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenBindColumnMaskResponse { return BindColumnMask403JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return BindColumnMask204Response{}, nil
}

// ListColumnMaskBindings implements the endpoint for listing bindings for a column mask.
func (h *APIHandler) ListColumnMaskBindings(ctx context.Context, req GenListColumnMaskBindingsRequest) (GenListColumnMaskBindingsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	bindings, err := h.columnMasks.ListBindings(ctx, req.ColumnMaskId)
	if err != nil {
		return nil, err
	}
	start := page.Offset()
	if start > len(bindings) {
		start = len(bindings)
	}
	end := start + page.Limit()
	if end > len(bindings) {
		end = len(bindings)
	}
	data := make([]ColumnMaskBinding, 0, end-start)
	for _, binding := range bindings[start:end] {
		data = append(data, ColumnMaskBinding{
			Id:            &binding.ID,
			ColumnMaskId:  &binding.ColumnMaskID,
			PrincipalId:   &binding.PrincipalID,
			PrincipalType: ptrPrincipalType(binding.PrincipalType),
			SeeOriginal:   &binding.SeeOriginal,
		})
	}
	next := domain.NextPageToken(start, page.Limit(), int64(len(bindings)))
	return ListColumnMaskBindings200JSONResponse{
		Body:    PaginatedColumnMaskBindings{Data: data, NextPageToken: optStr(next)},
		Headers: ListColumnMaskBindings200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UnbindColumnMask implements the endpoint for unbinding a column mask from a principal.
func (h *APIHandler) UnbindColumnMask(ctx context.Context, req GenUnbindColumnMaskRequest) (GenUnbindColumnMaskResponse, error) {
	if err := h.columnMasks.Unbind(ctx, domain.BindColumnMaskRequest{
		ColumnMaskID:  req.ColumnMaskId,
		PrincipalID:   req.PrincipalId,
		PrincipalType: string(req.PrincipalType),
	}); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUnbindColumnMaskResponse]("unbindColumnMask", err, domainErrorResponder[GenUnbindColumnMaskResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUnbindColumnMaskResponse {
				return UnbindColumnMask403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUnbindColumnMask204Response{}, nil
}
