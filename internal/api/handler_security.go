package api

import (
	"context"
	"errors"

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
}

// columnMaskService defines the column mask operations used by the API handler.
type columnMaskService interface {
	GetForTable(ctx context.Context, tableID string, page domain.PageRequest) ([]domain.ColumnMask, int64, error)
	Create(ctx context.Context, req domain.CreateColumnMaskRequest) (*domain.ColumnMask, error)
	Delete(ctx context.Context, id string) error
	Bind(ctx context.Context, req domain.BindColumnMaskRequest) error
	Unbind(ctx context.Context, req domain.BindColumnMaskRequest) error
}

// === Principals ===

// ListPrincipals implements the endpoint for listing all principals. Requires admin privileges.
func (h *APIHandler) ListPrincipals(ctx context.Context, req GenListPrincipalsRequest) (GenListPrincipalsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	ps, total, err := h.principals.List(ctx, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenListPrincipals403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]Principal, len(ps))
	for i, p := range ps {
		out[i] = principalToAPI(p)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListPrincipals200JSONResponse{
		Body:    PaginatedPrincipals{Data: &out, NextPageToken: optStr(npt)},
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenCreatePrincipal403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return GenCreatePrincipal400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return GenCreatePrincipal409JSONResponse{GenConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: GenConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenGetPrincipal404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenGetPrincipal200JSONResponse{
		Body:    principalToAPI(*p),
		Headers: GenGetPrincipal200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeletePrincipal implements the endpoint for deleting a principal by ID.
func (h *APIHandler) DeletePrincipal(ctx context.Context, req GenDeletePrincipalRequest) (GenDeletePrincipalResponse, error) {
	if err := h.principals.Delete(ctx, req.PrincipalId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenDeletePrincipal403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GenDeletePrincipal404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenDeletePrincipal204Response{}, nil
}

// UpdatePrincipalAdmin implements the endpoint for updating a principal's admin status.
func (h *APIHandler) UpdatePrincipalAdmin(ctx context.Context, req GenUpdatePrincipalAdminRequest) (GenUpdatePrincipalAdminResponse, error) {
	if err := h.principals.SetAdmin(ctx, req.PrincipalId, req.Body.IsAdmin); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenUpdatePrincipalAdmin403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GenUpdatePrincipalAdmin404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenUpdatePrincipalAdmin204Response{}, nil
}

// === Groups ===

// ListGroups implements the endpoint for listing all groups.
func (h *APIHandler) ListGroups(ctx context.Context, req GenListGroupsRequest) (GenListGroupsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	gs, total, err := h.groups.List(ctx, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenListGroups403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]Group, len(gs))
	for i, g := range gs {
		out[i] = groupToAPI(g)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListGroups200JSONResponse{
		Body:    PaginatedGroups{Data: &out, NextPageToken: optStr(npt)},
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenCreateGroup403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenGetGroup404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenGetGroup200JSONResponse{
		Body:    groupToAPI(*g),
		Headers: GenGetGroup200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteGroup implements the endpoint for deleting a group by ID.
func (h *APIHandler) DeleteGroup(ctx context.Context, req GenDeleteGroupRequest) (GenDeleteGroupResponse, error) {
	if err := h.groups.Delete(ctx, req.GroupId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenDeleteGroup403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenDeleteGroup204Response{}, nil
}

// ListGroupMembers implements the endpoint for listing members of a group.
func (h *APIHandler) ListGroupMembers(ctx context.Context, req GenListGroupMembersRequest) (GenListGroupMembersResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	ms, total, err := h.groups.ListMembers(ctx, req.GroupId, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenListGroupMembers403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]GroupMember, len(ms))
	for i, m := range ms {
		out[i] = groupMemberToAPI(m, req.GroupId)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListGroupMembers200JSONResponse{
		Body:    PaginatedGroupMembers{Data: &out, NextPageToken: optStr(npt)},
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenCreateGroupMember403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenCreateGroupMember204Response{}, nil
}

// DeleteGroupMember implements the endpoint for removing a member from a group.
func (h *APIHandler) DeleteGroupMember(ctx context.Context, req GenDeleteGroupMemberRequest) (GenDeleteGroupMemberResponse, error) {
	if err := h.groups.RemoveMember(ctx, domain.RemoveGroupMemberRequest{
		GroupID:    req.GroupId,
		MemberType: string(req.Params.MemberType),
		MemberID:   req.Params.MemberId,
	}); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenDeleteGroupMember403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenListGrants403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	out := make([]PrivilegeGrant, len(grants))
	for i, g := range grants {
		out[i] = grantToAPI(g)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListGrants200JSONResponse{
		Body:    PaginatedGrants{Data: &out, NextPageToken: optStr(npt)},
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenCreateGrant403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenDeleteGrant403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GenDeleteGrant404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenDeleteGrant204Response{}, nil
}

// === Row Filters ===

// ListRowFilters implements the endpoint for listing row filters for a table.
func (h *APIHandler) ListRowFilters(ctx context.Context, req GenListRowFiltersRequest) (GenListRowFiltersResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	fs, total, err := h.rowFilters.GetForTable(ctx, req.TableId, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenListRowFilters403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]RowFilter, len(fs))
	for i, f := range fs {
		out[i] = rowFilterToAPI(f)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListRowFilters200JSONResponse{
		Body:    PaginatedRowFilters{Data: &out, NextPageToken: optStr(npt)},
		Headers: GenListRowFilters200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateRowFilter implements the endpoint for creating a row filter on a table.
func (h *APIHandler) CreateRowFilter(ctx context.Context, req GenCreateRowFilterRequest) (GenCreateRowFilterResponse, error) {
	domReq := domain.CreateRowFilterRequest{
		TableID:   req.TableId,
		FilterSQL: req.Body.FilterSql,
	}
	if req.Body.Description != nil {
		domReq.Description = *req.Body.Description
	}
	result, err := h.rowFilters.Create(ctx, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenCreateRowFilter403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return GenCreateRowFilter400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenCreateRowFilter201JSONResponse{
		Body:    rowFilterToAPI(*result),
		Headers: GenCreateRowFilter201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteRowFilter implements the endpoint for deleting a row filter.
func (h *APIHandler) DeleteRowFilter(ctx context.Context, req GenDeleteRowFilterRequest) (GenDeleteRowFilterResponse, error) {
	if err := h.rowFilters.Delete(ctx, req.RowFilterId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenDeleteRowFilter403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GenDeleteRowFilter404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenBindRowFilter403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return GenBindRowFilter400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenBindRowFilter204Response{}, nil
}

// UnbindRowFilter implements the endpoint for unbinding a row filter from a principal.
func (h *APIHandler) UnbindRowFilter(ctx context.Context, req GenUnbindRowFilterRequest) (GenUnbindRowFilterResponse, error) {
	if err := h.rowFilters.Unbind(ctx, domain.BindRowFilterRequest{
		RowFilterID:   req.RowFilterId,
		PrincipalID:   req.Params.PrincipalId,
		PrincipalType: string(req.Params.PrincipalType),
	}); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenUnbindRowFilter403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GenUnbindRowFilter404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
		Body:    PaginatedColumnMasks{Data: &out, NextPageToken: optStr(npt)},
		Headers: GenListColumnMasks200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateColumnMask implements the endpoint for creating a column mask on a table.
func (h *APIHandler) CreateColumnMask(ctx context.Context, req GenCreateColumnMaskRequest) (GenCreateColumnMaskResponse, error) {
	domReq := domain.CreateColumnMaskRequest{
		TableID:        req.TableId,
		ColumnName:     req.Body.ColumnName,
		MaskExpression: req.Body.MaskExpression,
	}
	if req.Body.Description != nil {
		domReq.Description = *req.Body.Description
	}
	result, err := h.columnMasks.Create(ctx, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenCreateColumnMask403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return GenCreateColumnMask400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenCreateColumnMask201JSONResponse{
		Body:    columnMaskToAPI(*result),
		Headers: GenCreateColumnMask201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteColumnMask implements the endpoint for deleting a column mask.
func (h *APIHandler) DeleteColumnMask(ctx context.Context, req GenDeleteColumnMaskRequest) (GenDeleteColumnMaskResponse, error) {
	if err := h.columnMasks.Delete(ctx, req.ColumnMaskId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenDeleteColumnMask403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GenDeleteColumnMask404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenBindColumnMask403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return GenBindColumnMask400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenBindColumnMask204Response{}, nil
}

// UnbindColumnMask implements the endpoint for unbinding a column mask from a principal.
func (h *APIHandler) UnbindColumnMask(ctx context.Context, req GenUnbindColumnMaskRequest) (GenUnbindColumnMaskResponse, error) {
	if err := h.columnMasks.Unbind(ctx, domain.BindColumnMaskRequest{
		ColumnMaskID:  req.ColumnMaskId,
		PrincipalID:   req.Params.PrincipalId,
		PrincipalType: string(req.Params.PrincipalType),
	}); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenUnbindColumnMask403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenUnbindColumnMask204Response{}, nil
}
