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
func (h *APIHandler) ListPrincipals(ctx context.Context, req ListPrincipalsRequestObject) (ListPrincipalsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	ps, total, err := h.principals.List(ctx, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return ListPrincipals403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]Principal, len(ps))
	for i, p := range ps {
		out[i] = principalToAPI(p)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListPrincipals200JSONResponse{
		Body:    PaginatedPrincipals{Data: &out, NextPageToken: optStr(npt)},
		Headers: ListPrincipals200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreatePrincipal implements the endpoint for creating a new principal.
func (h *APIHandler) CreatePrincipal(ctx context.Context, req CreatePrincipalRequestObject) (CreatePrincipalResponseObject, error) {
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
			return CreatePrincipal403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreatePrincipal400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreatePrincipal409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreatePrincipal201JSONResponse{
		Body:    principalToAPI(*result),
		Headers: CreatePrincipal201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetPrincipal implements the endpoint for retrieving a principal by ID.
func (h *APIHandler) GetPrincipal(ctx context.Context, req GetPrincipalRequestObject) (GetPrincipalResponseObject, error) {
	p, err := h.principals.GetByID(ctx, req.PrincipalId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetPrincipal404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetPrincipal200JSONResponse{
		Body:    principalToAPI(*p),
		Headers: GetPrincipal200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeletePrincipal implements the endpoint for deleting a principal by ID.
func (h *APIHandler) DeletePrincipal(ctx context.Context, req DeletePrincipalRequestObject) (DeletePrincipalResponseObject, error) {
	if err := h.principals.Delete(ctx, req.PrincipalId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeletePrincipal403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeletePrincipal404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeletePrincipal204Response{}, nil
}

// UpdatePrincipalAdmin implements the endpoint for updating a principal's admin status.
func (h *APIHandler) UpdatePrincipalAdmin(ctx context.Context, req UpdatePrincipalAdminRequestObject) (UpdatePrincipalAdminResponseObject, error) {
	if err := h.principals.SetAdmin(ctx, req.PrincipalId, req.Body.IsAdmin); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdatePrincipalAdmin403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdatePrincipalAdmin404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdatePrincipalAdmin204Response{}, nil
}

// === Groups ===

// ListGroups implements the endpoint for listing all groups.
func (h *APIHandler) ListGroups(ctx context.Context, req ListGroupsRequestObject) (ListGroupsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	gs, total, err := h.groups.List(ctx, page)
	if err != nil {
		return nil, err
	}
	out := make([]Group, len(gs))
	for i, g := range gs {
		out[i] = groupToAPI(g)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListGroups200JSONResponse{
		Body:    PaginatedGroups{Data: &out, NextPageToken: optStr(npt)},
		Headers: ListGroups200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateGroup implements the endpoint for creating a new group.
func (h *APIHandler) CreateGroup(ctx context.Context, req CreateGroupRequestObject) (CreateGroupResponseObject, error) {
	domReq := domain.CreateGroupRequest{Name: req.Body.Name}
	if req.Body.Description != nil {
		domReq.Description = *req.Body.Description
	}
	result, err := h.groups.Create(ctx, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateGroup403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreateGroup201JSONResponse{
		Body:    groupToAPI(*result),
		Headers: CreateGroup201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetGroup implements the endpoint for retrieving a group by ID.
func (h *APIHandler) GetGroup(ctx context.Context, req GetGroupRequestObject) (GetGroupResponseObject, error) {
	g, err := h.groups.GetByID(ctx, req.GroupId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetGroup404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetGroup200JSONResponse{
		Body:    groupToAPI(*g),
		Headers: GetGroup200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteGroup implements the endpoint for deleting a group by ID.
func (h *APIHandler) DeleteGroup(ctx context.Context, req DeleteGroupRequestObject) (DeleteGroupResponseObject, error) {
	if err := h.groups.Delete(ctx, req.GroupId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteGroup403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteGroup204Response{}, nil
}

// ListGroupMembers implements the endpoint for listing members of a group.
func (h *APIHandler) ListGroupMembers(ctx context.Context, req ListGroupMembersRequestObject) (ListGroupMembersResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	ms, total, err := h.groups.ListMembers(ctx, req.GroupId, page)
	if err != nil {
		return nil, err
	}
	out := make([]GroupMember, len(ms))
	for i, m := range ms {
		out[i] = groupMemberToAPI(m, req.GroupId)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListGroupMembers200JSONResponse{
		Body:    PaginatedGroupMembers{Data: &out, NextPageToken: optStr(npt)},
		Headers: ListGroupMembers200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateGroupMember implements the endpoint for adding a member to a group.
func (h *APIHandler) CreateGroupMember(ctx context.Context, req CreateGroupMemberRequestObject) (CreateGroupMemberResponseObject, error) {
	if err := h.groups.AddMember(ctx, domain.AddGroupMemberRequest{
		GroupID:    req.GroupId,
		MemberType: string(req.Body.MemberType),
		MemberID:   req.Body.MemberId,
	}); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateGroupMember403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreateGroupMember204Response{}, nil
}

// DeleteGroupMember implements the endpoint for removing a member from a group.
func (h *APIHandler) DeleteGroupMember(ctx context.Context, req DeleteGroupMemberRequestObject) (DeleteGroupMemberResponseObject, error) {
	if err := h.groups.RemoveMember(ctx, domain.RemoveGroupMemberRequest{
		GroupID:    req.GroupId,
		MemberType: string(req.Params.MemberType),
		MemberID:   req.Params.MemberId,
	}); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteGroupMember403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteGroupMember204Response{}, nil
}

// === Grants ===

// ListGrants implements the endpoint for listing privilege grants filtered by principal or securable.
func (h *APIHandler) ListGrants(ctx context.Context, req ListGrantsRequestObject) (ListGrantsResponseObject, error) {
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
		return ListGrants400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: "either principal_id+principal_type or securable_type+securable_id query parameters are required"}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}
	if err != nil {
		return nil, err
	}

	out := make([]PrivilegeGrant, len(grants))
	for i, g := range grants {
		out[i] = grantToAPI(g)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListGrants200JSONResponse{
		Body:    PaginatedGrants{Data: &out, NextPageToken: optStr(npt)},
		Headers: ListGrants200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateGrant implements the endpoint for granting a privilege to a principal.
func (h *APIHandler) CreateGrant(ctx context.Context, req CreateGrantRequestObject) (CreateGrantResponseObject, error) {
	domReq := domain.CreateGrantRequest{
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: string(req.Body.PrincipalType),
		SecurableType: req.Body.SecurableType,
		SecurableID:   req.Body.SecurableId,
		Privilege:     req.Body.Privilege,
	}
	result, err := h.grants.Grant(ctx, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateGrant403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreateGrant201JSONResponse{
		Body:    grantToAPI(*result),
		Headers: CreateGrant201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteGrant implements the endpoint for revoking a privilege from a principal.
func (h *APIHandler) DeleteGrant(ctx context.Context, req DeleteGrantRequestObject) (DeleteGrantResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.grants.Revoke(ctx, principal, req.GrantId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteGrant403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteGrant404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteGrant204Response{}, nil
}

// === Row Filters ===

// ListRowFilters implements the endpoint for listing row filters for a table.
func (h *APIHandler) ListRowFilters(ctx context.Context, req ListRowFiltersRequestObject) (ListRowFiltersResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	fs, total, err := h.rowFilters.GetForTable(ctx, req.TableId, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return ListRowFilters403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]RowFilter, len(fs))
	for i, f := range fs {
		out[i] = rowFilterToAPI(f)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListRowFilters200JSONResponse{
		Body:    PaginatedRowFilters{Data: &out, NextPageToken: optStr(npt)},
		Headers: ListRowFilters200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateRowFilter implements the endpoint for creating a row filter on a table.
func (h *APIHandler) CreateRowFilter(ctx context.Context, req CreateRowFilterRequestObject) (CreateRowFilterResponseObject, error) {
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
			return CreateRowFilter403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateRowFilter400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreateRowFilter201JSONResponse{
		Body:    rowFilterToAPI(*result),
		Headers: CreateRowFilter201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteRowFilter implements the endpoint for deleting a row filter.
func (h *APIHandler) DeleteRowFilter(ctx context.Context, req DeleteRowFilterRequestObject) (DeleteRowFilterResponseObject, error) {
	if err := h.rowFilters.Delete(ctx, req.RowFilterId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteRowFilter403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteRowFilter404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteRowFilter204Response{}, nil
}

// BindRowFilter implements the endpoint for binding a row filter to a principal.
func (h *APIHandler) BindRowFilter(ctx context.Context, req BindRowFilterRequestObject) (BindRowFilterResponseObject, error) {
	if err := h.rowFilters.Bind(ctx, domain.BindRowFilterRequest{
		RowFilterID:   req.RowFilterId,
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: string(req.Body.PrincipalType),
	}); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return BindRowFilter403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return BindRowFilter400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return BindRowFilter204Response{}, nil
}

// UnbindRowFilter implements the endpoint for unbinding a row filter from a principal.
func (h *APIHandler) UnbindRowFilter(ctx context.Context, req UnbindRowFilterRequestObject) (UnbindRowFilterResponseObject, error) {
	if err := h.rowFilters.Unbind(ctx, domain.BindRowFilterRequest{
		RowFilterID:   req.RowFilterId,
		PrincipalID:   req.Params.PrincipalId,
		PrincipalType: string(req.Params.PrincipalType),
	}); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UnbindRowFilter403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UnbindRowFilter404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UnbindRowFilter204Response{}, nil
}

// === Column Masks ===

// ListColumnMasks implements the endpoint for listing column masks for a table.
func (h *APIHandler) ListColumnMasks(ctx context.Context, req ListColumnMasksRequestObject) (ListColumnMasksResponseObject, error) {
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
	return ListColumnMasks200JSONResponse{
		Body:    PaginatedColumnMasks{Data: &out, NextPageToken: optStr(npt)},
		Headers: ListColumnMasks200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateColumnMask implements the endpoint for creating a column mask on a table.
func (h *APIHandler) CreateColumnMask(ctx context.Context, req CreateColumnMaskRequestObject) (CreateColumnMaskResponseObject, error) {
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
			return CreateColumnMask403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreateColumnMask201JSONResponse{
		Body:    columnMaskToAPI(*result),
		Headers: CreateColumnMask201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteColumnMask implements the endpoint for deleting a column mask.
func (h *APIHandler) DeleteColumnMask(ctx context.Context, req DeleteColumnMaskRequestObject) (DeleteColumnMaskResponseObject, error) {
	if err := h.columnMasks.Delete(ctx, req.ColumnMaskId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteColumnMask403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteColumnMask204Response{}, nil
}

// BindColumnMask implements the endpoint for binding a column mask to a principal.
func (h *APIHandler) BindColumnMask(ctx context.Context, req BindColumnMaskRequestObject) (BindColumnMaskResponseObject, error) {
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
			return BindColumnMask403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return BindColumnMask204Response{}, nil
}

// UnbindColumnMask implements the endpoint for unbinding a column mask from a principal.
func (h *APIHandler) UnbindColumnMask(ctx context.Context, req UnbindColumnMaskRequestObject) (UnbindColumnMaskResponseObject, error) {
	if err := h.columnMasks.Unbind(ctx, domain.BindColumnMaskRequest{
		ColumnMaskID:  req.ColumnMaskId,
		PrincipalID:   req.Params.PrincipalId,
		PrincipalType: string(req.Params.PrincipalType),
	}); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UnbindColumnMask403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UnbindColumnMask204Response{}, nil
}
