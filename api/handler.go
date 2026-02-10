package api

import (
	"context"

	"duck-demo/domain"
	"duck-demo/internal/middleware"
	"duck-demo/internal/service"
)

// APIHandler implements the StrictServerInterface.
type APIHandler struct {
	query         *service.QueryService
	principals    *service.PrincipalService
	groups        *service.GroupService
	grants        *service.GrantService
	rowFilters    *service.RowFilterService
	columnMasks   *service.ColumnMaskService
	introspection *service.IntrospectionService
	audit         *service.AuditService
}

func NewHandler(
	query *service.QueryService,
	principals *service.PrincipalService,
	groups *service.GroupService,
	grants *service.GrantService,
	rowFilters *service.RowFilterService,
	columnMasks *service.ColumnMaskService,
	introspection *service.IntrospectionService,
	audit *service.AuditService,
) *APIHandler {
	return &APIHandler{
		query:         query,
		principals:    principals,
		groups:        groups,
		grants:        grants,
		rowFilters:    rowFilters,
		columnMasks:   columnMasks,
		introspection: introspection,
		audit:         audit,
	}
}

// Ensure Handler implements the interface.
var _ StrictServerInterface = (*APIHandler)(nil)

func (h *APIHandler) ExecuteQuery(ctx context.Context, req ExecuteQueryRequestObject) (ExecuteQueryResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.query.Execute(ctx, principal, req.Body.Sql)
	if err != nil {
		return ExecuteQuery403JSONResponse{Code: 403, Message: err.Error()}, nil
	}

	rows := make([][]interface{}, len(result.Rows))
	for i, r := range result.Rows {
		rows[i] = r
	}

	return ExecuteQuery200JSONResponse{
		Columns:  &result.Columns,
		Rows:     &rows,
		RowCount: &result.RowCount,
	}, nil
}

func (h *APIHandler) ListPrincipals(ctx context.Context, _ ListPrincipalsRequestObject) (ListPrincipalsResponseObject, error) {
	ps, err := h.principals.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make(ListPrincipals200JSONResponse, len(ps))
	for i, p := range ps {
		out[i] = principalToAPI(p)
	}
	return out, nil
}

func (h *APIHandler) CreatePrincipal(ctx context.Context, req CreatePrincipalRequestObject) (CreatePrincipalResponseObject, error) {
	p := &domain.Principal{
		Name: req.Body.Name,
	}
	if req.Body.Type != nil {
		p.Type = *req.Body.Type
	}
	if req.Body.IsAdmin != nil {
		p.IsAdmin = *req.Body.IsAdmin
	}
	result, err := h.principals.Create(ctx, p)
	if err != nil {
		return CreatePrincipal400JSONResponse{Code: 400, Message: err.Error()}, nil
	}
	return CreatePrincipal201JSONResponse(principalToAPI(*result)), nil
}

func (h *APIHandler) GetPrincipal(ctx context.Context, req GetPrincipalRequestObject) (GetPrincipalResponseObject, error) {
	p, err := h.principals.GetByID(ctx, req.Id)
	if err != nil {
		return GetPrincipal404JSONResponse{Code: 404, Message: err.Error()}, nil
	}
	return GetPrincipal200JSONResponse(principalToAPI(*p)), nil
}

func (h *APIHandler) DeletePrincipal(ctx context.Context, req DeletePrincipalRequestObject) (DeletePrincipalResponseObject, error) {
	if err := h.principals.Delete(ctx, req.Id); err != nil {
		return DeletePrincipal404JSONResponse{Code: 404, Message: err.Error()}, nil
	}
	return DeletePrincipal204Response{}, nil
}

func (h *APIHandler) SetAdmin(ctx context.Context, req SetAdminRequestObject) (SetAdminResponseObject, error) {
	if err := h.principals.SetAdmin(ctx, req.Id, req.Body.IsAdmin); err != nil {
		return SetAdmin404JSONResponse{Code: 404, Message: err.Error()}, nil
	}
	return SetAdmin204Response{}, nil
}

func (h *APIHandler) ListGroups(ctx context.Context, _ ListGroupsRequestObject) (ListGroupsResponseObject, error) {
	gs, err := h.groups.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make(ListGroups200JSONResponse, len(gs))
	for i, g := range gs {
		out[i] = groupToAPI(g)
	}
	return out, nil
}

func (h *APIHandler) CreateGroup(ctx context.Context, req CreateGroupRequestObject) (CreateGroupResponseObject, error) {
	g := &domain.Group{Name: req.Body.Name}
	if req.Body.Description != nil {
		g.Description = *req.Body.Description
	}
	result, err := h.groups.Create(ctx, g)
	if err != nil {
		return nil, err
	}
	return CreateGroup201JSONResponse(groupToAPI(*result)), nil
}

func (h *APIHandler) GetGroup(ctx context.Context, req GetGroupRequestObject) (GetGroupResponseObject, error) {
	g, err := h.groups.GetByID(ctx, req.Id)
	if err != nil {
		return GetGroup404JSONResponse{Code: 404, Message: err.Error()}, nil
	}
	return GetGroup200JSONResponse(groupToAPI(*g)), nil
}

func (h *APIHandler) DeleteGroup(ctx context.Context, req DeleteGroupRequestObject) (DeleteGroupResponseObject, error) {
	if err := h.groups.Delete(ctx, req.Id); err != nil {
		return nil, err
	}
	return DeleteGroup204Response{}, nil
}

func (h *APIHandler) ListGroupMembers(ctx context.Context, req ListGroupMembersRequestObject) (ListGroupMembersResponseObject, error) {
	ms, err := h.groups.ListMembers(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	out := make(ListGroupMembers200JSONResponse, len(ms))
	for i, m := range ms {
		out[i] = groupMemberToAPI(m, req.Id)
	}
	return out, nil
}

func (h *APIHandler) AddGroupMember(ctx context.Context, req AddGroupMemberRequestObject) (AddGroupMemberResponseObject, error) {
	if err := h.groups.AddMember(ctx, &domain.GroupMember{
		GroupID:    req.Id,
		MemberType: req.Body.MemberType,
		MemberID:   req.Body.MemberId,
	}); err != nil {
		return nil, err
	}
	return AddGroupMember204Response{}, nil
}

func (h *APIHandler) RemoveGroupMember(ctx context.Context, req RemoveGroupMemberRequestObject) (RemoveGroupMemberResponseObject, error) {
	if err := h.groups.RemoveMember(ctx, &domain.GroupMember{
		GroupID:    req.Id,
		MemberType: req.Body.MemberType,
		MemberID:   req.Body.MemberId,
	}); err != nil {
		return nil, err
	}
	return RemoveGroupMember204Response{}, nil
}

func (h *APIHandler) ListGrants(ctx context.Context, req ListGrantsRequestObject) (ListGrantsResponseObject, error) {
	var grants []domain.PrivilegeGrant
	var err error

	if req.Params.PrincipalId != nil && req.Params.PrincipalType != nil {
		grants, err = h.grants.ListForPrincipal(ctx, *req.Params.PrincipalId, *req.Params.PrincipalType)
	} else if req.Params.SecurableType != nil && req.Params.SecurableId != nil {
		grants, err = h.grants.ListForSecurable(ctx, *req.Params.SecurableType, *req.Params.SecurableId)
	} else {
		// Return empty list if no filter provided
		grants = []domain.PrivilegeGrant{}
	}
	if err != nil {
		return nil, err
	}

	out := make(ListGrants200JSONResponse, len(grants))
	for i, g := range grants {
		out[i] = grantToAPI(g)
	}
	return out, nil
}

func (h *APIHandler) GrantPrivilege(ctx context.Context, req GrantPrivilegeRequestObject) (GrantPrivilegeResponseObject, error) {
	g := &domain.PrivilegeGrant{
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: req.Body.PrincipalType,
		SecurableType: req.Body.SecurableType,
		SecurableID:   req.Body.SecurableId,
		Privilege:     req.Body.Privilege,
	}
	result, err := h.grants.Grant(ctx, g)
	if err != nil {
		return nil, err
	}
	return GrantPrivilege201JSONResponse(grantToAPI(*result)), nil
}

func (h *APIHandler) RevokePrivilege(ctx context.Context, req RevokePrivilegeRequestObject) (RevokePrivilegeResponseObject, error) {
	if err := h.grants.Revoke(ctx, &domain.PrivilegeGrant{
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: req.Body.PrincipalType,
		SecurableType: req.Body.SecurableType,
		SecurableID:   req.Body.SecurableId,
		Privilege:     req.Body.Privilege,
	}); err != nil {
		return nil, err
	}
	return RevokePrivilege204Response{}, nil
}

func (h *APIHandler) ListRowFilters(ctx context.Context, req ListRowFiltersRequestObject) (ListRowFiltersResponseObject, error) {
	fs, err := h.rowFilters.GetForTable(ctx, req.TableId)
	if err != nil {
		return nil, err
	}
	out := make(ListRowFilters200JSONResponse, len(fs))
	for i, f := range fs {
		out[i] = rowFilterToAPI(f)
	}
	return out, nil
}

func (h *APIHandler) CreateRowFilter(ctx context.Context, req CreateRowFilterRequestObject) (CreateRowFilterResponseObject, error) {
	f := &domain.RowFilter{
		TableID:   req.TableId,
		FilterSQL: req.Body.FilterSql,
	}
	if req.Body.Description != nil {
		f.Description = *req.Body.Description
	}
	result, err := h.rowFilters.Create(ctx, f)
	if err != nil {
		return nil, err
	}
	return CreateRowFilter201JSONResponse(rowFilterToAPI(*result)), nil
}

func (h *APIHandler) DeleteRowFilter(ctx context.Context, req DeleteRowFilterRequestObject) (DeleteRowFilterResponseObject, error) {
	if err := h.rowFilters.Delete(ctx, req.Id); err != nil {
		return nil, err
	}
	return DeleteRowFilter204Response{}, nil
}

func (h *APIHandler) BindRowFilter(ctx context.Context, req BindRowFilterRequestObject) (BindRowFilterResponseObject, error) {
	if err := h.rowFilters.Bind(ctx, &domain.RowFilterBinding{
		RowFilterID:   req.Id,
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: req.Body.PrincipalType,
	}); err != nil {
		return nil, err
	}
	return BindRowFilter204Response{}, nil
}

func (h *APIHandler) UnbindRowFilter(ctx context.Context, req UnbindRowFilterRequestObject) (UnbindRowFilterResponseObject, error) {
	if err := h.rowFilters.Unbind(ctx, &domain.RowFilterBinding{
		RowFilterID:   req.Id,
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: req.Body.PrincipalType,
	}); err != nil {
		return nil, err
	}
	return UnbindRowFilter204Response{}, nil
}

func (h *APIHandler) ListColumnMasks(ctx context.Context, req ListColumnMasksRequestObject) (ListColumnMasksResponseObject, error) {
	ms, err := h.columnMasks.GetForTable(ctx, req.TableId)
	if err != nil {
		return nil, err
	}
	out := make(ListColumnMasks200JSONResponse, len(ms))
	for i, m := range ms {
		out[i] = columnMaskToAPI(m)
	}
	return out, nil
}

func (h *APIHandler) CreateColumnMask(ctx context.Context, req CreateColumnMaskRequestObject) (CreateColumnMaskResponseObject, error) {
	m := &domain.ColumnMask{
		TableID:        req.TableId,
		ColumnName:     req.Body.ColumnName,
		MaskExpression: req.Body.MaskExpression,
	}
	if req.Body.Description != nil {
		m.Description = *req.Body.Description
	}
	result, err := h.columnMasks.Create(ctx, m)
	if err != nil {
		return nil, err
	}
	return CreateColumnMask201JSONResponse(columnMaskToAPI(*result)), nil
}

func (h *APIHandler) DeleteColumnMask(ctx context.Context, req DeleteColumnMaskRequestObject) (DeleteColumnMaskResponseObject, error) {
	if err := h.columnMasks.Delete(ctx, req.Id); err != nil {
		return nil, err
	}
	return DeleteColumnMask204Response{}, nil
}

func (h *APIHandler) BindColumnMask(ctx context.Context, req BindColumnMaskRequestObject) (BindColumnMaskResponseObject, error) {
	seeOriginal := false
	if req.Body.SeeOriginal != nil {
		seeOriginal = *req.Body.SeeOriginal
	}
	if err := h.columnMasks.Bind(ctx, &domain.ColumnMaskBinding{
		ColumnMaskID:  req.Id,
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: req.Body.PrincipalType,
		SeeOriginal:   seeOriginal,
	}); err != nil {
		return nil, err
	}
	return BindColumnMask204Response{}, nil
}

func (h *APIHandler) UnbindColumnMask(ctx context.Context, req UnbindColumnMaskRequestObject) (UnbindColumnMaskResponseObject, error) {
	if err := h.columnMasks.Unbind(ctx, &domain.ColumnMaskBinding{
		ColumnMaskID:  req.Id,
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: req.Body.PrincipalType,
	}); err != nil {
		return nil, err
	}
	return UnbindColumnMask204Response{}, nil
}

func (h *APIHandler) ListSchemas(ctx context.Context, _ ListSchemasRequestObject) (ListSchemasResponseObject, error) {
	ss, err := h.introspection.ListSchemas(ctx)
	if err != nil {
		return nil, err
	}
	out := make(ListSchemas200JSONResponse, len(ss))
	for i, s := range ss {
		out[i] = schemaToAPI(s)
	}
	return out, nil
}

func (h *APIHandler) ListTables(ctx context.Context, req ListTablesRequestObject) (ListTablesResponseObject, error) {
	ts, err := h.introspection.ListTables(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	out := make(ListTables200JSONResponse, len(ts))
	for i, t := range ts {
		out[i] = tableToAPI(t)
	}
	return out, nil
}

func (h *APIHandler) ListColumns(ctx context.Context, req ListColumnsRequestObject) (ListColumnsResponseObject, error) {
	cs, err := h.introspection.ListColumns(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	out := make(ListColumns200JSONResponse, len(cs))
	for i, c := range cs {
		out[i] = columnToAPI(c)
	}
	return out, nil
}

func (h *APIHandler) ListAuditLogs(ctx context.Context, req ListAuditLogsRequestObject) (ListAuditLogsResponseObject, error) {
	filter := domain.AuditFilter{
		PrincipalName: req.Params.PrincipalName,
		Action:        req.Params.Action,
		Status:        req.Params.Status,
		Limit:         50,
		Offset:        0,
	}
	if req.Params.Limit != nil {
		filter.Limit = *req.Params.Limit
	}
	if req.Params.Offset != nil {
		filter.Offset = *req.Params.Offset
	}

	entries, total, err := h.audit.List(ctx, filter)
	if err != nil {
		return nil, err
	}

	data := make([]AuditEntry, len(entries))
	for i, e := range entries {
		data[i] = auditEntryToAPI(e)
	}

	totalInt := int64(total)
	limit := filter.Limit
	offset := filter.Offset
	return ListAuditLogs200JSONResponse{
		Data:   &data,
		Total:  &totalInt,
		Limit:  &limit,
		Offset: &offset,
	}, nil
}

// --- Mapping helpers ---

func principalToAPI(p domain.Principal) Principal {
	t := p.CreatedAt
	return Principal{
		Id:        &p.ID,
		Name:      &p.Name,
		Type:      &p.Type,
		IsAdmin:   &p.IsAdmin,
		CreatedAt: &t,
	}
}

func groupToAPI(g domain.Group) Group {
	t := g.CreatedAt
	return Group{
		Id:          &g.ID,
		Name:        &g.Name,
		Description: &g.Description,
		CreatedAt:   &t,
	}
}

func groupMemberToAPI(m domain.GroupMember, groupID int64) GroupMember {
	return GroupMember{
		GroupId:    &groupID,
		MemberType: &m.MemberType,
		MemberId:   &m.MemberID,
	}
}

func grantToAPI(g domain.PrivilegeGrant) PrivilegeGrant {
	t := g.GrantedAt
	return PrivilegeGrant{
		Id:            &g.ID,
		PrincipalId:   &g.PrincipalID,
		PrincipalType: &g.PrincipalType,
		SecurableType: &g.SecurableType,
		SecurableId:   &g.SecurableID,
		Privilege:     &g.Privilege,
		GrantedBy:     g.GrantedBy,
		GrantedAt:     &t,
	}
}

func rowFilterToAPI(f domain.RowFilter) RowFilter {
	t := f.CreatedAt
	return RowFilter{
		Id:          &f.ID,
		TableId:     &f.TableID,
		FilterSql:   &f.FilterSQL,
		Description: &f.Description,
		CreatedAt:   &t,
	}
}

func columnMaskToAPI(m domain.ColumnMask) ColumnMask {
	t := m.CreatedAt
	return ColumnMask{
		Id:             &m.ID,
		TableId:        &m.TableID,
		ColumnName:     &m.ColumnName,
		MaskExpression: &m.MaskExpression,
		Description:    &m.Description,
		CreatedAt:      &t,
	}
}

func schemaToAPI(s domain.Schema) Schema {
	return Schema{
		Id:   &s.ID,
		Name: &s.Name,
	}
}

func tableToAPI(t domain.Table) Table {
	return Table{
		Id:       &t.ID,
		SchemaId: &t.SchemaID,
		Name:     &t.Name,
	}
}

func columnToAPI(c domain.Column) Column {
	return Column{
		Id:      &c.ID,
		TableId: &c.TableID,
		Name:    &c.Name,
		Type:    &c.Type,
	}
}

func auditEntryToAPI(e domain.AuditEntry) AuditEntry {
	t := e.CreatedAt
	return AuditEntry{
		Id:             &e.ID,
		PrincipalName:  &e.PrincipalName,
		Action:         &e.Action,
		StatementType:  e.StatementType,
		OriginalSql:    e.OriginalSQL,
		RewrittenSql:   e.RewrittenSQL,
		TablesAccessed: &e.TablesAccessed,
		Status:         &e.Status,
		ErrorMessage:   e.ErrorMessage,
		DurationMs:     e.DurationMs,
		CreatedAt:      &t,
	}
}
