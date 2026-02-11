package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
	"duck-demo/internal/service"
)

// APIHandler implements the StrictServerInterface.
type APIHandler struct {
	query             *service.QueryService
	principals        *service.PrincipalService
	groups            *service.GroupService
	grants            *service.GrantService
	rowFilters        *service.RowFilterService
	columnMasks       *service.ColumnMaskService
	introspection     *service.IntrospectionService
	audit             *service.AuditService
	manifest          *service.ManifestService
	catalog           *service.CatalogService
	queryHistory      *service.QueryHistoryService
	lineage           *service.LineageService
	search            *service.SearchService
	tags              *service.TagService
	views             *service.ViewService
	ingestion         *service.IngestionService
	storageCreds      *service.StorageCredentialService
	externalLocations *service.ExternalLocationService
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
	manifest *service.ManifestService,
	catalog *service.CatalogService,
	queryHistory *service.QueryHistoryService,
	lineage *service.LineageService,
	search *service.SearchService,
	tags *service.TagService,
	views *service.ViewService,
	ingestion *service.IngestionService,
	storageCreds *service.StorageCredentialService,
	externalLocations *service.ExternalLocationService,
) *APIHandler {
	return &APIHandler{
		query:             query,
		principals:        principals,
		groups:            groups,
		grants:            grants,
		rowFilters:        rowFilters,
		columnMasks:       columnMasks,
		introspection:     introspection,
		audit:             audit,
		manifest:          manifest,
		catalog:           catalog,
		queryHistory:      queryHistory,
		lineage:           lineage,
		search:            search,
		tags:              tags,
		views:             views,
		ingestion:         ingestion,
		storageCreds:      storageCreds,
		externalLocations: externalLocations,
	}
}

// Ensure Handler implements the interface.
var _ StrictServerInterface = (*APIHandler)(nil)

// --- helpers ---

// pageFromParams extracts a PageRequest from optional max_results/page_token params.
func pageFromParams(maxResults *MaxResults, pageToken *PageToken) domain.PageRequest {
	p := domain.PageRequest{}
	if maxResults != nil {
		p.MaxResults = *maxResults
	}
	if pageToken != nil {
		p.PageToken = *pageToken
	}
	return p
}

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

func (h *APIHandler) GetManifest(ctx context.Context, req GetManifestRequestObject) (GetManifestResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	schemaName := "main"
	if req.Body.Schema != nil {
		schemaName = *req.Body.Schema
	}

	result, err := h.manifest.GetManifest(ctx, principal, schemaName, req.Body.Table)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return GetManifest404JSONResponse{Code: 404, Message: err.Error()}, nil
		case *domain.AccessDeniedError:
			return GetManifest403JSONResponse{Code: 403, Message: err.Error()}, nil
		default:
			return GetManifest403JSONResponse{Code: 403, Message: err.Error()}, nil
		}
	}

	cols := make([]ManifestColumn, len(result.Columns))
	for i, c := range result.Columns {
		name := c.Name
		typ := c.Type
		cols[i] = ManifestColumn{Name: &name, Type: &typ}
	}

	return GetManifest200JSONResponse{
		Table:       &result.Table,
		Schema:      &result.Schema,
		Columns:     &cols,
		Files:       &result.Files,
		RowFilters:  &result.RowFilters,
		ColumnMasks: &result.ColumnMasks,
		ExpiresAt:   &result.ExpiresAt,
	}, nil
}

// === Principals ===

func (h *APIHandler) ListPrincipals(ctx context.Context, req ListPrincipalsRequestObject) (ListPrincipalsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	ps, total, err := h.principals.List(ctx, page)
	if err != nil {
		return nil, err
	}
	out := make([]Principal, len(ps))
	for i, p := range ps {
		out[i] = principalToAPI(p)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListPrincipals200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
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

// === Groups ===

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
	return ListGroups200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
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
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	ms, total, err := h.groups.ListMembers(ctx, req.Id, page)
	if err != nil {
		return nil, err
	}
	out := make([]GroupMember, len(ms))
	for i, m := range ms {
		out[i] = groupMemberToAPI(m, req.Id)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListGroupMembers200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
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

// === Grants ===

func (h *APIHandler) ListGrants(ctx context.Context, req ListGrantsRequestObject) (ListGrantsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	var grants []domain.PrivilegeGrant
	var total int64
	var err error

	if req.Params.PrincipalId != nil && req.Params.PrincipalType != nil {
		grants, total, err = h.grants.ListForPrincipal(ctx, *req.Params.PrincipalId, *req.Params.PrincipalType, page)
	} else if req.Params.SecurableType != nil && req.Params.SecurableId != nil {
		grants, total, err = h.grants.ListForSecurable(ctx, *req.Params.SecurableType, *req.Params.SecurableId, page)
	} else {
		grants = []domain.PrivilegeGrant{}
	}
	if err != nil {
		return nil, err
	}

	out := make([]PrivilegeGrant, len(grants))
	for i, g := range grants {
		out[i] = grantToAPI(g)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListGrants200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
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

// === Row Filters ===

func (h *APIHandler) ListRowFilters(ctx context.Context, req ListRowFiltersRequestObject) (ListRowFiltersResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	fs, total, err := h.rowFilters.GetForTable(ctx, req.TableId, page)
	if err != nil {
		return nil, err
	}
	out := make([]RowFilter, len(fs))
	for i, f := range fs {
		out[i] = rowFilterToAPI(f)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListRowFilters200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
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

// === Column Masks ===

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
	return ListColumnMasks200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
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

// === Introspection (deprecated) ===

func (h *APIHandler) ListSchemas(ctx context.Context, req ListSchemasRequestObject) (ListSchemasResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	ss, total, err := h.introspection.ListSchemas(ctx, page)
	if err != nil {
		return nil, err
	}
	out := make([]Schema, len(ss))
	for i, s := range ss {
		out[i] = schemaToAPI(s)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListSchemas200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) ListTables(ctx context.Context, req ListTablesRequestObject) (ListTablesResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	ts, total, err := h.introspection.ListTables(ctx, req.Id, page)
	if err != nil {
		return nil, err
	}
	out := make([]Table, len(ts))
	for i, t := range ts {
		out[i] = tableToAPI(t)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListTables200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) ListColumns(ctx context.Context, req ListColumnsRequestObject) (ListColumnsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	cs, total, err := h.introspection.ListColumns(ctx, req.Id, page)
	if err != nil {
		return nil, err
	}
	out := make([]Column, len(cs))
	for i, c := range cs {
		out[i] = columnToAPI(c)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListColumns200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
}

// === Audit Logs ===

func (h *APIHandler) ListAuditLogs(ctx context.Context, req ListAuditLogsRequestObject) (ListAuditLogsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	filter := domain.AuditFilter{
		PrincipalName: req.Params.PrincipalName,
		Action:        req.Params.Action,
		Status:        req.Params.Status,
		Page:          page,
	}

	entries, total, err := h.audit.List(ctx, filter)
	if err != nil {
		return nil, err
	}

	data := make([]AuditEntry, len(entries))
	for i, e := range entries {
		data[i] = auditEntryToAPI(e)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListAuditLogs200JSONResponse{Data: &data, NextPageToken: optStr(npt)}, nil
}

// === Catalog Management ===

func (h *APIHandler) GetCatalog(ctx context.Context, _ GetCatalogRequestObject) (GetCatalogResponseObject, error) {
	info, err := h.catalog.GetCatalogInfo(ctx)
	if err != nil {
		return nil, err
	}
	return GetCatalog200JSONResponse(catalogInfoToAPI(*info)), nil
}

func (h *APIHandler) UpdateCatalog(ctx context.Context, req UpdateCatalogRequestObject) (UpdateCatalogResponseObject, error) {
	domReq := domain.UpdateCatalogRequest{}
	if req.Body.Comment != nil {
		domReq.Comment = req.Body.Comment
	}

	result, err := h.catalog.UpdateCatalog(ctx, domReq)
	if err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return UpdateCatalog403JSONResponse{Code: 403, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateCatalog200JSONResponse(catalogInfoToAPI(*result)), nil
}

func (h *APIHandler) ListCatalogSchemas(ctx context.Context, req ListCatalogSchemasRequestObject) (ListCatalogSchemasResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	schemas, total, err := h.catalog.ListSchemas(ctx, page)
	if err != nil {
		return nil, err
	}
	out := make([]SchemaDetail, len(schemas))
	for i, s := range schemas {
		out[i] = schemaDetailToAPI(s)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListCatalogSchemas200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) CreateSchema(ctx context.Context, req CreateSchemaRequestObject) (CreateSchemaResponseObject, error) {
	domReq := domain.CreateSchemaRequest{
		Name: req.Body.Name,
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}
	if req.Body.Properties != nil {
		domReq.Properties = *req.Body.Properties
	}
	if req.Body.LocationName != nil {
		domReq.LocationName = *req.Body.LocationName
	}

	result, err := h.catalog.CreateSchema(ctx, domReq)
	if err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return CreateSchema403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.ValidationError:
			return CreateSchema400JSONResponse{Code: 400, Message: err.Error()}, nil
		case *domain.ConflictError:
			return CreateSchema409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return CreateSchema400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateSchema201JSONResponse(schemaDetailToAPI(*result)), nil
}

func (h *APIHandler) GetSchemaByName(ctx context.Context, req GetSchemaByNameRequestObject) (GetSchemaByNameResponseObject, error) {
	result, err := h.catalog.GetSchema(ctx, req.SchemaName)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return GetSchemaByName404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetSchemaByName200JSONResponse(schemaDetailToAPI(*result)), nil
}

func (h *APIHandler) UpdateSchemaMetadata(ctx context.Context, req UpdateSchemaMetadataRequestObject) (UpdateSchemaMetadataResponseObject, error) {
	var props map[string]string
	if req.Body.Properties != nil {
		props = *req.Body.Properties
	}

	result, err := h.catalog.UpdateSchema(ctx, req.SchemaName, req.Body.Comment, props)
	if err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return UpdateSchemaMetadata403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return UpdateSchemaMetadata404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateSchemaMetadata200JSONResponse(schemaDetailToAPI(*result)), nil
}

func (h *APIHandler) DeleteSchema(ctx context.Context, req DeleteSchemaRequestObject) (DeleteSchemaResponseObject, error) {
	force := false
	if req.Params.Force != nil {
		force = *req.Params.Force
	}

	if err := h.catalog.DeleteSchema(ctx, req.SchemaName, force); err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return DeleteSchema403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return DeleteSchema404JSONResponse{Code: 404, Message: err.Error()}, nil
		case *domain.ConflictError:
			return DeleteSchema403JSONResponse{Code: 403, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteSchema204Response{}, nil
}

func (h *APIHandler) ListCatalogTables(ctx context.Context, req ListCatalogTablesRequestObject) (ListCatalogTablesResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	tables, total, err := h.catalog.ListTables(ctx, req.SchemaName, page)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return ListCatalogTables404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	out := make([]TableDetail, len(tables))
	for i, t := range tables {
		out[i] = tableDetailToAPI(t)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListCatalogTables200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) CreateCatalogTable(ctx context.Context, req CreateCatalogTableRequestObject) (CreateCatalogTableResponseObject, error) {
	var cols []domain.CreateColumnDef
	if req.Body.Columns != nil {
		cols = make([]domain.CreateColumnDef, len(*req.Body.Columns))
		for i, c := range *req.Body.Columns {
			cols[i] = domain.CreateColumnDef{Name: c.Name, Type: c.Type}
		}
	}
	domReq := domain.CreateTableRequest{
		Name:    req.Body.Name,
		Columns: cols,
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}
	if req.Body.TableType != nil {
		domReq.TableType = string(*req.Body.TableType)
	}
	if req.Body.SourcePath != nil {
		domReq.SourcePath = *req.Body.SourcePath
	}
	if req.Body.FileFormat != nil {
		domReq.FileFormat = string(*req.Body.FileFormat)
	}
	if req.Body.LocationName != nil {
		domReq.LocationName = *req.Body.LocationName
	}

	result, err := h.catalog.CreateTable(ctx, req.SchemaName, domReq)
	if err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return CreateCatalogTable403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.ValidationError:
			return CreateCatalogTable400JSONResponse{Code: 400, Message: err.Error()}, nil
		case *domain.ConflictError:
			return CreateCatalogTable409JSONResponse{Code: 409, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return CreateCatalogTable400JSONResponse{Code: 400, Message: err.Error()}, nil
		default:
			return CreateCatalogTable400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateCatalogTable201JSONResponse(tableDetailToAPI(*result)), nil
}

func (h *APIHandler) GetTableByName(ctx context.Context, req GetTableByNameRequestObject) (GetTableByNameResponseObject, error) {
	result, err := h.catalog.GetTable(ctx, req.SchemaName, req.TableName)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return GetTableByName404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetTableByName200JSONResponse(tableDetailToAPI(*result)), nil
}

func (h *APIHandler) UpdateTableMetadata(ctx context.Context, req UpdateTableMetadataRequestObject) (UpdateTableMetadataResponseObject, error) {
	domReq := domain.UpdateTableRequest{}
	if req.Body.Comment != nil {
		domReq.Comment = req.Body.Comment
	}
	if req.Body.Properties != nil {
		domReq.Properties = *req.Body.Properties
	}
	if req.Body.Owner != nil {
		domReq.Owner = req.Body.Owner
	}

	result, err := h.catalog.UpdateTable(ctx, req.SchemaName, req.TableName, domReq)
	if err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return UpdateTableMetadata403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return UpdateTableMetadata404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateTableMetadata200JSONResponse(tableDetailToAPI(*result)), nil
}

func (h *APIHandler) DropTable(ctx context.Context, req DropTableRequestObject) (DropTableResponseObject, error) {
	if err := h.catalog.DeleteTable(ctx, req.SchemaName, req.TableName); err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return DropTable403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return DropTable404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DropTable204Response{}, nil
}

func (h *APIHandler) ListTableColumns(ctx context.Context, req ListTableColumnsRequestObject) (ListTableColumnsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	cols, total, err := h.catalog.ListColumns(ctx, req.SchemaName, req.TableName, page)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return ListTableColumns404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	out := make([]ColumnDetail, len(cols))
	for i, c := range cols {
		out[i] = columnDetailToAPI(c)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListTableColumns200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) UpdateColumnMetadata(ctx context.Context, req UpdateColumnMetadataRequestObject) (UpdateColumnMetadataResponseObject, error) {
	domReq := domain.UpdateColumnRequest{}
	if req.Body.Comment != nil {
		domReq.Comment = req.Body.Comment
	}
	if req.Body.Properties != nil {
		domReq.Properties = *req.Body.Properties
	}

	result, err := h.catalog.UpdateColumn(ctx, req.SchemaName, req.TableName, req.ColumnName, domReq)
	if err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return UpdateColumnMetadata403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return UpdateColumnMetadata404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateColumnMetadata200JSONResponse(columnDetailToAPI(*result)), nil
}

func (h *APIHandler) ProfileTable(ctx context.Context, req ProfileTableRequestObject) (ProfileTableResponseObject, error) {
	stats, err := h.catalog.ProfileTable(ctx, req.SchemaName, req.TableName)
	if err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return ProfileTable403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return ProfileTable404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return ProfileTable200JSONResponse(tableStatisticsToAPI(stats)), nil
}

func (h *APIHandler) GetMetastoreSummary(ctx context.Context, _ GetMetastoreSummaryRequestObject) (GetMetastoreSummaryResponseObject, error) {
	summary, err := h.catalog.GetMetastoreSummary(ctx)
	if err != nil {
		return nil, err
	}
	return GetMetastoreSummary200JSONResponse{
		CatalogName:    &summary.CatalogName,
		MetastoreType:  &summary.MetastoreType,
		StorageBackend: &summary.StorageBackend,
		DataPath:       &summary.DataPath,
		SchemaCount:    &summary.SchemaCount,
		TableCount:     &summary.TableCount,
	}, nil
}

// === Query History ===

func (h *APIHandler) ListQueryHistory(ctx context.Context, req ListQueryHistoryRequestObject) (ListQueryHistoryResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	filter := domain.QueryHistoryFilter{
		PrincipalName: req.Params.PrincipalName,
		Status:        req.Params.Status,
		From:          req.Params.From,
		To:            req.Params.To,
		Page:          page,
	}

	entries, total, err := h.queryHistory.List(ctx, filter)
	if err != nil {
		return nil, err
	}

	data := make([]QueryHistoryEntry, len(entries))
	for i, e := range entries {
		data[i] = queryHistoryEntryToAPI(e)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListQueryHistory200JSONResponse{Data: &data, NextPageToken: optStr(npt)}, nil
}

// === Search ===

func (h *APIHandler) SearchCatalog(ctx context.Context, req SearchCatalogRequestObject) (SearchCatalogResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)

	results, total, err := h.search.Search(ctx, req.Params.Query, req.Params.Type, page)
	if err != nil {
		return nil, err
	}

	data := make([]SearchResult, len(results))
	for i, r := range results {
		data[i] = searchResultToAPI(r)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return SearchCatalog200JSONResponse{Data: &data, NextPageToken: optStr(npt)}, nil
}

// === Lineage ===

func (h *APIHandler) GetTableLineage(ctx context.Context, req GetTableLineageRequestObject) (GetTableLineageResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	tableName := req.SchemaName + "." + req.TableName

	node, err := h.lineage.GetFullLineage(ctx, tableName, page)
	if err != nil {
		return nil, err
	}

	upstream := make([]LineageEdge, len(node.Upstream))
	for i, e := range node.Upstream {
		upstream[i] = lineageEdgeToAPI(e)
	}
	downstream := make([]LineageEdge, len(node.Downstream))
	for i, e := range node.Downstream {
		downstream[i] = lineageEdgeToAPI(e)
	}

	return GetTableLineage200JSONResponse{
		TableName:  &node.TableName,
		Upstream:   &upstream,
		Downstream: &downstream,
	}, nil
}

func (h *APIHandler) GetUpstreamLineage(ctx context.Context, req GetUpstreamLineageRequestObject) (GetUpstreamLineageResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	tableName := req.SchemaName + "." + req.TableName

	edges, total, err := h.lineage.GetUpstream(ctx, tableName, page)
	if err != nil {
		return nil, err
	}

	data := make([]LineageEdge, len(edges))
	for i, e := range edges {
		data[i] = lineageEdgeToAPI(e)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GetUpstreamLineage200JSONResponse{Data: &data, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) GetDownstreamLineage(ctx context.Context, req GetDownstreamLineageRequestObject) (GetDownstreamLineageResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	tableName := req.SchemaName + "." + req.TableName

	edges, total, err := h.lineage.GetDownstream(ctx, tableName, page)
	if err != nil {
		return nil, err
	}

	data := make([]LineageEdge, len(edges))
	for i, e := range edges {
		data[i] = lineageEdgeToAPI(e)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GetDownstreamLineage200JSONResponse{Data: &data, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) DeleteLineageEdge(ctx context.Context, req DeleteLineageEdgeRequestObject) (DeleteLineageEdgeResponseObject, error) {
	if err := h.lineage.DeleteEdge(ctx, req.EdgeId); err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return DeleteLineageEdge404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteLineageEdge204Response{}, nil
}

func (h *APIHandler) PurgeLineage(ctx context.Context, req PurgeLineageRequestObject) (PurgeLineageResponseObject, error) {
	deleted, err := h.lineage.PurgeOlderThan(ctx, req.Body.OlderThanDays)
	if err != nil {
		return PurgeLineage403JSONResponse{Code: 403, Message: err.Error()}, nil
	}
	return PurgeLineage200JSONResponse{DeletedCount: &deleted}, nil
}

// === Tags ===

func (h *APIHandler) ListTags(ctx context.Context, req ListTagsRequestObject) (ListTagsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	tags, total, err := h.tags.ListTags(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]Tag, len(tags))
	for i, t := range tags {
		data[i] = tagToAPI(t)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListTags200JSONResponse{Data: &data, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) CreateTag(ctx context.Context, req CreateTagRequestObject) (CreateTagResponseObject, error) {
	tag := &domain.Tag{
		Key:   req.Body.Key,
		Value: req.Body.Value,
	}
	result, err := h.tags.CreateTag(ctx, tag)
	if err != nil {
		switch err.(type) {
		case *domain.ConflictError:
			return CreateTag409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return CreateTag201JSONResponse(tagToAPI(*result)), nil
}

func (h *APIHandler) DeleteTag(ctx context.Context, req DeleteTagRequestObject) (DeleteTagResponseObject, error) {
	if err := h.tags.DeleteTag(ctx, req.TagId); err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return DeleteTag404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteTag204Response{}, nil
}

func (h *APIHandler) AssignTag(ctx context.Context, req AssignTagRequestObject) (AssignTagResponseObject, error) {
	assignment := &domain.TagAssignment{
		TagID:         req.TagId,
		SecurableType: req.Body.SecurableType,
		SecurableID:   req.Body.SecurableId,
		ColumnName:    req.Body.ColumnName,
	}
	result, err := h.tags.AssignTag(ctx, assignment)
	if err != nil {
		switch err.(type) {
		case *domain.ConflictError:
			return AssignTag409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return AssignTag201JSONResponse(tagAssignmentToAPI(*result)), nil
}

func (h *APIHandler) UnassignTag(ctx context.Context, req UnassignTagRequestObject) (UnassignTagResponseObject, error) {
	if err := h.tags.UnassignTag(ctx, req.AssignmentId); err != nil {
		return nil, err
	}
	return UnassignTag204Response{}, nil
}

func (h *APIHandler) ListClassifications(ctx context.Context, _ ListClassificationsRequestObject) (ListClassificationsResponseObject, error) {
	page := domain.PageRequest{MaxResults: 100}
	tags, _, err := h.tags.ListTags(ctx, page)
	if err != nil {
		return nil, err
	}

	// Filter to classification/sensitivity prefixes
	var filtered []Tag
	for _, t := range tags {
		if t.Key == domain.ClassificationPrefix || t.Key == domain.SensitivityPrefix {
			filtered = append(filtered, tagToAPI(t))
		}
	}

	return ListClassifications200JSONResponse{Data: &filtered}, nil
}

// === Views ===

func (h *APIHandler) ListViews(ctx context.Context, req ListViewsRequestObject) (ListViewsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	views, total, err := h.views.ListViews(ctx, req.SchemaName, page)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return ListViews404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}

	data := make([]ViewDetail, len(views))
	for i, v := range views {
		data[i] = viewDetailToAPI(v)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListViews200JSONResponse{Data: &data, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) CreateView(ctx context.Context, req CreateViewRequestObject) (CreateViewResponseObject, error) {
	domReq := domain.CreateViewRequest{
		Name:           req.Body.Name,
		ViewDefinition: req.Body.ViewDefinition,
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}

	result, err := h.views.CreateView(ctx, req.SchemaName, domReq)
	if err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return CreateView403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.ValidationError:
			return CreateView400JSONResponse{Code: 400, Message: err.Error()}, nil
		case *domain.ConflictError:
			return CreateView409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return CreateView400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateView201JSONResponse(viewDetailToAPI(*result)), nil
}

func (h *APIHandler) GetView(ctx context.Context, req GetViewRequestObject) (GetViewResponseObject, error) {
	result, err := h.views.GetView(ctx, req.SchemaName, req.ViewName)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return GetView404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetView200JSONResponse(viewDetailToAPI(*result)), nil
}

func (h *APIHandler) UpdateView(ctx context.Context, req UpdateViewRequestObject) (UpdateViewResponseObject, error) {
	domReq := domain.UpdateViewRequest{}
	if req.Body.Comment != nil {
		domReq.Comment = req.Body.Comment
	}
	if req.Body.Properties != nil {
		domReq.Properties = *req.Body.Properties
	}
	if req.Body.ViewDefinition != nil {
		domReq.ViewDefinition = req.Body.ViewDefinition
	}

	result, err := h.views.UpdateView(ctx, req.SchemaName, req.ViewName, domReq)
	if err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return UpdateView403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return UpdateView404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateView200JSONResponse(viewDetailToAPI(*result)), nil
}

func (h *APIHandler) DropView(ctx context.Context, req DropViewRequestObject) (DropViewResponseObject, error) {
	if err := h.views.DeleteView(ctx, req.SchemaName, req.ViewName); err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return DropView403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return DropView404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DropView204Response{}, nil
}

// === Ingestion ===

func (h *APIHandler) RequestUploadUrl(ctx context.Context, req RequestUploadUrlRequestObject) (RequestUploadUrlResponseObject, error) {
	if h.ingestion == nil {
		return RequestUploadUrl400JSONResponse{Code: 400, Message: "ingestion not available (S3 not configured)"}, nil
	}

	result, err := h.ingestion.RequestUploadURL(ctx, req.SchemaName, req.TableName, req.Body.Filename)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return RequestUploadUrl404JSONResponse{Code: 404, Message: err.Error()}, nil
		case *domain.AccessDeniedError:
			return RequestUploadUrl403JSONResponse{Code: 403, Message: err.Error()}, nil
		default:
			return RequestUploadUrl400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}

	t := result.ExpiresAt
	return RequestUploadUrl200JSONResponse{
		UploadUrl: &result.UploadURL,
		S3Key:     &result.S3Key,
		ExpiresAt: &t,
	}, nil
}

func (h *APIHandler) CommitIngestion(ctx context.Context, req CommitIngestionRequestObject) (CommitIngestionResponseObject, error) {
	if h.ingestion == nil {
		return CommitIngestion400JSONResponse{Code: 400, Message: "ingestion not available (S3 not configured)"}, nil
	}

	opts := domain.IngestionOptions{}
	if req.Body.Options != nil {
		if req.Body.Options.AllowMissingColumns != nil {
			opts.AllowMissingColumns = *req.Body.Options.AllowMissingColumns
		}
		if req.Body.Options.IgnoreExtraColumns != nil {
			opts.IgnoreExtraColumns = *req.Body.Options.IgnoreExtraColumns
		}
	}

	result, err := h.ingestion.CommitIngestion(ctx, req.SchemaName, req.TableName, req.Body.S3Keys, opts)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return CommitIngestion404JSONResponse{Code: 404, Message: err.Error()}, nil
		case *domain.AccessDeniedError:
			return CommitIngestion403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.ValidationError:
			return CommitIngestion400JSONResponse{Code: 400, Message: err.Error()}, nil
		default:
			return CommitIngestion400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}

	return CommitIngestion200JSONResponse{
		FilesRegistered: &result.FilesRegistered,
		FilesSkipped:    &result.FilesSkipped,
		Schema:          &result.Schema,
		Table:           &result.Table,
	}, nil
}

func (h *APIHandler) LoadExternalFiles(ctx context.Context, req LoadExternalFilesRequestObject) (LoadExternalFilesResponseObject, error) {
	if h.ingestion == nil {
		return LoadExternalFiles400JSONResponse{Code: 400, Message: "ingestion not available (S3 not configured)"}, nil
	}

	opts := domain.IngestionOptions{}
	if req.Body.Options != nil {
		if req.Body.Options.AllowMissingColumns != nil {
			opts.AllowMissingColumns = *req.Body.Options.AllowMissingColumns
		}
		if req.Body.Options.IgnoreExtraColumns != nil {
			opts.IgnoreExtraColumns = *req.Body.Options.IgnoreExtraColumns
		}
	}

	result, err := h.ingestion.LoadExternalFiles(ctx, req.SchemaName, req.TableName, req.Body.Paths, opts)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return LoadExternalFiles404JSONResponse{Code: 404, Message: err.Error()}, nil
		case *domain.AccessDeniedError:
			return LoadExternalFiles403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.ValidationError:
			return LoadExternalFiles400JSONResponse{Code: 400, Message: err.Error()}, nil
		default:
			return LoadExternalFiles400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}

	return LoadExternalFiles200JSONResponse{
		FilesRegistered: &result.FilesRegistered,
		FilesSkipped:    &result.FilesSkipped,
		Schema:          &result.Schema,
		Table:           &result.Table,
	}, nil
}

// === Mapping helpers ===

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

func catalogInfoToAPI(c domain.CatalogInfo) CatalogInfo {
	return CatalogInfo{
		Name:      &c.Name,
		Comment:   &c.Comment,
		CreatedAt: &c.CreatedAt,
		UpdatedAt: &c.UpdatedAt,
	}
}

func schemaDetailToAPI(s domain.SchemaDetail) SchemaDetail {
	tags := make([]Tag, len(s.Tags))
	for i, t := range s.Tags {
		tags[i] = tagToAPI(t)
	}
	return SchemaDetail{
		SchemaId:    &s.SchemaID,
		Name:        &s.Name,
		CatalogName: &s.CatalogName,
		Comment:     &s.Comment,
		Owner:       &s.Owner,
		Properties:  &s.Properties,
		CreatedAt:   &s.CreatedAt,
		UpdatedAt:   &s.UpdatedAt,
		Tags:        &tags,
		DeletedAt:   s.DeletedAt,
	}
}

func tableDetailToAPI(t domain.TableDetail) TableDetail {
	cols := make([]ColumnDetail, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = columnDetailToAPI(c)
	}
	tags := make([]Tag, len(t.Tags))
	for i, tg := range t.Tags {
		tags[i] = tagToAPI(tg)
	}
	td := TableDetail{
		TableId:     &t.TableID,
		Name:        &t.Name,
		SchemaName:  &t.SchemaName,
		CatalogName: &t.CatalogName,
		TableType:   &t.TableType,
		Columns:     &cols,
		Comment:     &t.Comment,
		Owner:       &t.Owner,
		Properties:  &t.Properties,
		CreatedAt:   &t.CreatedAt,
		UpdatedAt:   &t.UpdatedAt,
		Tags:        &tags,
		DeletedAt:   t.DeletedAt,
	}
	if t.Statistics != nil {
		td.Statistics = tableStatisticsPtr(t.Statistics)
	}
	if t.SourcePath != "" {
		td.SourcePath = &t.SourcePath
	}
	if t.FileFormat != "" {
		td.FileFormat = &t.FileFormat
	}
	if t.LocationName != "" {
		td.LocationName = &t.LocationName
	}
	return td
}

func columnDetailToAPI(c domain.ColumnDetail) ColumnDetail {
	return ColumnDetail{
		Name:       &c.Name,
		Type:       &c.Type,
		Position:   &c.Position,
		Comment:    &c.Comment,
		Properties: &c.Properties,
	}
}

func queryHistoryEntryToAPI(e domain.QueryHistoryEntry) QueryHistoryEntry {
	t := e.CreatedAt
	return QueryHistoryEntry{
		Id:             &e.ID,
		PrincipalName:  &e.PrincipalName,
		OriginalSql:    e.OriginalSQL,
		RewrittenSql:   e.RewrittenSQL,
		StatementType:  e.StatementType,
		TablesAccessed: &e.TablesAccessed,
		Status:         &e.Status,
		ErrorMessage:   e.ErrorMessage,
		DurationMs:     e.DurationMs,
		RowsReturned:   e.RowsReturned,
		CreatedAt:      &t,
	}
}

func searchResultToAPI(r domain.SearchResult) SearchResult {
	return SearchResult{
		Type:       &r.Type,
		Name:       &r.Name,
		SchemaName: r.SchemaName,
		TableName:  r.TableName,
		Comment:    r.Comment,
		MatchField: &r.MatchField,
	}
}

func lineageEdgeToAPI(e domain.LineageEdge) LineageEdge {
	t := e.CreatedAt
	return LineageEdge{
		Id:            &e.ID,
		SourceTable:   &e.SourceTable,
		TargetTable:   e.TargetTable,
		SourceSchema:  strPtrIfNonEmpty(e.SourceSchema),
		TargetSchema:  strPtrIfNonEmpty(e.TargetSchema),
		EdgeType:      &e.EdgeType,
		PrincipalName: &e.PrincipalName,
		CreatedAt:     &t,
	}
}

func strPtrIfNonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func tagToAPI(t domain.Tag) Tag {
	ct := t.CreatedAt
	return Tag{
		Id:        &t.ID,
		Key:       &t.Key,
		Value:     t.Value,
		CreatedBy: &t.CreatedBy,
		CreatedAt: &ct,
	}
}

func tagAssignmentToAPI(a domain.TagAssignment) TagAssignment {
	t := a.AssignedAt
	return TagAssignment{
		Id:            &a.ID,
		TagId:         &a.TagID,
		SecurableType: &a.SecurableType,
		SecurableId:   &a.SecurableID,
		ColumnName:    a.ColumnName,
		AssignedBy:    &a.AssignedBy,
		AssignedAt:    &t,
	}
}

func viewDetailToAPI(v domain.ViewDetail) ViewDetail {
	ct := v.CreatedAt
	ut := v.UpdatedAt
	return ViewDetail{
		Id:             &v.ID,
		SchemaId:       &v.SchemaID,
		SchemaName:     &v.SchemaName,
		CatalogName:    &v.CatalogName,
		Name:           &v.Name,
		ViewDefinition: &v.ViewDefinition,
		Comment:        v.Comment,
		Properties:     &v.Properties,
		Owner:          &v.Owner,
		SourceTables:   &v.SourceTables,
		CreatedAt:      &ct,
		UpdatedAt:      &ut,
	}
}

func tableStatisticsToAPI(s *domain.TableStatistics) TableStatistics {
	if s == nil {
		return TableStatistics{}
	}
	return TableStatistics{
		RowCount:       s.RowCount,
		SizeBytes:      s.SizeBytes,
		ColumnCount:    s.ColumnCount,
		LastProfiledAt: s.LastProfiledAt,
		ProfiledBy:     &s.ProfiledBy,
	}
}

func tableStatisticsPtr(s *domain.TableStatistics) *TableStatistics {
	if s == nil {
		return nil
	}
	ts := tableStatisticsToAPI(s)
	return &ts
}

// optStr returns a pointer to the string if non-empty, otherwise nil.
func optStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// === Storage Credentials ===

func (h *APIHandler) ListStorageCredentials(ctx context.Context, req ListStorageCredentialsRequestObject) (ListStorageCredentialsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	creds, total, err := h.storageCreds.List(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]StorageCredential, len(creds))
	for i, c := range creds {
		data[i] = storageCredentialToAPI(c)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListStorageCredentials200JSONResponse{
		Data:          &data,
		NextPageToken: optStr(nextToken),
	}, nil
}

func (h *APIHandler) CreateStorageCredential(ctx context.Context, req CreateStorageCredentialRequestObject) (CreateStorageCredentialResponseObject, error) {
	domReq := domain.CreateStorageCredentialRequest{
		Name:           req.Body.Name,
		CredentialType: domain.CredentialType(req.Body.CredentialType),
		KeyID:          req.Body.KeyId,
		Secret:         req.Body.Secret,
		Endpoint:       req.Body.Endpoint,
		Region:         req.Body.Region,
	}
	if req.Body.UrlStyle != nil {
		domReq.URLStyle = *req.Body.UrlStyle
	} else {
		domReq.URLStyle = "path"
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}

	result, err := h.storageCreds.Create(ctx, domReq)
	if err != nil {
		var accessErr *domain.AccessDeniedError
		var validErr *domain.ValidationError
		var conflictErr *domain.ConflictError
		switch {
		case errors.As(err, &accessErr):
			return CreateStorageCredential403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, &validErr):
			return CreateStorageCredential400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, &conflictErr):
			return CreateStorageCredential409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return CreateStorageCredential400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateStorageCredential201JSONResponse(storageCredentialToAPI(*result)), nil
}

func (h *APIHandler) GetStorageCredential(ctx context.Context, req GetStorageCredentialRequestObject) (GetStorageCredentialResponseObject, error) {
	result, err := h.storageCreds.GetByName(ctx, req.CredentialName)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return GetStorageCredential404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetStorageCredential200JSONResponse(storageCredentialToAPI(*result)), nil
}

func (h *APIHandler) UpdateStorageCredential(ctx context.Context, req UpdateStorageCredentialRequestObject) (UpdateStorageCredentialResponseObject, error) {
	domReq := domain.UpdateStorageCredentialRequest{
		KeyID:    req.Body.KeyId,
		Secret:   req.Body.Secret,
		Endpoint: req.Body.Endpoint,
		Region:   req.Body.Region,
		URLStyle: req.Body.UrlStyle,
		Comment:  req.Body.Comment,
	}

	result, err := h.storageCreds.Update(ctx, req.CredentialName, domReq)
	if err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return UpdateStorageCredential403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return UpdateStorageCredential404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateStorageCredential200JSONResponse(storageCredentialToAPI(*result)), nil
}

func (h *APIHandler) DeleteStorageCredential(ctx context.Context, req DeleteStorageCredentialRequestObject) (DeleteStorageCredentialResponseObject, error) {
	if err := h.storageCreds.Delete(ctx, req.CredentialName); err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return DeleteStorageCredential403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return DeleteStorageCredential404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteStorageCredential204Response{}, nil
}

// === External Locations ===

func (h *APIHandler) ListExternalLocations(ctx context.Context, req ListExternalLocationsRequestObject) (ListExternalLocationsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	locs, total, err := h.externalLocations.List(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]ExternalLocation, len(locs))
	for i, l := range locs {
		data[i] = externalLocationToAPI(l)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListExternalLocations200JSONResponse{
		Data:          &data,
		NextPageToken: optStr(nextToken),
	}, nil
}

func (h *APIHandler) CreateExternalLocation(ctx context.Context, req CreateExternalLocationRequestObject) (CreateExternalLocationResponseObject, error) {
	domReq := domain.CreateExternalLocationRequest{
		Name:           req.Body.Name,
		URL:            req.Body.Url,
		CredentialName: req.Body.CredentialName,
	}
	if req.Body.StorageType != nil {
		domReq.StorageType = domain.StorageType(*req.Body.StorageType)
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}
	if req.Body.ReadOnly != nil {
		domReq.ReadOnly = *req.Body.ReadOnly
	}

	result, err := h.externalLocations.Create(ctx, domReq)
	if err != nil {
		var accessErr *domain.AccessDeniedError
		var validErr *domain.ValidationError
		var conflictErr *domain.ConflictError
		var notFoundErr *domain.NotFoundError
		switch {
		case errors.As(err, &accessErr):
			return CreateExternalLocation403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, &validErr):
			return CreateExternalLocation400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, &conflictErr):
			return CreateExternalLocation409JSONResponse{Code: 409, Message: err.Error()}, nil
		case errors.As(err, &notFoundErr):
			// Referenced credential not found — report as 400 (bad request)
			return CreateExternalLocation400JSONResponse{Code: 400, Message: err.Error()}, nil
		default:
			return CreateExternalLocation400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateExternalLocation201JSONResponse(externalLocationToAPI(*result)), nil
}

func (h *APIHandler) GetExternalLocation(ctx context.Context, req GetExternalLocationRequestObject) (GetExternalLocationResponseObject, error) {
	result, err := h.externalLocations.GetByName(ctx, req.LocationName)
	if err != nil {
		switch err.(type) {
		case *domain.NotFoundError:
			return GetExternalLocation404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetExternalLocation200JSONResponse(externalLocationToAPI(*result)), nil
}

func (h *APIHandler) UpdateExternalLocation(ctx context.Context, req UpdateExternalLocationRequestObject) (UpdateExternalLocationResponseObject, error) {
	domReq := domain.UpdateExternalLocationRequest{
		URL:     req.Body.Url,
		Comment: req.Body.Comment,
		Owner:   req.Body.Owner,
	}
	if req.Body.CredentialName != nil {
		domReq.CredentialName = req.Body.CredentialName
	}
	if req.Body.ReadOnly != nil {
		domReq.ReadOnly = req.Body.ReadOnly
	}

	result, err := h.externalLocations.Update(ctx, req.LocationName, domReq)
	if err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return UpdateExternalLocation403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return UpdateExternalLocation404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateExternalLocation200JSONResponse(externalLocationToAPI(*result)), nil
}

func (h *APIHandler) DeleteExternalLocation(ctx context.Context, req DeleteExternalLocationRequestObject) (DeleteExternalLocationResponseObject, error) {
	if err := h.externalLocations.Delete(ctx, req.LocationName); err != nil {
		switch err.(type) {
		case *domain.AccessDeniedError:
			return DeleteExternalLocation403JSONResponse{Code: 403, Message: err.Error()}, nil
		case *domain.NotFoundError:
			return DeleteExternalLocation404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteExternalLocation204Response{}, nil
}

// === API Mappers for Storage Credentials / External Locations ===

// storageCredentialToAPI converts a domain StorageCredential to the API type.
// IMPORTANT: Never expose key_id or secret in API responses.
func storageCredentialToAPI(c domain.StorageCredential) StorageCredential {
	ct := string(c.CredentialType)
	return StorageCredential{
		Id:             &c.ID,
		Name:           &c.Name,
		CredentialType: &ct,
		Endpoint:       &c.Endpoint,
		Region:         &c.Region,
		UrlStyle:       &c.URLStyle,
		Comment:        optStr(c.Comment),
		Owner:          &c.Owner,
		CreatedAt:      &c.CreatedAt,
		UpdatedAt:      &c.UpdatedAt,
	}
}

func externalLocationToAPI(l domain.ExternalLocation) ExternalLocation {
	st := string(l.StorageType)
	return ExternalLocation{
		Id:             &l.ID,
		Name:           &l.Name,
		Url:            &l.URL,
		CredentialName: &l.CredentialName,
		StorageType:    &st,
		Comment:        optStr(l.Comment),
		Owner:          &l.Owner,
		ReadOnly:       &l.ReadOnly,
		CreatedAt:      &l.CreatedAt,
		UpdatedAt:      &l.UpdatedAt,
	}
}
