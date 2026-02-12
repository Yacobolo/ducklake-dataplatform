// Package api provides HTTP handlers for the data platform REST API.
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"

	"duck-demo/internal/db/mapper"
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
	volumes           *service.VolumeService
	computeEndpoints  *service.ComputeEndpointService
	apiKeys           *service.APIKeyService
}

// NewHandler creates a new APIHandler with all required service dependencies.
func NewHandler(
	query *service.QueryService,
	principals *service.PrincipalService,
	groups *service.GroupService,
	grants *service.GrantService,
	rowFilters *service.RowFilterService,
	columnMasks *service.ColumnMaskService,
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
	volumes *service.VolumeService,
	computeEndpoints *service.ComputeEndpointService,
	apiKeys *service.APIKeyService,
) *APIHandler {
	return &APIHandler{
		query:             query,
		principals:        principals,
		groups:            groups,
		grants:            grants,
		rowFilters:        rowFilters,
		columnMasks:       columnMasks,
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
		volumes:           volumes,
		computeEndpoints:  computeEndpoints,
		apiKeys:           apiKeys,
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

// httpStatusFromError returns the HTTP status code for a domain error using
// the centralized mapper. Unknown errors return 500 Internal Server Error.
func httpStatusFromError(err error) int {
	return mapper.HTTPStatusFromDomainError(err)
}

// errorCodeFromError returns the HTTP status code for building error JSON responses.
// This is a convenience alias for use in handler methods.
func errorCodeFromError(err error) int {
	return httpStatusFromError(err)
}

func (h *APIHandler) ExecuteQuery(ctx context.Context, req ExecuteQueryRequestObject) (ExecuteQueryResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.query.Execute(ctx, principal, req.Body.Sql)
	if err != nil {
		code := errorCodeFromError(err)
		return ExecuteQuery403JSONResponse{Code: code, Message: err.Error()}, nil
	}

	rows := make([][]interface{}, len(result.Rows))
	copy(rows, result.Rows)

	return ExecuteQuery200JSONResponse{
		Columns:  &result.Columns,
		Rows:     &rows,
		RowCount: &result.RowCount,
	}, nil
}

func (h *APIHandler) CreateManifest(ctx context.Context, req CreateManifestRequestObject) (CreateManifestResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	schemaName := "main"
	if req.Body.Schema != nil {
		schemaName = *req.Body.Schema
	}

	result, err := h.manifest.GetManifest(ctx, principal, schemaName, req.Body.Table)
	if err != nil {
		code := errorCodeFromError(err)
		switch code {
		case http.StatusNotFound:
			return CreateManifest404JSONResponse{Code: code, Message: err.Error()}, nil
		default:
			return CreateManifest403JSONResponse{Code: code, Message: err.Error()}, nil
		}
	}

	cols := make([]ManifestColumn, len(result.Columns))
	for i, c := range result.Columns {
		name := c.Name
		typ := c.Type
		cols[i] = ManifestColumn{Name: &name, Type: &typ}
	}

	return CreateManifest200JSONResponse{
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
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return CreatePrincipal400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreatePrincipal400JSONResponse{Code: 400, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return CreatePrincipal201JSONResponse(principalToAPI(*result)), nil
}

func (h *APIHandler) GetPrincipal(ctx context.Context, req GetPrincipalRequestObject) (GetPrincipalResponseObject, error) {
	p, err := h.principals.GetByID(ctx, req.PrincipalId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetPrincipal404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetPrincipal200JSONResponse(principalToAPI(*p)), nil
}

func (h *APIHandler) DeletePrincipal(ctx context.Context, req DeletePrincipalRequestObject) (DeletePrincipalResponseObject, error) {
	if err := h.principals.Delete(ctx, req.PrincipalId); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return DeletePrincipal404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeletePrincipal204Response{}, nil
}

func (h *APIHandler) UpdatePrincipalAdmin(ctx context.Context, req UpdatePrincipalAdminRequestObject) (UpdatePrincipalAdminResponseObject, error) {
	if err := h.principals.SetAdmin(ctx, req.PrincipalId, req.Body.IsAdmin); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdatePrincipalAdmin404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdatePrincipalAdmin204Response{}, nil
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
	g, err := h.groups.GetByID(ctx, req.GroupId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetGroup404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetGroup200JSONResponse(groupToAPI(*g)), nil
}

func (h *APIHandler) DeleteGroup(ctx context.Context, req DeleteGroupRequestObject) (DeleteGroupResponseObject, error) {
	if err := h.groups.Delete(ctx, req.GroupId); err != nil {
		return nil, err
	}
	return DeleteGroup204Response{}, nil
}

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
	return ListGroupMembers200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) CreateGroupMember(ctx context.Context, req CreateGroupMemberRequestObject) (CreateGroupMemberResponseObject, error) {
	if err := h.groups.AddMember(ctx, &domain.GroupMember{
		GroupID:    req.GroupId,
		MemberType: req.Body.MemberType,
		MemberID:   req.Body.MemberId,
	}); err != nil {
		return nil, err
	}
	return CreateGroupMember204Response{}, nil
}

func (h *APIHandler) DeleteGroupMember(ctx context.Context, req DeleteGroupMemberRequestObject) (DeleteGroupMemberResponseObject, error) {
	if err := h.groups.RemoveMember(ctx, &domain.GroupMember{
		GroupID:    req.GroupId,
		MemberType: req.Body.MemberType,
		MemberID:   req.Body.MemberId,
	}); err != nil {
		return nil, err
	}
	return DeleteGroupMember204Response{}, nil
}

// === Grants ===

func (h *APIHandler) ListGrants(ctx context.Context, req ListGrantsRequestObject) (ListGrantsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	var grants []domain.PrivilegeGrant
	var total int64
	var err error

	switch {
	case req.Params.PrincipalId != nil && req.Params.PrincipalType != nil:
		grants, total, err = h.grants.ListForPrincipal(ctx, *req.Params.PrincipalId, *req.Params.PrincipalType, page)
	case req.Params.SecurableType != nil && req.Params.SecurableId != nil:
		grants, total, err = h.grants.ListForSecurable(ctx, *req.Params.SecurableType, *req.Params.SecurableId, page)
	default:
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

func (h *APIHandler) CreateGrant(ctx context.Context, req CreateGrantRequestObject) (CreateGrantResponseObject, error) {
	g := &domain.PrivilegeGrant{
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: req.Body.PrincipalType,
		SecurableType: req.Body.SecurableType,
		SecurableID:   req.Body.SecurableId,
		Privilege:     req.Body.Privilege,
	}
	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.grants.Grant(ctx, principal, g)
	if err != nil {
		return nil, err
	}
	return CreateGrant201JSONResponse(grantToAPI(*result)), nil
}

func (h *APIHandler) DeleteGrant(ctx context.Context, req DeleteGrantRequestObject) (DeleteGrantResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.grants.Revoke(ctx, principal, &domain.PrivilegeGrant{
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: req.Body.PrincipalType,
		SecurableType: req.Body.SecurableType,
		SecurableID:   req.Body.SecurableId,
		Privilege:     req.Body.Privilege,
	}); err != nil {
		return nil, err
	}
	return DeleteGrant204Response{}, nil
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
	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.rowFilters.Create(ctx, principal, f)
	if err != nil {
		return nil, err
	}
	return CreateRowFilter201JSONResponse(rowFilterToAPI(*result)), nil
}

func (h *APIHandler) DeleteRowFilter(ctx context.Context, req DeleteRowFilterRequestObject) (DeleteRowFilterResponseObject, error) {
	if err := h.rowFilters.Delete(ctx, req.RowFilterId); err != nil {
		return nil, err
	}
	return DeleteRowFilter204Response{}, nil
}

func (h *APIHandler) BindRowFilter(ctx context.Context, req BindRowFilterRequestObject) (BindRowFilterResponseObject, error) {
	if err := h.rowFilters.Bind(ctx, &domain.RowFilterBinding{
		RowFilterID:   req.RowFilterId,
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: req.Body.PrincipalType,
	}); err != nil {
		return nil, err
	}
	return BindRowFilter204Response{}, nil
}

func (h *APIHandler) UnbindRowFilter(ctx context.Context, req UnbindRowFilterRequestObject) (UnbindRowFilterResponseObject, error) {
	if err := h.rowFilters.Unbind(ctx, &domain.RowFilterBinding{
		RowFilterID:   req.RowFilterId,
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: req.Body.PrincipalType,
	}); err != nil {
		return nil, err
	}
	return UnbindRowFilter204Response{}, nil
}

func (h *APIHandler) CreateRowFilterTopLevel(ctx context.Context, req CreateRowFilterTopLevelRequestObject) (CreateRowFilterTopLevelResponseObject, error) {
	if req.Body.TableId == nil {
		return CreateRowFilterTopLevel400JSONResponse{Code: 400, Message: "table_id is required"}, nil
	}
	f := &domain.RowFilter{
		TableID:   *req.Body.TableId,
		FilterSQL: req.Body.FilterSql,
	}
	if req.Body.Description != nil {
		f.Description = *req.Body.Description
	}
	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.rowFilters.Create(ctx, principal, f)
	if err != nil {
		return nil, err
	}
	return CreateRowFilterTopLevel201JSONResponse(rowFilterToAPI(*result)), nil
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
	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.columnMasks.Create(ctx, principal, m)
	if err != nil {
		return nil, err
	}
	return CreateColumnMask201JSONResponse(columnMaskToAPI(*result)), nil
}

func (h *APIHandler) DeleteColumnMask(ctx context.Context, req DeleteColumnMaskRequestObject) (DeleteColumnMaskResponseObject, error) {
	if err := h.columnMasks.Delete(ctx, req.ColumnMaskId); err != nil {
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
		ColumnMaskID:  req.ColumnMaskId,
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
		ColumnMaskID:  req.ColumnMaskId,
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: req.Body.PrincipalType,
	}); err != nil {
		return nil, err
	}
	return UnbindColumnMask204Response{}, nil
}

// === API Keys ===

func (h *APIHandler) CreateAPIKey(ctx context.Context, req CreateAPIKeyRequestObject) (CreateAPIKeyResponseObject, error) {
	rawKey, key, err := h.apiKeys.Create(ctx, req.Body.PrincipalId, req.Body.Name, req.Body.ExpiresAt)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return CreateAPIKey400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateAPIKey403JSONResponse{Code: 403, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return CreateAPIKey201JSONResponse{
		Id:        &key.ID,
		Key:       &rawKey,
		Name:      &key.Name,
		KeyPrefix: &key.KeyPrefix,
		ExpiresAt: key.ExpiresAt,
		CreatedAt: &key.CreatedAt,
	}, nil
}

func (h *APIHandler) ListAPIKeys(ctx context.Context, req ListAPIKeysRequestObject) (ListAPIKeysResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	keys, total, err := h.apiKeys.List(ctx, req.Params.PrincipalId, page)
	if err != nil {
		return nil, err
	}
	data := make([]APIKeyInfo, len(keys))
	for i, k := range keys {
		data[i] = apiKeyToAPI(k)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListAPIKeys200JSONResponse{Data: &data, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) DeleteAPIKey(ctx context.Context, req DeleteAPIKeyRequestObject) (DeleteAPIKeyResponseObject, error) {
	if err := h.apiKeys.Delete(ctx, req.ApiKeyId); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteAPIKey404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteAPIKey204Response{}, nil
}

func (h *APIHandler) CleanupExpiredAPIKeys(ctx context.Context, _ CleanupExpiredAPIKeysRequestObject) (CleanupExpiredAPIKeysResponseObject, error) {
	count, err := h.apiKeys.CleanupExpired(ctx)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CleanupExpiredAPIKeys403JSONResponse{Code: 403, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return CleanupExpiredAPIKeys200JSONResponse{DeletedCount: &count}, nil
}

// apiKeyToAPI converts a domain APIKey to the API representation.
func apiKeyToAPI(k domain.APIKey) APIKeyInfo {
	return APIKeyInfo{
		Id:          &k.ID,
		PrincipalId: &k.PrincipalID,
		Name:        &k.Name,
		KeyPrefix:   &k.KeyPrefix,
		ExpiresAt:   k.ExpiresAt,
		CreatedAt:   &k.CreatedAt,
	}
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

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.catalog.UpdateCatalog(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateCatalog403JSONResponse{Code: 403, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateCatalog200JSONResponse(catalogInfoToAPI(*result)), nil
}

func (h *APIHandler) ListSchemas(ctx context.Context, req ListSchemasRequestObject) (ListSchemasResponseObject, error) {
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
	return ListSchemas200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
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

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.catalog.CreateSchema(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateSchema403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateSchema400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateSchema409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return CreateSchema400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateSchema201JSONResponse(schemaDetailToAPI(*result)), nil
}

func (h *APIHandler) GetSchema(ctx context.Context, req GetSchemaRequestObject) (GetSchemaResponseObject, error) {
	result, err := h.catalog.GetSchema(ctx, req.SchemaName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetSchema404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetSchema200JSONResponse(schemaDetailToAPI(*result)), nil
}

func (h *APIHandler) UpdateSchema(ctx context.Context, req UpdateSchemaRequestObject) (UpdateSchemaResponseObject, error) {
	var props map[string]string
	if req.Body.Properties != nil {
		props = *req.Body.Properties
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.catalog.UpdateSchema(ctx, principal, req.SchemaName, req.Body.Comment, props)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateSchema403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateSchema404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateSchema200JSONResponse(schemaDetailToAPI(*result)), nil
}

func (h *APIHandler) DeleteSchema(ctx context.Context, req DeleteSchemaRequestObject) (DeleteSchemaResponseObject, error) {
	force := false
	if req.Params.Force != nil {
		force = *req.Params.Force
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.catalog.DeleteSchema(ctx, principal, req.SchemaName, force); err != nil {
		code := errorCodeFromError(err)
		switch code {
		case http.StatusForbidden:
			return DeleteSchema403JSONResponse{Code: code, Message: err.Error()}, nil
		case http.StatusNotFound:
			return DeleteSchema404JSONResponse{Code: code, Message: err.Error()}, nil
		case http.StatusConflict:
			return DeleteSchema409JSONResponse{Code: code, Message: err.Error()}, nil
		default:
			return DeleteSchema403JSONResponse{Code: code, Message: err.Error()}, nil
		}
	}
	return DeleteSchema204Response{}, nil
}

func (h *APIHandler) ListTables(ctx context.Context, req ListTablesRequestObject) (ListTablesResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	tables, total, err := h.catalog.ListTables(ctx, req.SchemaName, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListTables404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	out := make([]TableDetail, len(tables))
	for i, t := range tables {
		out[i] = tableDetailToAPI(t)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListTables200JSONResponse{Data: &out, NextPageToken: optStr(npt)}, nil
}

func (h *APIHandler) CreateTable(ctx context.Context, req CreateTableRequestObject) (CreateTableResponseObject, error) {
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

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.catalog.CreateTable(ctx, principal, req.SchemaName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateTable403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateTable400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateTable409JSONResponse{Code: 409, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return CreateTable400JSONResponse{Code: 400, Message: err.Error()}, nil
		default:
			return CreateTable400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateTable201JSONResponse(tableDetailToAPI(*result)), nil
}

func (h *APIHandler) GetTable(ctx context.Context, req GetTableRequestObject) (GetTableResponseObject, error) {
	result, err := h.catalog.GetTable(ctx, req.SchemaName, req.TableName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetTable404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetTable200JSONResponse(tableDetailToAPI(*result)), nil
}

func (h *APIHandler) UpdateTable(ctx context.Context, req UpdateTableRequestObject) (UpdateTableResponseObject, error) {
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

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.catalog.UpdateTable(ctx, principal, req.SchemaName, req.TableName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateTable403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateTable404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateTable200JSONResponse(tableDetailToAPI(*result)), nil
}

func (h *APIHandler) DeleteTable(ctx context.Context, req DeleteTableRequestObject) (DeleteTableResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.catalog.DeleteTable(ctx, principal, req.SchemaName, req.TableName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteTable403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteTable404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteTable204Response{}, nil
}

func (h *APIHandler) ListTableColumns(ctx context.Context, req ListTableColumnsRequestObject) (ListTableColumnsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	cols, total, err := h.catalog.ListColumns(ctx, req.SchemaName, req.TableName, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
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

func (h *APIHandler) UpdateColumn(ctx context.Context, req UpdateColumnRequestObject) (UpdateColumnResponseObject, error) {
	domReq := domain.UpdateColumnRequest{}
	if req.Body.Comment != nil {
		domReq.Comment = req.Body.Comment
	}
	if req.Body.Properties != nil {
		domReq.Properties = *req.Body.Properties
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.catalog.UpdateColumn(ctx, principal, req.SchemaName, req.TableName, req.ColumnName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateColumn403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateColumn404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateColumn200JSONResponse(columnDetailToAPI(*result)), nil
}

func (h *APIHandler) ProfileTable(ctx context.Context, req ProfileTableRequestObject) (ProfileTableResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	stats, err := h.catalog.ProfileTable(ctx, principal, req.SchemaName, req.TableName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return ProfileTable403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
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
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
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
		code := errorCodeFromError(err)
		return PurgeLineage403JSONResponse{Code: code, Message: err.Error()}, nil
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
	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.tags.CreateTag(ctx, principal, tag)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ConflictError)):
			return CreateTag409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return CreateTag201JSONResponse(tagToAPI(*result)), nil
}

func (h *APIHandler) DeleteTag(ctx context.Context, req DeleteTagRequestObject) (DeleteTagResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.tags.DeleteTag(ctx, principal, req.TagId); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteTag404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteTag204Response{}, nil
}

func (h *APIHandler) CreateTagAssignment(ctx context.Context, req CreateTagAssignmentRequestObject) (CreateTagAssignmentResponseObject, error) {
	assignment := &domain.TagAssignment{
		TagID:         req.TagId,
		SecurableType: req.Body.SecurableType,
		SecurableID:   req.Body.SecurableId,
		ColumnName:    req.Body.ColumnName,
	}
	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.tags.AssignTag(ctx, principal, assignment)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ConflictError)):
			return CreateTagAssignment409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return CreateTagAssignment201JSONResponse(tagAssignmentToAPI(*result)), nil
}

func (h *APIHandler) DeleteTagAssignment(ctx context.Context, req DeleteTagAssignmentRequestObject) (DeleteTagAssignmentResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.tags.UnassignTag(ctx, principal, req.AssignmentId); err != nil {
		return nil, err
	}
	return DeleteTagAssignment204Response{}, nil
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
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
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

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.views.CreateView(ctx, principal, req.SchemaName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateView403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateView400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ConflictError)):
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
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
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

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.views.UpdateView(ctx, principal, req.SchemaName, req.ViewName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateView403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateView404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateView200JSONResponse(viewDetailToAPI(*result)), nil
}

func (h *APIHandler) DeleteView(ctx context.Context, req DeleteViewRequestObject) (DeleteViewResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.views.DeleteView(ctx, principal, req.SchemaName, req.ViewName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteView403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteView404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteView204Response{}, nil
}

// === Ingestion ===

func (h *APIHandler) CreateUploadUrl(ctx context.Context, req CreateUploadUrlRequestObject) (CreateUploadUrlResponseObject, error) {
	if h.ingestion == nil {
		return CreateUploadUrl400JSONResponse{Code: 400, Message: "ingestion not available (S3 not configured)"}, nil
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.ingestion.RequestUploadURL(ctx, principal, req.SchemaName, req.TableName, req.Body.Filename)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return CreateUploadUrl404JSONResponse{Code: 404, Message: err.Error()}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateUploadUrl403JSONResponse{Code: 403, Message: err.Error()}, nil
		default:
			return CreateUploadUrl400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}

	t := result.ExpiresAt
	return CreateUploadUrl200JSONResponse{
		UploadUrl: &result.UploadURL,
		S3Key:     &result.S3Key,
		ExpiresAt: &t,
	}, nil
}

func (h *APIHandler) CommitTableIngestion(ctx context.Context, req CommitTableIngestionRequestObject) (CommitTableIngestionResponseObject, error) {
	if h.ingestion == nil {
		return CommitTableIngestion400JSONResponse{Code: 400, Message: "ingestion not available (S3 not configured)"}, nil
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

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.ingestion.CommitIngestion(ctx, principal, req.SchemaName, req.TableName, req.Body.S3Keys, opts)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return CommitTableIngestion404JSONResponse{Code: 404, Message: err.Error()}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CommitTableIngestion403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CommitTableIngestion400JSONResponse{Code: 400, Message: err.Error()}, nil
		default:
			return CommitTableIngestion400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}

	return CommitTableIngestion200JSONResponse{
		FilesRegistered: &result.FilesRegistered,
		FilesSkipped:    &result.FilesSkipped,
		Schema:          &result.Schema,
		Table:           &result.Table,
	}, nil
}

func (h *APIHandler) LoadTableExternalFiles(ctx context.Context, req LoadTableExternalFilesRequestObject) (LoadTableExternalFilesResponseObject, error) {
	if h.ingestion == nil {
		return LoadTableExternalFiles400JSONResponse{Code: 400, Message: "ingestion not available (S3 not configured)"}, nil
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

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.ingestion.LoadExternalFiles(ctx, principal, req.SchemaName, req.TableName, req.Body.Paths, opts)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return LoadTableExternalFiles404JSONResponse{Code: 404, Message: err.Error()}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return LoadTableExternalFiles403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return LoadTableExternalFiles400JSONResponse{Code: 400, Message: err.Error()}, nil
		default:
			return LoadTableExternalFiles400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}

	return LoadTableExternalFiles200JSONResponse{
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
	if t.StoragePath != "" {
		td.StoragePath = &t.StoragePath
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
		Nullable:   &c.Nullable,
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
	}
	// S3 fields
	if req.Body.KeyId != nil {
		domReq.KeyID = *req.Body.KeyId
	}
	if req.Body.Secret != nil {
		domReq.Secret = *req.Body.Secret
	}
	if req.Body.Endpoint != nil {
		domReq.Endpoint = *req.Body.Endpoint
	}
	if req.Body.Region != nil {
		domReq.Region = *req.Body.Region
	}
	if req.Body.UrlStyle != nil {
		domReq.URLStyle = *req.Body.UrlStyle
	} else {
		domReq.URLStyle = "path"
	}
	// Azure fields
	if req.Body.AzureAccountName != nil {
		domReq.AzureAccountName = *req.Body.AzureAccountName
	}
	if req.Body.AzureAccountKey != nil {
		domReq.AzureAccountKey = *req.Body.AzureAccountKey
	}
	if req.Body.AzureClientId != nil {
		domReq.AzureClientID = *req.Body.AzureClientId
	}
	if req.Body.AzureTenantId != nil {
		domReq.AzureTenantID = *req.Body.AzureTenantId
	}
	if req.Body.AzureClientSecret != nil {
		domReq.AzureClientSecret = *req.Body.AzureClientSecret
	}
	// GCS fields
	if req.Body.GcsKeyFilePath != nil {
		domReq.GCSKeyFilePath = *req.Body.GcsKeyFilePath
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.storageCreds.Create(ctx, principal, domReq)
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
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetStorageCredential404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetStorageCredential200JSONResponse(storageCredentialToAPI(*result)), nil
}

func (h *APIHandler) UpdateStorageCredential(ctx context.Context, req UpdateStorageCredentialRequestObject) (UpdateStorageCredentialResponseObject, error) {
	domReq := domain.UpdateStorageCredentialRequest{
		// S3 fields
		KeyID:    req.Body.KeyId,
		Secret:   req.Body.Secret,
		Endpoint: req.Body.Endpoint,
		Region:   req.Body.Region,
		URLStyle: req.Body.UrlStyle,
		// Azure fields
		AzureAccountName:  req.Body.AzureAccountName,
		AzureAccountKey:   req.Body.AzureAccountKey,
		AzureClientID:     req.Body.AzureClientId,
		AzureTenantID:     req.Body.AzureTenantId,
		AzureClientSecret: req.Body.AzureClientSecret,
		// GCS fields
		GCSKeyFilePath: req.Body.GcsKeyFilePath,
		Comment:        req.Body.Comment,
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.storageCreds.Update(ctx, principal, req.CredentialName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateStorageCredential403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateStorageCredential404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateStorageCredential200JSONResponse(storageCredentialToAPI(*result)), nil
}

func (h *APIHandler) DeleteStorageCredential(ctx context.Context, req DeleteStorageCredentialRequestObject) (DeleteStorageCredentialResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.storageCreds.Delete(ctx, principal, req.CredentialName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteStorageCredential403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
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
		if *req.Body.StorageType != CreateExternalLocationRequestStorageTypeS3 {
			return CreateExternalLocation400JSONResponse{
				Code: 400, Message: fmt.Sprintf("unsupported storage type %q; supported: S3", string(*req.Body.StorageType)),
			}, nil
		}
		domReq.StorageType = domain.StorageType(*req.Body.StorageType)
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}
	if req.Body.ReadOnly != nil {
		domReq.ReadOnly = *req.Body.ReadOnly
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.externalLocations.Create(ctx, principal, domReq)
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
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
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

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.externalLocations.Update(ctx, principal, req.LocationName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateExternalLocation403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateExternalLocation404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateExternalLocation200JSONResponse(externalLocationToAPI(*result)), nil
}

func (h *APIHandler) DeleteExternalLocation(ctx context.Context, req DeleteExternalLocationRequestObject) (DeleteExternalLocationResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.externalLocations.Delete(ctx, principal, req.LocationName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteExternalLocation403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteExternalLocation404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteExternalLocation204Response{}, nil
}

// === API Mappers for Storage Credentials / External Locations ===

// storageCredentialToAPI converts a domain StorageCredential to the API type.
// IMPORTANT: Never expose key_id, secret, azure_account_key, or azure_client_secret in API responses.
func storageCredentialToAPI(c domain.StorageCredential) StorageCredential {
	ct := StorageCredentialCredentialType(c.CredentialType)
	resp := StorageCredential{
		Id:             &c.ID,
		Name:           &c.Name,
		CredentialType: &ct,
		// S3 fields (non-sensitive)
		Endpoint: &c.Endpoint,
		Region:   &c.Region,
		UrlStyle: &c.URLStyle,
		// Azure fields (non-sensitive only)
		AzureAccountName: optStr(c.AzureAccountName),
		AzureClientId:    optStr(c.AzureClientID),
		AzureTenantId:    optStr(c.AzureTenantID),
		// GCS fields
		GcsKeyFilePath: optStr(c.GCSKeyFilePath),
		Comment:        optStr(c.Comment),
		Owner:          &c.Owner,
		CreatedAt:      &c.CreatedAt,
		UpdatedAt:      &c.UpdatedAt,
	}
	return resp
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

// === Volumes ===

func (h *APIHandler) ListVolumes(ctx context.Context, req ListVolumesRequestObject) (ListVolumesResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	vols, total, err := h.volumes.List(ctx, req.SchemaName, page)
	if err != nil {
		return nil, err
	}

	data := make([]VolumeDetail, len(vols))
	for i, v := range vols {
		data[i] = volumeToAPI(v)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListVolumes200JSONResponse{
		Data:          &data,
		NextPageToken: optStr(nextToken),
	}, nil
}

func (h *APIHandler) CreateVolume(ctx context.Context, req CreateVolumeRequestObject) (CreateVolumeResponseObject, error) {
	domReq := domain.CreateVolumeRequest{
		Name:       req.Body.Name,
		VolumeType: string(req.Body.VolumeType),
	}
	if req.Body.StorageLocation != nil {
		domReq.StorageLocation = *req.Body.StorageLocation
	}
	if req.Body.Comment != nil {
		domReq.Comment = *req.Body.Comment
	}

	result, err := h.volumes.Create(ctx, req.SchemaName, domReq)
	if err != nil {
		var accessErr *domain.AccessDeniedError
		var validErr *domain.ValidationError
		var conflictErr *domain.ConflictError
		switch {
		case errors.As(err, &accessErr):
			return CreateVolume403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, &validErr):
			return CreateVolume400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, &conflictErr):
			return CreateVolume409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return CreateVolume400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateVolume201JSONResponse(volumeToAPI(*result)), nil
}

func (h *APIHandler) GetVolume(ctx context.Context, req GetVolumeRequestObject) (GetVolumeResponseObject, error) {
	result, err := h.volumes.GetByName(ctx, req.SchemaName, req.VolumeName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetVolume404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetVolume200JSONResponse(volumeToAPI(*result)), nil
}

func (h *APIHandler) UpdateVolume(ctx context.Context, req UpdateVolumeRequestObject) (UpdateVolumeResponseObject, error) {
	domReq := domain.UpdateVolumeRequest{
		NewName: req.Body.NewName,
		Comment: req.Body.Comment,
		Owner:   req.Body.Owner,
	}

	result, err := h.volumes.Update(ctx, req.SchemaName, req.VolumeName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateVolume403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateVolume404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateVolume200JSONResponse(volumeToAPI(*result)), nil
}

func (h *APIHandler) DeleteVolume(ctx context.Context, req DeleteVolumeRequestObject) (DeleteVolumeResponseObject, error) {
	if err := h.volumes.Delete(ctx, req.SchemaName, req.VolumeName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteVolume403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteVolume404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteVolume204Response{}, nil
}

// volumeToAPI converts a domain Volume to the API VolumeDetail type.
func volumeToAPI(v domain.Volume) VolumeDetail {
	vt := VolumeDetailVolumeType(v.VolumeType)
	return VolumeDetail{
		Id:              &v.ID,
		Name:            &v.Name,
		SchemaName:      &v.SchemaName,
		CatalogName:     &v.CatalogName,
		VolumeType:      &vt,
		StorageLocation: optStr(v.StorageLocation),
		Comment:         optStr(v.Comment),
		Owner:           &v.Owner,
		CreatedAt:       &v.CreatedAt,
		UpdatedAt:       &v.UpdatedAt,
	}
}

// === Compute Endpoints ===

func (h *APIHandler) ListComputeEndpoints(ctx context.Context, req ListComputeEndpointsRequestObject) (ListComputeEndpointsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	eps, total, err := h.computeEndpoints.List(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]ComputeEndpoint, len(eps))
	for i, ep := range eps {
		data[i] = computeEndpointToAPI(ep)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListComputeEndpoints200JSONResponse{
		Data:          &data,
		NextPageToken: optStr(nextToken),
	}, nil
}

func (h *APIHandler) CreateComputeEndpoint(ctx context.Context, req CreateComputeEndpointRequestObject) (CreateComputeEndpointResponseObject, error) {
	domReq := domain.CreateComputeEndpointRequest{
		Name: req.Body.Name,
		URL:  req.Body.Url,
		Type: string(req.Body.Type),
	}
	if req.Body.Size != nil {
		domReq.Size = string(*req.Body.Size)
	}
	if req.Body.MaxMemoryGb != nil {
		domReq.MaxMemoryGB = req.Body.MaxMemoryGb
	}
	if req.Body.AuthToken != nil {
		domReq.AuthToken = *req.Body.AuthToken
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.computeEndpoints.Create(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateComputeEndpoint403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateComputeEndpoint400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateComputeEndpoint409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return CreateComputeEndpoint400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateComputeEndpoint201JSONResponse(computeEndpointToAPI(*result)), nil
}

func (h *APIHandler) GetComputeEndpoint(ctx context.Context, req GetComputeEndpointRequestObject) (GetComputeEndpointResponseObject, error) {
	result, err := h.computeEndpoints.GetByName(ctx, req.EndpointName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetComputeEndpoint404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return GetComputeEndpoint200JSONResponse(computeEndpointToAPI(*result)), nil
}

func (h *APIHandler) UpdateComputeEndpoint(ctx context.Context, req UpdateComputeEndpointRequestObject) (UpdateComputeEndpointResponseObject, error) {
	domReq := domain.UpdateComputeEndpointRequest{
		URL:         req.Body.Url,
		MaxMemoryGB: req.Body.MaxMemoryGb,
		AuthToken:   req.Body.AuthToken,
	}
	if req.Body.Size != nil {
		s := string(*req.Body.Size)
		domReq.Size = &s
	}
	if req.Body.Status != nil {
		s := string(*req.Body.Status)
		domReq.Status = &s
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.computeEndpoints.Update(ctx, principal, req.EndpointName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateComputeEndpoint403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateComputeEndpoint404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return UpdateComputeEndpoint200JSONResponse(computeEndpointToAPI(*result)), nil
}

func (h *APIHandler) DeleteComputeEndpoint(ctx context.Context, req DeleteComputeEndpointRequestObject) (DeleteComputeEndpointResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.computeEndpoints.Delete(ctx, principal, req.EndpointName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteComputeEndpoint403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteComputeEndpoint404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteComputeEndpoint204Response{}, nil
}

func (h *APIHandler) ListComputeAssignments(ctx context.Context, req ListComputeAssignmentsRequestObject) (ListComputeAssignmentsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	assignments, total, err := h.computeEndpoints.ListAssignments(ctx, req.EndpointName, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListComputeAssignments404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}

	data := make([]ComputeAssignment, len(assignments))
	for i, a := range assignments {
		data[i] = computeAssignmentToAPI(a)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListComputeAssignments200JSONResponse{
		Data:          &data,
		NextPageToken: optStr(nextToken),
	}, nil
}

func (h *APIHandler) CreateComputeAssignment(ctx context.Context, req CreateComputeAssignmentRequestObject) (CreateComputeAssignmentResponseObject, error) {
	domReq := domain.CreateComputeAssignmentRequest{
		PrincipalID:   req.Body.PrincipalId,
		PrincipalType: string(req.Body.PrincipalType),
	}
	if req.Body.IsDefault != nil {
		domReq.IsDefault = *req.Body.IsDefault
	} else {
		domReq.IsDefault = true
	}
	if req.Body.FallbackLocal != nil {
		domReq.FallbackLocal = *req.Body.FallbackLocal
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.computeEndpoints.Assign(ctx, principal, req.EndpointName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateComputeAssignment403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateComputeAssignment400JSONResponse{Code: 400, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateComputeAssignment409JSONResponse{Code: 409, Message: err.Error()}, nil
		default:
			return CreateComputeAssignment400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}
	return CreateComputeAssignment201JSONResponse(computeAssignmentToAPI(*result)), nil
}

func (h *APIHandler) GetComputeEndpointHealth(ctx context.Context, req GetComputeEndpointHealthRequestObject) (GetComputeEndpointHealthResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	result, err := h.computeEndpoints.HealthCheck(ctx, principal, req.EndpointName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GetComputeEndpointHealth403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return GetComputeEndpointHealth404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return GetComputeEndpointHealth502JSONResponse{Code: 502, Message: err.Error()}, nil
		}
	}

	return GetComputeEndpointHealth200JSONResponse{
		Status:        result.Status,
		UptimeSeconds: result.UptimeSeconds,
		DuckdbVersion: result.DuckdbVersion,
		MemoryUsedMb:  result.MemoryUsedMb,
		MaxMemoryGb:   result.MaxMemoryGb,
		EndpointName:  &req.EndpointName,
	}, nil
}

func (h *APIHandler) DeleteComputeAssignment(ctx context.Context, req DeleteComputeAssignmentRequestObject) (DeleteComputeAssignmentResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	if err := h.computeEndpoints.Unassign(ctx, principal, req.AssignmentId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteComputeAssignment403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteComputeAssignment404JSONResponse{Code: 404, Message: err.Error()}, nil
		default:
			return nil, err
		}
	}
	return DeleteComputeAssignment204Response{}, nil
}

// === Compute Endpoint Mappers ===

// computeEndpointToAPI converts a domain ComputeEndpoint to the API type.
// IMPORTANT: Never expose auth_token in API responses.
func computeEndpointToAPI(ep domain.ComputeEndpoint) ComputeEndpoint {
	ct := ep.CreatedAt
	ut := ep.UpdatedAt
	t := ComputeEndpointType(ep.Type)
	st := ComputeEndpointStatus(ep.Status)
	extID, _ := uuid.Parse(ep.ExternalID)
	resp := ComputeEndpoint{
		Id:         &ep.ID,
		ExternalId: &extID,
		Name:       &ep.Name,
		Url:        &ep.URL,
		Type:       &t,
		Status:     &st,
		Owner:      &ep.Owner,
		CreatedAt:  &ct,
		UpdatedAt:  &ut,
	}
	if ep.Size != "" {
		s := ComputeEndpointSize(ep.Size)
		resp.Size = &s
	}
	if ep.MaxMemoryGB != nil {
		resp.MaxMemoryGb = ep.MaxMemoryGB
	}
	return resp
}

func computeAssignmentToAPI(a domain.ComputeAssignment) ComputeAssignment {
	ct := a.CreatedAt
	pt := ComputeAssignmentPrincipalType(a.PrincipalType)
	return ComputeAssignment{
		Id:            &a.ID,
		PrincipalId:   &a.PrincipalID,
		PrincipalType: &pt,
		EndpointId:    &a.EndpointID,
		EndpointName:  optStr(a.EndpointName),
		IsDefault:     &a.IsDefault,
		FallbackLocal: &a.FallbackLocal,
		CreatedAt:     &ct,
	}
}
