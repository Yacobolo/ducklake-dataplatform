package api

import (
	"context"

	"duck-demo/internal/domain"
)

// auditService defines the audit operations used by the API handler.
type auditService interface {
	List(ctx context.Context, filter domain.AuditFilter) ([]domain.AuditEntry, int64, error)
}

// queryHistoryService defines the query history operations used by the API handler.
type queryHistoryService interface {
	List(ctx context.Context, filter domain.QueryHistoryFilter) ([]domain.QueryHistoryEntry, int64, error)
}

// searchService defines the search operations used by the API handler.
type searchService interface {
	Search(ctx context.Context, query string, objectType *string, catalogName *string, page domain.PageRequest) ([]domain.SearchResult, int64, error)
}

// lineageService defines the lineage operations used by the API handler.
type lineageService interface {
	GetFullLineage(ctx context.Context, tableName string, page domain.PageRequest) (*domain.LineageNode, error)
	GetUpstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
	GetDownstream(ctx context.Context, tableName string, page domain.PageRequest) ([]domain.LineageEdge, int64, error)
	DeleteEdge(ctx context.Context, id string) error
	PurgeOlderThan(ctx context.Context, olderThanDays int) (int64, error)
	GetColumnLineageForTable(ctx context.Context, schema, table string) ([]domain.ColumnLineageEdge, error)
	GetColumnLineageForSourceColumn(ctx context.Context, schema, table, column string) ([]domain.ColumnLineageEdge, error)
}

// tagService defines the tag operations used by the API handler.
type tagService interface {
	ListTags(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error)
	CreateTag(ctx context.Context, principal string, req domain.CreateTagRequest) (*domain.Tag, error)
	DeleteTag(ctx context.Context, principal string, id string) error
	AssignTag(ctx context.Context, principal string, req domain.AssignTagRequest) (*domain.TagAssignment, error)
	UnassignTag(ctx context.Context, principal string, id string) error
	ListAssignmentsForTag(ctx context.Context, tagID string) ([]domain.TagAssignment, error)
}

// === Audit Logs ===

// ListAuditLogs implements the endpoint for listing audit log entries.
func (h *APIHandler) ListAuditLogs(ctx context.Context, req GenListAuditLogsRequest) (GenListAuditLogsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	filter := domain.AuditFilter{
		PrincipalName: req.Params.PrincipalName,
		Action:        req.Params.Action,
		Page:          page,
	}
	if req.Params.Status != nil {
		status := string(*req.Params.Status)
		filter.Status = &status
	}

	entries, total, err := h.audit.List(ctx, filter)
	if err != nil {
		if resp, ok := respondDomainError[GenListAuditLogsResponse](err, domainErrorResponder[GenListAuditLogsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListAuditLogsResponse { return ListAuditLogs403JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]AuditEntry, len(entries))
	for i, e := range entries {
		data[i] = auditEntryToAPI(e)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListAuditLogs200JSONResponse{
		Body:    PaginatedAuditLogs{Data: data, NextPageToken: optStr(npt)},
		Headers: GenListAuditLogs200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Query History ===

// ListQueryHistory implements the endpoint for listing query history entries.
func (h *APIHandler) ListQueryHistory(ctx context.Context, req GenListQueryHistoryRequest) (GenListQueryHistoryResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	filter := domain.QueryHistoryFilter{
		PrincipalName: req.Params.PrincipalName,
		From:          req.Params.From,
		To:            req.Params.To,
		Page:          page,
	}
	if req.Params.Status != nil {
		status := string(*req.Params.Status)
		filter.Status = &status
	}

	entries, total, err := h.queryHistory.List(ctx, filter)
	if err != nil {
		if resp, ok := respondDomainError[GenListQueryHistoryResponse](err, domainErrorResponder[GenListQueryHistoryResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListQueryHistoryResponse {
				return ListQueryHistory403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]QueryHistoryEntry, len(entries))
	for i, e := range entries {
		data[i] = queryHistoryEntryToAPI(e)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListQueryHistory200JSONResponse{
		Body:    PaginatedQueryHistoryEntries{Data: data, NextPageToken: optStr(npt)},
		Headers: GenListQueryHistory200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Search ===

// SearchCatalog implements the endpoint for searching catalog objects.
func (h *APIHandler) SearchCatalog(ctx context.Context, req GenSearchCatalogRequest) (GenSearchCatalogResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)

	results, total, err := h.search.Search(ctx, req.Params.Query, req.Params.Type, req.Params.Catalog, page)
	if err != nil {
		if resp, ok := respondDomainError[GenSearchCatalogResponse](err, domainErrorResponder[GenSearchCatalogResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenSearchCatalogResponse { return SearchCatalog400JSONResponse{resp} },
			NotFound: func(resp NotFoundJSONResponse) GenSearchCatalogResponse {
				return SearchCatalog404JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenSearchCatalogResponse {
				return GenSearchCatalog500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return GenSearchCatalog500JSONResponse{GenInternalErrorJSONResponse(internalErrorResponse(err))}, nil
	}

	data := make([]SearchResult, len(results))
	for i, r := range results {
		data[i] = searchResultToAPI(r)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenSearchCatalog200JSONResponse{
		Body:    PaginatedSearchResults{Data: data, NextPageToken: optStr(npt)},
		Headers: GenSearchCatalog200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Lineage ===

// GetTableLineage implements the endpoint for retrieving full lineage of a table.
func (h *APIHandler) GetTableLineage(ctx context.Context, req GenGetTableLineageRequest) (GenGetTableLineageResponse, error) {
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

	return GenGetTableLineage200JSONResponse{
		Body: LineageNode{
			TableName:  &node.TableName,
			Upstream:   &upstream,
			Downstream: &downstream,
		},
		Headers: GenGetTableLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetUpstreamLineage implements the endpoint for retrieving upstream lineage edges.
func (h *APIHandler) GetUpstreamLineage(ctx context.Context, req GenGetUpstreamLineageRequest) (GenGetUpstreamLineageResponse, error) {
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
	return GenGetUpstreamLineage200JSONResponse{
		Body:    PaginatedLineageEdges{Data: data, NextPageToken: optStr(npt)},
		Headers: GenGetUpstreamLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetDownstreamLineage implements the endpoint for retrieving downstream lineage edges.
func (h *APIHandler) GetDownstreamLineage(ctx context.Context, req GenGetDownstreamLineageRequest) (GenGetDownstreamLineageResponse, error) {
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
	return GenGetDownstreamLineage200JSONResponse{
		Body:    PaginatedLineageEdges{Data: data, NextPageToken: optStr(npt)},
		Headers: GenGetDownstreamLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteLineageEdge implements the endpoint for deleting a lineage edge by ID.
func (h *APIHandler) DeleteLineageEdge(ctx context.Context, req GenDeleteLineageEdgeRequest) (GenDeleteLineageEdgeResponse, error) {
	if err := h.lineage.DeleteEdge(ctx, req.EdgeId); err != nil {
		if resp, ok := respondDomainError[GenDeleteLineageEdgeResponse](err, domainErrorResponder[GenDeleteLineageEdgeResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenDeleteLineageEdgeResponse {
				return DeleteLineageEdge404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteLineageEdge204Response{}, nil
}

// PurgeLineage implements the endpoint for purging lineage data older than a threshold.
func (h *APIHandler) PurgeLineage(ctx context.Context, req GenPurgeLineageRequest) (GenPurgeLineageResponse, error) {
	caller, ok := domain.PrincipalFromContext(ctx)
	if !ok || !caller.IsAdmin {
		return PurgeLineage403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: "admin privileges required"}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	deleted, err := h.lineage.PurgeOlderThan(ctx, int(req.Body.OlderThanDays))
	if err != nil {
		if resp, ok := respondDomainError[GenPurgeLineageResponse](err, domainErrorResponder[GenPurgeLineageResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenPurgeLineageResponse { return PurgeLineage403JSONResponse{resp} },
			Internal: func(resp InternalErrorJSONResponse) GenPurgeLineageResponse {
				return GenPurgeLineage500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return GenPurgeLineage500JSONResponse{GenInternalErrorJSONResponse(internalErrorResponse(err))}, nil
	}
	deletedCount := safeInt64ToInt32(deleted)
	return PurgeLineage200JSONResponse{
		Body:    PurgeLineageResponse{DeletedCount: &deletedCount},
		Headers: PurgeLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Column Lineage ===

// GetColumnLineage implements the endpoint for retrieving column-level lineage for a table.
func (h *APIHandler) GetColumnLineage(ctx context.Context, req GenGetColumnLineageRequest) (GenGetColumnLineageResponse, error) {
	edges, err := h.lineage.GetColumnLineageForTable(ctx, req.SchemaName, req.TableName)
	if err != nil {
		return nil, err
	}

	data := make([]ColumnLineageEdge, len(edges))
	for i, e := range edges {
		data[i] = columnLineageEdgeToAPI(e)
	}

	return GenGetColumnLineage200JSONResponse{
		Body:    PaginatedColumnLineageEdges{Data: data},
		Headers: GenGetColumnLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetColumnImpact implements the endpoint for impact analysis on a source column.
func (h *APIHandler) GetColumnImpact(ctx context.Context, req GenGetColumnImpactRequest) (GenGetColumnImpactResponse, error) {
	edges, err := h.lineage.GetColumnLineageForSourceColumn(ctx, req.SchemaName, req.TableName, req.ColumnName)
	if err != nil {
		return nil, err
	}

	data := make([]ColumnLineageEdge, len(edges))
	for i, e := range edges {
		data[i] = columnLineageEdgeToAPI(e)
	}

	return GenGetColumnImpact200JSONResponse{
		Body:    PaginatedColumnLineageEdges{Data: data},
		Headers: GenGetColumnImpact200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Tags ===

// ListTags implements the endpoint for listing all tags.
func (h *APIHandler) ListTags(ctx context.Context, req GenListTagsRequest) (GenListTagsResponse, error) {
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
	return GenListTags200JSONResponse{
		Body:    PaginatedTags{Data: data, NextPageToken: optStr(npt)},
		Headers: GenListTags200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateTag implements the endpoint for creating a new tag.
func (h *APIHandler) CreateTag(ctx context.Context, req GenCreateTagRequest) (GenCreateTagResponse, error) {
	caller, ok := domain.PrincipalFromContext(ctx)
	if !ok || !caller.IsAdmin {
		return CreateTag403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: "admin privileges required"}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	domReq := domain.CreateTagRequest{
		Key:   req.Body.Key,
		Value: req.Body.Value,
	}
	principal := caller.Name
	result, err := h.tags.CreateTag(ctx, principal, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateTagResponse](err, domainErrorResponder[GenCreateTagResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateTagResponse { return CreateTag400JSONResponse{resp} },
			Conflict:   func(resp ConflictJSONResponse) GenCreateTagResponse { return CreateTag409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreateTag201JSONResponse{
		Body:    tagToAPI(*result),
		Headers: GenCreateTag201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteTag implements the endpoint for deleting a tag by ID.
func (h *APIHandler) DeleteTag(ctx context.Context, req GenDeleteTagRequest) (GenDeleteTagResponse, error) {
	caller, ok := domain.PrincipalFromContext(ctx)
	if !ok || !caller.IsAdmin {
		return DeleteTag403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: "admin privileges required"}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	principal := caller.Name
	if err := h.tags.DeleteTag(ctx, principal, req.TagId); err != nil {
		if resp, ok := respondDomainError[GenDeleteTagResponse](err, domainErrorResponder[GenDeleteTagResponse]{
			NotFound: func(resp NotFoundJSONResponse) GenDeleteTagResponse { return DeleteTag404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteTag204Response{}, nil
}

// CreateTagAssignment implements the endpoint for assigning a tag to a securable object.
func (h *APIHandler) CreateTagAssignment(ctx context.Context, req GenCreateTagAssignmentRequest) (GenCreateTagAssignmentResponse, error) {
	domReq := domain.AssignTagRequest{
		TagID:         req.TagId,
		SecurableType: string(req.Body.SecurableType),
		SecurableID:   req.Body.SecurableId,
		ColumnName:    req.Body.ColumnName,
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.tags.AssignTag(ctx, principal, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateTagAssignmentResponse](err, domainErrorResponder[GenCreateTagAssignmentResponse]{
			Conflict: func(resp ConflictJSONResponse) GenCreateTagAssignmentResponse {
				return CreateTagAssignment409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreateTagAssignment201JSONResponse{
		Body:    tagAssignmentToAPI(*result),
		Headers: GenCreateTagAssignment201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListTagAssignments implements the endpoint for listing assignments for a tag.
func (h *APIHandler) ListTagAssignments(ctx context.Context, req GenListTagAssignmentsRequest) (GenListTagAssignmentsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	assignments, err := h.tags.ListAssignmentsForTag(ctx, req.TagId)
	if err != nil {
		return nil, err
	}
	start := page.Offset()
	if start > len(assignments) {
		start = len(assignments)
	}
	end := start + page.Limit()
	if end > len(assignments) {
		end = len(assignments)
	}
	data := make([]TagAssignment, 0, end-start)
	for _, assignment := range assignments[start:end] {
		data = append(data, tagAssignmentToAPI(assignment))
	}
	next := domain.NextPageToken(start, page.Limit(), int64(len(assignments)))
	return ListTagAssignments200JSONResponse{
		Body:    PaginatedTagAssignments{Data: data, NextPageToken: optStr(next)},
		Headers: ListTagAssignments200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteTagAssignment implements the endpoint for removing a tag assignment.
func (h *APIHandler) DeleteTagAssignment(ctx context.Context, req GenDeleteTagAssignmentRequest) (GenDeleteTagAssignmentResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.tags.UnassignTag(ctx, principal, req.AssignmentId); err != nil {
		if resp, ok := respondDomainError[GenDeleteTagAssignmentResponse](err, domainErrorResponder[GenDeleteTagAssignmentResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteTagAssignmentResponse {
				return DeleteTagAssignment400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteTagAssignmentResponse {
				return DeleteTagAssignment403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteTagAssignmentResponse {
				return DeleteTagAssignment404JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenDeleteTagAssignmentResponse {
				return GenDeleteTagAssignment500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return GenDeleteTagAssignment500JSONResponse{GenInternalErrorJSONResponse(internalErrorResponse(err))}, nil
	}
	return GenDeleteTagAssignment204Response{}, nil
}

// ListClassifications implements the endpoint for listing classification and sensitivity tags.
func (h *APIHandler) ListClassifications(ctx context.Context, _ GenListClassificationsRequest) (GenListClassificationsResponse, error) {
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

	return GenListClassifications200JSONResponse{
		Body:    PaginatedTags{Data: filtered},
		Headers: GenListClassifications200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}
