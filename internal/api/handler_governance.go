package api

import (
	"context"
	"errors"

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
}

// tagService defines the tag operations used by the API handler.
type tagService interface {
	ListTags(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error)
	CreateTag(ctx context.Context, principal string, req domain.CreateTagRequest) (*domain.Tag, error)
	DeleteTag(ctx context.Context, principal string, id string) error
	AssignTag(ctx context.Context, principal string, req domain.AssignTagRequest) (*domain.TagAssignment, error)
	UnassignTag(ctx context.Context, principal string, id string) error
}

// === Audit Logs ===

// ListAuditLogs implements the endpoint for listing audit log entries.
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return ListAuditLogs403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]AuditEntry, len(entries))
	for i, e := range entries {
		data[i] = auditEntryToAPI(e)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListAuditLogs200JSONResponse{
		Body:    PaginatedAuditLogs{Data: &data, NextPageToken: optStr(npt)},
		Headers: ListAuditLogs200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Query History ===

// ListQueryHistory implements the endpoint for listing query history entries.
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
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return ListQueryHistory403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]QueryHistoryEntry, len(entries))
	for i, e := range entries {
		data[i] = queryHistoryEntryToAPI(e)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListQueryHistory200JSONResponse{
		Body:    PaginatedQueryHistoryEntries{Data: &data, NextPageToken: optStr(npt)},
		Headers: ListQueryHistory200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Search ===

// SearchCatalog implements the endpoint for searching catalog objects.
func (h *APIHandler) SearchCatalog(ctx context.Context, req SearchCatalogRequestObject) (SearchCatalogResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)

	results, total, err := h.search.Search(ctx, req.Params.Query, req.Params.Type, req.Params.Catalog, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return SearchCatalog400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil //nolint:nilerr // oapi-codegen strict handler: typed response encodes the error
		default:
			return SearchCatalog500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: 500, Message: err.Error()}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil //nolint:nilerr // oapi-codegen strict handler: typed response encodes the error
		}
	}

	data := make([]SearchResult, len(results))
	for i, r := range results {
		data[i] = searchResultToAPI(r)
	}

	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return SearchCatalog200JSONResponse{
		Body:    PaginatedSearchResults{Data: &data, NextPageToken: optStr(npt)},
		Headers: SearchCatalog200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Lineage ===

// GetTableLineage implements the endpoint for retrieving full lineage of a table.
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
		Body: LineageNode{
			TableName:  &node.TableName,
			Upstream:   &upstream,
			Downstream: &downstream,
		},
		Headers: GetTableLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetUpstreamLineage implements the endpoint for retrieving upstream lineage edges.
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
	return GetUpstreamLineage200JSONResponse{
		Body:    PaginatedLineageEdges{Data: &data, NextPageToken: optStr(npt)},
		Headers: GetUpstreamLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetDownstreamLineage implements the endpoint for retrieving downstream lineage edges.
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
	return GetDownstreamLineage200JSONResponse{
		Body:    PaginatedLineageEdges{Data: &data, NextPageToken: optStr(npt)},
		Headers: GetDownstreamLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteLineageEdge implements the endpoint for deleting a lineage edge by ID.
func (h *APIHandler) DeleteLineageEdge(ctx context.Context, req DeleteLineageEdgeRequestObject) (DeleteLineageEdgeResponseObject, error) {
	if err := h.lineage.DeleteEdge(ctx, req.EdgeId); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteLineageEdge404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteLineageEdge204Response{}, nil
}

// PurgeLineage implements the endpoint for purging lineage data older than a threshold.
func (h *APIHandler) PurgeLineage(ctx context.Context, req PurgeLineageRequestObject) (PurgeLineageResponseObject, error) {
	caller, ok := domain.PrincipalFromContext(ctx)
	if !ok || !caller.IsAdmin {
		return PurgeLineage403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: "admin privileges required"}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	deleted, err := h.lineage.PurgeOlderThan(ctx, int(req.Body.OlderThanDays))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return PurgeLineage403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return PurgeLineage500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: 500, Message: err.Error()}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return PurgeLineage200JSONResponse{
		Body:    PurgeLineageResponse{DeletedCount: &deleted},
		Headers: PurgeLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Tags ===

// ListTags implements the endpoint for listing all tags.
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
	return ListTags200JSONResponse{
		Body:    PaginatedTags{Data: &data, NextPageToken: optStr(npt)},
		Headers: ListTags200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateTag implements the endpoint for creating a new tag.
func (h *APIHandler) CreateTag(ctx context.Context, req CreateTagRequestObject) (CreateTagResponseObject, error) {
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
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return CreateTag400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateTag409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreateTag201JSONResponse{
		Body:    tagToAPI(*result),
		Headers: CreateTag201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteTag implements the endpoint for deleting a tag by ID.
func (h *APIHandler) DeleteTag(ctx context.Context, req DeleteTagRequestObject) (DeleteTagResponseObject, error) {
	caller, ok := domain.PrincipalFromContext(ctx)
	if !ok || !caller.IsAdmin {
		return DeleteTag403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: "admin privileges required"}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	principal := caller.Name
	if err := h.tags.DeleteTag(ctx, principal, req.TagId); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteTag404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteTag204Response{}, nil
}

// CreateTagAssignment implements the endpoint for assigning a tag to a securable object.
func (h *APIHandler) CreateTagAssignment(ctx context.Context, req CreateTagAssignmentRequestObject) (CreateTagAssignmentResponseObject, error) {
	domReq := domain.AssignTagRequest{
		TagID:         req.TagId,
		SecurableType: req.Body.SecurableType,
		SecurableID:   req.Body.SecurableId,
		ColumnName:    req.Body.ColumnName,
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.tags.AssignTag(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ConflictError)):
			return CreateTagAssignment409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreateTagAssignment201JSONResponse{
		Body:    tagAssignmentToAPI(*result),
		Headers: CreateTagAssignment201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteTagAssignment implements the endpoint for removing a tag assignment.
func (h *APIHandler) DeleteTagAssignment(ctx context.Context, req DeleteTagAssignmentRequestObject) (DeleteTagAssignmentResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.tags.UnassignTag(ctx, principal, req.AssignmentId); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteTagAssignment404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteTagAssignment403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return DeleteTagAssignment400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return DeleteTagAssignment500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: 500, Message: err.Error()}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return DeleteTagAssignment204Response{}, nil
}

// ListClassifications implements the endpoint for listing classification and sensitivity tags.
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

	return ListClassifications200JSONResponse{
		Body:    PaginatedTags{Data: &filtered},
		Headers: ListClassifications200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}
