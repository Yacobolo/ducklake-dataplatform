package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

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
