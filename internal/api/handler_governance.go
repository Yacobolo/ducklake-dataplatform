package api

import (
	"context"

	"github.com/Yacobolo/quackstack/internal/domain"
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
	GetTag(ctx context.Context, id string) (*domain.Tag, error)
	ListTags(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error)
	CreateTag(ctx context.Context, principal string, req domain.CreateTagRequest) (*domain.Tag, error)
	UpdateTag(ctx context.Context, principal string, id string, req domain.UpdateTagRequest) (*domain.Tag, error)
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
		if resp, ok := respondDomainErrorForOperation[GenListAuditLogsResponse]("listAuditLogs", err, domainErrorResponder[GenListAuditLogsResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenListQueryHistoryResponse]("listQueryHistory", err, domainErrorResponder[GenListQueryHistoryResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenSearchCatalogResponse]("searchCatalog", err, domainErrorResponder[GenSearchCatalogResponse]{
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

// GetCatalogTableLineage implements the canonical catalog lineage endpoint for a table.
func (h *APIHandler) GetCatalogTableLineage(ctx context.Context, req GenGetCatalogTableLineageRequest) (GenGetCatalogTableLineageResponse, error) {
	resp, err := h.GetTableLineage(ctx, GenGetTableLineageRequest{SchemaName: req.SchemaName, TableName: req.TableName, Params: GenGetTableLineageParams(req.Params)})
	if err != nil {
		return nil, err
	}
	switch typed := resp.(type) {
	case GenGetTableLineage200JSONResponse:
		return GenGetCatalogTableLineage200JSONResponse{Body: typed.Body, Headers: GenGetCatalogTableLineage200ResponseHeaders(typed.Headers)}, nil
	case GenGetTableLineage400JSONResponse:
		return GenGetCatalogTableLineage400JSONResponse(typed), nil
	default:
		return nil, domain.ErrValidation("unexpected response type for catalog table lineage")
	}
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

// GetCatalogUpstreamLineage implements the canonical catalog upstream lineage endpoint.
func (h *APIHandler) GetCatalogUpstreamLineage(ctx context.Context, req GenGetCatalogUpstreamLineageRequest) (GenGetCatalogUpstreamLineageResponse, error) {
	resp, err := h.GetUpstreamLineage(ctx, GenGetUpstreamLineageRequest{SchemaName: req.SchemaName, TableName: req.TableName, Params: GenGetUpstreamLineageParams(req.Params)})
	if err != nil {
		return nil, err
	}
	switch typed := resp.(type) {
	case GenGetUpstreamLineage200JSONResponse:
		return GenGetCatalogUpstreamLineage200JSONResponse{Body: typed.Body, Headers: GenGetCatalogUpstreamLineage200ResponseHeaders(typed.Headers)}, nil
	case GenGetUpstreamLineage400JSONResponse:
		return GenGetCatalogUpstreamLineage400JSONResponse(typed), nil
	default:
		return nil, domain.ErrValidation("unexpected response type for catalog upstream lineage")
	}
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

// GetCatalogDownstreamLineage implements the canonical catalog downstream lineage endpoint.
func (h *APIHandler) GetCatalogDownstreamLineage(ctx context.Context, req GenGetCatalogDownstreamLineageRequest) (GenGetCatalogDownstreamLineageResponse, error) {
	resp, err := h.GetDownstreamLineage(ctx, GenGetDownstreamLineageRequest{SchemaName: req.SchemaName, TableName: req.TableName, Params: GenGetDownstreamLineageParams(req.Params)})
	if err != nil {
		return nil, err
	}
	switch typed := resp.(type) {
	case GenGetDownstreamLineage200JSONResponse:
		return GenGetCatalogDownstreamLineage200JSONResponse{Body: typed.Body, Headers: GenGetCatalogDownstreamLineage200ResponseHeaders(typed.Headers)}, nil
	case GenGetDownstreamLineage400JSONResponse:
		return GenGetCatalogDownstreamLineage400JSONResponse(typed), nil
	default:
		return nil, domain.ErrValidation("unexpected response type for catalog downstream lineage")
	}
}

// DeleteLineageEdge implements the endpoint for deleting a lineage edge by ID.
func (h *APIHandler) DeleteLineageEdge(ctx context.Context, req GenDeleteLineageEdgeRequest) (GenDeleteLineageEdgeResponse, error) {
	if err := h.lineage.DeleteEdge(ctx, req.EdgeId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteLineageEdgeResponse]("deleteLineageEdge", err, domainErrorResponder[GenDeleteLineageEdgeResponse]{
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

// DeleteCatalogLineageEdge implements the canonical catalog lineage edge deletion endpoint.
func (h *APIHandler) DeleteCatalogLineageEdge(ctx context.Context, req GenDeleteCatalogLineageEdgeRequest) (GenDeleteCatalogLineageEdgeResponse, error) {
	resp, err := h.DeleteLineageEdge(ctx, GenDeleteLineageEdgeRequest(req))
	if err != nil {
		return nil, err
	}
	switch typed := resp.(type) {
	case GenDeleteLineageEdge204Response:
		return GenDeleteCatalogLineageEdge204Response{Headers: GenDeleteCatalogLineageEdge204ResponseHeaders(typed.Headers)}, nil
	case DeleteLineageEdge404JSONResponse:
		return DeleteCatalogLineageEdge404JSONResponse(typed), nil
	default:
		return nil, domain.ErrValidation("unexpected response type for catalog lineage edge deletion")
	}
}

// PurgeLineage implements the endpoint for purging lineage data older than a threshold.
func (h *APIHandler) PurgeLineage(ctx context.Context, req GenPurgeLineageRequest) (GenPurgeLineageResponse, error) {
	caller, ok := domain.PrincipalFromContext(ctx)
	if !ok || !caller.IsAdmin {
		return PurgeLineage403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: "admin privileges required"}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	deleted, err := h.lineage.PurgeOlderThan(ctx, int(req.Body.OlderThanDays))
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenPurgeLineageResponse]("purgeLineage", err, domainErrorResponder[GenPurgeLineageResponse]{
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

// PurgeCatalogLineage implements the canonical catalog lineage purge endpoint.
func (h *APIHandler) PurgeCatalogLineage(ctx context.Context, req GenPurgeCatalogLineageRequest) (GenPurgeCatalogLineageResponse, error) {
	resp, err := h.PurgeLineage(ctx, GenPurgeLineageRequest(req))
	if err != nil {
		return nil, err
	}
	switch typed := resp.(type) {
	case PurgeLineage200JSONResponse:
		return PurgeCatalogLineage200JSONResponse{Body: typed.Body, Headers: PurgeCatalogLineage200ResponseHeaders(typed.Headers)}, nil
	case PurgeLineage403JSONResponse:
		return PurgeCatalogLineage403JSONResponse(typed), nil
	case GenPurgeLineage500JSONResponse:
		return GenPurgeCatalogLineage500JSONResponse(typed), nil
	default:
		return nil, domain.ErrValidation("unexpected response type for catalog lineage purge")
	}
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

// GetCatalogColumnLineage implements the canonical catalog column lineage endpoint.
func (h *APIHandler) GetCatalogColumnLineage(ctx context.Context, req GenGetCatalogColumnLineageRequest) (GenGetCatalogColumnLineageResponse, error) {
	resp, err := h.GetColumnLineage(ctx, GenGetColumnLineageRequest{SchemaName: req.SchemaName, TableName: req.TableName, Params: GenGetColumnLineageParams(req.Params)})
	if err != nil {
		return nil, err
	}
	switch typed := resp.(type) {
	case GenGetColumnLineage200JSONResponse:
		return GenGetCatalogColumnLineage200JSONResponse{Body: typed.Body, Headers: GenGetCatalogColumnLineage200ResponseHeaders(typed.Headers)}, nil
	default:
		return nil, domain.ErrValidation("unexpected response type for catalog column lineage")
	}
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

// GetCatalogColumnImpact implements the canonical catalog column impact endpoint.
func (h *APIHandler) GetCatalogColumnImpact(ctx context.Context, req GenGetCatalogColumnImpactRequest) (GenGetCatalogColumnImpactResponse, error) {
	resp, err := h.GetColumnImpact(ctx, GenGetColumnImpactRequest{SchemaName: req.SchemaName, TableName: req.TableName, ColumnName: req.ColumnName, Params: GenGetColumnImpactParams(req.Params)})
	if err != nil {
		return nil, err
	}
	switch typed := resp.(type) {
	case GenGetColumnImpact200JSONResponse:
		return GenGetCatalogColumnImpact200JSONResponse{Body: typed.Body, Headers: GenGetCatalogColumnImpact200ResponseHeaders(typed.Headers)}, nil
	default:
		return nil, domain.ErrValidation("unexpected response type for catalog column impact")
	}
}

// GetBuildColumnLineage implements compile-time column lineage retrieval for a build.
func (h *APIHandler) GetBuildColumnLineage(ctx context.Context, req GenGetBuildColumnLineageRequest) (GenGetBuildColumnLineageResponse, error) {
	items, err := h.models.GetBuildLineage(ctx, req.BuildId, req.Params.ModelName)
	if err != nil {
		return nil, err
	}
	data := make([]CompiledColumnLineage, 0, len(items))
	for _, item := range items {
		data = append(data, compiledColumnLineageToAPI(item))
	}
	return GenGetBuildColumnLineage200JSONResponse{
		Body:    PaginatedCompiledColumnLineage{Data: data},
		Headers: GenGetBuildColumnLineage200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetBuildDiagnostics implements structured build diagnostic retrieval.
func (h *APIHandler) GetBuildDiagnostics(ctx context.Context, req GenGetBuildDiagnosticsRequest) (GenGetBuildDiagnosticsResponse, error) {
	filter := domain.BuildDiagnosticsFilter{
		ModelName: req.Params.ModelName,
		Code:      req.Params.Code,
	}
	if req.Params.Severity != nil {
		severity := domain.DiagnosticSeverity(*req.Params.Severity)
		filter.Severity = &severity
	}
	items, err := h.models.GetBuildDiagnostics(ctx, req.BuildId, filter)
	if err != nil {
		return nil, err
	}
	data := make([]CompileDiagnostic, 0, len(items))
	for _, item := range items {
		data = append(data, compileDiagnosticToAPI(item))
	}
	return GenGetBuildDiagnostics200JSONResponse{
		Body:    PaginatedCompileDiagnostics{Data: data},
		Headers: GenGetBuildDiagnostics200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetBuildSourceColumnImpact implements build-specific source column impact retrieval.
func (h *APIHandler) GetBuildSourceColumnImpact(ctx context.Context, req GenGetBuildSourceColumnImpactRequest) (GenGetBuildSourceColumnImpactResponse, error) {
	items, err := h.models.GetBuildSourceColumnImpact(ctx, req.BuildId, req.SchemaName, req.TableName, req.ColumnName)
	if err != nil {
		return nil, err
	}
	data := make([]CompiledColumnLineage, 0, len(items))
	for _, item := range items {
		data = append(data, compiledColumnLineageToAPI(item))
	}
	return GenGetBuildSourceColumnImpact200JSONResponse{
		Body:    PaginatedCompiledColumnLineage{Data: data},
		Headers: GenGetBuildSourceColumnImpact200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// PlanRebuild returns a code and data aware rebuild plan.
func (h *APIHandler) PlanRebuild(ctx context.Context, req GenPlanRebuildRequest) (GenPlanRebuildResponse, error) {
	if req.Body == nil {
		return GenPlanRebuild400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	plan, err := h.models.PlanRebuild(ctx, cp.Name, domain.PlanRebuildRequest{
		ProjectName:     req.Body.ProjectName,
		EnvironmentName: req.Body.EnvironmentName,
		Selector:        derefString(req.Body.Selector),
	})
	if err != nil {
		return nil, err
	}
	return GenPlanRebuild200JSONResponse{
		Body:    rebuildPlanToAPI(*plan),
		Headers: GenPlanRebuild200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CompareBuilds compares two builds or a build to current head.
func (h *APIHandler) CompareBuilds(ctx context.Context, req GenCompareBuildsRequest) (GenCompareBuildsResponse, error) {
	if req.Body == nil {
		return GenCompareBuilds400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.models.CompareBuilds(ctx, cp.Name, domain.CompareBuildsRequest{
		ProjectName:   derefString(req.Body.ProjectName),
		FromBuildID:   req.Body.FromBuildId,
		ToBuildID:     req.Body.ToBuildId,
		CompareToHead: derefBoolDefault(req.Body.CompareToHead, false),
	})
	if err != nil {
		return nil, err
	}
	return GenCompareBuilds200JSONResponse{
		Body:    buildCompareResultToAPI(*result),
		Headers: GenCompareBuilds200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetModelImpactAnalysis returns downstream impact for a model.
func (h *APIHandler) GetModelImpactAnalysis(ctx context.Context, req GenGetModelImpactAnalysisRequest) (GenGetModelImpactAnalysisResponse, error) {
	result, err := h.models.GetModelImpact(ctx, req.ProjectName, req.Params.BuildId, req.ProjectName+"."+req.ModelName)
	if err != nil {
		return nil, err
	}
	return GenGetModelImpactAnalysis200JSONResponse{
		Body:    buildImpactResultToAPI(*result),
		Headers: GenGetModelImpactAnalysis200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetMacroImpactAnalysis returns downstream impact for a macro.
func (h *APIHandler) GetMacroImpactAnalysis(ctx context.Context, req GenGetMacroImpactAnalysisRequest) (GenGetMacroImpactAnalysisResponse, error) {
	result, err := h.models.GetMacroImpact(ctx, req.ProjectName, req.Params.BuildId, req.MacroName)
	if err != nil {
		return nil, err
	}
	return GenGetMacroImpactAnalysis200JSONResponse{
		Body:    buildImpactResultToAPI(*result),
		Headers: GenGetMacroImpactAnalysis200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
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
		if resp, ok := respondDomainErrorForOperation[GenCreateTagResponse]("createTag", err, domainErrorResponder[GenCreateTagResponse]{
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

// GetTag implements the endpoint for retrieving a tag by ID.
func (h *APIHandler) GetTag(ctx context.Context, req GenGetTagRequest) (GenGetTagResponse, error) {
	tag, err := h.tags.GetTag(ctx, req.TagId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetTagResponse]("getTag", err, domainErrorResponder[GenGetTagResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenGetTagResponse {
				return GetTag403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenGetTagResponse {
				return GetTag404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetTag200JSONResponse{
		Body:    tagToAPI(*tag),
		Headers: GenGetTag200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateTag implements the endpoint for partially updating a tag.
func (h *APIHandler) UpdateTag(ctx context.Context, req GenUpdateTagRequest) (GenUpdateTagResponse, error) {
	caller, ok := domain.PrincipalFromContext(ctx)
	if !ok || !caller.IsAdmin {
		return UpdateTag403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: "admin privileges required"}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	result, err := h.tags.UpdateTag(ctx, caller.Name, req.TagId, domain.UpdateTagRequest{
		Key:   req.Body.Key,
		Value: req.Body.Value,
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateTagResponse]("updateTag", err, domainErrorResponder[GenUpdateTagResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateTagResponse {
				return UpdateTag400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateTagResponse {
				return UpdateTag403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateTagResponse {
				return UpdateTag404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUpdateTagResponse {
				return UpdateTag409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateTag200JSONResponse{
		Body:    tagToAPI(*result),
		Headers: GenUpdateTag200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
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
		if resp, ok := respondDomainErrorForOperation[GenDeleteTagResponse]("deleteTag", err, domainErrorResponder[GenDeleteTagResponse]{
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
	if req.Body == nil {
		return CreateTagAssignment400JSONResponse{badRequestErrorResponse(domain.ErrValidation("request body is required"))}, nil
	}
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
		if resp, ok := respondDomainErrorForOperation[GenCreateTagAssignmentResponse]("createTagAssignment", err, domainErrorResponder[GenCreateTagAssignmentResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateTagAssignmentResponse {
				return CreateTagAssignment400JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateTagAssignmentResponse {
				return CreateTagAssignment409JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateTagAssignmentResponse {
				return CreateTagAssignment403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateTagAssignmentResponse {
				return CreateTagAssignment404JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenCreateTagAssignmentResponse {
				return GenCreateTagAssignment500JSONResponse{GenInternalErrorJSONResponse(resp)}
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
		if resp, ok := respondDomainErrorForOperation[GenListTagAssignmentsResponse]("listTagAssignments", err, domainErrorResponder[GenListTagAssignmentsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListTagAssignmentsResponse {
				return ListTagAssignments403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListTagAssignmentsResponse {
				return ListTagAssignments404JSONResponse{resp}
			},
			Internal: func(resp InternalErrorJSONResponse) GenListTagAssignmentsResponse {
				return GenListTagAssignments500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
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
		if resp, ok := respondDomainErrorForOperation[GenDeleteTagAssignmentResponse]("deleteTagAssignment", err, domainErrorResponder[GenDeleteTagAssignmentResponse]{
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
