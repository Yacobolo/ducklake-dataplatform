package api

import (
	"context"
	"net/http"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"
)

// queryService defines the query operations used by the API handler.
type queryService interface {
	Execute(ctx context.Context, principalName, sqlQuery string) (*query.QueryResult, error)
}

type queryAsyncService interface {
	SubmitAsync(ctx context.Context, principalName, sqlQuery, requestID string) (*domain.QueryJob, error)
	ListAsyncJobs(ctx context.Context, principalName string, status *domain.QueryJobStatus, page domain.PageRequest) ([]domain.QueryJob, int64, error)
	GetAsyncJob(ctx context.Context, principalName, jobID string) (*domain.QueryJob, error)
	CancelAsyncJob(ctx context.Context, principalName, jobID string) error
	DeleteAsyncJob(ctx context.Context, principalName, jobID string) error
}

// ManifestService defines the manifest operations used by the API handler.
// Exported because callers need to handle nil-to-interface conversion for
// this optional service.
type ManifestService = manifestService

// manifestService defines the manifest operations used by the API handler.
type manifestService interface {
	GetManifest(ctx context.Context, principalName, catalogName, schemaName, tableName string) (*query.ManifestResult, error)
}

// ExecuteQuery implements the endpoint for executing a SQL query.
func (h *APIHandler) ExecuteQuery(ctx context.Context, req GenExecuteQueryRequest) (GenExecuteQueryResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	var err error
	ctx, err = withComputeRequestDefaults(ctx, h.computeEndpoints, principal, computeExecutionRequestFromQueryBody(req.Body))
	if err != nil {
		code := errorCodeFromError(err)
		return ExecuteQuery400JSONResponse{BadRequestJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}
	result, err := h.query.Execute(ctx, principal, req.Body.Sql)
	if err != nil {
		code := errorCodeFromError(err)
		msg := err.Error()
		switch int(code) {
		case http.StatusBadRequest:
			return ExecuteQuery400JSONResponse{BadRequestJSONResponse{Body: Error{Code: code, Message: msg}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case http.StatusForbidden:
			return ExecuteQuery403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: code, Message: msg}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return GenExecuteQuery500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: code, Message: msg}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	rowCount := safeIntToInt32(result.RowCount)

	return ExecuteQuery200JSONResponse{
		Body: QueryResult{
			Columns:  tabularColumns(result.Columns, result.Rows),
			Rows:     rowsToRecords(result.Columns, result.Rows),
			RowCount: &rowCount,
		},
		Headers: ExecuteQuery200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// SubmitQuery implements async query submission endpoint.
func (h *APIHandler) SubmitQuery(ctx context.Context, req GenSubmitQueryRequest) (GenSubmitQueryResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	var err error
	ctx, err = withComputeRequestDefaults(ctx, h.computeEndpoints, principal, computeExecutionRequestFromSubmitBody(req.Body))
	if err != nil {
		code := errorCodeFromError(err)
		return SubmitQuery400JSONResponse{BadRequestJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	asyncSvc, ok := h.query.(queryAsyncService)
	if !ok {
		return GenSubmitQuery500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: 500, Message: "async query service is not configured"}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	requestID := ""
	if req.Body.RequestId != nil {
		requestID = *req.Body.RequestId
	}
	job, err := asyncSvc.SubmitAsync(ctx, principal, req.Body.Sql, requestID)
	if err != nil {
		code := errorCodeFromError(err)
		switch int(code) {
		case http.StatusBadRequest:
			return SubmitQuery400JSONResponse{BadRequestJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case http.StatusConflict:
			return SubmitQuery409JSONResponse{ConflictJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return GenSubmitQuery500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	status := string(job.Status)
	apiStatus := QueryJobStatus(status)
	return SubmitQuery202JSONResponse{
		Body:    SubmitQueryResponse{QueryId: job.ID, Status: apiStatus},
		Headers: SubmitQuery202ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListQueries implements async query listing endpoint.
func (h *APIHandler) ListQueries(ctx context.Context, req GenListQueriesRequest) (GenListQueriesResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name

	asyncSvc, ok := h.query.(queryAsyncService)
	if !ok {
		return GenListQueries500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: 500, Message: "async query service is not configured"}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	var status *domain.QueryJobStatus
	if req.Params.Status != nil {
		value := domain.QueryJobStatus(*req.Params.Status)
		status = &value
	}
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, total, err := asyncSvc.ListAsyncJobs(ctx, principal, status, page)
	if err != nil {
		code := errorCodeFromError(err)
		switch int(code) {
		case http.StatusForbidden:
			return ListQueries403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return GenListQueries500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	data := make([]QueryJob, len(items))
	for i := range items {
		item := items[i]
		data[i] = queryJobToAPI(&item)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListQueries200JSONResponse{
		Body: PaginatedQueryJobs{
			Data:          data,
			NextPageToken: optStr(nextToken),
		},
		Headers: GenListQueries200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetQuery implements async query status endpoint.
func (h *APIHandler) GetQuery(ctx context.Context, req GenGetQueryRequest) (GenGetQueryResponse, error) {
	job, err := h.lookupAsyncJob(ctx, req.QueryId)
	if err != nil {
		code := errorCodeFromError(err)
		if int(code) == http.StatusNotFound {
			return GenGetQuery404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return GenGetQuery500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	body := queryJobToAPI(job)
	return GenGetQuery200JSONResponse{
		Body:    body,
		Headers: GenGetQuery200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetQueryResults returns a page of async query results.
func (h *APIHandler) GetQueryResults(ctx context.Context, req GenGetQueryResultsRequest) (GenGetQueryResultsResponse, error) {
	job, err := h.lookupAsyncJob(ctx, req.QueryId)
	if err != nil {
		code := errorCodeFromError(err)
		if int(code) == http.StatusNotFound {
			return GenGetQueryResults404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return GenGetQueryResults500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	if job.Status == domain.QueryJobStatusQueued || job.Status == domain.QueryJobStatusRunning {
		return GetQueryResults409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: "query is not ready"}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}
	if job.Status == domain.QueryJobStatusFailed || job.Status == domain.QueryJobStatusCanceled {
		msg := "query results are not available"
		if job.ErrorMessage != nil && *job.ErrorMessage != "" {
			msg = *job.ErrorMessage
		}
		return GetQueryResults409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: msg}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	maxResults := int32(100)
	if req.Params.MaxResults != nil {
		maxResults = *req.Params.MaxResults
	}
	pageToken := ""
	if req.Params.PageToken != nil {
		pageToken = *req.Params.PageToken
		pageTokenMessage := ""
		if _, decodePageTokenErr := decodePageToken(pageToken); decodePageTokenErr != nil {
			pageTokenMessage = decodePageTokenErr.Error()
		}
		if pageTokenMessage != "" {
			return GetQueryResults400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: pageTokenMessage}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	offset := domain.PageRequest{PageToken: pageToken}.Offset()
	limit := int(maxResults)
	if limit <= 0 {
		limit = 100
	}
	if offset > len(job.Rows) {
		return GetQueryResults400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: "page_token points past the available results"}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	end := offset + limit
	if end > len(job.Rows) {
		end = len(job.Rows)
	}
	rows := make([][]interface{}, 0, end-offset)
	for i := offset; i < end; i++ {
		row := make([]interface{}, len(job.Rows[i]))
		copy(row, job.Rows[i])
		rows = append(rows, row)
	}
	nextPageToken := ""
	if end < len(job.Rows) {
		nextPageToken = domain.EncodePageToken(end)
	}

	result := QueryResult{
		Columns: tabularColumns(job.Columns, rows),
		Rows:    rowsToRecords(job.Columns, rows),
	}
	rowCount := safeIntToInt32(job.RowCount)
	result.RowCount = &rowCount
	if nextPageToken != "" {
		result.NextPageToken = &nextPageToken
	}

	return GenGetQueryResults200JSONResponse{
		Body:    result,
		Headers: GenGetQueryResults200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CancelQuery cancels async query execution.
func (h *APIHandler) CancelQuery(ctx context.Context, req GenCancelQueryRequest) (GenCancelQueryResponse, error) {
	job, err := h.lookupAsyncJob(ctx, req.QueryId)
	if err != nil {
		code := errorCodeFromError(err)
		if int(code) == http.StatusNotFound {
			return CancelQuery404JSONResponse{NotFoundJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return GenCancelQuery500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	asyncSvc, ok := h.query.(queryAsyncService)
	if !ok {
		return GenCancelQuery500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: 500, Message: "async query service is not configured"}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}
	if err := asyncSvc.CancelAsyncJob(ctx, principal, req.QueryId); err != nil {
		code := errorCodeFromError(err)
		if int(code) == http.StatusNotFound {
			return CancelQuery404JSONResponse{NotFoundJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return GenCancelQuery500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	status := string(job.Status)
	if job.Status == domain.QueryJobStatusQueued || job.Status == domain.QueryJobStatusRunning {
		status = string(domain.QueryJobStatusCanceled)
	}
	apiStatus := QueryJobStatus(status)
	return CancelQuery202JSONResponse{
		Body:    CancelQueryResponse{QueryId: job.ID, Status: apiStatus},
		Headers: CancelQuery202ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteQuery deletes async query state.
func (h *APIHandler) DeleteQuery(ctx context.Context, req GenDeleteQueryRequest) (GenDeleteQueryResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name

	asyncSvc, ok := h.query.(queryAsyncService)
	if !ok {
		return GenDeleteQuery500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: 500, Message: "async query service is not configured"}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	if err := asyncSvc.DeleteAsyncJob(ctx, principal, req.QueryId); err != nil {
		code := errorCodeFromError(err)
		if int(code) == http.StatusNotFound {
			return DeleteQuery404JSONResponse{NotFoundJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return GenDeleteQuery500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	return GenDeleteQuery204Response{Headers: GenDeleteQuery204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
}

func (h *APIHandler) lookupAsyncJob(ctx context.Context, queryID string) (*domain.QueryJob, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name

	asyncSvc, ok := h.query.(queryAsyncService)
	if !ok {
		return nil, domain.ErrNotImplemented("async query service is not configured")
	}

	job, err := asyncSvc.GetAsyncJob(ctx, principal, queryID)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func queryJobToAPI(job *domain.QueryJob) QueryJob {
	status := string(job.Status)
	rowCount := safeIntToInt32(job.RowCount)
	resp := QueryJob{
		QueryId:   job.ID,
		Status:    QueryJobStatus(status),
		RowCount:  rowCount,
		RequestId: &job.RequestID,
		CreatedAt: formatTimePtr(&job.CreatedAt),
	}
	if job.ErrorMessage != nil {
		resp.Error = job.ErrorMessage
	}
	if job.StartedAt != nil {
		resp.StartedAt = formatTimePtr(job.StartedAt)
	}
	if job.CompletedAt != nil {
		resp.CompletedAt = formatTimePtr(job.CompletedAt)
	}
	return resp
}

func computeExecutionRequestFromQueryBody(_ *QueryRequest) domain.ComputeExecutionRequest {
	return domain.ComputeExecutionRequest{}
}

func computeExecutionRequestFromSubmitBody(_ *SubmitQueryRequest) domain.ComputeExecutionRequest {
	return domain.ComputeExecutionRequest{}
}

func withComputeRequestDefaults(ctx context.Context, svc computeEndpointService, principal string, req domain.ComputeExecutionRequest) (context.Context, error) {
	req = req.Normalize()
	if req.WorkloadType == "" {
		req.WorkloadType = domain.ComputeWorkloadInteractive
	}

	if req.Mode == "" && svc != nil {
		defaults, err := svc.GetRoutingDefaults(ctx, principal)
		if err == nil && defaults != nil {
			switch strings.ToUpper(req.WorkloadType) {
			case domain.ComputeWorkloadScheduled, domain.ComputeWorkloadHeavy:
				req.Mode = defaults.ScheduledMode
			case domain.ComputeWorkloadNotebook:
				req.Mode = defaults.NotebookMode
			default:
				req.Mode = defaults.InteractiveMode
			}
		}
	}

	if req.Mode == "" {
		req.Mode = domain.ComputeModeAuto
	}

	if err := req.Validate(); err != nil {
		return nil, err
	}
	return domain.WithComputeExecutionRequest(ctx, req), nil
}

// CreateManifest implements the endpoint for generating a table read manifest.
func (h *APIHandler) CreateManifest(ctx context.Context, req GenCreateManifestRequest) (GenCreateManifestResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name

	result, err := h.manifest.GetManifest(ctx, principal, req.CatalogName, req.SchemaName, req.TableName)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateManifestResponse]("createManifest", err, domainErrorResponder[GenCreateManifestResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateManifestResponse {
				return CreateManifest400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateManifestResponse { return CreateManifest403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenCreateManifestResponse { return CreateManifest404JSONResponse{resp} },
			Internal: func(resp InternalErrorJSONResponse) GenCreateManifestResponse {
				return GenCreateManifest500JSONResponse{GenInternalErrorJSONResponse(resp)}
			},
		}); ok {
			return resp, nil
		}
		return GenCreateManifest500JSONResponse{GenInternalErrorJSONResponse(internalErrorResponse(err))}, nil
	}

	cols := make([]ManifestColumn, len(result.Columns))
	for i, c := range result.Columns {
		cols[i] = ManifestColumn{Name: c.Name, Type: c.Type}
	}

	return CreateManifest200JSONResponse{
		Body: ManifestResponse{
			Table:       result.Table,
			Schema:      &result.Schema,
			Columns:     &cols,
			Files:       &result.Files,
			RowFilters:  &result.RowFilters,
			ColumnMasks: stringMapToRecord(result.ColumnMasks),
			ExpiresAt:   formatTimePtr(&result.ExpiresAt),
		},
		Headers: CreateManifest200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}
