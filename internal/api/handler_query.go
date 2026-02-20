package api

import (
	"context"
	"errors"
	"net/http"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"
)

// queryService defines the query operations used by the API handler.
type queryService interface {
	Execute(ctx context.Context, principalName, sqlQuery string) (*query.QueryResult, error)
}

type queryAsyncService interface {
	SubmitAsync(ctx context.Context, principalName, sqlQuery, requestID string) (*domain.QueryJob, error)
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
func (h *APIHandler) ExecuteQuery(ctx context.Context, req ExecuteQueryRequestObject) (ExecuteQueryResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
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
			return ExecuteQuery500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: code, Message: msg}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	rows := make([][]interface{}, len(result.Rows))
	for i, row := range result.Rows {
		mapped := make([]interface{}, len(row))
		copy(mapped, row)
		rows[i] = mapped
	}
	rowCount := int64(result.RowCount)

	return ExecuteQuery200JSONResponse{
		Body: QueryResult{
			Columns:  &result.Columns,
			Rows:     &rows,
			RowCount: &rowCount,
		},
		Headers: ExecuteQuery200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// SubmitQuery implements async query submission endpoint.
func (h *APIHandler) SubmitQuery(ctx context.Context, req SubmitQueryRequestObject) (SubmitQueryResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name

	asyncSvc, ok := h.query.(queryAsyncService)
	if !ok {
		return SubmitQuery500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: 500, Message: "async query service is not configured"}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		default:
			return SubmitQuery500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	status := string(job.Status)
	apiStatus := SubmitQueryResponseStatus(status)
	return SubmitQuery202JSONResponse{
		Body:    SubmitQueryResponse{QueryId: job.ID, Status: apiStatus},
		Headers: SubmitQuery202ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetQuery implements async query status endpoint.
func (h *APIHandler) GetQuery(ctx context.Context, req GetQueryRequestObject) (GetQueryResponseObject, error) {
	job, err := h.lookupAsyncJob(ctx, req.QueryId)
	if err != nil {
		code := errorCodeFromError(err)
		if int(code) == http.StatusNotFound {
			return GetQuery404JSONResponse{NotFoundJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return GetQuery500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	body := queryJobToAPI(job)
	return GetQuery200JSONResponse{
		Body:    body,
		Headers: GetQuery200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetQueryResults returns a page of async query results.
func (h *APIHandler) GetQueryResults(ctx context.Context, req GetQueryResultsRequestObject) (GetQueryResultsResponseObject, error) {
	job, err := h.lookupAsyncJob(ctx, req.QueryId)
	if err != nil {
		code := errorCodeFromError(err)
		if int(code) == http.StatusNotFound {
			return GetQueryResults404JSONResponse{NotFoundJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return GetQueryResults500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
	}

	offset := domain.PageRequest{PageToken: pageToken}.Offset()
	limit := int(maxResults)
	if limit <= 0 {
		limit = 100
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

	result := QueryResult{Columns: &job.Columns, Rows: &rows}
	rowCount := int64(job.RowCount)
	result.RowCount = &rowCount
	if nextPageToken != "" {
		result.NextPageToken = &nextPageToken
	}

	return GetQueryResults200JSONResponse{
		Body:    result,
		Headers: GetQueryResults200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CancelQuery cancels async query execution.
func (h *APIHandler) CancelQuery(ctx context.Context, req CancelQueryRequestObject) (CancelQueryResponseObject, error) {
	job, err := h.lookupAsyncJob(ctx, req.QueryId)
	if err != nil {
		code := errorCodeFromError(err)
		if int(code) == http.StatusNotFound {
			return CancelQuery404JSONResponse{NotFoundJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return CancelQuery500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	asyncSvc, ok := h.query.(queryAsyncService)
	if !ok {
		return CancelQuery500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: 500, Message: "async query service is not configured"}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}
	if err := asyncSvc.CancelAsyncJob(ctx, principal, req.QueryId); err != nil {
		code := errorCodeFromError(err)
		if int(code) == http.StatusNotFound {
			return CancelQuery404JSONResponse{NotFoundJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return CancelQuery500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	status := string(job.Status)
	if job.Status == domain.QueryJobStatusQueued || job.Status == domain.QueryJobStatusRunning {
		status = string(domain.QueryJobStatusCanceled)
	}
	apiStatus := CancelQueryResponseStatus(status)
	return CancelQuery200JSONResponse{
		Body:    CancelQueryResponse{QueryId: job.ID, Status: apiStatus},
		Headers: CancelQuery200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteQuery deletes async query state.
func (h *APIHandler) DeleteQuery(ctx context.Context, req DeleteQueryRequestObject) (DeleteQueryResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name

	asyncSvc, ok := h.query.(queryAsyncService)
	if !ok {
		return DeleteQuery500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: 500, Message: "async query service is not configured"}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	if err := asyncSvc.DeleteAsyncJob(ctx, principal, req.QueryId); err != nil {
		code := errorCodeFromError(err)
		if int(code) == http.StatusNotFound {
			return DeleteQuery404JSONResponse{NotFoundJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
		return DeleteQuery500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	return DeleteQuery204Response{Headers: DeleteQuery204ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
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
	rowCount := int64(job.RowCount)
	resp := QueryJob{
		QueryId:   job.ID,
		Status:    QueryJobStatus(status),
		RowCount:  rowCount,
		RequestId: &job.RequestID,
		CreatedAt: &job.CreatedAt,
	}
	if job.ErrorMessage != nil {
		resp.Error = job.ErrorMessage
	}
	if job.StartedAt != nil {
		resp.StartedAt = job.StartedAt
	}
	if job.CompletedAt != nil {
		resp.CompletedAt = job.CompletedAt
	}
	return resp
}

// CreateManifest implements the endpoint for generating a table read manifest.
func (h *APIHandler) CreateManifest(ctx context.Context, req CreateManifestRequestObject) (CreateManifestResponseObject, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name

	schemaName := "main"
	if req.Body.Schema != nil {
		schemaName = *req.Body.Schema
	}

	result, err := h.manifest.GetManifest(ctx, principal, "", schemaName, req.Body.Table)
	if err != nil {
		code := errorCodeFromError(err)
		msg := err.Error()
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return CreateManifest404JSONResponse{NotFoundJSONResponse{Body: Error{Code: code, Message: msg}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateManifest403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: code, Message: msg}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateManifest400JSONResponse{BadRequestJSONResponse{Body: Error{Code: code, Message: msg}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateManifest500JSONResponse{InternalErrorJSONResponse{Body: Error{Code: code, Message: msg}, Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	cols := make([]ManifestColumn, len(result.Columns))
	for i, c := range result.Columns {
		name := c.Name
		typ := c.Type
		cols[i] = ManifestColumn{Name: &name, Type: &typ}
	}

	return CreateManifest200JSONResponse{
		Body: ManifestResponse{
			Table:       &result.Table,
			Schema:      &result.Schema,
			Columns:     &cols,
			Files:       &result.Files,
			RowFilters:  &result.RowFilters,
			ColumnMasks: &result.ColumnMasks,
			ExpiresAt:   &result.ExpiresAt,
		},
		Headers: CreateManifest200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}
