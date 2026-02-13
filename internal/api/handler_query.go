package api

import (
	"context"
	"net/http"

	"duck-demo/internal/service/query"

	"duck-demo/internal/middleware"
)

// queryService defines the query operations used by the API handler.
type queryService interface {
	Execute(ctx context.Context, principalName, sqlQuery string) (*query.QueryResult, error)
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
	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.query.Execute(ctx, principal, req.Body.Sql)
	if err != nil {
		code := errorCodeFromError(err)
		return ExecuteQuery403JSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: ExecuteQuery403ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
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

// CreateManifest implements the endpoint for generating a table read manifest.
func (h *APIHandler) CreateManifest(ctx context.Context, req CreateManifestRequestObject) (CreateManifestResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	schemaName := "main"
	if req.Body.Schema != nil {
		schemaName = *req.Body.Schema
	}

	// The manifest endpoint is not catalog-scoped in the URL; use empty string
	// to let the service resolve the default catalog.
	result, err := h.manifest.GetManifest(ctx, principal, "", schemaName, req.Body.Table)
	if err != nil {
		code := errorCodeFromError(err)
		switch code {
		case http.StatusNotFound:
			return CreateManifest404JSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: CreateManifest404ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
		default:
			return CreateManifest403JSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: CreateManifest403ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}, nil
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
