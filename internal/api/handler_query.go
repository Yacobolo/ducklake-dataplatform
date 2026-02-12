package api

import (
	"context"
	"net/http"

	"duck-demo/internal/middleware"
)

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
