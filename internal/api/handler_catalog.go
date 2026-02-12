package api

import (
	"context"
	"errors"
	"net/http"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

// catalogService defines the catalog operations used by the API handler.
type catalogService interface {
	GetCatalogInfo(ctx context.Context) (*domain.CatalogInfo, error)
	UpdateCatalog(ctx context.Context, principal string, req domain.UpdateCatalogRequest) (*domain.CatalogInfo, error)
	ListSchemas(ctx context.Context, page domain.PageRequest) ([]domain.SchemaDetail, int64, error)
	CreateSchema(ctx context.Context, principal string, req domain.CreateSchemaRequest) (*domain.SchemaDetail, error)
	GetSchema(ctx context.Context, name string) (*domain.SchemaDetail, error)
	UpdateSchema(ctx context.Context, principal string, name string, comment *string, props map[string]string) (*domain.SchemaDetail, error)
	DeleteSchema(ctx context.Context, principal string, name string, force bool) error
	ListTables(ctx context.Context, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error)
	CreateTable(ctx context.Context, principal string, schemaName string, req domain.CreateTableRequest) (*domain.TableDetail, error)
	GetTable(ctx context.Context, schemaName, tableName string) (*domain.TableDetail, error)
	UpdateTable(ctx context.Context, principal string, schemaName, tableName string, req domain.UpdateTableRequest) (*domain.TableDetail, error)
	DeleteTable(ctx context.Context, principal string, schemaName, tableName string) error
	ListColumns(ctx context.Context, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error)
	UpdateColumn(ctx context.Context, principal string, schemaName, tableName, columnName string, req domain.UpdateColumnRequest) (*domain.ColumnDetail, error)
	ProfileTable(ctx context.Context, principal string, schemaName, tableName string) (*domain.TableStatistics, error)
	GetMetastoreSummary(ctx context.Context) (*domain.MetastoreSummary, error)
}

// === Catalog Management ===

// GetCatalog implements the endpoint for retrieving catalog information.
func (h *APIHandler) GetCatalog(ctx context.Context, _ GetCatalogRequestObject) (GetCatalogResponseObject, error) {
	info, err := h.catalog.GetCatalogInfo(ctx)
	if err != nil {
		return nil, err
	}
	return GetCatalog200JSONResponse(catalogInfoToAPI(*info)), nil
}

// UpdateCatalog implements the endpoint for updating catalog metadata.
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

// ListSchemas implements the endpoint for listing schemas in the catalog.
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

// CreateSchema implements the endpoint for creating a new schema.
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

// GetSchema implements the endpoint for retrieving a schema by name.
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

// UpdateSchema implements the endpoint for updating schema metadata.
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

// DeleteSchema implements the endpoint for deleting a schema by name.
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

// ListTables implements the endpoint for listing tables in a schema.
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

// CreateTable implements the endpoint for creating a new table in a schema.
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

// GetTable implements the endpoint for retrieving a table by name.
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

// UpdateTable implements the endpoint for updating table metadata.
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

// DeleteTable implements the endpoint for deleting a table by name.
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

// ListTableColumns implements the endpoint for listing columns of a table.
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

// UpdateColumn implements the endpoint for updating column metadata.
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

// ProfileTable implements the endpoint for profiling table statistics.
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

// GetMetastoreSummary implements the endpoint for retrieving the metastore summary.
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
