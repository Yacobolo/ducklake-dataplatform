package api

import (
	"context"
	"errors"
	"net/http"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

// principalFromCtx extracts the principal name from the context.
func principalFromCtx(ctx context.Context) string {
	p, _ := middleware.PrincipalFromContext(ctx)
	return p
}

// catalogService defines the catalog operations used by the API handler.
type catalogService interface {
	GetCatalogInfo(ctx context.Context, catalogName string) (*domain.CatalogInfo, error)
	ListSchemas(ctx context.Context, catalogName string, page domain.PageRequest) ([]domain.SchemaDetail, int64, error)
	CreateSchema(ctx context.Context, catalogName string, principal string, req domain.CreateSchemaRequest) (*domain.SchemaDetail, error)
	GetSchema(ctx context.Context, catalogName string, name string) (*domain.SchemaDetail, error)
	UpdateSchema(ctx context.Context, catalogName string, principal string, name string, comment *string, props map[string]string) (*domain.SchemaDetail, error)
	DeleteSchema(ctx context.Context, catalogName string, principal string, name string, force bool) error
	ListTables(ctx context.Context, catalogName string, schemaName string, page domain.PageRequest) ([]domain.TableDetail, int64, error)
	CreateTable(ctx context.Context, catalogName string, principal string, schemaName string, req domain.CreateTableRequest) (*domain.TableDetail, error)
	GetTable(ctx context.Context, catalogName string, schemaName, tableName string) (*domain.TableDetail, error)
	UpdateTable(ctx context.Context, catalogName string, principal string, schemaName, tableName string, req domain.UpdateTableRequest) (*domain.TableDetail, error)
	DeleteTable(ctx context.Context, catalogName string, principal string, schemaName, tableName string) error
	ListColumns(ctx context.Context, catalogName string, schemaName, tableName string, page domain.PageRequest) ([]domain.ColumnDetail, int64, error)
	UpdateColumn(ctx context.Context, catalogName string, principal string, schemaName, tableName, columnName string, req domain.UpdateColumnRequest) (*domain.ColumnDetail, error)
	ProfileTable(ctx context.Context, catalogName string, principal string, schemaName, tableName string) (*domain.TableStatistics, error)
	GetMetastoreSummary(ctx context.Context, catalogName string) (*domain.MetastoreSummary, error)
}

// === Catalog Management ===

// GetCatalog implements the endpoint for retrieving catalog information.
func (h *APIHandler) GetCatalog(ctx context.Context, request GetCatalogRequestObject) (GetCatalogResponseObject, error) {
	info, err := h.catalog.GetCatalogInfo(ctx, string(request.CatalogName))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetCatalog404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetCatalog200JSONResponse{
		Body:    catalogInfoToAPI(*info),
		Headers: GetCatalog200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListSchemas implements the endpoint for listing schemas in the catalog.
func (h *APIHandler) ListSchemas(ctx context.Context, request ListSchemasRequestObject) (ListSchemasResponseObject, error) {
	page := pageFromParams(request.Params.MaxResults, request.Params.PageToken)
	schemas, total, err := h.catalog.ListSchemas(ctx, string(request.CatalogName), page)
	if err != nil {
		return nil, err
	}
	out := make([]SchemaDetail, len(schemas))
	for i, s := range schemas {
		out[i] = schemaDetailToAPI(s)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListSchemas200JSONResponse{
		Body:    PaginatedSchemaDetails{Data: &out, NextPageToken: optStr(npt)},
		Headers: ListSchemas200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateSchema implements the endpoint for creating a new schema.
func (h *APIHandler) CreateSchema(ctx context.Context, request CreateSchemaRequestObject) (CreateSchemaResponseObject, error) {
	domReq := domain.CreateSchemaRequest{
		Name: request.Body.Name,
	}
	if request.Body.Comment != nil {
		domReq.Comment = *request.Body.Comment
	}
	if request.Body.Properties != nil {
		domReq.Properties = *request.Body.Properties
	}
	if request.Body.LocationName != nil {
		domReq.LocationName = *request.Body.LocationName
	}

	principal := principalFromCtx(ctx)
	result, err := h.catalog.CreateSchema(ctx, string(request.CatalogName), principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateSchema403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateSchema400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateSchema409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateSchema400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateSchema201JSONResponse{
		Body:    schemaDetailToAPI(*result),
		Headers: CreateSchema201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetSchema implements the endpoint for retrieving a schema by name.
func (h *APIHandler) GetSchema(ctx context.Context, request GetSchemaRequestObject) (GetSchemaResponseObject, error) {
	result, err := h.catalog.GetSchema(ctx, string(request.CatalogName), request.SchemaName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetSchema404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetSchema200JSONResponse{
		Body:    schemaDetailToAPI(*result),
		Headers: GetSchema200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateSchema implements the endpoint for updating schema metadata.
func (h *APIHandler) UpdateSchema(ctx context.Context, request UpdateSchemaRequestObject) (UpdateSchemaResponseObject, error) {
	var props map[string]string
	if request.Body.Properties != nil {
		props = *request.Body.Properties
	}

	principal := principalFromCtx(ctx)
	result, err := h.catalog.UpdateSchema(ctx, string(request.CatalogName), principal, request.SchemaName, request.Body.Comment, props)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateSchema403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateSchema404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateSchema200JSONResponse{
		Body:    schemaDetailToAPI(*result),
		Headers: UpdateSchema200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteSchema implements the endpoint for deleting a schema by name.
func (h *APIHandler) DeleteSchema(ctx context.Context, request DeleteSchemaRequestObject) (DeleteSchemaResponseObject, error) {
	force := false
	if request.Params.Force != nil {
		force = *request.Params.Force
	}

	principal := principalFromCtx(ctx)
	if err := h.catalog.DeleteSchema(ctx, string(request.CatalogName), principal, request.SchemaName, force); err != nil {
		code := errorCodeFromError(err)
		switch code {
		case http.StatusForbidden:
			return DeleteSchema403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case http.StatusNotFound:
			return DeleteSchema404JSONResponse{NotFoundJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case http.StatusConflict:
			return DeleteSchema409JSONResponse{ConflictJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return DeleteSchema403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return DeleteSchema204Response{}, nil
}

// ListTables implements the endpoint for listing tables in a schema.
func (h *APIHandler) ListTables(ctx context.Context, request ListTablesRequestObject) (ListTablesResponseObject, error) {
	page := pageFromParams(request.Params.MaxResults, request.Params.PageToken)
	tables, total, err := h.catalog.ListTables(ctx, string(request.CatalogName), request.SchemaName, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListTables404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]TableDetail, len(tables))
	for i, t := range tables {
		out[i] = tableDetailToAPI(t)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListTables200JSONResponse{
		Body:    PaginatedTableDetails{Data: &out, NextPageToken: optStr(npt)},
		Headers: ListTables200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateTable implements the endpoint for creating a new table in a schema.
func (h *APIHandler) CreateTable(ctx context.Context, request CreateTableRequestObject) (CreateTableResponseObject, error) {
	var cols []domain.CreateColumnDef
	if request.Body.Columns != nil {
		cols = make([]domain.CreateColumnDef, len(*request.Body.Columns))
		for i, c := range *request.Body.Columns {
			cols[i] = domain.CreateColumnDef{Name: c.Name, Type: c.Type}
		}
	}
	domReq := domain.CreateTableRequest{
		Name:    request.Body.Name,
		Columns: cols,
	}
	if request.Body.Comment != nil {
		domReq.Comment = *request.Body.Comment
	}
	if request.Body.TableType != nil {
		domReq.TableType = string(*request.Body.TableType)
	}
	if request.Body.SourcePath != nil {
		domReq.SourcePath = *request.Body.SourcePath
	}
	if request.Body.FileFormat != nil {
		domReq.FileFormat = string(*request.Body.FileFormat)
	}
	if request.Body.LocationName != nil {
		domReq.LocationName = *request.Body.LocationName
	}

	principal := principalFromCtx(ctx)
	result, err := h.catalog.CreateTable(ctx, string(request.CatalogName), principal, request.SchemaName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateTable403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateTable400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateTable409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return CreateTable400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateTable400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateTable201JSONResponse{
		Body:    tableDetailToAPI(*result),
		Headers: CreateTable201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetTable implements the endpoint for retrieving a table by name.
func (h *APIHandler) GetTable(ctx context.Context, request GetTableRequestObject) (GetTableResponseObject, error) {
	result, err := h.catalog.GetTable(ctx, string(request.CatalogName), request.SchemaName, request.TableName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetTable404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetTable200JSONResponse{
		Body:    tableDetailToAPI(*result),
		Headers: GetTable200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateTable implements the endpoint for updating table metadata.
func (h *APIHandler) UpdateTable(ctx context.Context, request UpdateTableRequestObject) (UpdateTableResponseObject, error) {
	domReq := domain.UpdateTableRequest{}
	if request.Body.Comment != nil {
		domReq.Comment = request.Body.Comment
	}
	if request.Body.Properties != nil {
		domReq.Properties = *request.Body.Properties
	}
	if request.Body.Owner != nil {
		domReq.Owner = request.Body.Owner
	}

	principal := principalFromCtx(ctx)
	result, err := h.catalog.UpdateTable(ctx, string(request.CatalogName), principal, request.SchemaName, request.TableName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateTable403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateTable404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateTable200JSONResponse{
		Body:    tableDetailToAPI(*result),
		Headers: UpdateTable200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteTable implements the endpoint for deleting a table by name.
func (h *APIHandler) DeleteTable(ctx context.Context, request DeleteTableRequestObject) (DeleteTableResponseObject, error) {
	principal := principalFromCtx(ctx)
	if err := h.catalog.DeleteTable(ctx, string(request.CatalogName), principal, request.SchemaName, request.TableName); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteTable403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteTable404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteTable204Response{}, nil
}

// ListTableColumns implements the endpoint for listing columns of a table.
func (h *APIHandler) ListTableColumns(ctx context.Context, request ListTableColumnsRequestObject) (ListTableColumnsResponseObject, error) {
	page := pageFromParams(request.Params.MaxResults, request.Params.PageToken)
	cols, total, err := h.catalog.ListColumns(ctx, string(request.CatalogName), request.SchemaName, request.TableName, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListTableColumns404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]ColumnDetail, len(cols))
	for i, c := range cols {
		out[i] = columnDetailToAPI(c)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListTableColumns200JSONResponse{
		Body:    PaginatedColumnDetails{Data: &out, NextPageToken: optStr(npt)},
		Headers: ListTableColumns200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateColumn implements the endpoint for updating column metadata.
func (h *APIHandler) UpdateColumn(ctx context.Context, request UpdateColumnRequestObject) (UpdateColumnResponseObject, error) {
	domReq := domain.UpdateColumnRequest{}
	if request.Body.Comment != nil {
		domReq.Comment = request.Body.Comment
	}
	if request.Body.Properties != nil {
		domReq.Properties = *request.Body.Properties
	}

	principal := principalFromCtx(ctx)
	result, err := h.catalog.UpdateColumn(ctx, string(request.CatalogName), principal, request.SchemaName, request.TableName, request.ColumnName, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateColumn403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateColumn404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateColumn200JSONResponse{
		Body:    columnDetailToAPI(*result),
		Headers: UpdateColumn200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ProfileTable implements the endpoint for profiling table statistics.
func (h *APIHandler) ProfileTable(ctx context.Context, request ProfileTableRequestObject) (ProfileTableResponseObject, error) {
	principal := principalFromCtx(ctx)
	stats, err := h.catalog.ProfileTable(ctx, string(request.CatalogName), principal, request.SchemaName, request.TableName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return ProfileTable403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return ProfileTable404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return ProfileTable200JSONResponse{
		Body:    tableStatisticsToAPI(stats),
		Headers: ProfileTable200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetMetastoreSummary implements the endpoint for retrieving the metastore summary.
func (h *APIHandler) GetMetastoreSummary(ctx context.Context, request GetMetastoreSummaryRequestObject) (GetMetastoreSummaryResponseObject, error) {
	summary, err := h.catalog.GetMetastoreSummary(ctx, string(request.CatalogName))
	if err != nil {
		return nil, err
	}
	return GetMetastoreSummary200JSONResponse{
		Body: MetastoreSummary{
			CatalogName:    &summary.CatalogName,
			MetastoreType:  &summary.MetastoreType,
			StorageBackend: &summary.StorageBackend,
			DataPath:       &summary.DataPath,
			SchemaCount:    &summary.SchemaCount,
			TableCount:     &summary.TableCount,
		},
		Headers: GetMetastoreSummary200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}
