package api

import (
	"context"
	"errors"
	"net/http"

	"duck-demo/internal/domain"
)

// principalFromCtx extracts the principal name from the context.
func principalFromCtx(ctx context.Context) string {
	p, _ := domain.PrincipalFromContext(ctx)
	return p.Name
}

// catalogService defines the catalog operations used by the API handler.
type catalogService interface {
	GetCatalogInfo(ctx context.Context, catalogName string) (*domain.CatalogInfo, error)
	GetCatalogVersionSummary(ctx context.Context, catalogName string) (*domain.CatalogVersionSummary, error)
	ListCatalogHistory(ctx context.Context, catalogName string, filter domain.CatalogHistoryFilter) ([]domain.CatalogHistoryEntry, error)
	ListSchemas(ctx context.Context, catalogName string, page domain.PageRequest) ([]domain.SchemaDetail, int64, error)
	CreateSchema(ctx context.Context, catalogName string, principal string, req domain.CreateSchemaRequest) (*domain.SchemaDetail, error)
	GetSchema(ctx context.Context, catalogName string, name string) (*domain.SchemaDetail, error)
	UpdateSchema(ctx context.Context, catalogName string, principal string, name string, req domain.UpdateSchemaRequest) (*domain.SchemaDetail, error)
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
func (h *APIHandler) GetCatalog(ctx context.Context, request GenGetCatalogRequest) (GenGetCatalogResponse, error) {
	info, err := h.catalog.GetCatalogInfo(ctx, string(request.CatalogName))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenGetCatalog404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenGetCatalog200JSONResponse{
		Body:    catalogInfoToAPI(*info),
		Headers: GenGetCatalog200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetCatalogVersionSummary implements the endpoint for retrieving catalog version metadata.
func (h *APIHandler) GetCatalogVersionSummary(ctx context.Context, request GenGetCatalogVersionSummaryRequest) (GenGetCatalogVersionSummaryResponse, error) {
	summary, err := h.catalog.GetCatalogVersionSummary(ctx, string(request.CatalogName))
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetCatalogVersionSummary404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetCatalogVersionSummary200JSONResponse{
		Body:    catalogVersionSummaryToAPI(*summary),
		Headers: GetCatalogVersionSummary200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListCatalogHistory implements the endpoint for retrieving snapshot-aware catalog history.
func (h *APIHandler) ListCatalogHistory(ctx context.Context, request GenListCatalogHistoryRequest) (GenListCatalogHistoryResponse, error) {
	filter := domain.CatalogHistoryFilter{}
	if request.Params.EntityType != nil {
		filter.EntityType = string(*request.Params.EntityType)
	}
	if request.Params.SchemaName != nil {
		filter.SchemaName = *request.Params.SchemaName
	}
	if request.Params.TableName != nil {
		filter.TableName = *request.Params.TableName
	}
	if request.Params.Limit != nil {
		filter.Limit = int(*request.Params.Limit)
	}

	entries, err := h.catalog.ListCatalogHistory(ctx, string(request.CatalogName), filter)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return ListCatalogHistory400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return ListCatalogHistory404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]CatalogHistoryEntry, len(entries))
	for i := range entries {
		out[i] = catalogHistoryEntryToAPI(entries[i])
	}
	return ListCatalogHistory200JSONResponse{
		Body:    CatalogHistoryResponse{Data: out},
		Headers: ListCatalogHistory200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ListSchemas implements the endpoint for listing schemas in the catalog.
func (h *APIHandler) ListSchemas(ctx context.Context, request GenListSchemasRequest) (GenListSchemasResponse, error) {
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
	return GenListSchemas200JSONResponse{
		Body:    PaginatedSchemaDetails{Data: out, NextPageToken: optStr(npt)},
		Headers: GenListSchemas200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateSchema implements the endpoint for creating a new schema.
func (h *APIHandler) CreateSchema(ctx context.Context, request GenCreateSchemaRequest) (GenCreateSchemaResponse, error) {
	domReq := domain.CreateSchemaRequest{
		Name: request.Body.Name,
	}
	if request.Body.Comment != nil {
		domReq.Comment = *request.Body.Comment
	}
	if request.Body.Properties != nil {
		domReq.Properties = recordToStringMap(request.Body.Properties)
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
	return GenCreateSchema201JSONResponse{
		Body:    schemaDetailToAPI(*result),
		Headers: GenCreateSchema201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetSchema implements the endpoint for retrieving a schema by name.
func (h *APIHandler) GetSchema(ctx context.Context, request GenGetSchemaRequest) (GenGetSchemaResponse, error) {
	result, err := h.catalog.GetSchema(ctx, string(request.CatalogName), request.SchemaName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenGetSchema404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenGetSchema200JSONResponse{
		Body:    schemaDetailToAPI(*result),
		Headers: GenGetSchema200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateSchema implements the endpoint for updating schema metadata.
func (h *APIHandler) UpdateSchema(ctx context.Context, request GenUpdateSchemaRequest) (GenUpdateSchemaResponse, error) {
	domReq := domain.UpdateSchemaRequest{
		Comment: request.Body.Comment,
	}
	if request.Body.Properties != nil {
		domReq.Properties = recordToStringMap(request.Body.Properties)
	}

	principal := principalFromCtx(ctx)
	result, err := h.catalog.UpdateSchema(ctx, string(request.CatalogName), principal, request.SchemaName, domReq)
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
	return GenUpdateSchema200JSONResponse{
		Body:    schemaDetailToAPI(*result),
		Headers: GenUpdateSchema200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteSchema implements the endpoint for deleting a schema by name.
func (h *APIHandler) DeleteSchema(ctx context.Context, request GenDeleteSchemaRequest) (GenDeleteSchemaResponse, error) {
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
			return GenDeleteSchema500JSONResponse{GenInternalErrorJSONResponse{Body: Error{Code: code, Message: err.Error()}, Headers: GenInternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return GenDeleteSchema204Response{}, nil
}

// ListTables implements the endpoint for listing tables in a schema.
func (h *APIHandler) ListTables(ctx context.Context, request GenListTablesRequest) (GenListTablesResponse, error) {
	page := pageFromParams(request.Params.MaxResults, request.Params.PageToken)
	tables, total, err := h.catalog.ListTables(ctx, string(request.CatalogName), request.SchemaName, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenListTables404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]TableDetail, len(tables))
	for i, t := range tables {
		out[i] = tableDetailToAPI(t)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListTables200JSONResponse{
		Body:    PaginatedTableDetails{Data: out, NextPageToken: optStr(npt)},
		Headers: GenListTables200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateTable implements the endpoint for creating a new table in a schema.
func (h *APIHandler) CreateTable(ctx context.Context, request GenCreateTableRequest) (GenCreateTableResponse, error) {
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
	return GenCreateTable201JSONResponse{
		Body:    tableDetailToAPI(*result),
		Headers: GenCreateTable201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetTable implements the endpoint for retrieving a table by name.
func (h *APIHandler) GetTable(ctx context.Context, request GenGetTableRequest) (GenGetTableResponse, error) {
	result, err := h.catalog.GetTable(ctx, string(request.CatalogName), request.SchemaName, request.TableName)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenGetTable404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GenGetTable200JSONResponse{
		Body:    tableDetailToAPI(*result),
		Headers: GenGetTable200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateTable implements the endpoint for updating table metadata.
func (h *APIHandler) UpdateTable(ctx context.Context, request GenUpdateTableRequest) (GenUpdateTableResponse, error) {
	domReq := domain.UpdateTableRequest{}
	if request.Body.Comment != nil {
		domReq.Comment = request.Body.Comment
	}
	if request.Body.Owner != nil {
		domReq.Owner = request.Body.Owner
	}
	if request.Body.Properties != nil {
		domReq.Properties = recordToStringMap(request.Body.Properties)
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
	return GenUpdateTable200JSONResponse{
		Body:    tableDetailToAPI(*result),
		Headers: GenUpdateTable200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteTable implements the endpoint for deleting a table by name.
func (h *APIHandler) DeleteTable(ctx context.Context, request GenDeleteTableRequest) (GenDeleteTableResponse, error) {
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
	return GenDeleteTable204Response{}, nil
}

// ListTableColumns implements the endpoint for listing columns of a table.
func (h *APIHandler) ListTableColumns(ctx context.Context, request GenListTableColumnsRequest) (GenListTableColumnsResponse, error) {
	page := pageFromParams(request.Params.MaxResults, request.Params.PageToken)
	cols, total, err := h.catalog.ListColumns(ctx, string(request.CatalogName), request.SchemaName, request.TableName, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenListTableColumns404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	out := make([]ColumnDetail, len(cols))
	for i, c := range cols {
		out[i] = columnDetailToAPI(c)
	}
	npt := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListTableColumns200JSONResponse{
		Body:    PaginatedColumnDetails{Data: out, NextPageToken: optStr(npt)},
		Headers: GenListTableColumns200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateColumn implements the endpoint for updating column metadata.
func (h *APIHandler) UpdateColumn(ctx context.Context, request GenUpdateColumnRequest) (GenUpdateColumnResponse, error) {
	domReq := domain.UpdateColumnRequest{}
	if request.Body.Comment != nil {
		domReq.Comment = request.Body.Comment
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
	return GenUpdateColumn200JSONResponse{
		Body:    columnDetailToAPI(*result),
		Headers: GenUpdateColumn200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ProfileTable implements the endpoint for profiling table statistics.
func (h *APIHandler) ProfileTable(ctx context.Context, request GenProfileTableRequest) (GenProfileTableResponse, error) {
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
func (h *APIHandler) GetMetastoreSummary(ctx context.Context, request GenGetMetastoreSummaryRequest) (GenGetMetastoreSummaryResponse, error) {
	summary, err := h.catalog.GetMetastoreSummary(ctx, string(request.CatalogName))
	if err != nil {
		return nil, err
	}
	return GenGetMetastoreSummary200JSONResponse{
		Body: MetastoreSummary{
			CatalogName:    summary.CatalogName,
			MetastoreType:  &summary.MetastoreType,
			StorageBackend: &summary.StorageBackend,
			DataPath:       &summary.DataPath,
			SchemaCount:    ptrI32(safeInt64ToInt32(summary.SchemaCount)),
			TableCount:     ptrI32(safeInt64ToInt32(summary.TableCount)),
		},
		Headers: GenGetMetastoreSummary200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}
