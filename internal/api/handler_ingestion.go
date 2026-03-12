package api

import (
	"context"
	"time"

	"duck-demo/internal/domain"
)

// IngestionService defines the ingestion operations used by the API handler.
// Exported because callers need to handle nil-to-interface conversion for
// this optional service.
type IngestionService = ingestionService

// ingestionService defines the ingestion operations used by the API handler.
type ingestionService interface {
	RequestUploadURL(ctx context.Context, principal string, catalogName string, schemaName, tableName string, filename *string) (*domain.UploadURLResult, error)
	CommitIngestion(ctx context.Context, principal string, catalogName string, schemaName, tableName string, s3Keys []string, opts domain.IngestionOptions) (*domain.IngestionResult, error)
	LoadExternalFiles(ctx context.Context, principal string, catalogName string, schemaName, tableName string, paths []string, opts domain.IngestionOptions) (*domain.IngestionResult, error)
}

// === Ingestion ===

// CreateUploadUrl implements the endpoint for generating a pre-signed upload URL.
func (h *APIHandler) CreateUploadUrl(ctx context.Context, request GenCreateUploadUrlRequest) (GenCreateUploadUrlResponse, error) {
	if h.ingestion == nil {
		return CreateUploadUrl400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: "ingestion not available (S3 not configured)"}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	principal := principalFromCtx(ctx)
	result, err := h.ingestion.RequestUploadURL(ctx, principal, string(request.CatalogName), request.SchemaName, request.TableName, request.Body.Filename)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateUploadUrlResponse](err, domainErrorResponder[GenCreateUploadUrlResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateUploadUrlResponse {
				return CreateUploadUrl403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateUploadUrlResponse {
				return CreateUploadUrl404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return CreateUploadUrl400JSONResponse{badRequestErrorResponse(err)}, nil
	}

	t := result.ExpiresAt.UTC().Format(time.RFC3339)
	return CreateUploadUrl200JSONResponse{
		Body: UploadUrlResponse{
			UploadUrl: result.UploadURL,
			S3Key:     result.S3Key,
			ExpiresAt: t,
		},
		Headers: CreateUploadUrl200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CommitTableIngestion implements the endpoint for committing uploaded files to a table.
func (h *APIHandler) CommitTableIngestion(ctx context.Context, request GenCommitTableIngestionRequest) (GenCommitTableIngestionResponse, error) {
	if h.ingestion == nil {
		return CommitTableIngestion400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: "ingestion not available (S3 not configured)"}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	opts := domain.IngestionOptions{}
	if request.Body.Options != nil {
		if request.Body.Options.AllowMissingColumns != nil {
			opts.AllowMissingColumns = *request.Body.Options.AllowMissingColumns
		}
		if request.Body.Options.IgnoreExtraColumns != nil {
			opts.IgnoreExtraColumns = *request.Body.Options.IgnoreExtraColumns
		}
	}

	principal := principalFromCtx(ctx)
	result, err := h.ingestion.CommitIngestion(ctx, principal, string(request.CatalogName), request.SchemaName, request.TableName, request.Body.S3Keys, opts)
	if err != nil {
		if resp, ok := respondDomainError[GenCommitTableIngestionResponse](err, domainErrorResponder[GenCommitTableIngestionResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCommitTableIngestionResponse {
				return CommitTableIngestion400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCommitTableIngestionResponse {
				return CommitTableIngestion403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCommitTableIngestionResponse {
				return CommitTableIngestion404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return CommitTableIngestion400JSONResponse{badRequestErrorResponse(err)}, nil
	}

	return CommitTableIngestion200JSONResponse{
		Body: IngestionResult{
			FilesRegistered: safeIntToInt32(result.FilesRegistered),
			FilesSkipped:    safeIntToInt32(result.FilesSkipped),
			Schema:          result.Schema,
			Table:           result.Table,
		},
		Headers: CommitTableIngestion200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// LoadTableExternalFiles implements the endpoint for loading external files into a table.
func (h *APIHandler) LoadTableExternalFiles(ctx context.Context, request GenLoadTableExternalFilesRequest) (GenLoadTableExternalFilesResponse, error) {
	if h.ingestion == nil {
		return LoadTableExternalFiles400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: "ingestion not available (S3 not configured)"}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	opts := domain.IngestionOptions{}
	if request.Body.Options != nil {
		if request.Body.Options.AllowMissingColumns != nil {
			opts.AllowMissingColumns = *request.Body.Options.AllowMissingColumns
		}
		if request.Body.Options.IgnoreExtraColumns != nil {
			opts.IgnoreExtraColumns = *request.Body.Options.IgnoreExtraColumns
		}
	}

	principal := principalFromCtx(ctx)
	result, err := h.ingestion.LoadExternalFiles(ctx, principal, string(request.CatalogName), request.SchemaName, request.TableName, request.Body.Paths, opts)
	if err != nil {
		if resp, ok := respondDomainError[GenLoadTableExternalFilesResponse](err, domainErrorResponder[GenLoadTableExternalFilesResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenLoadTableExternalFilesResponse {
				return LoadTableExternalFiles400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenLoadTableExternalFilesResponse {
				return LoadTableExternalFiles403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenLoadTableExternalFilesResponse {
				return LoadTableExternalFiles404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return LoadTableExternalFiles400JSONResponse{badRequestErrorResponse(err)}, nil
	}

	return LoadTableExternalFiles200JSONResponse{
		Body: IngestionResult{
			FilesRegistered: safeIntToInt32(result.FilesRegistered),
			FilesSkipped:    safeIntToInt32(result.FilesSkipped),
			Schema:          result.Schema,
			Table:           result.Table,
		},
		Headers: LoadTableExternalFiles200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}
