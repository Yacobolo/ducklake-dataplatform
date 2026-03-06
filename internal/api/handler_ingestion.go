package api

import (
	"context"
	"errors"

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
		return GenCreateUploadUrl400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: "ingestion not available (S3 not configured)"}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
	}

	principal := principalFromCtx(ctx)
	result, err := h.ingestion.RequestUploadURL(ctx, principal, string(request.CatalogName), request.SchemaName, request.TableName, request.Body.Filename)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenCreateUploadUrl404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenCreateUploadUrl403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return GenCreateUploadUrl400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	t := result.ExpiresAt
	return GenCreateUploadUrl200JSONResponse{
		Body: UploadUrlResponse{
			UploadUrl: &result.UploadURL,
			S3Key:     &result.S3Key,
			ExpiresAt: &t,
		},
		Headers: GenCreateUploadUrl200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CommitTableIngestion implements the endpoint for committing uploaded files to a table.
func (h *APIHandler) CommitTableIngestion(ctx context.Context, request GenCommitTableIngestionRequest) (GenCommitTableIngestionResponse, error) {
	if h.ingestion == nil {
		return GenCommitTableIngestion400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: "ingestion not available (S3 not configured)"}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenCommitTableIngestion404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenCommitTableIngestion403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return GenCommitTableIngestion400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return GenCommitTableIngestion400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	filesRegistered := int64(result.FilesRegistered)
	filesSkipped := int64(result.FilesSkipped)
	return GenCommitTableIngestion200JSONResponse{
		Body: IngestionResult{
			FilesRegistered: &filesRegistered,
			FilesSkipped:    &filesSkipped,
			Schema:          &result.Schema,
			Table:           &result.Table,
		},
		Headers: GenCommitTableIngestion200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// LoadTableExternalFiles implements the endpoint for loading external files into a table.
func (h *APIHandler) LoadTableExternalFiles(ctx context.Context, request GenLoadTableExternalFilesRequest) (GenLoadTableExternalFilesResponse, error) {
	if h.ingestion == nil {
		return GenLoadTableExternalFiles400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: "ingestion not available (S3 not configured)"}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
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
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GenLoadTableExternalFiles404JSONResponse{GenNotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: GenNotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return GenLoadTableExternalFiles403JSONResponse{GenForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: GenForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return GenLoadTableExternalFiles400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return GenLoadTableExternalFiles400JSONResponse{GenBadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: GenBadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}

	filesRegistered := int64(result.FilesRegistered)
	filesSkipped := int64(result.FilesSkipped)
	return GenLoadTableExternalFiles200JSONResponse{
		Body: IngestionResult{
			FilesRegistered: &filesRegistered,
			FilesSkipped:    &filesSkipped,
			Schema:          &result.Schema,
			Table:           &result.Table,
		},
		Headers: GenLoadTableExternalFiles200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}
