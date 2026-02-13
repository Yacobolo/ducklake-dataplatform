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
func (h *APIHandler) CreateUploadUrl(ctx context.Context, request CreateUploadUrlRequestObject) (CreateUploadUrlResponseObject, error) {
	if h.ingestion == nil {
		return CreateUploadUrl400JSONResponse{Code: 400, Message: "ingestion not available (S3 not configured)"}, nil
	}

	principal := principalFromCtx(ctx)
	result, err := h.ingestion.RequestUploadURL(ctx, principal, string(request.CatalogName), request.SchemaName, request.TableName, request.Body.Filename)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return CreateUploadUrl404JSONResponse{Code: 404, Message: err.Error()}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateUploadUrl403JSONResponse{Code: 403, Message: err.Error()}, nil
		default:
			return CreateUploadUrl400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}

	t := result.ExpiresAt
	return CreateUploadUrl200JSONResponse{
		UploadUrl: &result.UploadURL,
		S3Key:     &result.S3Key,
		ExpiresAt: &t,
	}, nil
}

// CommitTableIngestion implements the endpoint for committing uploaded files to a table.
func (h *APIHandler) CommitTableIngestion(ctx context.Context, request CommitTableIngestionRequestObject) (CommitTableIngestionResponseObject, error) {
	if h.ingestion == nil {
		return CommitTableIngestion400JSONResponse{Code: 400, Message: "ingestion not available (S3 not configured)"}, nil
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
			return CommitTableIngestion404JSONResponse{Code: 404, Message: err.Error()}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CommitTableIngestion403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CommitTableIngestion400JSONResponse{Code: 400, Message: err.Error()}, nil
		default:
			return CommitTableIngestion400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}

	filesRegistered := int64(result.FilesRegistered)
	filesSkipped := int64(result.FilesSkipped)
	return CommitTableIngestion200JSONResponse{
		FilesRegistered: &filesRegistered,
		FilesSkipped:    &filesSkipped,
		Schema:          &result.Schema,
		Table:           &result.Table,
	}, nil
}

// LoadTableExternalFiles implements the endpoint for loading external files into a table.
func (h *APIHandler) LoadTableExternalFiles(ctx context.Context, request LoadTableExternalFilesRequestObject) (LoadTableExternalFilesResponseObject, error) {
	if h.ingestion == nil {
		return LoadTableExternalFiles400JSONResponse{Code: 400, Message: "ingestion not available (S3 not configured)"}, nil
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
			return LoadTableExternalFiles404JSONResponse{Code: 404, Message: err.Error()}, nil
		case errors.As(err, new(*domain.AccessDeniedError)):
			return LoadTableExternalFiles403JSONResponse{Code: 403, Message: err.Error()}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return LoadTableExternalFiles400JSONResponse{Code: 400, Message: err.Error()}, nil
		default:
			return LoadTableExternalFiles400JSONResponse{Code: 400, Message: err.Error()}, nil
		}
	}

	filesRegistered := int64(result.FilesRegistered)
	filesSkipped := int64(result.FilesSkipped)
	return LoadTableExternalFiles200JSONResponse{
		FilesRegistered: &filesRegistered,
		FilesSkipped:    &filesSkipped,
		Schema:          &result.Schema,
		Table:           &result.Table,
	}, nil
}
