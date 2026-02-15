package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Mock ===

type mockIngestionService struct {
	requestUploadURLFn  func(ctx context.Context, principal string, catalogName string, schemaName, tableName string, filename *string) (*domain.UploadURLResult, error)
	commitIngestionFn   func(ctx context.Context, principal string, catalogName string, schemaName, tableName string, s3Keys []string, opts domain.IngestionOptions) (*domain.IngestionResult, error)
	loadExternalFilesFn func(ctx context.Context, principal string, catalogName string, schemaName, tableName string, paths []string, opts domain.IngestionOptions) (*domain.IngestionResult, error)
}

func (m *mockIngestionService) RequestUploadURL(ctx context.Context, principal string, catalogName string, schemaName, tableName string, filename *string) (*domain.UploadURLResult, error) {
	if m.requestUploadURLFn == nil {
		panic("mockIngestionService.RequestUploadURL called but not configured")
	}
	return m.requestUploadURLFn(ctx, principal, catalogName, schemaName, tableName, filename)
}

func (m *mockIngestionService) CommitIngestion(ctx context.Context, principal string, catalogName string, schemaName, tableName string, s3Keys []string, opts domain.IngestionOptions) (*domain.IngestionResult, error) {
	if m.commitIngestionFn == nil {
		panic("mockIngestionService.CommitIngestion called but not configured")
	}
	return m.commitIngestionFn(ctx, principal, catalogName, schemaName, tableName, s3Keys, opts)
}

func (m *mockIngestionService) LoadExternalFiles(ctx context.Context, principal string, catalogName string, schemaName, tableName string, paths []string, opts domain.IngestionOptions) (*domain.IngestionResult, error) {
	if m.loadExternalFilesFn == nil {
		panic("mockIngestionService.LoadExternalFiles called but not configured")
	}
	return m.loadExternalFilesFn(ctx, principal, catalogName, schemaName, tableName, paths, opts)
}

// === Helpers ===

func ingestionTestCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name:    "test-user",
		IsAdmin: true,
	})
}

var ingestionFixedTime = time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

func ingestionStrPtr(s string) *string { return &s }

// === Tests ===

func TestHandler_CreateUploadUrl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svc      ingestionService // nil tests the nil-check path
		body     CreateUploadUrlJSONRequestBody
		svcFn    func(ctx context.Context, principal string, catalogName string, schemaName, tableName string, filename *string) (*domain.UploadURLResult, error)
		assertFn func(t *testing.T, resp CreateUploadUrlResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			body: CreateUploadUrlJSONRequestBody{Filename: ingestionStrPtr("data.csv")},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ *string) (*domain.UploadURLResult, error) {
				return &domain.UploadURLResult{
					UploadURL: "https://s3.amazonaws.com/bucket/key",
					S3Key:     "uploads/data.csv",
					ExpiresAt: ingestionFixedTime.Add(time.Hour),
				}, nil
			},
			assertFn: func(t *testing.T, resp CreateUploadUrlResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(CreateUploadUrl200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "https://s3.amazonaws.com/bucket/key", *ok200.Body.UploadUrl)
				assert.Equal(t, "uploads/data.csv", *ok200.Body.S3Key)
			},
		},
		{
			name:  "ingestion nil returns 400",
			svc:   nil, // explicitly nil
			body:  CreateUploadUrlJSONRequestBody{},
			svcFn: nil,
			assertFn: func(t *testing.T, resp CreateUploadUrlResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateUploadUrl400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
				assert.Contains(t, badReq.Body.Message, "not available")
			},
		},
		{
			name: "not found returns 404",
			body: CreateUploadUrlJSONRequestBody{},
			svcFn: func(_ context.Context, _ string, _ string, _, tableName string, _ *string) (*domain.UploadURLResult, error) {
				return nil, domain.ErrNotFound("table %s not found", tableName)
			},
			assertFn: func(t *testing.T, resp CreateUploadUrlResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(CreateUploadUrl404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name: "access denied returns 403",
			body: CreateUploadUrlJSONRequestBody{},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ *string) (*domain.UploadURLResult, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp CreateUploadUrlResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CreateUploadUrl403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "unknown error returns 400",
			body: CreateUploadUrlJSONRequestBody{},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ *string) (*domain.UploadURLResult, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp CreateUploadUrlResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateUploadUrl400JSONResponse)
				require.True(t, ok, "expected 400 response for unknown error, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var handler *APIHandler
			if tt.name == "ingestion nil returns 400" {
				handler = &APIHandler{ingestion: nil}
			} else {
				svc := &mockIngestionService{requestUploadURLFn: tt.svcFn}
				handler = &APIHandler{ingestion: svc}
			}
			body := tt.body
			resp, err := handler.CreateUploadUrl(ingestionTestCtx(), CreateUploadUrlRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				TableName:   "test-table",
				Body:        &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CommitTableIngestion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		nilSvc   bool
		body     CommitTableIngestionJSONRequestBody
		svcFn    func(ctx context.Context, principal string, catalogName string, schemaName, tableName string, s3Keys []string, opts domain.IngestionOptions) (*domain.IngestionResult, error)
		assertFn func(t *testing.T, resp CommitTableIngestionResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			body: CommitTableIngestionJSONRequestBody{S3Keys: []string{"uploads/data.csv"}},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ []string, _ domain.IngestionOptions) (*domain.IngestionResult, error) {
				return &domain.IngestionResult{
					FilesRegistered: 1,
					FilesSkipped:    0,
					Table:           "test-table",
					Schema:          "test-schema",
				}, nil
			},
			assertFn: func(t *testing.T, resp CommitTableIngestionResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(CommitTableIngestion200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, int64(1), *ok200.Body.FilesRegistered)
				assert.Equal(t, int64(0), *ok200.Body.FilesSkipped)
				assert.Equal(t, "test-table", *ok200.Body.Table)
			},
		},
		{
			name:   "ingestion nil returns 400",
			nilSvc: true,
			body:   CommitTableIngestionJSONRequestBody{S3Keys: []string{"key"}},
			svcFn:  nil,
			assertFn: func(t *testing.T, resp CommitTableIngestionResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CommitTableIngestion400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Contains(t, badReq.Body.Message, "not available")
			},
		},
		{
			name: "not found returns 404",
			body: CommitTableIngestionJSONRequestBody{S3Keys: []string{"key"}},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ []string, _ domain.IngestionOptions) (*domain.IngestionResult, error) {
				return nil, domain.ErrNotFound("table not found")
			},
			assertFn: func(t *testing.T, resp CommitTableIngestionResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(CommitTableIngestion404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name: "access denied returns 403",
			body: CommitTableIngestionJSONRequestBody{S3Keys: []string{"key"}},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ []string, _ domain.IngestionOptions) (*domain.IngestionResult, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp CommitTableIngestionResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CommitTableIngestion403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "validation error returns 400",
			body: CommitTableIngestionJSONRequestBody{S3Keys: []string{}},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ []string, _ domain.IngestionOptions) (*domain.IngestionResult, error) {
				return nil, domain.ErrValidation("s3_keys must not be empty")
			},
			assertFn: func(t *testing.T, resp CommitTableIngestionResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CommitTableIngestion400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "unknown error falls through to 400",
			body: CommitTableIngestionJSONRequestBody{S3Keys: []string{"key"}},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ []string, _ domain.IngestionOptions) (*domain.IngestionResult, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp CommitTableIngestionResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CommitTableIngestion400JSONResponse)
				require.True(t, ok, "expected 400 response for unknown error, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var handler *APIHandler
			if tt.nilSvc {
				handler = &APIHandler{ingestion: nil}
			} else {
				svc := &mockIngestionService{commitIngestionFn: tt.svcFn}
				handler = &APIHandler{ingestion: svc}
			}
			body := tt.body
			resp, err := handler.CommitTableIngestion(ingestionTestCtx(), CommitTableIngestionRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				TableName:   "test-table",
				Body:        &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_LoadTableExternalFiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		nilSvc   bool
		body     LoadTableExternalFilesJSONRequestBody
		svcFn    func(ctx context.Context, principal string, catalogName string, schemaName, tableName string, paths []string, opts domain.IngestionOptions) (*domain.IngestionResult, error)
		assertFn func(t *testing.T, resp LoadTableExternalFilesResponseObject, err error)
	}{
		{
			name: "happy path returns 200",
			body: LoadTableExternalFilesJSONRequestBody{Paths: []string{"s3://bucket/data.csv"}},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ []string, _ domain.IngestionOptions) (*domain.IngestionResult, error) {
				return &domain.IngestionResult{
					FilesRegistered: 1,
					FilesSkipped:    0,
					Table:           "test-table",
					Schema:          "test-schema",
				}, nil
			},
			assertFn: func(t *testing.T, resp LoadTableExternalFilesResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(LoadTableExternalFiles200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, int64(1), *ok200.Body.FilesRegistered)
				assert.Equal(t, "test-table", *ok200.Body.Table)
			},
		},
		{
			name:   "ingestion nil returns 400",
			nilSvc: true,
			body:   LoadTableExternalFilesJSONRequestBody{Paths: []string{"path"}},
			svcFn:  nil,
			assertFn: func(t *testing.T, resp LoadTableExternalFilesResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(LoadTableExternalFiles400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Contains(t, badReq.Body.Message, "not available")
			},
		},
		{
			name: "not found returns 404",
			body: LoadTableExternalFilesJSONRequestBody{Paths: []string{"path"}},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ []string, _ domain.IngestionOptions) (*domain.IngestionResult, error) {
				return nil, domain.ErrNotFound("table not found")
			},
			assertFn: func(t *testing.T, resp LoadTableExternalFilesResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(LoadTableExternalFiles404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
		{
			name: "access denied returns 403",
			body: LoadTableExternalFilesJSONRequestBody{Paths: []string{"path"}},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ []string, _ domain.IngestionOptions) (*domain.IngestionResult, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp LoadTableExternalFilesResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(LoadTableExternalFiles403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "validation error returns 400",
			body: LoadTableExternalFilesJSONRequestBody{Paths: []string{}},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ []string, _ domain.IngestionOptions) (*domain.IngestionResult, error) {
				return nil, domain.ErrValidation("paths must not be empty")
			},
			assertFn: func(t *testing.T, resp LoadTableExternalFilesResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(LoadTableExternalFiles400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "unknown error falls through to 400",
			body: LoadTableExternalFilesJSONRequestBody{Paths: []string{"path"}},
			svcFn: func(_ context.Context, _ string, _ string, _, _ string, _ []string, _ domain.IngestionOptions) (*domain.IngestionResult, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp LoadTableExternalFilesResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(LoadTableExternalFiles400JSONResponse)
				require.True(t, ok, "expected 400 response for unknown error, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var handler *APIHandler
			if tt.nilSvc {
				handler = &APIHandler{ingestion: nil}
			} else {
				svc := &mockIngestionService{loadExternalFilesFn: tt.svcFn}
				handler = &APIHandler{ingestion: svc}
			}
			body := tt.body
			resp, err := handler.LoadTableExternalFiles(ingestionTestCtx(), LoadTableExternalFilesRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				TableName:   "test-table",
				Body:        &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}
