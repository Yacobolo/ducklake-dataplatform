package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Mocks ===

type mockStorageCredentialService struct {
	listFn   func(ctx context.Context, principal string, page domain.PageRequest) ([]domain.StorageCredential, int64, error)
	createFn func(ctx context.Context, principal string, req domain.CreateStorageCredentialRequest) (*domain.StorageCredential, error)
	getFn    func(ctx context.Context, principal, name string) (*domain.StorageCredential, error)
	updateFn func(ctx context.Context, principal string, name string, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error)
	deleteFn func(ctx context.Context, principal string, name string) error
}

func (m *mockStorageCredentialService) List(ctx context.Context, principal string, page domain.PageRequest) ([]domain.StorageCredential, int64, error) {
	if m.listFn == nil {
		panic("mockStorageCredentialService.List called but not configured")
	}
	return m.listFn(ctx, principal, page)
}

func (m *mockStorageCredentialService) Create(ctx context.Context, principal string, req domain.CreateStorageCredentialRequest) (*domain.StorageCredential, error) {
	if m.createFn == nil {
		panic("mockStorageCredentialService.Create called but not configured")
	}
	return m.createFn(ctx, principal, req)
}

func (m *mockStorageCredentialService) GetByName(ctx context.Context, principal, name string) (*domain.StorageCredential, error) {
	if m.getFn == nil {
		panic("mockStorageCredentialService.GetByName called but not configured")
	}
	return m.getFn(ctx, principal, name)
}

func (m *mockStorageCredentialService) Update(ctx context.Context, principal string, name string, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error) {
	if m.updateFn == nil {
		panic("mockStorageCredentialService.Update called but not configured")
	}
	return m.updateFn(ctx, principal, name, req)
}

func (m *mockStorageCredentialService) Delete(ctx context.Context, principal string, name string) error {
	if m.deleteFn == nil {
		panic("mockStorageCredentialService.Delete called but not configured")
	}
	return m.deleteFn(ctx, principal, name)
}

type mockExternalLocationService struct {
	listFn   func(ctx context.Context, principal string, page domain.PageRequest) ([]domain.ExternalLocation, int64, error)
	createFn func(ctx context.Context, principal string, req domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error)
	getFn    func(ctx context.Context, principal, name string) (*domain.ExternalLocation, error)
	updateFn func(ctx context.Context, principal string, name string, req domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error)
	deleteFn func(ctx context.Context, principal string, name string) error
}

func (m *mockExternalLocationService) List(ctx context.Context, principal string, page domain.PageRequest) ([]domain.ExternalLocation, int64, error) {
	if m.listFn == nil {
		panic("mockExternalLocationService.List called but not configured")
	}
	return m.listFn(ctx, principal, page)
}

func (m *mockExternalLocationService) Create(ctx context.Context, principal string, req domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error) {
	if m.createFn == nil {
		panic("mockExternalLocationService.Create called but not configured")
	}
	return m.createFn(ctx, principal, req)
}

func (m *mockExternalLocationService) GetByName(ctx context.Context, principal, name string) (*domain.ExternalLocation, error) {
	if m.getFn == nil {
		panic("mockExternalLocationService.GetByName called but not configured")
	}
	return m.getFn(ctx, principal, name)
}

func (m *mockExternalLocationService) Update(ctx context.Context, principal string, name string, req domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error) {
	if m.updateFn == nil {
		panic("mockExternalLocationService.Update called but not configured")
	}
	return m.updateFn(ctx, principal, name, req)
}

func (m *mockExternalLocationService) Delete(ctx context.Context, principal string, name string) error {
	if m.deleteFn == nil {
		panic("mockExternalLocationService.Delete called but not configured")
	}
	return m.deleteFn(ctx, principal, name)
}

type mockVolumeService struct {
	listFn   func(ctx context.Context, principal, catalogName string, schemaName string, page domain.PageRequest) ([]domain.Volume, int64, error)
	createFn func(ctx context.Context, catalogName string, principal, schemaName string, req domain.CreateVolumeRequest) (*domain.Volume, error)
	getFn    func(ctx context.Context, principal, catalogName string, schemaName, name string) (*domain.Volume, error)
	updateFn func(ctx context.Context, catalogName string, principal, schemaName, name string, req domain.UpdateVolumeRequest) (*domain.Volume, error)
	deleteFn func(ctx context.Context, catalogName string, principal, schemaName, name string) error
}

func (m *mockVolumeService) List(ctx context.Context, principal, catalogName string, schemaName string, page domain.PageRequest) ([]domain.Volume, int64, error) {
	if m.listFn == nil {
		panic("mockVolumeService.List called but not configured")
	}
	return m.listFn(ctx, principal, catalogName, schemaName, page)
}

func (m *mockVolumeService) Create(ctx context.Context, catalogName string, principal, schemaName string, req domain.CreateVolumeRequest) (*domain.Volume, error) {
	if m.createFn == nil {
		panic("mockVolumeService.Create called but not configured")
	}
	return m.createFn(ctx, catalogName, principal, schemaName, req)
}

func (m *mockVolumeService) GetByName(ctx context.Context, principal, catalogName string, schemaName, name string) (*domain.Volume, error) {
	if m.getFn == nil {
		panic("mockVolumeService.GetByName called but not configured")
	}
	return m.getFn(ctx, principal, catalogName, schemaName, name)
}

func (m *mockVolumeService) Update(ctx context.Context, catalogName string, principal, schemaName, name string, req domain.UpdateVolumeRequest) (*domain.Volume, error) {
	if m.updateFn == nil {
		panic("mockVolumeService.Update called but not configured")
	}
	return m.updateFn(ctx, catalogName, principal, schemaName, name, req)
}

func (m *mockVolumeService) Delete(ctx context.Context, catalogName string, principal, schemaName, name string) error {
	if m.deleteFn == nil {
		panic("mockVolumeService.Delete called but not configured")
	}
	return m.deleteFn(ctx, catalogName, principal, schemaName, name)
}

// === Helpers ===

func storageTestCtx() context.Context {
	return domain.WithPrincipal(context.Background(), domain.ContextPrincipal{
		Name:    "test-user",
		IsAdmin: true,
	})
}

var storageFixedTime = time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

func storageStrPtr(s string) *string { return &s }

func sampleStorageCredential() domain.StorageCredential {
	return domain.StorageCredential{
		ID:             "cred-1",
		Name:           "my-s3-cred",
		CredentialType: domain.CredentialTypeS3,
		Endpoint:       "https://s3.amazonaws.com",
		Region:         "us-east-1",
		URLStyle:       "path",
		Owner:          "test-user",
		CreatedAt:      storageFixedTime,
		UpdatedAt:      storageFixedTime,
	}
}

func sampleExternalLocation() domain.ExternalLocation {
	return domain.ExternalLocation{
		ID:             "loc-1",
		Name:           "my-s3-location",
		URL:            "s3://my-bucket/prefix/",
		CredentialName: "my-s3-cred",
		StorageType:    domain.StorageTypeS3,
		Owner:          "test-user",
		CreatedAt:      storageFixedTime,
		UpdatedAt:      storageFixedTime,
	}
}

func sampleVolume() domain.Volume {
	return domain.Volume{
		ID:          "vol-1",
		Name:        "my-volume",
		SchemaName:  "test-schema",
		CatalogName: "test-catalog",
		VolumeType:  domain.VolumeTypeManaged,
		Owner:       "test-user",
		CreatedAt:   storageFixedTime,
		UpdatedAt:   storageFixedTime,
	}
}

// === Storage Credential Tests ===

func TestHandler_ListStorageCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   ListStorageCredentialsParams
		svcFn    func(ctx context.Context, principal string, page domain.PageRequest) ([]domain.StorageCredential, int64, error)
		assertFn func(t *testing.T, resp ListStorageCredentialsResponseObject, err error)
	}{
		{
			name:   "happy path returns 200 with results",
			params: ListStorageCredentialsParams{},
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.StorageCredential, int64, error) {
				return []domain.StorageCredential{sampleStorageCredential()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListStorageCredentialsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListStorageCredentials200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "my-s3-cred", *(*ok200.Body.Data)[0].Name)
			},
		},
		{
			name:   "empty list returns 200 with empty data",
			params: ListStorageCredentialsParams{},
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.StorageCredential, int64, error) {
				return []domain.StorageCredential{}, 0, nil
			},
			assertFn: func(t *testing.T, resp ListStorageCredentialsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListStorageCredentials200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				assert.Empty(t, *ok200.Body.Data)
			},
		},
		{
			name:   "service error propagates",
			params: ListStorageCredentialsParams{},
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.StorageCredential, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp ListStorageCredentialsResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockStorageCredentialService{listFn: tt.svcFn}
			handler := &APIHandler{storageCreds: svc}
			resp, err := handler.ListStorageCredentials(storageTestCtx(), ListStorageCredentialsRequestObject{Params: tt.params})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CreateStorageCredential(t *testing.T) {
	t.Parallel()

	ct := CreateStorageCredentialRequestCredentialTypeS3
	tests := []struct {
		name     string
		body     CreateStorageCredentialJSONRequestBody
		svcFn    func(ctx context.Context, principal string, req domain.CreateStorageCredentialRequest) (*domain.StorageCredential, error)
		assertFn func(t *testing.T, resp CreateStorageCredentialResponseObject, err error)
	}{
		{
			name: "happy path returns 201",
			body: CreateStorageCredentialJSONRequestBody{Name: "my-s3-cred", CredentialType: ct},
			svcFn: func(_ context.Context, _ string, req domain.CreateStorageCredentialRequest) (*domain.StorageCredential, error) {
				if req.URLStyle != "" {
					return nil, domain.ErrValidation("url_style must be explicit")
				}
				c := sampleStorageCredential()
				c.URLStyle = req.URLStyle
				return &c, nil
			},
			assertFn: func(t *testing.T, resp CreateStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(CreateStorageCredential201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "my-s3-cred", *created.Body.Name)
				assert.Equal(t, "cred-1", *created.Body.Id)
			},
		},
		{
			name: "validation error returns 400",
			body: CreateStorageCredentialJSONRequestBody{Name: "", CredentialType: ct},
			svcFn: func(_ context.Context, _ string, _ domain.CreateStorageCredentialRequest) (*domain.StorageCredential, error) {
				return nil, domain.ErrValidation("credential name is required")
			},
			assertFn: func(t *testing.T, resp CreateStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateStorageCredential400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
				assert.Contains(t, badReq.Body.Message, "credential name is required")
			},
		},
		{
			name: "access denied returns 403",
			body: CreateStorageCredentialJSONRequestBody{Name: "my-s3-cred", CredentialType: ct},
			svcFn: func(_ context.Context, _ string, _ domain.CreateStorageCredentialRequest) (*domain.StorageCredential, error) {
				return nil, domain.ErrAccessDenied("only admins can create credentials")
			},
			assertFn: func(t *testing.T, resp CreateStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CreateStorageCredential403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "conflict error returns 409",
			body: CreateStorageCredentialJSONRequestBody{Name: "my-s3-cred", CredentialType: ct},
			svcFn: func(_ context.Context, _ string, _ domain.CreateStorageCredentialRequest) (*domain.StorageCredential, error) {
				return nil, domain.ErrConflict("credential my-s3-cred already exists")
			},
			assertFn: func(t *testing.T, resp CreateStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				conflict, ok := resp.(CreateStorageCredential409JSONResponse)
				require.True(t, ok, "expected 409 response, got %T", resp)
				assert.Equal(t, int32(409), conflict.Body.Code)
				assert.Contains(t, conflict.Body.Message, "already exists")
			},
		},
		{
			name: "unknown error falls through to 400",
			body: CreateStorageCredentialJSONRequestBody{Name: "fail", CredentialType: ct},
			svcFn: func(_ context.Context, _ string, _ domain.CreateStorageCredentialRequest) (*domain.StorageCredential, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp CreateStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateStorageCredential400JSONResponse)
				require.True(t, ok, "expected 400 response for unknown error, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockStorageCredentialService{createFn: tt.svcFn}
			handler := &APIHandler{storageCreds: svc}
			body := tt.body
			resp, err := handler.CreateStorageCredential(storageTestCtx(), CreateStorageCredentialRequestObject{Body: &body})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetStorageCredential(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		credName string
		svcFn    func(ctx context.Context, principal, name string) (*domain.StorageCredential, error)
		assertFn func(t *testing.T, resp GetStorageCredentialResponseObject, err error)
	}{
		{
			name:     "happy path returns 200",
			credName: "my-s3-cred",
			svcFn: func(_ context.Context, _ string, _ string) (*domain.StorageCredential, error) {
				c := sampleStorageCredential()
				return &c, nil
			},
			assertFn: func(t *testing.T, resp GetStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetStorageCredential200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "my-s3-cred", *ok200.Body.Name)
			},
		},
		{
			name:     "not found returns 404",
			credName: "nonexistent",
			svcFn: func(_ context.Context, _ string, name string) (*domain.StorageCredential, error) {
				return nil, domain.ErrNotFound("credential %s not found", name)
			},
			assertFn: func(t *testing.T, resp GetStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GetStorageCredential404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
				assert.Contains(t, notFound.Body.Message, "nonexistent")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockStorageCredentialService{getFn: tt.svcFn}
			handler := &APIHandler{storageCreds: svc}
			resp, err := handler.GetStorageCredential(storageTestCtx(), GetStorageCredentialRequestObject{CredentialName: tt.credName})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_UpdateStorageCredential(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		credName string
		body     UpdateStorageCredentialJSONRequestBody
		svcFn    func(ctx context.Context, principal string, name string, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error)
		assertFn func(t *testing.T, resp UpdateStorageCredentialResponseObject, err error)
	}{
		{
			name:     "happy path returns 200",
			credName: "my-s3-cred",
			body:     UpdateStorageCredentialJSONRequestBody{Comment: storageStrPtr("updated comment")},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error) {
				c := sampleStorageCredential()
				c.Comment = "updated comment"
				return &c, nil
			},
			assertFn: func(t *testing.T, resp UpdateStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(UpdateStorageCredential200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "my-s3-cred", *ok200.Body.Name)
			},
		},
		{
			name:     "access denied returns 403",
			credName: "my-s3-cred",
			body:     UpdateStorageCredentialJSONRequestBody{Comment: storageStrPtr("x")},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp UpdateStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(UpdateStorageCredential403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:     "not found returns 404",
			credName: "nonexistent",
			body:     UpdateStorageCredentialJSONRequestBody{Comment: storageStrPtr("x")},
			svcFn: func(_ context.Context, _ string, name string, _ domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error) {
				return nil, domain.ErrNotFound("credential %s not found", name)
			},
			assertFn: func(t *testing.T, resp UpdateStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(UpdateStorageCredential404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockStorageCredentialService{updateFn: tt.svcFn}
			handler := &APIHandler{storageCreds: svc}
			body := tt.body
			resp, err := handler.UpdateStorageCredential(storageTestCtx(), UpdateStorageCredentialRequestObject{
				CredentialName: tt.credName,
				Body:           &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteStorageCredential(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		credName string
		svcFn    func(ctx context.Context, principal string, name string) error
		assertFn func(t *testing.T, resp DeleteStorageCredentialResponseObject, err error)
	}{
		{
			name:     "happy path returns 204",
			credName: "my-s3-cred",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteStorageCredential204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name:     "access denied returns 403",
			credName: "my-s3-cred",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp DeleteStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteStorageCredential403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:     "not found returns 404",
			credName: "nonexistent",
			svcFn: func(_ context.Context, _ string, name string) error {
				return domain.ErrNotFound("credential %s not found", name)
			},
			assertFn: func(t *testing.T, resp DeleteStorageCredentialResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteStorageCredential404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockStorageCredentialService{deleteFn: tt.svcFn}
			handler := &APIHandler{storageCreds: svc}
			resp, err := handler.DeleteStorageCredential(storageTestCtx(), DeleteStorageCredentialRequestObject{CredentialName: tt.credName})
			tt.assertFn(t, resp, err)
		})
	}
}

// === External Location Tests ===

func TestHandler_ListExternalLocations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   ListExternalLocationsParams
		svcFn    func(ctx context.Context, principal string, page domain.PageRequest) ([]domain.ExternalLocation, int64, error)
		assertFn func(t *testing.T, resp ListExternalLocationsResponseObject, err error)
	}{
		{
			name:   "happy path returns 200 with results",
			params: ListExternalLocationsParams{},
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.ExternalLocation, int64, error) {
				return []domain.ExternalLocation{sampleExternalLocation()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListExternalLocationsResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListExternalLocations200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "my-s3-location", *(*ok200.Body.Data)[0].Name)
			},
		},
		{
			name:   "service error propagates",
			params: ListExternalLocationsParams{},
			svcFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.ExternalLocation, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp ListExternalLocationsResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockExternalLocationService{listFn: tt.svcFn}
			handler := &APIHandler{externalLocations: svc}
			resp, err := handler.ListExternalLocations(storageTestCtx(), ListExternalLocationsRequestObject{Params: tt.params})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CreateExternalLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     CreateExternalLocationJSONRequestBody
		svcFn    func(ctx context.Context, principal string, req domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error)
		assertFn func(t *testing.T, resp CreateExternalLocationResponseObject, err error)
	}{
		{
			name: "happy path returns 201",
			body: CreateExternalLocationJSONRequestBody{Name: "my-s3-location", Url: "s3://bucket/", CredentialName: "my-s3-cred"},
			svcFn: func(_ context.Context, _ string, _ domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error) {
				l := sampleExternalLocation()
				return &l, nil
			},
			assertFn: func(t *testing.T, resp CreateExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(CreateExternalLocation201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "my-s3-location", *created.Body.Name)
				assert.Equal(t, "loc-1", *created.Body.Id)
			},
		},
		{
			name: "validation error returns 400",
			body: CreateExternalLocationJSONRequestBody{Name: "", Url: "", CredentialName: ""},
			svcFn: func(_ context.Context, _ string, _ domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error) {
				return nil, domain.ErrValidation("location name is required")
			},
			assertFn: func(t *testing.T, resp CreateExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateExternalLocation400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "access denied returns 403",
			body: CreateExternalLocationJSONRequestBody{Name: "loc", Url: "s3://b/", CredentialName: "c"},
			svcFn: func(_ context.Context, _ string, _ domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp CreateExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CreateExternalLocation403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "conflict error returns 409",
			body: CreateExternalLocationJSONRequestBody{Name: "my-s3-location", Url: "s3://b/", CredentialName: "c"},
			svcFn: func(_ context.Context, _ string, _ domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error) {
				return nil, domain.ErrConflict("location already exists")
			},
			assertFn: func(t *testing.T, resp CreateExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				conflict, ok := resp.(CreateExternalLocation409JSONResponse)
				require.True(t, ok, "expected 409 response, got %T", resp)
				assert.Equal(t, int32(409), conflict.Body.Code)
			},
		},
		{
			name: "not found credential returns 400",
			body: CreateExternalLocationJSONRequestBody{Name: "loc", Url: "s3://b/", CredentialName: "missing-cred"},
			svcFn: func(_ context.Context, _ string, _ domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error) {
				return nil, domain.ErrNotFound("credential missing-cred not found")
			},
			assertFn: func(t *testing.T, resp CreateExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateExternalLocation400JSONResponse)
				require.True(t, ok, "expected 400 response for not-found credential, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "unknown error falls through to 400",
			body: CreateExternalLocationJSONRequestBody{Name: "fail", Url: "s3://b/", CredentialName: "c"},
			svcFn: func(_ context.Context, _ string, _ domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp CreateExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateExternalLocation400JSONResponse)
				require.True(t, ok, "expected 400 response for unknown error, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockExternalLocationService{createFn: tt.svcFn}
			handler := &APIHandler{externalLocations: svc}
			body := tt.body
			resp, err := handler.CreateExternalLocation(storageTestCtx(), CreateExternalLocationRequestObject{Body: &body})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetExternalLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		locName  string
		svcFn    func(ctx context.Context, principal, name string) (*domain.ExternalLocation, error)
		assertFn func(t *testing.T, resp GetExternalLocationResponseObject, err error)
	}{
		{
			name:    "happy path returns 200",
			locName: "my-s3-location",
			svcFn: func(_ context.Context, _ string, _ string) (*domain.ExternalLocation, error) {
				l := sampleExternalLocation()
				return &l, nil
			},
			assertFn: func(t *testing.T, resp GetExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetExternalLocation200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "my-s3-location", *ok200.Body.Name)
			},
		},
		{
			name:    "not found returns 404",
			locName: "nonexistent",
			svcFn: func(_ context.Context, _ string, name string) (*domain.ExternalLocation, error) {
				return nil, domain.ErrNotFound("location %s not found", name)
			},
			assertFn: func(t *testing.T, resp GetExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GetExternalLocation404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
				assert.Contains(t, notFound.Body.Message, "nonexistent")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockExternalLocationService{getFn: tt.svcFn}
			handler := &APIHandler{externalLocations: svc}
			resp, err := handler.GetExternalLocation(storageTestCtx(), GetExternalLocationRequestObject{LocationName: tt.locName})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_UpdateExternalLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		locName  string
		body     UpdateExternalLocationJSONRequestBody
		svcFn    func(ctx context.Context, principal string, name string, req domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error)
		assertFn func(t *testing.T, resp UpdateExternalLocationResponseObject, err error)
	}{
		{
			name:    "happy path returns 200",
			locName: "my-s3-location",
			body:    UpdateExternalLocationJSONRequestBody{Comment: storageStrPtr("updated")},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error) {
				l := sampleExternalLocation()
				l.Comment = "updated"
				return &l, nil
			},
			assertFn: func(t *testing.T, resp UpdateExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(UpdateExternalLocation200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "my-s3-location", *ok200.Body.Name)
			},
		},
		{
			name:    "access denied returns 403",
			locName: "my-s3-location",
			body:    UpdateExternalLocationJSONRequestBody{Comment: storageStrPtr("x")},
			svcFn: func(_ context.Context, _ string, _ string, _ domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp UpdateExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(UpdateExternalLocation403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:    "not found returns 404",
			locName: "nonexistent",
			body:    UpdateExternalLocationJSONRequestBody{Comment: storageStrPtr("x")},
			svcFn: func(_ context.Context, _ string, name string, _ domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error) {
				return nil, domain.ErrNotFound("location %s not found", name)
			},
			assertFn: func(t *testing.T, resp UpdateExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(UpdateExternalLocation404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockExternalLocationService{updateFn: tt.svcFn}
			handler := &APIHandler{externalLocations: svc}
			body := tt.body
			resp, err := handler.UpdateExternalLocation(storageTestCtx(), UpdateExternalLocationRequestObject{
				LocationName: tt.locName,
				Body:         &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteExternalLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		locName  string
		svcFn    func(ctx context.Context, principal string, name string) error
		assertFn func(t *testing.T, resp DeleteExternalLocationResponseObject, err error)
	}{
		{
			name:    "happy path returns 204",
			locName: "my-s3-location",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteExternalLocation204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name:    "access denied returns 403",
			locName: "my-s3-location",
			svcFn: func(_ context.Context, _ string, _ string) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp DeleteExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteExternalLocation403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:    "not found returns 404",
			locName: "nonexistent",
			svcFn: func(_ context.Context, _ string, name string) error {
				return domain.ErrNotFound("location %s not found", name)
			},
			assertFn: func(t *testing.T, resp DeleteExternalLocationResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteExternalLocation404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockExternalLocationService{deleteFn: tt.svcFn}
			handler := &APIHandler{externalLocations: svc}
			resp, err := handler.DeleteExternalLocation(storageTestCtx(), DeleteExternalLocationRequestObject{LocationName: tt.locName})
			tt.assertFn(t, resp, err)
		})
	}
}

// === Volume Tests ===

func TestHandler_ListVolumes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svcFn    func(ctx context.Context, principal, catalogName string, schemaName string, page domain.PageRequest) ([]domain.Volume, int64, error)
		assertFn func(t *testing.T, resp ListVolumesResponseObject, err error)
	}{
		{
			name: "happy path returns 200 with results",
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.PageRequest) ([]domain.Volume, int64, error) {
				return []domain.Volume{sampleVolume()}, 1, nil
			},
			assertFn: func(t *testing.T, resp ListVolumesResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(ListVolumes200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				require.NotNil(t, ok200.Body.Data)
				require.Len(t, *ok200.Body.Data, 1)
				assert.Equal(t, "my-volume", *(*ok200.Body.Data)[0].Name)
			},
		},
		{
			name: "service error propagates",
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.PageRequest) ([]domain.Volume, int64, error) {
				return nil, 0, assert.AnError
			},
			assertFn: func(t *testing.T, resp ListVolumesResponseObject, err error) {
				t.Helper()
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockVolumeService{listFn: tt.svcFn}
			handler := &APIHandler{volumes: svc}
			resp, err := handler.ListVolumes(storageTestCtx(), ListVolumesRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				Params:      ListVolumesParams{},
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_CreateVolume(t *testing.T) {
	t.Parallel()

	vt := CreateVolumeRequestVolumeTypeMANAGED
	tests := []struct {
		name     string
		body     CreateVolumeJSONRequestBody
		svcFn    func(ctx context.Context, catalogName string, principal, schemaName string, req domain.CreateVolumeRequest) (*domain.Volume, error)
		assertFn func(t *testing.T, resp CreateVolumeResponseObject, err error)
	}{
		{
			name: "happy path returns 201",
			body: CreateVolumeJSONRequestBody{Name: "my-volume", VolumeType: vt},
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.CreateVolumeRequest) (*domain.Volume, error) {
				v := sampleVolume()
				return &v, nil
			},
			assertFn: func(t *testing.T, resp CreateVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				created, ok := resp.(CreateVolume201JSONResponse)
				require.True(t, ok, "expected 201 response, got %T", resp)
				assert.Equal(t, "my-volume", *created.Body.Name)
				assert.Equal(t, "vol-1", *created.Body.Id)
			},
		},
		{
			name: "validation error returns 400",
			body: CreateVolumeJSONRequestBody{Name: "", VolumeType: vt},
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.CreateVolumeRequest) (*domain.Volume, error) {
				return nil, domain.ErrValidation("volume name is required")
			},
			assertFn: func(t *testing.T, resp CreateVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateVolume400JSONResponse)
				require.True(t, ok, "expected 400 response, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
		{
			name: "access denied returns 403",
			body: CreateVolumeJSONRequestBody{Name: "vol", VolumeType: vt},
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.CreateVolumeRequest) (*domain.Volume, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp CreateVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(CreateVolume403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name: "conflict error returns 409",
			body: CreateVolumeJSONRequestBody{Name: "my-volume", VolumeType: vt},
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.CreateVolumeRequest) (*domain.Volume, error) {
				return nil, domain.ErrConflict("volume already exists")
			},
			assertFn: func(t *testing.T, resp CreateVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				conflict, ok := resp.(CreateVolume409JSONResponse)
				require.True(t, ok, "expected 409 response, got %T", resp)
				assert.Equal(t, int32(409), conflict.Body.Code)
			},
		},
		{
			name: "unknown error falls through to 400",
			body: CreateVolumeJSONRequestBody{Name: "fail", VolumeType: vt},
			svcFn: func(_ context.Context, _ string, _ string, _ string, _ domain.CreateVolumeRequest) (*domain.Volume, error) {
				return nil, assert.AnError
			},
			assertFn: func(t *testing.T, resp CreateVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				badReq, ok := resp.(CreateVolume400JSONResponse)
				require.True(t, ok, "expected 400 response for unknown error, got %T", resp)
				assert.Equal(t, int32(400), badReq.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockVolumeService{createFn: tt.svcFn}
			handler := &APIHandler{volumes: svc}
			body := tt.body
			resp, err := handler.CreateVolume(storageTestCtx(), CreateVolumeRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				Body:        &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_GetVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		volumeName string
		svcFn      func(ctx context.Context, principal, catalogName string, schemaName, name string) (*domain.Volume, error)
		assertFn   func(t *testing.T, resp GetVolumeResponseObject, err error)
	}{
		{
			name:       "happy path returns 200",
			volumeName: "my-volume",
			svcFn: func(_ context.Context, _ string, _ string, _, _ string) (*domain.Volume, error) {
				v := sampleVolume()
				return &v, nil
			},
			assertFn: func(t *testing.T, resp GetVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(GetVolume200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "my-volume", *ok200.Body.Name)
			},
		},
		{
			name:       "not found returns 404",
			volumeName: "nonexistent",
			svcFn: func(_ context.Context, _ string, _ string, _, name string) (*domain.Volume, error) {
				return nil, domain.ErrNotFound("volume %s not found", name)
			},
			assertFn: func(t *testing.T, resp GetVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(GetVolume404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockVolumeService{getFn: tt.svcFn}
			handler := &APIHandler{volumes: svc}
			resp, err := handler.GetVolume(storageTestCtx(), GetVolumeRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				VolumeName:  tt.volumeName,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_UpdateVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		volumeName string
		body       UpdateVolumeJSONRequestBody
		svcFn      func(ctx context.Context, catalogName string, principal, schemaName, name string, req domain.UpdateVolumeRequest) (*domain.Volume, error)
		assertFn   func(t *testing.T, resp UpdateVolumeResponseObject, err error)
	}{
		{
			name:       "happy path returns 200",
			volumeName: "my-volume",
			body:       UpdateVolumeJSONRequestBody{Comment: storageStrPtr("updated")},
			svcFn: func(_ context.Context, _ string, _, _, _ string, _ domain.UpdateVolumeRequest) (*domain.Volume, error) {
				v := sampleVolume()
				v.Comment = "updated"
				return &v, nil
			},
			assertFn: func(t *testing.T, resp UpdateVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				ok200, ok := resp.(UpdateVolume200JSONResponse)
				require.True(t, ok, "expected 200 response, got %T", resp)
				assert.Equal(t, "my-volume", *ok200.Body.Name)
			},
		},
		{
			name:       "access denied returns 403",
			volumeName: "my-volume",
			body:       UpdateVolumeJSONRequestBody{Comment: storageStrPtr("x")},
			svcFn: func(_ context.Context, _ string, _, _, _ string, _ domain.UpdateVolumeRequest) (*domain.Volume, error) {
				return nil, domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp UpdateVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(UpdateVolume403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:       "not found returns 404",
			volumeName: "nonexistent",
			body:       UpdateVolumeJSONRequestBody{Comment: storageStrPtr("x")},
			svcFn: func(_ context.Context, _ string, _, _, name string, _ domain.UpdateVolumeRequest) (*domain.Volume, error) {
				return nil, domain.ErrNotFound("volume %s not found", name)
			},
			assertFn: func(t *testing.T, resp UpdateVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(UpdateVolume404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockVolumeService{updateFn: tt.svcFn}
			handler := &APIHandler{volumes: svc}
			body := tt.body
			resp, err := handler.UpdateVolume(storageTestCtx(), UpdateVolumeRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				VolumeName:  tt.volumeName,
				Body:        &body,
			})
			tt.assertFn(t, resp, err)
		})
	}
}

func TestHandler_DeleteVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		volumeName string
		svcFn      func(ctx context.Context, catalogName string, principal, schemaName, name string) error
		assertFn   func(t *testing.T, resp DeleteVolumeResponseObject, err error)
	}{
		{
			name:       "happy path returns 204",
			volumeName: "my-volume",
			svcFn: func(_ context.Context, _ string, _, _, _ string) error {
				return nil
			},
			assertFn: func(t *testing.T, resp DeleteVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				_, ok := resp.(DeleteVolume204Response)
				require.True(t, ok, "expected 204 response, got %T", resp)
			},
		},
		{
			name:       "access denied returns 403",
			volumeName: "my-volume",
			svcFn: func(_ context.Context, _ string, _, _, _ string) error {
				return domain.ErrAccessDenied("not allowed")
			},
			assertFn: func(t *testing.T, resp DeleteVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				forbidden, ok := resp.(DeleteVolume403JSONResponse)
				require.True(t, ok, "expected 403 response, got %T", resp)
				assert.Equal(t, int32(403), forbidden.Body.Code)
			},
		},
		{
			name:       "not found returns 404",
			volumeName: "nonexistent",
			svcFn: func(_ context.Context, _ string, _, _, name string) error {
				return domain.ErrNotFound("volume %s not found", name)
			},
			assertFn: func(t *testing.T, resp DeleteVolumeResponseObject, err error) {
				t.Helper()
				require.NoError(t, err)
				notFound, ok := resp.(DeleteVolume404JSONResponse)
				require.True(t, ok, "expected 404 response, got %T", resp)
				assert.Equal(t, int32(404), notFound.Body.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &mockVolumeService{deleteFn: tt.svcFn}
			handler := &APIHandler{volumes: svc}
			resp, err := handler.DeleteVolume(storageTestCtx(), DeleteVolumeRequestObject{
				CatalogName: CatalogName("test-catalog"),
				SchemaName:  "test-schema",
				VolumeName:  tt.volumeName,
			})
			tt.assertFn(t, resp, err)
		})
	}
}
