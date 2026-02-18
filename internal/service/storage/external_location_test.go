//go:build integration

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
)

// === Create ===

func TestExternalLocationService_Create(t *testing.T) {
	validReq := domain.CreateExternalLocationRequest{
		Name:           "my-location",
		URL:            "s3://my-bucket/data/",
		CredentialName: "my-cred",
		StorageType:    domain.StorageTypeS3,
		Comment:        "test location",
	}

	testCred := &domain.StorageCredential{
		ID:             "1",
		Name:           "my-cred",
		CredentialType: domain.CredentialTypeS3,
		KeyID:          "AKID",
		Secret:         "SECRET",
		Endpoint:       "s3.example.com",
		Region:         "us-east-1",
		URLStyle:       "path",
	}

	t.Run("happy_path", func(t *testing.T) {
		duckDB := testDuckDB(t)

		credRepo := &mockStorageCredentialRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.StorageCredential, error) {
				return testCred, nil
			},
		}
		locRepo := &mockExternalLocationRepo{
			CreateFn: func(_ context.Context, loc *domain.ExternalLocation) (*domain.ExternalLocation, error) {
				return &domain.ExternalLocation{
					ID:             "1",
					Name:           loc.Name,
					URL:            loc.URL,
					CredentialName: loc.CredentialName,
					StorageType:    loc.StorageType,
					Comment:        loc.Comment,
					Owner:          loc.Owner,
					ReadOnly:       loc.ReadOnly,
					CreatedAt:      time.Now(),
					UpdatedAt:      time.Now(),
				}, nil
			},
		}
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}

		secretMgr := testSecretManager(duckDB)
		svc := NewExternalLocationService(locRepo, credRepo, auth, audit, secretMgr, discardLogger())

		result, err := svc.Create(ctxWithPrincipal("admin_user"), "admin_user", validReq)

		require.NoError(t, err)
		assert.Equal(t, "my-location", result.Name)
		assert.Equal(t, "s3://my-bucket/data/", result.URL)
		assert.Equal(t, "admin_user", result.Owner)
		assert.True(t, audit.HasAction("CREATE_EXTERNAL_LOCATION"))
	})

	t.Run("access_denied", func(t *testing.T) {
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
				return false, nil
			},
		}
		locRepo := &mockExternalLocationRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ExternalLocation, error) {
				return &domain.ExternalLocation{ID: "1", Name: "my-loc", CredentialName: "my-cred"}, nil
			},
		}
		svc := NewExternalLocationService(
			locRepo, &mockStorageCredentialRepo{},
			auth, &mockAuditRepo{}, nil, discardLogger(),
		)

		_, err := svc.Create(ctxWithPrincipal("nobody"), "nobody", validReq)

		require.Error(t, err)
		var denied *domain.AccessDeniedError
		require.ErrorAs(t, err, &denied)
	})

	t.Run("validation_error", func(t *testing.T) {
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
				return true, nil
			},
		}
		svc := NewExternalLocationService(
			&mockExternalLocationRepo{}, &mockStorageCredentialRepo{},
			auth, &mockAuditRepo{}, nil, discardLogger(),
		)

		_, err := svc.Create(ctxWithPrincipal("admin_user"), "admin_user", domain.CreateExternalLocationRequest{})

		require.Error(t, err)
		var valErr *domain.ValidationError
		require.ErrorAs(t, err, &valErr)
	})

	t.Run("credential_not_found", func(t *testing.T) {
		credRepo := &mockStorageCredentialRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.StorageCredential, error) {
				return nil, domain.ErrNotFound("credential not found")
			},
		}
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
				return true, nil
			},
		}
		svc := NewExternalLocationService(
			&mockExternalLocationRepo{}, credRepo,
			auth, &mockAuditRepo{}, nil, discardLogger(),
		)

		_, err := svc.Create(ctxWithPrincipal("admin_user"), "admin_user", validReq)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "credential")
	})

	t.Run("repo_error_on_persist", func(t *testing.T) {
		credRepo := &mockStorageCredentialRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.StorageCredential, error) {
				return testCred, nil
			},
		}
		locRepo := &mockExternalLocationRepo{
			CreateFn: func(_ context.Context, _ *domain.ExternalLocation) (*domain.ExternalLocation, error) {
				return nil, errTest
			},
		}
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
				return true, nil
			},
		}
		svc := NewExternalLocationService(locRepo, credRepo, auth, &mockAuditRepo{}, nil, discardLogger())

		_, err := svc.Create(ctxWithPrincipal("admin_user"), "admin_user", validReq)

		require.Error(t, err)
	})
}

// === GetByName ===

func TestExternalLocationService_GetByName(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockExternalLocationRepo{
			GetByNameFn: func(_ context.Context, name string) (*domain.ExternalLocation, error) {
				return &domain.ExternalLocation{ID: "1", Name: name, URL: "s3://bucket/path/"}, nil
			},
		}
		svc := NewExternalLocationService(repo, &mockStorageCredentialRepo{}, &mockAuthService{}, &mockAuditRepo{}, nil, discardLogger())

		result, err := svc.GetByName(context.Background(), "my-loc")

		require.NoError(t, err)
		assert.Equal(t, "my-loc", result.Name)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockExternalLocationRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ExternalLocation, error) {
				return nil, domain.ErrNotFound("not found")
			},
		}
		svc := NewExternalLocationService(repo, &mockStorageCredentialRepo{}, &mockAuthService{}, &mockAuditRepo{}, nil, discardLogger())

		_, err := svc.GetByName(context.Background(), "missing")

		require.Error(t, err)
		var notFound *domain.NotFoundError
		require.ErrorAs(t, err, &notFound)
	})
}

// === List ===

func TestExternalLocationService_List(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockExternalLocationRepo{
			ListFn: func(_ context.Context, _ domain.PageRequest) ([]domain.ExternalLocation, int64, error) {
				return []domain.ExternalLocation{
					{ID: "1", Name: "loc-1"},
					{ID: "2", Name: "loc-2"},
				}, 2, nil
			},
		}
		svc := NewExternalLocationService(repo, &mockStorageCredentialRepo{}, &mockAuthService{}, &mockAuditRepo{}, nil, discardLogger())

		locs, total, err := svc.List(context.Background(), domain.PageRequest{MaxResults: 100})

		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		assert.Len(t, locs, 2)
	})
}

// === Delete ===

func TestExternalLocationService_Delete(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		duckDB := testDuckDB(t)

		// Pre-create the secret so DropS3Secret doesn't fail
		require.NoError(t, engine.CreateS3Secret(context.Background(), duckDB,
			"cred_my-cred", "AKID", "SECRET", "s3.example.com", "us-east-1", "path"))

		locRepo := &mockExternalLocationRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ExternalLocation, error) {
				return &domain.ExternalLocation{ID: "1", Name: "my-loc", CredentialName: "my-cred"}, nil
			},
			DeleteFn: func(_ context.Context, _ string) error {
				return nil
			},
		}
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}
		secretMgr := testSecretManager(duckDB)
		svc := NewExternalLocationService(locRepo, &mockStorageCredentialRepo{}, auth, audit, secretMgr, discardLogger())

		err := svc.Delete(ctxWithPrincipal("admin_user"), "admin_user", "my-loc")

		require.NoError(t, err)
		assert.True(t, audit.HasAction("DELETE_EXTERNAL_LOCATION"))
	})

	t.Run("access_denied", func(t *testing.T) {
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
				return false, nil
			},
		}
		locRepo := &mockExternalLocationRepo{
			GetByNameFn: func(_ context.Context, _ string) (*domain.ExternalLocation, error) {
				return &domain.ExternalLocation{ID: "1", Name: "my-loc", CredentialName: "my-cred"}, nil
			},
		}
		svc := NewExternalLocationService(
			locRepo, &mockStorageCredentialRepo{},
			auth, &mockAuditRepo{}, nil, discardLogger(),
		)

		err := svc.Delete(ctxWithPrincipal("nobody"), "nobody", "my-loc")

		require.Error(t, err)
		var denied *domain.AccessDeniedError
		require.ErrorAs(t, err, &denied)
	})
}

// === RestoreSecrets ===

func TestExternalLocationService_RestoreSecrets(t *testing.T) {
	t.Run("no_credentials_noop", func(t *testing.T) {
		credRepo := &mockStorageCredentialRepo{
			ListFn: func(_ context.Context, _ domain.PageRequest) ([]domain.StorageCredential, int64, error) {
				return nil, 0, nil
			},
		}
		svc := NewExternalLocationService(nil, credRepo, &mockAuthService{}, &mockAuditRepo{}, nil, discardLogger())

		err := svc.RestoreSecrets(context.Background())

		require.NoError(t, err)
	})

	t.Run("with_locations_creates_secrets", func(t *testing.T) {
		duckDB := testDuckDB(t)

		locRepo := &mockExternalLocationRepo{
			ListFn: func(_ context.Context, _ domain.PageRequest) ([]domain.ExternalLocation, int64, error) {
				return []domain.ExternalLocation{
					{ID: "1", Name: "loc-1", URL: "s3://bucket/data/"},
				}, 1, nil
			},
		}
		credRepo := &mockStorageCredentialRepo{
			ListFn: func(_ context.Context, _ domain.PageRequest) ([]domain.StorageCredential, int64, error) {
				return []domain.StorageCredential{
					{
						Name: "cred-1", CredentialType: domain.CredentialTypeS3,
						KeyID: "AKID", Secret: "SECRET",
						Endpoint: "s3.example.com", Region: "us-east-1", URLStyle: "path",
					},
				}, 1, nil
			},
		}

		secretMgr := testSecretManager(duckDB)
		svc := NewExternalLocationService(locRepo, credRepo, &mockAuthService{}, &mockAuditRepo{}, secretMgr, discardLogger())

		err := svc.RestoreSecrets(context.Background())

		require.NoError(t, err)
	})
}
