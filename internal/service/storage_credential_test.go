package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Create ===

func TestStorageCredentialService_Create(t *testing.T) {
	validReq := domain.CreateStorageCredentialRequest{
		Name:           "my-cred",
		CredentialType: domain.CredentialTypeS3,
		KeyID:          "AKID",
		Secret:         "SECRET",
		Endpoint:       "s3.example.com",
		Region:         "us-east-1",
		URLStyle:       "path",
		Comment:        "test credential",
	}

	t.Run("happy_path", func(t *testing.T) {
		repo := &mockStorageCredentialRepo{
			createFn: func(_ context.Context, cred *domain.StorageCredential) (*domain.StorageCredential, error) {
				return &domain.StorageCredential{
					ID:             1,
					Name:           cred.Name,
					CredentialType: cred.CredentialType,
					KeyID:          cred.KeyID,
					Secret:         cred.Secret,
					Endpoint:       cred.Endpoint,
					Region:         cred.Region,
					URLStyle:       cred.URLStyle,
					Comment:        cred.Comment,
					Owner:          cred.Owner,
					CreatedAt:      time.Now(),
					UpdatedAt:      time.Now(),
				}, nil
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewStorageCredentialService(repo, auth, audit)

		result, err := svc.Create(ctxWithPrincipal("admin_user"), "admin_user", validReq)

		require.NoError(t, err)
		assert.Equal(t, "my-cred", result.Name)
		assert.Equal(t, "admin_user", result.Owner)
		assert.True(t, audit.hasAction("CREATE_STORAGE_CREDENTIAL"))
	})

	t.Run("access_denied", func(t *testing.T) {
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return false, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewStorageCredentialService(&mockStorageCredentialRepo{}, auth, audit)

		_, err := svc.Create(ctxWithPrincipal("nobody"), "nobody", validReq)

		require.Error(t, err)
		var denied *domain.AccessDeniedError
		assert.True(t, errors.As(err, &denied))
		assert.Empty(t, audit.entries)
	})

	t.Run("validation_error", func(t *testing.T) {
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewStorageCredentialService(&mockStorageCredentialRepo{}, auth, audit)

		_, err := svc.Create(ctxWithPrincipal("admin_user"), "admin_user", domain.CreateStorageCredentialRequest{})

		require.Error(t, err)
		var valErr *domain.ValidationError
		assert.True(t, errors.As(err, &valErr))
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockStorageCredentialRepo{
			createFn: func(_ context.Context, _ *domain.StorageCredential) (*domain.StorageCredential, error) {
				return nil, errTest
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewStorageCredentialService(repo, auth, audit)

		_, err := svc.Create(ctxWithPrincipal("admin_user"), "admin_user", validReq)

		require.Error(t, err)
		assert.Empty(t, audit.entries)
	})
}

// === GetByName ===

func TestStorageCredentialService_GetByName(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockStorageCredentialRepo{
			getByNameFn: func(_ context.Context, name string) (*domain.StorageCredential, error) {
				return &domain.StorageCredential{ID: 1, Name: name}, nil
			},
		}
		svc := NewStorageCredentialService(repo, &mockAuthService{}, &mockAuditRepo{})

		result, err := svc.GetByName(context.Background(), "my-cred")

		require.NoError(t, err)
		assert.Equal(t, "my-cred", result.Name)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockStorageCredentialRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.StorageCredential, error) {
				return nil, domain.ErrNotFound("credential not found")
			},
		}
		svc := NewStorageCredentialService(repo, &mockAuthService{}, &mockAuditRepo{})

		_, err := svc.GetByName(context.Background(), "missing")

		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.True(t, errors.As(err, &notFound))
	})
}

// === List ===

func TestStorageCredentialService_List(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockStorageCredentialRepo{
			listFn: func(_ context.Context, _ domain.PageRequest) ([]domain.StorageCredential, int64, error) {
				return []domain.StorageCredential{
					{ID: 1, Name: "cred-1"},
					{ID: 2, Name: "cred-2"},
				}, 2, nil
			},
		}
		svc := NewStorageCredentialService(repo, &mockAuthService{}, &mockAuditRepo{})

		creds, total, err := svc.List(context.Background(), domain.PageRequest{MaxResults: 100})

		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		assert.Len(t, creds, 2)
	})

	t.Run("empty", func(t *testing.T) {
		repo := &mockStorageCredentialRepo{
			listFn: func(_ context.Context, _ domain.PageRequest) ([]domain.StorageCredential, int64, error) {
				return nil, 0, nil
			},
		}
		svc := NewStorageCredentialService(repo, &mockAuthService{}, &mockAuditRepo{})

		creds, total, err := svc.List(context.Background(), domain.PageRequest{MaxResults: 100})

		require.NoError(t, err)
		assert.Equal(t, int64(0), total)
		assert.Empty(t, creds)
	})
}

// === Delete ===

func TestStorageCredentialService_Delete(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockStorageCredentialRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.StorageCredential, error) {
				return &domain.StorageCredential{ID: 1, Name: "my-cred"}, nil
			},
			deleteFn: func(_ context.Context, _ int64) error {
				return nil
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewStorageCredentialService(repo, auth, audit)

		err := svc.Delete(ctxWithPrincipal("admin_user"), "admin_user", "my-cred")

		require.NoError(t, err)
		assert.True(t, audit.hasAction("DELETE_STORAGE_CREDENTIAL"))
	})

	t.Run("access_denied", func(t *testing.T) {
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return false, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewStorageCredentialService(&mockStorageCredentialRepo{}, auth, audit)

		err := svc.Delete(ctxWithPrincipal("nobody"), "nobody", "my-cred")

		require.Error(t, err)
		var denied *domain.AccessDeniedError
		assert.True(t, errors.As(err, &denied))
		assert.Empty(t, audit.entries)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockStorageCredentialRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.StorageCredential, error) {
				return nil, domain.ErrNotFound("not found")
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		svc := NewStorageCredentialService(repo, auth, &mockAuditRepo{})

		err := svc.Delete(ctxWithPrincipal("admin_user"), "admin_user", "missing")

		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.True(t, errors.As(err, &notFound))
	})
}

// === Update ===

func TestStorageCredentialService_Update(t *testing.T) {
	newEndpoint := "s3-new.example.com"
	updateReq := domain.UpdateStorageCredentialRequest{
		Endpoint: &newEndpoint,
	}

	t.Run("happy_path", func(t *testing.T) {
		repo := &mockStorageCredentialRepo{
			getByNameFn: func(_ context.Context, _ string) (*domain.StorageCredential, error) {
				return &domain.StorageCredential{ID: 1, Name: "my-cred", Endpoint: "s3.example.com"}, nil
			},
			updateFn: func(_ context.Context, _ int64, _ domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error) {
				return &domain.StorageCredential{ID: 1, Name: "my-cred", Endpoint: "s3-new.example.com"}, nil
			},
		}
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewStorageCredentialService(repo, auth, audit)

		result, err := svc.Update(ctxWithPrincipal("admin_user"), "admin_user", "my-cred", updateReq)

		require.NoError(t, err)
		assert.Equal(t, "s3-new.example.com", result.Endpoint)
		assert.True(t, audit.hasAction("UPDATE_STORAGE_CREDENTIAL"))
	})

	t.Run("access_denied", func(t *testing.T) {
		auth := &mockAuthService{
			checkPrivilegeFn: func(_ context.Context, _, _ string, _ int64, _ string) (bool, error) {
				return false, nil
			},
		}
		svc := NewStorageCredentialService(&mockStorageCredentialRepo{}, auth, &mockAuditRepo{})

		_, err := svc.Update(ctxWithPrincipal("nobody"), "nobody", "my-cred", updateReq)

		require.Error(t, err)
		var denied *domain.AccessDeniedError
		assert.True(t, errors.As(err, &denied))
	})
}
