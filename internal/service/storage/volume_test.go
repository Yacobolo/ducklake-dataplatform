package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"
)

type mockVolumeRepo = testutil.MockVolumeRepo

func TestVolumeService_Create(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		repo := &mockVolumeRepo{
			CreateFn: func(_ context.Context, vol *domain.Volume) (*domain.Volume, error) {
				vol.ID = "vol-1"
				vol.CreatedAt = time.Now()
				vol.UpdatedAt = time.Now()
				return vol, nil
			},
		}
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _, _ string, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}

		svc := NewVolumeService(repo, auth, audit)
		got, err := svc.Create(context.Background(), "alice", "catalog1", "schema1", domain.CreateVolumeRequest{
			Name:       "my-vol",
			VolumeType: domain.VolumeTypeManaged,
			Comment:    "test",
		})
		require.NoError(t, err)
		assert.Equal(t, "vol-1", got.ID)
		assert.Equal(t, "my-vol", got.Name)
		assert.Equal(t, "schema1", got.SchemaName)
		assert.Equal(t, "catalog1", got.CatalogName)
		assert.Equal(t, "alice", got.Owner)
		assert.True(t, audit.HasAction("CREATE_VOLUME"))
	})

	t.Run("access_denied", func(t *testing.T) {
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _, _ string, _ string) (bool, error) {
				return false, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewVolumeService(&mockVolumeRepo{}, auth, audit)

		_, err := svc.Create(context.Background(), "bob", "catalog1", "schema1", domain.CreateVolumeRequest{
			Name:       "my-vol",
			VolumeType: domain.VolumeTypeManaged,
		})
		require.Error(t, err)
		var denied *domain.AccessDeniedError
		require.ErrorAs(t, err, &denied)
	})

	t.Run("validation_error", func(t *testing.T) {
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _, _ string, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewVolumeService(&mockVolumeRepo{}, auth, audit)

		_, err := svc.Create(context.Background(), "alice", "catalog1", "schema1", domain.CreateVolumeRequest{
			Name: "", // missing name
		})
		require.Error(t, err)
		var validationErr *domain.ValidationError
		require.ErrorAs(t, err, &validationErr)
	})

	t.Run("repo_error", func(t *testing.T) {
		repo := &mockVolumeRepo{
			CreateFn: func(_ context.Context, _ *domain.Volume) (*domain.Volume, error) {
				return nil, errTest
			},
		}
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _, _ string, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewVolumeService(repo, auth, audit)

		_, err := svc.Create(context.Background(), "alice", "catalog1", "schema1", domain.CreateVolumeRequest{
			Name:       "my-vol",
			VolumeType: domain.VolumeTypeManaged,
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, errTest)
	})
}

func TestVolumeService_GetByName(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		expected := &domain.Volume{ID: "vol-1", Name: "my-vol", SchemaName: "schema1"}
		repo := &mockVolumeRepo{
			GetByNameFn: func(_ context.Context, schemaName, name string) (*domain.Volume, error) {
				assert.Equal(t, "schema1", schemaName)
				assert.Equal(t, "my-vol", name)
				return expected, nil
			},
		}
		svc := NewVolumeService(repo, &mockAuthService{}, &mockAuditRepo{})

		got, err := svc.GetByName(context.Background(), "catalog1", "schema1", "my-vol")
		require.NoError(t, err)
		assert.Equal(t, expected, got)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockVolumeRepo{
			GetByNameFn: func(_ context.Context, _, _ string) (*domain.Volume, error) {
				return nil, domain.ErrNotFound("volume not found")
			},
		}
		svc := NewVolumeService(repo, &mockAuthService{}, &mockAuditRepo{})

		_, err := svc.GetByName(context.Background(), "catalog1", "schema1", "missing")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		require.ErrorAs(t, err, &notFound)
	})
}

func TestVolumeService_List(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		vols := []domain.Volume{
			{ID: "vol-1", Name: "vol-a"},
			{ID: "vol-2", Name: "vol-b"},
		}
		repo := &mockVolumeRepo{
			ListFn: func(_ context.Context, schemaName string, _ domain.PageRequest) ([]domain.Volume, int64, error) {
				assert.Equal(t, "schema1", schemaName)
				return vols, 2, nil
			},
		}
		svc := NewVolumeService(repo, &mockAuthService{}, &mockAuditRepo{})

		got, total, err := svc.List(context.Background(), "catalog1", "schema1", domain.PageRequest{})
		require.NoError(t, err)
		assert.Len(t, got, 2)
		assert.Equal(t, int64(2), total)
	})
}

func TestVolumeService_Update(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		existing := &domain.Volume{ID: "vol-1", Name: "my-vol", SchemaName: "schema1"}
		newComment := "updated"
		updated := &domain.Volume{ID: "vol-1", Name: "my-vol", Comment: "updated"}

		repo := &mockVolumeRepo{
			GetByNameFn: func(_ context.Context, _, _ string) (*domain.Volume, error) {
				return existing, nil
			},
			UpdateFn: func(_ context.Context, id string, req domain.UpdateVolumeRequest) (*domain.Volume, error) {
				assert.Equal(t, "vol-1", id)
				assert.Equal(t, &newComment, req.Comment)
				return updated, nil
			},
		}
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _, _ string, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewVolumeService(repo, auth, audit)

		got, err := svc.Update(context.Background(), "alice", "catalog1", "schema1", "my-vol", domain.UpdateVolumeRequest{
			Comment: &newComment,
		})
		require.NoError(t, err)
		assert.Equal(t, "updated", got.Comment)
		assert.True(t, audit.HasAction("UPDATE_VOLUME"))
	})

	t.Run("access_denied", func(t *testing.T) {
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _, _ string, _ string) (bool, error) {
				return false, nil
			},
		}
		svc := NewVolumeService(&mockVolumeRepo{}, auth, &mockAuditRepo{})

		_, err := svc.Update(context.Background(), "bob", "catalog1", "schema1", "my-vol", domain.UpdateVolumeRequest{})
		require.Error(t, err)
		var denied *domain.AccessDeniedError
		require.ErrorAs(t, err, &denied)
	})

	t.Run("not_found", func(t *testing.T) {
		repo := &mockVolumeRepo{
			GetByNameFn: func(_ context.Context, _, _ string) (*domain.Volume, error) {
				return nil, domain.ErrNotFound("volume not found")
			},
		}
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _, _ string, _ string) (bool, error) {
				return true, nil
			},
		}
		svc := NewVolumeService(repo, auth, &mockAuditRepo{})

		_, err := svc.Update(context.Background(), "alice", "catalog1", "schema1", "missing", domain.UpdateVolumeRequest{})
		require.Error(t, err)
		var notFound *domain.NotFoundError
		require.ErrorAs(t, err, &notFound)
	})
}

func TestVolumeService_Delete(t *testing.T) {
	t.Run("happy_path", func(t *testing.T) {
		existing := &domain.Volume{ID: "vol-1", Name: "my-vol", SchemaName: "schema1"}
		repo := &mockVolumeRepo{
			GetByNameFn: func(_ context.Context, _, _ string) (*domain.Volume, error) {
				return existing, nil
			},
			DeleteFn: func(_ context.Context, id string) error {
				assert.Equal(t, "vol-1", id)
				return nil
			},
		}
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _, _ string, _ string) (bool, error) {
				return true, nil
			},
		}
		audit := &mockAuditRepo{}
		svc := NewVolumeService(repo, auth, audit)

		err := svc.Delete(context.Background(), "alice", "catalog1", "schema1", "my-vol")
		require.NoError(t, err)
		assert.True(t, audit.HasAction("DELETE_VOLUME"))
	})

	t.Run("access_denied", func(t *testing.T) {
		auth := &mockAuthService{
			CheckPrivilegeFn: func(_ context.Context, _, _, _ string, _ string) (bool, error) {
				return false, nil
			},
		}
		svc := NewVolumeService(&mockVolumeRepo{}, auth, &mockAuditRepo{})

		err := svc.Delete(context.Background(), "bob", "catalog1", "schema1", "my-vol")
		require.Error(t, err)
		var denied *domain.AccessDeniedError
		require.ErrorAs(t, err, &denied)
	})
}
