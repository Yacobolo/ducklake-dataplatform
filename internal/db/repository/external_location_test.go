package repository

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/crypto"
	"duck-demo/internal/domain"
)

// setupExternalLocationRepo creates the repo and seeds storage credentials
// needed to satisfy the FK constraint on external_locations.credential_name.
func setupExternalLocationRepo(t *testing.T) (*ExternalLocationRepo, *sql.DB) {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewExternalLocationRepo(writeDB), writeDB
}

// seedCredential inserts a storage credential so the FK on credential_name is satisfied.
func seedCredential(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	enc, err := crypto.NewEncryptor("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	credRepo := NewStorageCredentialRepo(db, enc)
	_, err = credRepo.Create(context.Background(), &domain.StorageCredential{
		Name:           name,
		CredentialType: domain.CredentialTypeS3,
		KeyID:          "AKID",
		Secret:         "secret",
		Endpoint:       "https://s3.amazonaws.com",
		Region:         "us-east-1",
		Owner:          "admin",
	})
	require.NoError(t, err)
}

func TestExternalLocationRepo_CRUD(t *testing.T) {
	repo, db := setupExternalLocationRepo(t)
	ctx := context.Background()
	seedCredential(t, db, "my-cred")

	loc := &domain.ExternalLocation{
		Name:           "test-location",
		URL:            "s3://my-bucket/path",
		CredentialName: "my-cred",
		StorageType:    domain.StorageTypeS3,
		Comment:        "test comment",
		Owner:          "test-owner",
		ReadOnly:       false,
	}

	t.Run("create", func(t *testing.T) {
		created, err := repo.Create(ctx, loc)
		require.NoError(t, err)
		require.NotNil(t, created)

		assert.NotEmpty(t, created.ID)
		assert.Equal(t, "test-location", created.Name)
		assert.Equal(t, "s3://my-bucket/path", created.URL)
		assert.Equal(t, "my-cred", created.CredentialName)
		assert.Equal(t, domain.StorageTypeS3, created.StorageType)
		assert.Equal(t, "test comment", created.Comment)
		assert.Equal(t, "test-owner", created.Owner)
		assert.False(t, created.ReadOnly)
		assert.False(t, created.CreatedAt.IsZero())
		assert.False(t, created.UpdatedAt.IsZero())

		loc.ID = created.ID
	})

	t.Run("get by id", func(t *testing.T) {
		fetched, err := repo.GetByID(ctx, loc.ID)
		require.NoError(t, err)
		require.NotNil(t, fetched)

		assert.Equal(t, loc.ID, fetched.ID)
		assert.Equal(t, "test-location", fetched.Name)
		assert.Equal(t, "s3://my-bucket/path", fetched.URL)
		assert.Equal(t, "my-cred", fetched.CredentialName)
		assert.Equal(t, domain.StorageTypeS3, fetched.StorageType)
		assert.Equal(t, "test comment", fetched.Comment)
		assert.Equal(t, "test-owner", fetched.Owner)
		assert.False(t, fetched.ReadOnly)
	})

	t.Run("get by name", func(t *testing.T) {
		fetched, err := repo.GetByName(ctx, "test-location")
		require.NoError(t, err)
		require.NotNil(t, fetched)

		assert.Equal(t, loc.ID, fetched.ID)
		assert.Equal(t, "test-location", fetched.Name)
		assert.Equal(t, "s3://my-bucket/path", fetched.URL)
		assert.Equal(t, "my-cred", fetched.CredentialName)
		assert.Equal(t, domain.StorageTypeS3, fetched.StorageType)
	})
}

func TestExternalLocationRepo_Update(t *testing.T) {
	repo, db := setupExternalLocationRepo(t)
	ctx := context.Background()
	seedCredential(t, db, "original-cred")

	loc := &domain.ExternalLocation{
		Name:           "update-location",
		URL:            "s3://original-bucket/path",
		CredentialName: "original-cred",
		StorageType:    domain.StorageTypeS3,
		Comment:        "original comment",
		Owner:          "original-owner",
		ReadOnly:       false,
	}

	created, err := repo.Create(ctx, loc)
	require.NoError(t, err)

	newURL := "s3://updated-bucket/new-path"
	newComment := "updated comment"

	updated, err := repo.Update(ctx, created.ID, domain.UpdateExternalLocationRequest{
		URL:     &newURL,
		Comment: &newComment,
	})
	require.NoError(t, err)
	require.NotNil(t, updated)

	assert.Equal(t, created.ID, updated.ID)
	assert.Equal(t, "update-location", updated.Name)
	assert.Equal(t, "s3://updated-bucket/new-path", updated.URL)
	assert.Equal(t, "updated comment", updated.Comment)
	assert.Equal(t, "original-cred", updated.CredentialName)
	assert.Equal(t, "original-owner", updated.Owner)
	assert.False(t, updated.ReadOnly)
	assert.True(t, updated.UpdatedAt.After(created.UpdatedAt) || updated.UpdatedAt.Equal(created.UpdatedAt))
}

func TestExternalLocationRepo_Delete(t *testing.T) {
	repo, db := setupExternalLocationRepo(t)
	ctx := context.Background()
	seedCredential(t, db, "delete-cred")

	loc := &domain.ExternalLocation{
		Name:           "delete-location",
		URL:            "s3://delete-bucket/path",
		CredentialName: "delete-cred",
		StorageType:    domain.StorageTypeS3,
		Comment:        "to be deleted",
		Owner:          "delete-owner",
		ReadOnly:       false,
	}

	created, err := repo.Create(ctx, loc)
	require.NoError(t, err)

	err = repo.Delete(ctx, created.ID)
	require.NoError(t, err)

	_, err = repo.GetByID(ctx, created.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestExternalLocationRepo_List(t *testing.T) {
	repo, db := setupExternalLocationRepo(t)
	ctx := context.Background()
	seedCredential(t, db, "cred-list")

	loc1 := &domain.ExternalLocation{
		Name:           "list-location-1",
		URL:            "s3://bucket-1/path",
		CredentialName: "cred-list",
		StorageType:    domain.StorageTypeS3,
		Comment:        "first location",
		Owner:          "owner-1",
		ReadOnly:       false,
	}
	loc2 := &domain.ExternalLocation{
		Name:           "list-location-2",
		URL:            "s3://bucket-2/path",
		CredentialName: "cred-list",
		StorageType:    domain.StorageTypeS3,
		Comment:        "second location",
		Owner:          "owner-2",
		ReadOnly:       true,
	}

	_, err := repo.Create(ctx, loc1)
	require.NoError(t, err)
	_, err = repo.Create(ctx, loc2)
	require.NoError(t, err)

	locations, count, err := repo.List(ctx, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
	assert.Len(t, locations, 2)
}

func TestExternalLocationRepo_UniqueNameConstraint(t *testing.T) {
	repo, db := setupExternalLocationRepo(t)
	ctx := context.Background()
	seedCredential(t, db, "cred-dup")

	loc1 := &domain.ExternalLocation{
		Name:           "duplicate-name",
		URL:            "s3://bucket-1/path",
		CredentialName: "cred-dup",
		StorageType:    domain.StorageTypeS3,
		Comment:        "first",
		Owner:          "owner-1",
		ReadOnly:       false,
	}
	loc2 := &domain.ExternalLocation{
		Name:           "duplicate-name",
		URL:            "s3://bucket-2/path",
		CredentialName: "cred-dup",
		StorageType:    domain.StorageTypeS3,
		Comment:        "second",
		Owner:          "owner-2",
		ReadOnly:       false,
	}

	_, err := repo.Create(ctx, loc1)
	require.NoError(t, err)

	_, err = repo.Create(ctx, loc2)
	require.Error(t, err)
	var conflict *domain.ConflictError
	assert.ErrorAs(t, err, &conflict)
}

func TestExternalLocationRepo_ReadOnlyFlag(t *testing.T) {
	repo, db := setupExternalLocationRepo(t)
	ctx := context.Background()
	seedCredential(t, db, "readonly-cred")

	loc := &domain.ExternalLocation{
		Name:           "readonly-location",
		URL:            "s3://readonly-bucket/path",
		CredentialName: "readonly-cred",
		StorageType:    domain.StorageTypeS3,
		Comment:        "read only test",
		Owner:          "readonly-owner",
		ReadOnly:       true,
	}

	created, err := repo.Create(ctx, loc)
	require.NoError(t, err)
	assert.True(t, created.ReadOnly)

	fetched, err := repo.GetByID(ctx, created.ID)
	require.NoError(t, err)
	assert.True(t, fetched.ReadOnly)

	readOnlyFalse := false
	updated, err := repo.Update(ctx, created.ID, domain.UpdateExternalLocationRequest{
		ReadOnly: &readOnlyFalse,
	})
	require.NoError(t, err)
	assert.False(t, updated.ReadOnly)

	fetched, err = repo.GetByID(ctx, created.ID)
	require.NoError(t, err)
	assert.False(t, fetched.ReadOnly)
}
