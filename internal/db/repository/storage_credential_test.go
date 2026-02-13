package repository

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/crypto"
	"duck-demo/internal/domain"
)

func setupStorageCredentialRepo(t *testing.T) *StorageCredentialRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	enc, err := crypto.NewEncryptor("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	return NewStorageCredentialRepo(writeDB, enc)
}

func TestStorageCredentialRepo_CRUD(t *testing.T) {
	repo := setupStorageCredentialRepo(t)
	ctx := context.Background()

	cred, err := repo.Create(ctx, &domain.StorageCredential{
		Name:           "my-s3-cred",
		CredentialType: domain.CredentialTypeS3,
		KeyID:          "AKIAIOSFODNN7EXAMPLE",
		Secret:         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Endpoint:       "https://s3.amazonaws.com",
		Region:         "us-east-1",
		URLStyle:       "path",
		Comment:        "test S3 credential",
		Owner:          "admin",
	})
	require.NoError(t, err)
	require.NotNil(t, cred)

	t.Run("fields set correctly after create", func(t *testing.T) {
		assert.NotEmpty(t, cred.ID)
		assert.Equal(t, "my-s3-cred", cred.Name)
		assert.Equal(t, domain.CredentialTypeS3, cred.CredentialType)
		assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", cred.KeyID)
		assert.Equal(t, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", cred.Secret)
		assert.Equal(t, "https://s3.amazonaws.com", cred.Endpoint)
		assert.Equal(t, "us-east-1", cred.Region)
		assert.Equal(t, "path", cred.URLStyle)
		assert.Equal(t, "test S3 credential", cred.Comment)
		assert.Equal(t, "admin", cred.Owner)
		assert.False(t, cred.CreatedAt.IsZero())
		assert.False(t, cred.UpdatedAt.IsZero())
	})

	t.Run("GetByID returns decrypted credential", func(t *testing.T) {
		found, err := repo.GetByID(ctx, cred.ID)
		require.NoError(t, err)
		assert.Equal(t, cred.ID, found.ID)
		assert.Equal(t, "my-s3-cred", found.Name)
		assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", found.KeyID)
		assert.Equal(t, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", found.Secret)
		assert.Equal(t, "us-east-1", found.Region)
	})

	t.Run("GetByName returns decrypted credential", func(t *testing.T) {
		found, err := repo.GetByName(ctx, "my-s3-cred")
		require.NoError(t, err)
		assert.Equal(t, cred.ID, found.ID)
		assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", found.KeyID)
		assert.Equal(t, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", found.Secret)
	})

	t.Run("GetByID not found", func(t *testing.T) {
		_, err := repo.GetByID(ctx, "nonexistent-id")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("GetByName not found", func(t *testing.T) {
		_, err := repo.GetByName(ctx, "nonexistent-name")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

func TestStorageCredentialRepo_Update(t *testing.T) {
	repo := setupStorageCredentialRepo(t)
	ctx := context.Background()

	cred, err := repo.Create(ctx, &domain.StorageCredential{
		Name:           "update-cred",
		CredentialType: domain.CredentialTypeS3,
		KeyID:          "AKIAIOSFODNN7EXAMPLE",
		Secret:         "originalSecret",
		Endpoint:       "https://s3.amazonaws.com",
		Region:         "us-east-1",
		URLStyle:       "path",
		Comment:        "original comment",
		Owner:          "admin",
	})
	require.NoError(t, err)

	newSecret := "updatedSecret"
	newRegion := "eu-west-1"
	updated, err := repo.Update(ctx, cred.ID, domain.UpdateStorageCredentialRequest{
		Secret: &newSecret,
		Region: &newRegion,
	})
	require.NoError(t, err)
	require.NotNil(t, updated)

	t.Run("updated fields changed", func(t *testing.T) {
		assert.Equal(t, "updatedSecret", updated.Secret)
		assert.Equal(t, "eu-west-1", updated.Region)
	})

	t.Run("non-updated fields unchanged", func(t *testing.T) {
		assert.Equal(t, cred.ID, updated.ID)
		assert.Equal(t, "update-cred", updated.Name)
		assert.Equal(t, domain.CredentialTypeS3, updated.CredentialType)
		assert.Equal(t, "AKIAIOSFODNN7EXAMPLE", updated.KeyID)
		assert.Equal(t, "https://s3.amazonaws.com", updated.Endpoint)
		assert.Equal(t, "path", updated.URLStyle)
		assert.Equal(t, "original comment", updated.Comment)
		assert.Equal(t, "admin", updated.Owner)
	})

	t.Run("update not found", func(t *testing.T) {
		s := "val"
		_, err := repo.Update(ctx, "nonexistent-id", domain.UpdateStorageCredentialRequest{
			Secret: &s,
		})
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

func TestStorageCredentialRepo_Delete(t *testing.T) {
	repo := setupStorageCredentialRepo(t)
	ctx := context.Background()

	cred, err := repo.Create(ctx, &domain.StorageCredential{
		Name:           "delete-cred",
		CredentialType: domain.CredentialTypeS3,
		KeyID:          "AKIAIOSFODNN7EXAMPLE",
		Secret:         "secret",
		Endpoint:       "https://s3.amazonaws.com",
		Region:         "us-east-1",
		URLStyle:       "path",
		Owner:          "admin",
	})
	require.NoError(t, err)

	err = repo.Delete(ctx, cred.ID)
	require.NoError(t, err)

	_, err = repo.GetByID(ctx, cred.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestStorageCredentialRepo_List(t *testing.T) {
	repo := setupStorageCredentialRepo(t)
	ctx := context.Background()

	_, err := repo.Create(ctx, &domain.StorageCredential{
		Name:           "cred-alpha",
		CredentialType: domain.CredentialTypeS3,
		KeyID:          "key1",
		Secret:         "secret1",
		Endpoint:       "https://s3.amazonaws.com",
		Region:         "us-east-1",
		URLStyle:       "path",
		Owner:          "admin",
	})
	require.NoError(t, err)

	_, err = repo.Create(ctx, &domain.StorageCredential{
		Name:           "cred-beta",
		CredentialType: domain.CredentialTypeS3,
		KeyID:          "key2",
		Secret:         "secret2",
		Endpoint:       "https://s3.eu-west-1.amazonaws.com",
		Region:         "eu-west-1",
		URLStyle:       "vhost",
		Owner:          "admin",
	})
	require.NoError(t, err)

	creds, total, err := repo.List(ctx, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, creds, 2)
}

func TestStorageCredentialRepo_UniqueNameConstraint(t *testing.T) {
	repo := setupStorageCredentialRepo(t)
	ctx := context.Background()

	_, err := repo.Create(ctx, &domain.StorageCredential{
		Name:           "duplicate-name",
		CredentialType: domain.CredentialTypeS3,
		KeyID:          "key1",
		Secret:         "secret1",
		Endpoint:       "https://s3.amazonaws.com",
		Region:         "us-east-1",
		URLStyle:       "path",
		Owner:          "admin",
	})
	require.NoError(t, err)

	_, err = repo.Create(ctx, &domain.StorageCredential{
		Name:           "duplicate-name",
		CredentialType: domain.CredentialTypeS3,
		KeyID:          "key2",
		Secret:         "secret2",
		Endpoint:       "https://s3.eu-west-1.amazonaws.com",
		Region:         "eu-west-1",
		URLStyle:       "vhost",
		Owner:          "user2",
	})
	require.Error(t, err)
	var conflict *domain.ConflictError
	assert.ErrorAs(t, err, &conflict)
}

func TestStorageCredentialRepo_EncryptionRoundTrip(t *testing.T) {
	repo := setupStorageCredentialRepo(t)
	ctx := context.Background()

	plainKeyID := "AKIAIOSFODNN7EXAMPLE"
	plainSecret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	plainAzureAccountKey := "azure-account-key-value"
	plainAzureClientSecret := "azure-client-secret-value"

	cred, err := repo.Create(ctx, &domain.StorageCredential{
		Name:              "encryption-test",
		CredentialType:    domain.CredentialTypeS3,
		KeyID:             plainKeyID,
		Secret:            plainSecret,
		Endpoint:          "https://s3.amazonaws.com",
		Region:            "us-east-1",
		URLStyle:          "path",
		AzureAccountKey:   plainAzureAccountKey,
		AzureClientSecret: plainAzureClientSecret,
		Owner:             "admin",
	})
	require.NoError(t, err)

	// Verify returned values from Create match plaintext
	assert.Equal(t, plainKeyID, cred.KeyID)
	assert.Equal(t, plainSecret, cred.Secret)
	assert.Equal(t, plainAzureAccountKey, cred.AzureAccountKey)
	assert.Equal(t, plainAzureClientSecret, cred.AzureClientSecret)

	// Verify values from GetByID match plaintext (proves full round-trip)
	fetched, err := repo.GetByID(ctx, cred.ID)
	require.NoError(t, err)
	assert.Equal(t, plainKeyID, fetched.KeyID)
	assert.Equal(t, plainSecret, fetched.Secret)
	assert.Equal(t, plainAzureAccountKey, fetched.AzureAccountKey)
	assert.Equal(t, plainAzureClientSecret, fetched.AzureClientSecret)

	// Verify values from GetByName also match
	byName, err := repo.GetByName(ctx, "encryption-test")
	require.NoError(t, err)
	assert.Equal(t, plainKeyID, byName.KeyID)
	assert.Equal(t, plainSecret, byName.Secret)
	assert.Equal(t, plainAzureAccountKey, byName.AzureAccountKey)
	assert.Equal(t, plainAzureClientSecret, byName.AzureClientSecret)
}
