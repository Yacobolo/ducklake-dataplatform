package repository

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

func setupCatalogRegistrationRepo(t *testing.T) *CatalogRegistrationRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewCatalogRegistrationRepo(writeDB)
}

func TestCatalogRegistrationRepo_CRUD(t *testing.T) {
	repo := setupCatalogRegistrationRepo(t)
	ctx := context.Background()

	cat, err := repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "test-catalog",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/test.db",
		DataPath:      "/tmp/data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
		Comment:       "a test catalog",
	})
	require.NoError(t, err)
	require.NotNil(t, cat)

	t.Run("fields set correctly after create", func(t *testing.T) {
		assert.NotEmpty(t, cat.ID)
		assert.Equal(t, "test-catalog", cat.Name)
		assert.Equal(t, domain.MetastoreTypeSQLite, cat.MetastoreType)
		assert.Equal(t, "/tmp/test.db", cat.DSN)
		assert.Equal(t, "/tmp/data", cat.DataPath)
		assert.Equal(t, domain.CatalogStatusActive, cat.Status)
		assert.False(t, cat.IsDefault)
		assert.Equal(t, "a test catalog", cat.Comment)
		assert.False(t, cat.CreatedAt.IsZero())
		assert.False(t, cat.UpdatedAt.IsZero())
	})

	t.Run("GetByID returns catalog", func(t *testing.T) {
		found, err := repo.GetByID(ctx, cat.ID)
		require.NoError(t, err)
		assert.Equal(t, cat.ID, found.ID)
		assert.Equal(t, "test-catalog", found.Name)
		assert.Equal(t, domain.MetastoreTypeSQLite, found.MetastoreType)
		assert.Equal(t, "/tmp/test.db", found.DSN)
		assert.Equal(t, "/tmp/data", found.DataPath)
		assert.Equal(t, domain.CatalogStatusActive, found.Status)
		assert.False(t, found.IsDefault)
		assert.Equal(t, "a test catalog", found.Comment)
	})

	t.Run("GetByName returns catalog", func(t *testing.T) {
		found, err := repo.GetByName(ctx, "test-catalog")
		require.NoError(t, err)
		assert.Equal(t, cat.ID, found.ID)
		assert.Equal(t, "test-catalog", found.Name)
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

func TestCatalogRegistrationRepo_Update(t *testing.T) {
	repo := setupCatalogRegistrationRepo(t)
	ctx := context.Background()

	cat, err := repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "update-catalog",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/update.db",
		DataPath:      "/tmp/update-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
		Comment:       "original comment",
	})
	require.NoError(t, err)

	newComment := "updated comment"
	updated, err := repo.Update(ctx, cat.ID, domain.UpdateCatalogRegistrationRequest{
		Comment: &newComment,
	})
	require.NoError(t, err)
	require.NotNil(t, updated)

	t.Run("updated field changed", func(t *testing.T) {
		assert.Equal(t, "updated comment", updated.Comment)
	})

	t.Run("non-updated fields unchanged", func(t *testing.T) {
		assert.Equal(t, cat.ID, updated.ID)
		assert.Equal(t, "update-catalog", updated.Name)
		assert.Equal(t, domain.MetastoreTypeSQLite, updated.MetastoreType)
	})
}

func TestCatalogRegistrationRepo_Delete(t *testing.T) {
	repo := setupCatalogRegistrationRepo(t)
	ctx := context.Background()

	cat, err := repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "delete-catalog",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/delete.db",
		DataPath:      "/tmp/delete-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
	})
	require.NoError(t, err)

	err = repo.Delete(ctx, cat.ID)
	require.NoError(t, err)

	_, err = repo.GetByID(ctx, cat.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestCatalogRegistrationRepo_List(t *testing.T) {
	repo := setupCatalogRegistrationRepo(t)
	ctx := context.Background()

	_, err := repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "catalog-alpha",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/alpha.db",
		DataPath:      "/tmp/alpha-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
	})
	require.NoError(t, err)

	_, err = repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "catalog-beta",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/beta.db",
		DataPath:      "/tmp/beta-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
	})
	require.NoError(t, err)

	catalogs, total, err := repo.List(ctx, domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, catalogs, 2)
}

func TestCatalogRegistrationRepo_UniqueNameConstraint(t *testing.T) {
	repo := setupCatalogRegistrationRepo(t)
	ctx := context.Background()

	_, err := repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "duplicate-name",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/dup1.db",
		DataPath:      "/tmp/dup1-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
	})
	require.NoError(t, err)

	_, err = repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "duplicate-name",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/dup2.db",
		DataPath:      "/tmp/dup2-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
	})
	require.Error(t, err)
	var conflict *domain.ConflictError
	assert.ErrorAs(t, err, &conflict)
}

func TestCatalogRegistrationRepo_UpdateStatus(t *testing.T) {
	repo := setupCatalogRegistrationRepo(t)
	ctx := context.Background()

	cat, err := repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "status-catalog",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/status.db",
		DataPath:      "/tmp/status-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
	})
	require.NoError(t, err)

	err = repo.UpdateStatus(ctx, cat.ID, domain.CatalogStatusError, "connection failed")
	require.NoError(t, err)

	found, err := repo.GetByID(ctx, cat.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.CatalogStatusError, found.Status)
	assert.Equal(t, "connection failed", found.StatusMessage)
}

func TestCatalogRegistrationRepo_SetDefault(t *testing.T) {
	repo := setupCatalogRegistrationRepo(t)
	ctx := context.Background()

	t.Run("GetDefault returns NotFoundError when no default set", func(t *testing.T) {
		_, err := repo.GetDefault(ctx)
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	cat1, err := repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "default-catalog-1",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/default1.db",
		DataPath:      "/tmp/default1-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
	})
	require.NoError(t, err)

	cat2, err := repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "default-catalog-2",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/default2.db",
		DataPath:      "/tmp/default2-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
	})
	require.NoError(t, err)

	t.Run("SetDefault on first catalog", func(t *testing.T) {
		err := repo.SetDefault(ctx, cat1.ID)
		require.NoError(t, err)

		def, err := repo.GetDefault(ctx)
		require.NoError(t, err)
		assert.Equal(t, cat1.ID, def.ID)
		assert.True(t, def.IsDefault)
	})

	t.Run("SetDefault on second catalog clears previous", func(t *testing.T) {
		err := repo.SetDefault(ctx, cat2.ID)
		require.NoError(t, err)

		def, err := repo.GetDefault(ctx)
		require.NoError(t, err)
		assert.Equal(t, cat2.ID, def.ID)
		assert.True(t, def.IsDefault)

		// Verify the first catalog is no longer default
		first, err := repo.GetByID(ctx, cat1.ID)
		require.NoError(t, err)
		assert.False(t, first.IsDefault)
	})
}

func TestCatalogRegistrationRepo_SetDefault_Atomic(t *testing.T) {
	repo := setupCatalogRegistrationRepo(t)
	ctx := context.Background()

	cat1, err := repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "atomic-default-1",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/atomic1.db",
		DataPath:      "/tmp/atomic1-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
	})
	require.NoError(t, err)

	cat2, err := repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "atomic-default-2",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/atomic2.db",
		DataPath:      "/tmp/atomic2-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
	})
	require.NoError(t, err)

	cat3, err := repo.Create(ctx, &domain.CatalogRegistration{
		Name:          "atomic-default-3",
		MetastoreType: domain.MetastoreTypeSQLite,
		DSN:           "/tmp/atomic3.db",
		DataPath:      "/tmp/atomic3-data",
		Status:        domain.CatalogStatusActive,
		IsDefault:     false,
	})
	require.NoError(t, err)

	t.Run("exactly one default after rapid succession", func(t *testing.T) {
		// Rapidly set different catalogs as default — the transaction
		// ensures clear + set happen atomically, so we always end up
		// with exactly one default.
		require.NoError(t, repo.SetDefault(ctx, cat1.ID))
		require.NoError(t, repo.SetDefault(ctx, cat2.ID))
		require.NoError(t, repo.SetDefault(ctx, cat3.ID))

		all, _, err := repo.List(ctx, domain.PageRequest{})
		require.NoError(t, err)

		defaultCount := 0
		var defaultID string
		for _, c := range all {
			if c.IsDefault {
				defaultCount++
				defaultID = c.ID
			}
		}
		assert.Equal(t, 1, defaultCount, "exactly one catalog should be default")
		assert.Equal(t, cat3.ID, defaultID, "last SetDefault call should win")
	})

	t.Run("SetDefault is idempotent", func(t *testing.T) {
		// Setting the same catalog twice should still leave exactly one default
		require.NoError(t, repo.SetDefault(ctx, cat1.ID))
		require.NoError(t, repo.SetDefault(ctx, cat1.ID))

		all, _, err := repo.List(ctx, domain.PageRequest{})
		require.NoError(t, err)

		defaultCount := 0
		for _, c := range all {
			if c.IsDefault {
				defaultCount++
			}
		}
		assert.Equal(t, 1, defaultCount, "exactly one catalog should be default")

		def, err := repo.GetDefault(ctx)
		require.NoError(t, err)
		assert.Equal(t, cat1.ID, def.ID)
	})
}
