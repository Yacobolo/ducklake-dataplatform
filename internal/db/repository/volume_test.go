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

func setupVolumeRepo(t *testing.T) *VolumeRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewVolumeRepo(writeDB)
}

func TestVolume_CreateAndGet(t *testing.T) {
	repo := setupVolumeRepo(t)
	ctx := context.Background()

	vol, err := repo.Create(ctx, &domain.Volume{
		Name:            "my_volume",
		SchemaName:      "analytics",
		CatalogName:     "lake",
		VolumeType:      domain.VolumeTypeExternal,
		StorageLocation: "s3://bucket/data/volumes/my_volume",
		Comment:         "test volume",
		Owner:           "admin",
	})
	require.NoError(t, err)
	require.NotNil(t, vol)
	assert.NotEmpty(t, vol.ID)
	assert.Equal(t, "my_volume", vol.Name)
	assert.Equal(t, "analytics", vol.SchemaName)
	assert.Equal(t, "lake", vol.CatalogName)
	assert.Equal(t, domain.VolumeTypeExternal, vol.VolumeType)
	assert.Equal(t, "s3://bucket/data/volumes/my_volume", vol.StorageLocation)
	assert.Equal(t, "test volume", vol.Comment)
	assert.Equal(t, "admin", vol.Owner)
	assert.False(t, vol.CreatedAt.IsZero())
	assert.False(t, vol.UpdatedAt.IsZero())

	// GetByName
	found, err := repo.GetByName(ctx, "analytics", "my_volume")
	require.NoError(t, err)
	assert.Equal(t, vol.ID, found.ID)
	assert.Equal(t, vol.Name, found.Name)
	assert.Equal(t, vol.StorageLocation, found.StorageLocation)
}

func TestVolume_GetByName_NotFound(t *testing.T) {
	repo := setupVolumeRepo(t)
	ctx := context.Background()

	_, err := repo.GetByName(ctx, "analytics", "nonexistent")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestVolume_List(t *testing.T) {
	repo := setupVolumeRepo(t)
	ctx := context.Background()

	// Create two volumes in "analytics" and one in "other".
	for _, name := range []string{"alpha", "beta"} {
		_, err := repo.Create(ctx, &domain.Volume{
			Name:            name,
			SchemaName:      "analytics",
			CatalogName:     "lake",
			VolumeType:      domain.VolumeTypeManaged,
			StorageLocation: "s3://bucket/" + name,
			Owner:           "admin",
		})
		require.NoError(t, err)
	}
	_, err := repo.Create(ctx, &domain.Volume{
		Name:            "gamma",
		SchemaName:      "other",
		CatalogName:     "lake",
		VolumeType:      domain.VolumeTypeExternal,
		StorageLocation: "s3://bucket/gamma",
		Owner:           "admin",
	})
	require.NoError(t, err)

	// List analytics
	vols, total, err := repo.List(ctx, "analytics", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Len(t, vols, 2)
	assert.Equal(t, "alpha", vols[0].Name) // sorted by name

	// List other
	vols, total, err = repo.List(ctx, "other", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	assert.Len(t, vols, 1)
	assert.Equal(t, "gamma", vols[0].Name)

	// List empty schema
	vols, total, err = repo.List(ctx, "empty", domain.PageRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Empty(t, vols)
}

func TestVolume_Update(t *testing.T) {
	repo := setupVolumeRepo(t)
	ctx := context.Background()

	vol, err := repo.Create(ctx, &domain.Volume{
		Name:        "original",
		SchemaName:  "analytics",
		CatalogName: "lake",
		VolumeType:  domain.VolumeTypeManaged,
		Comment:     "original comment",
		Owner:       "admin",
	})
	require.NoError(t, err)

	newName := "renamed"
	newComment := "updated comment"
	updated, err := repo.Update(ctx, vol.ID, domain.UpdateVolumeRequest{
		NewName: &newName,
		Comment: &newComment,
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", updated.Name)
	assert.Equal(t, "updated comment", updated.Comment)
	assert.Equal(t, "admin", updated.Owner) // unchanged

	// Verify via GetByName with new name
	found, err := repo.GetByName(ctx, "analytics", "renamed")
	require.NoError(t, err)
	assert.Equal(t, updated.ID, found.ID)
}

func TestVolume_Update_PartialFields(t *testing.T) {
	repo := setupVolumeRepo(t)
	ctx := context.Background()

	vol, err := repo.Create(ctx, &domain.Volume{
		Name:        "vol1",
		SchemaName:  "analytics",
		CatalogName: "lake",
		VolumeType:  domain.VolumeTypeManaged,
		Comment:     "initial",
		Owner:       "admin",
	})
	require.NoError(t, err)

	// Only update owner
	newOwner := "user2"
	updated, err := repo.Update(ctx, vol.ID, domain.UpdateVolumeRequest{
		Owner: &newOwner,
	})
	require.NoError(t, err)
	assert.Equal(t, "vol1", updated.Name)       // unchanged
	assert.Equal(t, "initial", updated.Comment) // unchanged
	assert.Equal(t, "user2", updated.Owner)     // changed
}

func TestVolume_Delete(t *testing.T) {
	repo := setupVolumeRepo(t)
	ctx := context.Background()

	vol, err := repo.Create(ctx, &domain.Volume{
		Name:        "to_delete",
		SchemaName:  "analytics",
		CatalogName: "lake",
		VolumeType:  domain.VolumeTypeManaged,
		Owner:       "admin",
	})
	require.NoError(t, err)

	err = repo.Delete(ctx, vol.ID)
	require.NoError(t, err)

	_, err = repo.GetByName(ctx, "analytics", "to_delete")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestVolume_UniqueConstraint(t *testing.T) {
	repo := setupVolumeRepo(t)
	ctx := context.Background()

	_, err := repo.Create(ctx, &domain.Volume{
		Name:        "dup",
		SchemaName:  "analytics",
		CatalogName: "lake",
		VolumeType:  domain.VolumeTypeManaged,
		Owner:       "admin",
	})
	require.NoError(t, err)

	// Duplicate name in same schema should fail
	_, err = repo.Create(ctx, &domain.Volume{
		Name:            "dup",
		SchemaName:      "analytics",
		CatalogName:     "lake",
		VolumeType:      domain.VolumeTypeExternal,
		StorageLocation: "s3://bucket/dup",
		Owner:           "admin",
	})
	require.Error(t, err)
	var conflict *domain.ConflictError
	assert.ErrorAs(t, err, &conflict)
}

func TestVolume_SameNameDifferentSchema(t *testing.T) {
	repo := setupVolumeRepo(t)
	ctx := context.Background()

	_, err := repo.Create(ctx, &domain.Volume{
		Name:        "shared_name",
		SchemaName:  "schema_a",
		CatalogName: "lake",
		VolumeType:  domain.VolumeTypeManaged,
		Owner:       "admin",
	})
	require.NoError(t, err)

	// Same name in different schema should succeed
	_, err = repo.Create(ctx, &domain.Volume{
		Name:        "shared_name",
		SchemaName:  "schema_b",
		CatalogName: "lake",
		VolumeType:  domain.VolumeTypeManaged,
		Owner:       "admin",
	})
	require.NoError(t, err)
}
