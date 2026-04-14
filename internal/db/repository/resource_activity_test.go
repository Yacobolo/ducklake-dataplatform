package repository

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	dbpkg "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceAccessRepo_TrackVisitAppendsEventsAndKeepsLatestProjection(t *testing.T) {
	writeDB, _ := dbpkg.OpenTestSQLite(t)
	principal := createResourceTestPrincipal(t, writeDB, "alice")
	repo := NewResourceAccessRepo(writeDB)

	ctx := context.Background()
	err := repo.TrackVisit(ctx, principal.ID, domain.ResourceRef{
		ResourceType: "product",
		ResourceKey:  "orders",
		DisplayName:  "Orders",
		Section:      "Discover",
	})
	require.NoError(t, err)

	err = repo.TrackVisit(ctx, principal.ID, domain.ResourceRef{
		ResourceType: "product",
		ResourceKey:  "orders",
		DisplayName:  "Orders v2",
		Section:      "Discover",
	})
	require.NoError(t, err)

	items, err := repo.ListRecent(ctx, principal.ID, 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "Orders v2", items[0].DisplayName)
	assert.False(t, items[0].AccessedAt.IsZero())

	var count int
	require.NoError(t, writeDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM resource_access_events WHERE principal_id = ?`, principal.ID).Scan(&count))
	assert.Equal(t, 2, count)
}

func TestResourceAccessRepo_ListRecentAllowsNullResourcePath(t *testing.T) {
	writeDB, _ := dbpkg.OpenTestSQLite(t)
	principal := createResourceTestPrincipal(t, writeDB, "alice")
	repo := NewResourceAccessRepo(writeDB)

	ctx := context.Background()
	_, err := writeDB.ExecContext(ctx, `
		INSERT INTO resource_access_events (
			id, principal_id, resource_type, resource_key, display_name, resource_path, section, accessed_at
		) VALUES (?, ?, ?, ?, ?, NULL, ?, CURRENT_TIMESTAMP)
	`, newID(), principal.ID, "notebook", "019d43e3-9377-79f6-a368-01b6ae805b7b", "hi", "Build")
	require.NoError(t, err)

	items, err := repo.ListRecent(ctx, principal.ID, 10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Empty(t, items[0].ResourcePath)
}

func TestSavedResourceRepo_SaveAndUnsaveReuseSameRow(t *testing.T) {
	writeDB, _ := dbpkg.OpenTestSQLite(t)
	principal := createResourceTestPrincipal(t, writeDB, "alice")
	accessRepo := NewResourceAccessRepo(writeDB)
	savedRepo := NewSavedResourceRepo(writeDB)

	ctx := context.Background()
	resource := domain.ResourceRef{
		ResourceType: "model",
		ResourceKey:  "project/orders",
		DisplayName:  "project.orders",
		Section:      "Build",
	}

	require.NoError(t, accessRepo.TrackVisit(ctx, principal.ID, resource))
	require.NoError(t, savedRepo.Save(ctx, principal.ID, resource))

	saved, err := savedRepo.ListSaved(ctx, principal.ID, 10)
	require.NoError(t, err)
	require.Len(t, saved, 1)
	assert.Equal(t, resource.ResourceKey, saved[0].ResourceKey)
	assert.False(t, saved[0].SavedAt.IsZero())
	assert.NotNil(t, saved[0].LastAccessedAt)

	require.NoError(t, savedRepo.Unsave(ctx, principal.ID, resource.ResourceType, resource.ResourceKey))

	saved, err = savedRepo.ListSaved(ctx, principal.ID, 10)
	require.NoError(t, err)
	assert.Empty(t, saved)

	recent, err := accessRepo.ListRecent(ctx, principal.ID, 10)
	require.NoError(t, err)
	require.Len(t, recent, 1)
	assert.Equal(t, resource.ResourceKey, recent[0].ResourceKey)
}

func TestResourceAccessAndSavedResourceRepos_ListOrdering(t *testing.T) {
	writeDB, _ := dbpkg.OpenTestSQLite(t)
	principal := createResourceTestPrincipal(t, writeDB, "alice")
	accessRepo := NewResourceAccessRepo(writeDB)
	savedRepo := NewSavedResourceRepo(writeDB)

	ctx := context.Background()
	savedOnly := domain.ResourceRef{
		ResourceType: "product",
		ResourceKey:  "important",
		DisplayName:  "Important product",
		Section:      "Discover",
	}
	require.NoError(t, savedRepo.Save(ctx, principal.ID, savedOnly))

	for i := 0; i < 55; i++ {
		require.NoError(t, accessRepo.TrackVisit(ctx, principal.ID, domain.ResourceRef{
			ResourceType: "product",
			ResourceKey:  fmt.Sprintf("item-%02d", i),
			DisplayName:  fmt.Sprintf("Item %02d", i),
			Section:      "Discover",
		}))
	}

	recent, err := accessRepo.ListRecent(ctx, principal.ID, 100)
	require.NoError(t, err)
	require.Len(t, recent, 55)
	assert.Equal(t, "item-54", recent[0].ResourceKey)
	assert.Equal(t, "item-00", recent[len(recent)-1].ResourceKey)

	saved, err := savedRepo.ListSaved(ctx, principal.ID, 10)
	require.NoError(t, err)
	require.Len(t, saved, 1)
	assert.Equal(t, savedOnly.ResourceKey, saved[0].ResourceKey)

	var eventsCount int
	require.NoError(t, writeDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM resource_access_events WHERE principal_id = ?
	`, principal.ID).Scan(&eventsCount))
	assert.Equal(t, 55, eventsCount)
}

func createResourceTestPrincipal(t *testing.T, db *sql.DB, name string) *domain.Principal {
	t.Helper()

	repo := NewPrincipalRepo(db)
	principal, err := repo.Create(context.Background(), &domain.Principal{
		Name: name,
		Type: "user",
	})
	require.NoError(t, err)
	return principal
}
