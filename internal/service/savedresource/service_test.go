package savedresource

import (
	"context"
	"testing"
	"time"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_IsolatesSavedStateByPrincipal(t *testing.T) {
	t.Parallel()

	repo := newMemoryRepo()
	service := NewService(repo)
	alice := domain.ContextPrincipal{ID: "alice-id", Name: "alice", Type: "user"}
	bob := domain.ContextPrincipal{ID: "bob-id", Name: "bob", Type: "user"}

	require.NoError(t, service.Save(context.Background(), alice, domain.ResourceRef{
		ResourceType: "notebook",
		ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7b",
		DisplayName:  "alice-notebook",
		Section:      "Build",
	}))

	aliceSaved, err := service.ListSaved(context.Background(), alice, 10)
	require.NoError(t, err)
	require.Len(t, aliceSaved, 1)
	assert.Equal(t, "/ui/notebooks/019d43e3-9377-79f6-a368-01b6ae805b7b", aliceSaved[0].Href)

	bobSaved, err := service.ListSaved(context.Background(), bob, 10)
	require.NoError(t, err)
	assert.Empty(t, bobSaved)
}

func TestService_IgnoresNonUUIDSavedEntriesAndRejectsSavingThem(t *testing.T) {
	t.Parallel()

	repo := newMemoryRepo()
	repo.saved["alice-id"] = []domain.SavedResource{
		{
			ResourceRef: domain.ResourceRef{
				ResourceType: "workspace",
				ResourceKey:  "explore",
				DisplayName:  "Explore",
			},
			SavedAt: time.Now().UTC(),
		},
		{
			ResourceRef: domain.ResourceRef{
				ResourceType: "notebook",
				ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7c",
				DisplayName:  "uuid-notebook",
			},
			SavedAt: time.Now().UTC(),
		},
	}
	service := NewService(repo)
	principal := domain.ContextPrincipal{ID: "alice-id", Name: "alice", Type: "user"}

	saved, err := service.ListSaved(context.Background(), principal, 10)
	require.NoError(t, err)
	require.Len(t, saved, 1)
	assert.Equal(t, "019d43e3-9377-79f6-a368-01b6ae805b7c", saved[0].ResourceKey)

	err = service.Save(context.Background(), principal, domain.ResourceRef{
		ResourceType: "product",
		ResourceKey:  "orders",
		DisplayName:  "Orders",
	})
	require.Error(t, err)
}

type memoryRepo struct {
	saved map[string][]domain.SavedResource
}

func newMemoryRepo() *memoryRepo {
	return &memoryRepo{
		saved: map[string][]domain.SavedResource{},
	}
}

func (m *memoryRepo) Save(_ context.Context, principalID string, resource domain.ResourceRef) error {
	m.saved[principalID] = []domain.SavedResource{{
		ResourceRef: resource,
		SavedAt:     time.Now().UTC(),
	}}
	return nil
}

func (m *memoryRepo) Unsave(_ context.Context, principalID string, resourceType string, resourceKey string) error {
	items := m.saved[principalID]
	filtered := make([]domain.SavedResource, 0, len(items))
	for i := range items {
		if items[i].ResourceType == resourceType && items[i].ResourceKey == resourceKey {
			continue
		}
		filtered = append(filtered, items[i])
	}
	m.saved[principalID] = filtered
	return nil
}

func (m *memoryRepo) ListSaved(_ context.Context, principalID string, limit int) ([]domain.SavedResource, error) {
	items := m.saved[principalID]
	if limit <= 0 || len(items) <= limit {
		return append([]domain.SavedResource(nil), items...), nil
	}
	return append([]domain.SavedResource(nil), items[:limit]...), nil
}
