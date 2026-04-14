package resourceaccess

import (
	"context"
	"testing"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_IsolatesRecentStateByPrincipal(t *testing.T) {
	t.Parallel()

	repo := newMemoryRepo()
	service := NewService(repo)
	alice := domain.ContextPrincipal{ID: "alice-id", Name: "alice", Type: "user"}
	bob := domain.ContextPrincipal{ID: "bob-id", Name: "bob", Type: "user"}

	require.NoError(t, service.TrackVisit(context.Background(), alice, domain.ResourceRef{
		ResourceType: "notebook",
		ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7b",
		DisplayName:  "alice-notebook",
		Section:      "Build",
	}))
	require.NoError(t, service.TrackVisit(context.Background(), bob, domain.ResourceRef{
		ResourceType: "notebook",
		ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7c",
		DisplayName:  "bob-notebook",
		Section:      "Build",
	}))

	aliceRecent, err := service.ListRecent(context.Background(), alice, 10)
	require.NoError(t, err)
	require.Len(t, aliceRecent, 1)
	assert.Equal(t, "/ui/notebooks/019d43e3-9377-79f6-a368-01b6ae805b7b", aliceRecent[0].Href)
	assert.Equal(t, "alice-notebook", aliceRecent[0].DisplayName)

	bobRecent, err := service.ListRecent(context.Background(), bob, 10)
	require.NoError(t, err)
	require.Len(t, bobRecent, 1)
	assert.Equal(t, "/ui/notebooks/019d43e3-9377-79f6-a368-01b6ae805b7c", bobRecent[0].Href)
}

func TestService_IgnoresNonUUIDAndWorkspaceVisits(t *testing.T) {
	t.Parallel()

	repo := newMemoryRepo()
	service := NewService(repo)
	principal := domain.ContextPrincipal{ID: "alice-id", Name: "alice", Type: "user"}

	require.NoError(t, service.TrackVisit(context.Background(), principal, domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "explore",
		DisplayName:  "Explore",
		Section:      "Discover",
	}))
	require.NoError(t, service.TrackVisit(context.Background(), principal, domain.ResourceRef{
		ResourceType: "product",
		ResourceKey:  "orders",
		DisplayName:  "Orders",
		Section:      "Discover",
	}))
	require.NoError(t, service.TrackVisit(context.Background(), principal, domain.ResourceRef{
		ResourceType: "notebook",
		ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7b",
		DisplayName:  "hi",
		Section:      "Build",
	}))

	recent, err := service.ListRecent(context.Background(), principal, 10)
	require.NoError(t, err)
	require.Len(t, recent, 1)
	assert.Equal(t, "notebook", recent[0].ResourceType)
	assert.Equal(t, "019d43e3-9377-79f6-a368-01b6ae805b7b", recent[0].ResourceKey)
}

type memoryRepo struct {
	recent map[string][]domain.ResourceAccessEvent
}

func newMemoryRepo() *memoryRepo {
	return &memoryRepo{
		recent: map[string][]domain.ResourceAccessEvent{},
	}
}

func (m *memoryRepo) TrackVisit(_ context.Context, principalID string, resource domain.ResourceRef) error {
	m.recent[principalID] = append([]domain.ResourceAccessEvent{{
		ResourceRef: resource,
		AccessedAt:  time.Now().UTC(),
	}}, m.recent[principalID]...)
	return nil
}

func (m *memoryRepo) ListRecent(_ context.Context, principalID string, limit int) ([]domain.ResourceAccessEvent, error) {
	items := m.recent[principalID]
	if limit <= 0 || len(items) <= limit {
		return append([]domain.ResourceAccessEvent(nil), items...), nil
	}
	return append([]domain.ResourceAccessEvent(nil), items[:limit]...), nil
}
