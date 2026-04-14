package resourceref

import (
	"testing"

	"github.com/Yacobolo/quackstack/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeHref_StripsQueryAndFragment(t *testing.T) {
	t.Parallel()

	href, err := NormalizeHref("/ui/products/orders?tab=health#section")
	require.NoError(t, err)
	assert.Equal(t, "/ui/products/orders", href)
}

func TestNormalizeHref_RejectsExcludedPaths(t *testing.T) {
	t.Parallel()

	cases := []string{
		"/ui/components",
		"/ui/login",
		"/ui/resources/save",
		"/ui/products/new",
		"/outside",
	}

	for _, href := range cases {
		href := href
		t.Run(href, func(t *testing.T) {
			t.Parallel()
			_, err := NormalizeHref(href)
			require.Error(t, err)
		})
	}
}

func TestNormalizeHref_AllowsHomeReturnPath(t *testing.T) {
	t.Parallel()

	href, err := NormalizeHref("/ui")
	require.NoError(t, err)
	assert.Equal(t, "/ui", href)
}

func TestIsRecentResource_RequiresUUIDBackedNonWorkspaceResource(t *testing.T) {
	t.Parallel()

	assert.True(t, IsRecentResource(domain.ResourceRef{
		ResourceType: "notebook",
		ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7b",
	}))
	assert.False(t, IsRecentResource(domain.ResourceRef{
		ResourceType: "workspace",
		ResourceKey:  "explore",
	}))
	assert.False(t, IsRecentResource(domain.ResourceRef{
		ResourceType: "product",
		ResourceKey:  "orders",
	}))
}

func TestHydrateRecent_SkipsLegacyNonResourceRows(t *testing.T) {
	t.Parallel()

	items, err := HydrateRecent([]domain.ResourceAccessEvent{
		{
			ResourceRef: domain.ResourceRef{
				ResourceType: "workspace",
				ResourceKey:  "explore",
				DisplayName:  "Explore",
			},
		},
		{
			ResourceRef: domain.ResourceRef{
				ResourceType: "notebook",
				ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7b",
				DisplayName:  "hi",
				ResourcePath: "Home/",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "notebook", items[0].ResourceType)
	assert.Equal(t, "Home/", items[0].ResourcePath)
}

func TestHydrateSaved_SkipsLegacyNonResourceRows(t *testing.T) {
	t.Parallel()

	items, err := HydrateSaved([]domain.SavedResource{
		{
			ResourceRef: domain.ResourceRef{
				ResourceType: "workspace",
				ResourceKey:  "models",
				DisplayName:  "Models",
			},
		},
		{
			ResourceRef: domain.ResourceRef{
				ResourceType: "dashboard",
				ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7b",
				DisplayName:  "Q1 exec",
				ResourcePath: "Finance/Quarterly/",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "dashboard", items[0].ResourceType)
	assert.Equal(t, "Finance/Quarterly/", items[0].ResourcePath)
}
