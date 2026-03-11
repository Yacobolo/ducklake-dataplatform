package policy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestRequireAdmin(t *testing.T) {
	t.Parallel()

	t.Run("missing principal denied", func(t *testing.T) {
		t.Parallel()

		err := RequireAdmin(context.Background())
		require.Error(t, err)
		assert.EqualError(t, err, "authentication required")
	})

	t.Run("non admin denied", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		err := RequireAdmin(ctx)
		require.Error(t, err)
		assert.EqualError(t, err, "admin privileges required")
	})

	t.Run("admin allowed", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
		require.NoError(t, RequireAdmin(ctx))
	})
}

func TestRequireAdminForAction(t *testing.T) {
	t.Parallel()

	t.Run("missing principal denied", func(t *testing.T) {
		t.Parallel()

		err := RequireAdminForAction(context.Background(), "list catalog registrations")
		require.Error(t, err)
		assert.EqualError(t, err, "authentication required")
	})

	t.Run("non admin denied with action message", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		err := RequireAdminForAction(ctx, "list catalog registrations")
		require.Error(t, err)
		assert.EqualError(t, err, "list catalog registrations requires admin privileges")
	})

	t.Run("admin allowed", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
		require.NoError(t, RequireAdminForAction(ctx, "list catalog registrations"))
	})
}

func TestRequireAdminIfPresentForAction(t *testing.T) {
	t.Parallel()

	t.Run("missing principal allowed", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, RequireAdminIfPresentForAction(context.Background(), "attach catalogs"))
	})

	t.Run("non admin denied with action message", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		err := RequireAdminIfPresentForAction(ctx, "set default catalog")
		require.Error(t, err)
		assert.EqualError(t, err, "set default catalog requires admin privileges")
	})

	t.Run("admin allowed", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
		require.NoError(t, RequireAdminIfPresentForAction(ctx, "set default catalog"))
	})
}

func TestCanReadOwnedResource(t *testing.T) {
	t.Parallel()

	t.Run("admin allowed", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice", IsAdmin: true})
		assert.True(t, CanReadOwnedResource(ctx, "bob", "carol"))
	})

	t.Run("owner allowed", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		assert.True(t, CanReadOwnedResource(ctx, "alice", "alice"))
	})

	t.Run("non owner denied", func(t *testing.T) {
		t.Parallel()

		ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
		assert.False(t, CanReadOwnedResource(ctx, "alice", "bob"))
	})
}

func TestCallerName(t *testing.T) {
	t.Parallel()

	assert.Empty(t, CallerName(context.Background()))

	ctx := domain.WithPrincipal(context.Background(), domain.ContextPrincipal{Name: "alice"})
	assert.Equal(t, "alice", CallerName(ctx))
}
