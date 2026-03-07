package orchestration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilesystemIOManager_LoadInput(t *testing.T) {
	t.Run("returns empty map for missing key", func(t *testing.T) {
		manager, err := NewFilesystemIOManager(t.TempDir())
		require.NoError(t, err)

		got, err := manager.LoadInput(context.Background(), "missing")
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("loads stored output", func(t *testing.T) {
		manager, err := NewFilesystemIOManager(t.TempDir())
		require.NoError(t, err)

		err = manager.StoreOutput(context.Background(), "asset-a", map[string]any{
			"status":    "ok",
			"row_count": 12,
		})
		require.NoError(t, err)

		got, err := manager.LoadInput(context.Background(), "asset-a")
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"status": "ok", "row_count": float64(12)}, got)
	})
}

func TestFilesystemIOManager_StoreOutput_Overwrite(t *testing.T) {
	manager, err := NewFilesystemIOManager(t.TempDir())
	require.NoError(t, err)

	err = manager.StoreOutput(context.Background(), "asset-a", map[string]any{"status": "old"})
	require.NoError(t, err)

	err = manager.StoreOutput(context.Background(), "asset-a", map[string]any{"status": "new"})
	require.NoError(t, err)

	got, err := manager.LoadInput(context.Background(), "asset-a")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"status": "new"}, got)
}

func TestNewFilesystemIOManager_EmptyRoot(t *testing.T) {
	manager, err := NewFilesystemIOManager("  ")
	assert.Nil(t, manager)
	require.Error(t, err)
}
