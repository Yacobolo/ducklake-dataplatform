package router

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestActiveFirstSelector_Select(t *testing.T) {
	selector := NewActiveFirstSelector()

	t.Run("empty candidates", func(t *testing.T) {
		ep, err := selector.Select(context.Background(), nil)
		require.NoError(t, err)
		assert.Nil(t, ep)
	})

	t.Run("prefers active endpoint", func(t *testing.T) {
		ep, err := selector.Select(context.Background(), []domain.ComputeEndpoint{
			{ID: "1", Name: "cold", Status: "INACTIVE"},
			{ID: "2", Name: "hot", Status: "ACTIVE"},
		})
		require.NoError(t, err)
		require.NotNil(t, ep)
		assert.Equal(t, "2", ep.ID)
	})

	t.Run("falls back to first when none active", func(t *testing.T) {
		ep, err := selector.Select(context.Background(), []domain.ComputeEndpoint{
			{ID: "1", Name: "first", Status: "STARTING"},
			{ID: "2", Name: "second", Status: "INACTIVE"},
		})
		require.NoError(t, err)
		require.NotNil(t, ep)
		assert.Equal(t, "1", ep.ID)
	})
}
