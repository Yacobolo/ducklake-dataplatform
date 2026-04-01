package dashboards

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestInteractiveFiltersFromOriginRaw_UsesFilterOriginKeyOnly(t *testing.T) {
	t.Parallel()

	widgets := []domain.DashboardWidget{
		{
			ID:              "widget-uuid-1",
			FilterOriginKey: "table-zone-revenue-detail",
		},
	}

	t.Run("origin key resolves to widget filter", func(t *testing.T) {
		t.Parallel()

		filters := interactiveFiltersFromOriginRaw([]string{
			"table-zone-revenue-detail|borough:Queens",
		}, widgets)

		require.Len(t, filters, 1)
		assert.Equal(t, "widget-uuid-1", filters[0].WidgetID)
		assert.Equal(t, "borough", filters[0].Dimension)
		assert.Equal(t, []string{"Queens"}, filters[0].Values)
	})

	t.Run("legacy uuid origin is ignored", func(t *testing.T) {
		t.Parallel()

		filters := interactiveFiltersFromOriginRaw([]string{
			"widget-uuid-1|borough:Queens",
		}, widgets)

		assert.Empty(t, filters)
	})
}
