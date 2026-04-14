package overview

import (
	"bytes"
	"testing"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOverviewPage_RendersHomeExperience(t *testing.T) {
	now := time.Now().Add(-10 * time.Minute)
	page := overviewPage(overviewPageData{
		Principal: domain.ContextPrincipal{Name: "alice", Type: "user"},
		Recent: []domain.ResourceAccessEvent{
			{
				ResourceRef: domain.ResourceRef{
					ResourceType: "notebook",
					ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7b",
					DisplayName:  "Orders notebook",
					ResourcePath: "finance/quarterly/",
					Href:         "/ui/notebooks/019d43e3-9377-79f6-a368-01b6ae805b7b",
					Section:      "Build",
				},
				AccessedAt: now,
			},
			{
				ResourceRef: domain.ResourceRef{
					ResourceType: "dashboard",
					ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7d",
					DisplayName:  "Revenue dashboard",
					ResourcePath: "finance/quarterly/",
					Href:         "/ui/dashboards/019d43e3-9377-79f6-a368-01b6ae805b7d",
					Section:      "Discover",
				},
				AccessedAt: now,
			},
		},
		Saved: []domain.SavedResource{
			{
				ResourceRef: domain.ResourceRef{
					ResourceType: "dashboard",
					ResourceKey:  "019d43e3-9377-79f6-a368-01b6ae805b7d",
					DisplayName:  "Revenue dashboard",
					ResourcePath: "finance/quarterly/",
					Href:         "/ui/dashboards/019d43e3-9377-79f6-a368-01b6ae805b7d",
					Section:      "Discover",
				},
				SavedAt: now,
			},
		},
	})

	var buf bytes.Buffer
	require.NoError(t, page.Render(&buf))
	html := buf.String()

	assert.Contains(t, html, "quack-home-hero")
	assert.Contains(t, html, "Recent resources")
	assert.Contains(t, html, "Saved resources")
	assert.Contains(t, html, ">Path<")
	assert.Contains(t, html, "Orders notebook")
	assert.Contains(t, html, "Revenue dashboard")
	assert.Contains(t, html, "finance/quarterly/")
	assert.Contains(t, html, `/ui/resources/save`)
	assert.Contains(t, html, `/ui/resources/unsave`)
	assert.Contains(t, html, "Resource already saved")
	assert.Contains(t, html, "Save resource")
	assert.Contains(t, html, "Remove saved resource")
	assert.Contains(t, html, "disabled")
	assert.Contains(t, html, `/ui/static/js/home-hero`)
	assert.NotContains(t, html, "quack-ui-recent-resources")
}
