package explore

import (
	"bytes"
	"testing"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomponents "maragu.dev/gomponents"
)

func TestListPage_UsesApplicationLayout(t *testing.T) {
	page := listPage(
		domain.ContextPrincipal{Name: "alice", Type: "user"},
		nil,
		[]breadcrumbItem{{Label: "Personal workspace", Current: true}},
		[]core.ExploreNavigatorItem{
			{Name: "Personal workspace", URL: "/ui/explore?folder_id=root", Icon: "folder-tree", Active: true},
		},
		"root",
		[]string{"dashboard"},
		nil,
		"revenue",
		[]string{"alice"},
		"stream-123",
		"csrf-123",
		domain.PageRequest{MaxResults: 30},
		7,
	)

	html := renderExploreNode(t, page)

	assert.Contains(t, html, "app-layout-shell")
	assert.Contains(t, html, "data-shell-compact-locked=\"true\"")
	assert.Contains(t, html, "app-layout-center")
	assert.Contains(t, html, "app-primary-rail")
	assert.Contains(t, html, "app-secondary-aside")
	assert.Contains(t, html, "explore-aside")
	assert.Contains(t, html, "workspace-aside-plain")
	assert.Contains(t, html, "app-layout-main-region")
	assert.Contains(t, html, "app-layout-footer")
	assert.Contains(t, html, "EXPLORE")
	assert.Contains(t, html, "stream-123")
	assert.Contains(t, html, "id=\"main-content\"")
	assert.Contains(t, html, "/ui/static/js/explore.")
	assert.NotContains(t, html, "class=\"workspace-layout")
	assert.NotContains(t, html, "id=\"sidebar-toggle\"")
}

func renderExploreNode(t *testing.T, node gomponents.Node) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, node.Render(&buf))
	return buf.String()
}
