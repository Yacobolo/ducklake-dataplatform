package core

import (
	"bytes"
	"testing"

	"github.com/Yacobolo/quackstack/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomponents "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func TestApplicationPage_RendersMultiRailShell(t *testing.T) {
	page := ApplicationPage("Catalog", "catalogs", domain.ContextPrincipal{Name: "alice", Type: "user"}, ApplicationLayoutSlots{
		CenterAttributes: []gomponents.Node{
			ID("layout-center"),
		},
		SecondaryAside: Div(ID("catalog-explorer"), gomponents.Text("Explorer")),
		Main:           MainContentSection("Catalog", "flex min-h-0 w-full flex-1 flex-col", Div(gomponents.Text("Main content"))),
		Footer:         Span(gomponents.Text("Status ready")),
	})

	html := renderNode(t, page)

	assert.Contains(t, html, "app-layout-shell")
	assert.Contains(t, html, "data-shell-compact-locked=\"true\"")
	assert.Contains(t, html, "app-layout-center")
	assert.Contains(t, html, "id=\"layout-center\"")
	assert.Contains(t, html, "app-primary-rail")
	assert.Contains(t, html, "app-primary-rail-nav")
	assert.Contains(t, html, "app-secondary-aside")
	assert.Contains(t, html, "catalog-explorer")
	assert.Contains(t, html, "app-layout-main-region")
	assert.Contains(t, html, "app-layout-footer")
	assert.Contains(t, html, "Status ready")
	assert.Contains(t, html, "data-workspace-layout=\"true\"")
	assert.NotContains(t, html, "id=\"sidebar-toggle\"")
}

func TestAppPage_KeepsLegacyShell(t *testing.T) {
	page := AppPage("Explore", "explore", domain.ContextPrincipal{Name: "alice", Type: "user"}, Div(gomponents.Text("Legacy body")))

	html := renderNode(t, page)

	assert.Contains(t, html, "app-body")
	assert.Contains(t, html, "id=\"app-sidebar\"")
	assert.Contains(t, html, "id=\"sidebar-toggle\"")
	assert.NotContains(t, html, "class=\"app-layout-center")
	assert.NotContains(t, html, "data-shell-compact-locked=\"true\"")
}

func renderNode(t *testing.T, node gomponents.Node) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, node.Render(&buf))
	return buf.String()
}
