package ui

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestUIComponentsPage_CoreSectionsRender(t *testing.T) {
	html := renderComponentsPageForTest(t)

	assert.Contains(t, html, "Action Bar")
	assert.Contains(t, html, "Breadcrumbs")
	assert.Contains(t, html, "Avatar")
	assert.Contains(t, html, "Tree View")
	assert.Contains(t, html, "Loading")
	assert.Contains(t, html, "Theme Tokens")
}

func TestUIComponentsPage_UsesDataStarSignalsInActionBar(t *testing.T) {
	html := renderComponentsPageForTest(t)

	assert.Contains(t, html, `data-signals="{&#34;q&#34;:&#34;&#34;,&#34;sort&#34;:&#34;updated&#34;}"`)
	assert.Contains(t, html, "component-search")
	assert.Contains(t, html, "component-sort")
}

func TestUIComponentsPage_SelectionAndLoadingPatternsRender(t *testing.T) {
	html := renderComponentsPageForTest(t)

	assert.Contains(t, html, "Enable row-level security")
	assert.Contains(t, html, "Notify on failed runs")
	assert.Contains(t, html, "Spinner")
	assert.Contains(t, html, "ProgressBar")
}

func renderComponentsPageForTest(t *testing.T) string {
	t.Helper()

	var buf strings.Builder
	err := componentsPage(domain.ContextPrincipal{Name: "tester", Type: "user"}).Render(&buf)
	require.NoError(t, err)
	return buf.String()
}
