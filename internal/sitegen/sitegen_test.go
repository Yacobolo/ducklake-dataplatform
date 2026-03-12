package sitegen

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransformDirectives_CalloutAndCardGroup(t *testing.T) {
	source := strings.TrimSpace(`
:::callout kind=tip title="Start here"
Use the quickstart first.
:::

:::card-group
:::card title="Quickstart" href="/start-here/quickstart/"
Run your first secure query.
:::
:::
`)

	htmlOut, mirrorOut, err := transformDirectives(source)
	require.NoError(t, err)
	assert.Contains(t, htmlOut, `class="site-callout"`)
	assert.Contains(t, htmlOut, `class="site-card-grid"`)
	assert.Contains(t, htmlOut, `href="/start-here/quickstart/"`)
	assert.Contains(t, mirrorOut, "## Start here")
	assert.Contains(t, mirrorOut, "## Quickstart")
}

func TestRouteForRelPath_CleanPaths(t *testing.T) {
	kind, path := routeForRelPath("index.md")
	assert.Equal(t, pageKindHome, kind)
	assert.Equal(t, "/", path)

	kind, path = routeForRelPath("start-here/index.md")
	assert.Equal(t, pageKindDocs, kind)
	assert.Equal(t, "/docs/", path)

	kind, path = routeForRelPath("start-here/quickstart.md")
	assert.Equal(t, pageKindDocs, kind)
	assert.Equal(t, "/docs/start-here/quickstart/", path)

	kind, path = routeForRelPath("reference/generated/api/index.md")
	assert.Equal(t, pageKindAPI, kind)
	assert.Equal(t, "/api-reference/", path)
}

func TestExtractHeadings_FiltersMarkdownHeadings(t *testing.T) {
	headings := extractHeadings("# Title\n\n## First\n\n### Second\n\nText")
	require.Len(t, headings, 3)
	assert.Equal(t, "title", headings[0].ID)
	assert.Equal(t, "First", headings[1].Title)
	assert.Equal(t, "second", headings[2].ID)
}

func TestRewriteContentLinks_DocsAndAPIPaths(t *testing.T) {
	source := "[Quickstart](/start-here/quickstart) and [API](/reference/generated/api/endpoints/queries)"
	rewritten := rewriteContentLinks(source)
	assert.Contains(t, rewritten, "/docs/start-here/quickstart")
	assert.Contains(t, rewritten, "/api-reference/endpoints/queries")
}
