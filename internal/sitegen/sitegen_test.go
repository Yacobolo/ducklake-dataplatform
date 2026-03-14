package sitegen

import (
	"os"
	"path/filepath"
	"regexp"
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

func TestEnhanceCodeBlocks_AddsChromeAndCopyTarget(t *testing.T) {
	source := `<p>Example</p><pre><code class="language-bash">echo hi
</code></pre>`

	rendered := enhanceCodeBlocks(source)

	assert.Contains(t, rendered, `class="site-codeblock"`)
	assert.Contains(t, rendered, `class="site-codeblock-lang">BASH<`)
	assert.Contains(t, rendered, `data-site-copy="#site-codeblock-0"`)
	assert.Contains(t, rendered, `id="site-codeblock-0"`)
}

func TestEnhanceCodeBlocks_PreservesHighlightedAttributes(t *testing.T) {
	source := `<pre tabindex="0" class="chroma language-bash" data-code-language="bash"><span class="chroma-nb">echo</span></pre>`

	rendered := enhanceCodeBlocks(source)

	assert.Contains(t, rendered, `<div class="site-codeblock">`)
	assert.Contains(t, rendered, `<pre tabindex="0" class="chroma language-bash" data-code-language="bash">`)
	assert.Contains(t, rendered, `data-code-language="bash"`)
	assert.Contains(t, rendered, `id="site-codeblock-0"`)
	assert.Contains(t, rendered, `<span class="chroma-nb">echo</span>`)
}

func TestRenderMarkdown_HighlightsFencedCode(t *testing.T) {
	rendered, err := renderMarkdown("```bash\necho hi\n```")
	require.NoError(t, err)

	assert.Contains(t, rendered, `class="chroma language-bash"`)
	assert.Contains(t, rendered, `data-code-language="bash"`)
	assert.Contains(t, rendered, `class="chroma-nb"`)
}

func TestEnhanceTables_WrapsTables(t *testing.T) {
	source := `<p>Example</p><table><thead><tr><th>A</th></tr></thead><tbody><tr><td>1</td></tr></tbody></table>`

	rendered := enhanceTables(source)

	assert.Contains(t, rendered, `class="site-table-wrap"`)
	assert.Contains(t, rendered, `<table>`)
}

func TestCopySiteAssets_CopiesDiagrams(t *testing.T) {
	tempDir := t.TempDir()
	assetsDir := filepath.Join(tempDir, "assets")
	require.NoError(t, os.MkdirAll(filepath.Join(assetsDir, "generated"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(assetsDir, "js"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(assetsDir, "icons"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(assetsDir, "diagrams"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(assetsDir, "generated", "site.css"), []byte("body{}"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(assetsDir, "js", "site.js"), []byte(""), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(assetsDir, "icons", "favicon.svg"), []byte("<svg></svg>"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(assetsDir, "diagrams", "flow.svg"), []byte("<svg><text>diagram</text></svg>"), 0o600))

	err := copySiteAssets(assetsDir, filepath.Join(tempDir, "out"))
	require.NoError(t, err)

	body, err := os.ReadFile(filepath.Join(tempDir, "out", "_site", "diagrams", "flow.svg"))
	require.NoError(t, err)
	assert.Contains(t, string(body), "diagram")
}

func TestNormalizeSiteRoot_UsesBaseURLPath(t *testing.T) {
	assert.Equal(t, "/ducklake-dataplatform", normalizeSiteRoot("https://yacobolo.github.io/ducklake-dataplatform/"))
	assert.Empty(t, normalizeSiteRoot("https://yacobolo.github.io/"))
	assert.Equal(t, "/preview", normalizeSiteRoot("/preview/"))
}

func TestJoinSiteURL_PrefixesInternalPaths(t *testing.T) {
	assert.Equal(t, "/ducklake-dataplatform/docs/", joinSiteURL("/ducklake-dataplatform", "/docs/"))
	assert.Equal(t, "/docs/", joinSiteURL("", "/docs/"))
	assert.Equal(t, "https://example.com/docs/", joinSiteURL("/ducklake-dataplatform", "https://example.com/docs/"))
	assert.Equal(t, "#overview", joinSiteURL("/ducklake-dataplatform", "#overview"))
}

func TestPrefixSiteRootInHTML_RewritesInternalHrefAndSrc(t *testing.T) {
	source := `<p><a href="/docs/start-here/">Docs</a><img src="/_site/diagrams/a.svg" alt=""></p>`

	rendered := prefixSiteRootInHTML(source, "/ducklake-dataplatform")

	assert.Contains(t, rendered, `href="/ducklake-dataplatform/docs/start-here/"`)
	assert.Contains(t, rendered, `src="/ducklake-dataplatform/_site/diagrams/a.svg"`)
}

func TestAPIOperationNodes_ExtractMethodRouteAndAnchor(t *testing.T) {
	p := page{
		URLPath: "/api-reference/endpoints/queries/",
		BodyMarkdown: strings.TrimSpace(`
## ` + "`GET /audit-logs`" + `

List audit logs

## ` + "`POST /queries`" + `

Submit query
`),
	}

	nodes := apiOperationNodes(p)
	require.Len(t, nodes, 2)
	assert.Equal(t, "GET", nodes[0].Method)
	assert.Equal(t, "/audit-logs", nodes[0].RoutePath)
	assert.Equal(t, "List audit logs", nodes[0].Description)
	assert.Equal(t, "/api-reference/endpoints/queries/#get-audit-logs", nodes[0].Path)
	assert.Equal(t, "/queries", nodes[1].RoutePath)
}

func TestEnhanceAPIHTML_AddsMethodDataAttribute(t *testing.T) {
	source := `<h2 id="get-queries"><code>GET /queries</code></h2>`

	rendered := enhanceAPIHTML(source)

	assert.Contains(t, rendered, `class="api-method" data-api-method="GET"`)
	assert.Contains(t, rendered, `class="api-path">/queries<`)
}

func TestDocsPages_InternalLinksResolveToKnownRoutes(t *testing.T) {
	contentRoot := filepath.Join("..", "..", "site", "content")
	pages, err := loadPages(contentRoot)
	require.NoError(t, err)

	validRoutes := make(map[string]struct{}, len(pages)*2)
	for _, p := range pages {
		validRoutes[canonicalTestRoute(p.URLPath)] = struct{}{}
	}

	hrefRE := regexp.MustCompile(`href="([^"#?][^"]*|/[^"]*)"` + `|href='([^'#?][^']*|/[^']*)'`)
	var broken []string

	for _, p := range pages {
		matches := hrefRE.FindAllStringSubmatch(string(p.BodyHTML), -1)
		for _, match := range matches {
			href := firstNonEmpty(match[1], match[2])
			if href == "" || !strings.HasPrefix(href, "/") {
				continue
			}
			if isIgnoredInternalHref(href) {
				continue
			}

			target := canonicalTestRoute(href)
			if _, ok := validRoutes[target]; ok {
				continue
			}

			broken = append(broken, p.RelPath+" -> "+href)
		}
	}

	require.Empty(t, broken, "found broken internal docs links:\n%s", strings.Join(broken, "\n"))
}

func canonicalTestRoute(href string) string {
	href = strings.TrimSpace(href)
	if href == "" {
		return ""
	}
	if idx := strings.IndexAny(href, "?#"); idx >= 0 {
		href = href[:idx]
	}
	if href == "" {
		return "/"
	}
	if href != "/" {
		href = strings.TrimRight(href, "/")
	}
	if href == "" {
		return "/"
	}
	return href
}

func isIgnoredInternalHref(href string) bool {
	switch {
	case strings.HasPrefix(href, "/_site/"):
		return true
	case strings.HasPrefix(href, "/llms/"):
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
