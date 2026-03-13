package sitegen

import (
	"os"
	"path/filepath"
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

func TestReplaceMermaidBlocks_RewritesFenceToImage(t *testing.T) {
	source := `<p>Flow</p><pre><code class="language-mermaid">graph TD
A--&gt;B
</code></pre>`

	rendered, diags := replaceMermaidBlocks(source, "site/content/example.md")

	require.Len(t, diags, 1)
	assert.Contains(t, rendered, `class="site-mermaid"`)
	assert.Contains(t, rendered, `/_site/mermaid/`+diags[0].ID+`.svg`)
	assert.Equal(t, "graph TD\nA-->B\n", diags[0].Source)
	assert.Equal(t, "site/content/example.md", diags[0].SourcePath)
}

func TestEnhanceTables_WrapsTables(t *testing.T) {
	source := `<p>Example</p><table><thead><tr><th>A</th></tr></thead><tbody><tr><td>1</td></tr></tbody></table>`

	rendered := enhanceTables(source)

	assert.Contains(t, rendered, `class="site-table-wrap"`)
	assert.Contains(t, rendered, `<table>`)
}

func TestRenderMermaidAssets_RendersUniqueDiagrams(t *testing.T) {
	tempDir := t.TempDir()
	cliPath := filepath.Join(tempDir, "fake-mmdc")
	script := `#!/bin/sh
set -eu
input=""
output=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -i)
      input="$2"
      shift 2
      ;;
    -o)
      output="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
printf '<svg xmlns="http://www.w3.org/2000/svg"><text>' > "$output"
cat "$input" >> "$output"
printf '</text></svg>' >> "$output"
`
	require.NoError(t, os.WriteFile(cliPath, []byte(script), 0o700))

	diag := mermaidDiagram{
		ID:         mermaidDiagramID("graph TD\nA-->B"),
		Source:     "graph TD\nA-->B\n",
		SourcePath: "site/content/example.md",
	}

	err := renderMermaidAssets(tempDir, []page{
		{MermaidDiags: []mermaidDiagram{diag}},
		{MermaidDiags: []mermaidDiagram{diag}},
	}, cliPath)
	require.NoError(t, err)

	body, err := os.ReadFile(filepath.Join(tempDir, "_site", "mermaid", diag.ID+".svg"))
	require.NoError(t, err)
	assert.Contains(t, string(body), "graph TD")
	assert.Contains(t, string(body), "A-->B")
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
