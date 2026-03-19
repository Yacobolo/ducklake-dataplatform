package sitegen

import (
	"bytes"
	"fmt"
	"html"
	"regexp"
	"strings"

	chromahtml "github.com/alecthomas/chroma/v2/formatters/html"
	"github.com/yuin/goldmark"
	highlighting "github.com/yuin/goldmark-highlighting/v2"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	ghtml "github.com/yuin/goldmark/renderer/html"
)

func renderMarkdown(source string) (string, error) {
	md := goldmark.New(
		goldmark.WithExtensions(
			extension.GFM,
			extension.DefinitionList,
			highlighting.NewHighlighting(
				highlighting.WithCodeBlockOptions(func(ctx highlighting.CodeBlockContext) []chromahtml.Option {
					return []chromahtml.Option{
						chromahtml.WithPreWrapper(siteCodePreWrapper{Language: highlightedLanguage(ctx)}),
					}
				}),
				highlighting.WithFormatOptions(
					chromahtml.WithClasses(true),
					chromahtml.ClassPrefix("chroma-"),
				),
			),
		),
		goldmark.WithParserOptions(parser.WithAutoHeadingID()),
		goldmark.WithRendererOptions(ghtml.WithUnsafe()),
	)
	var buf bytes.Buffer
	if err := md.Convert([]byte(source), &buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}

type siteCodePreWrapper struct {
	Language string
}

func (p siteCodePreWrapper) Start(code bool, _ string) string {
	if !code {
		return `<pre tabindex="0">`
	}

	language := strings.TrimSpace(strings.ToLower(p.Language))
	if language == "" {
		language = "text"
	}
	return `<pre tabindex="0" class="chroma language-` + html.EscapeString(language) + `" data-code-language="` + html.EscapeString(language) + `">`
}

func (p siteCodePreWrapper) End(bool) string {
	return `</pre>`
}

func highlightedLanguage(ctx highlighting.CodeBlockContext) string {
	if language, ok := ctx.Language(); ok && len(language) > 0 {
		return string(language)
	}
	return "text"
}

func attrValue(attrs, name string) string {
	pattern := regexp.MustCompile(regexp.QuoteMeta(name) + `="([^"]*)"`)
	match := pattern.FindStringSubmatch(attrs)
	if len(match) != 2 {
		return ""
	}
	return html.UnescapeString(match[1])
}

func hasAttr(attrs, name string) bool {
	pattern := regexp.MustCompile(`(?:^|\s)` + regexp.QuoteMeta(name) + `=`)
	return pattern.FindStringIndex(attrs) != nil
}

func enhanceAPIHTML(htmlSource string) string {
	opRE := regexp.MustCompile(`<h2 id="([^"]+)"><code>(GET|POST|PUT|PATCH|DELETE) ([^<]+)</code></h2>`)
	matches := opRE.FindAllStringSubmatchIndex(htmlSource, -1)
	if len(matches) == 0 {
		return htmlSource
	}

	var out strings.Builder
	cursor := 0
	for i, match := range matches {
		if len(match) < 8 {
			continue
		}
		start := match[0]
		end := len(htmlSource)
		if i+1 < len(matches) {
			end = matches[i+1][0]
		}
		out.WriteString(htmlSource[cursor:start])
		out.WriteString(transformAPIOperationHTML(htmlSource[start:end], opRE))
		cursor = end
	}
	out.WriteString(htmlSource[cursor:])
	return out.String()
}

func transformAPIOperationHTML(chunk string, opRE *regexp.Regexp) string {
	match := opRE.FindStringSubmatchIndex(chunk)
	if len(match) < 8 {
		return chunk
	}

	method := chunk[match[4]:match[5]]
	routePath := chunk[match[6]:match[7]]
	sectionID := slug(method + " " + routePath)
	rest := strings.TrimSpace(chunk[match[1]:])

	paragraphRE := regexp.MustCompile(`(?s)^<p>(.*?)</p>\s*`)
	opIDRE := regexp.MustCompile(`(?s)^<ul>\s*<li>Operation ID: <code>([^<]+)</code></li>\s*</ul>\s*`)

	titleHTML := routePath
	descriptionHTML := ""

	if paragraph := paragraphRE.FindStringSubmatchIndex(rest); len(paragraph) == 4 {
		titleHTML = rest[paragraph[2]:paragraph[3]]
		rest = strings.TrimSpace(rest[paragraph[1]:])
		if nextParagraph := paragraphRE.FindStringSubmatchIndex(rest); len(nextParagraph) == 4 {
			descriptionHTML = rest[nextParagraph[2]:nextParagraph[3]]
			rest = strings.TrimSpace(rest[nextParagraph[1]:])
		}
	}

	operationID := ""
	if opID := opIDRE.FindStringSubmatchIndex(rest); len(opID) == 4 {
		operationID = rest[opID[2]:opID[3]]
		rest = strings.TrimSpace(rest[opID[1]:])
	}

	methodClass := apiMethodBadgeClass(method)

	var out strings.Builder
	out.WriteString(`<section class="mt-12 pt-8 first:mt-8">`)
	out.WriteString(`<header class="mb-6">`)
	out.WriteString(`<h2 id="` + sectionID + `" class="mt-0 flex flex-wrap items-center justify-between gap-3 text-[1.55rem] font-semibold leading-[1.1]"><span class="min-w-0 flex-1">` + titleHTML + `</span><span class="inline-flex min-w-[4.5rem] items-center justify-center rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] ` + methodClass + `" data-api-method="` + method + `">` + method + `</span></h2>`)
	out.WriteString(`<p class="mt-3"><code class="bg-transparent p-0 font-mono text-sm text-[var(--fgColor-muted)]">` + html.EscapeString(routePath) + `</code></p>`)
	if descriptionHTML != "" {
		out.WriteString(`<p class="mt-3 max-w-[56rem]">` + descriptionHTML + `</p>`)
	}
	if operationID != "" {
		out.WriteString(`<div class="mt-4 flex flex-wrap items-center gap-3"><span class="text-[0.72rem] font-semibold uppercase tracking-[0.16em] text-[var(--fgColor-muted)]">Operation ID</span><code>` + html.EscapeString(operationID) + `</code></div>`)
	}
	out.WriteString(`</header>`)
	if rest != "" {
		out.WriteString(rest)
	}
	out.WriteString(`</section>`)
	return out.String()
}

func apiMethodBadgeClass(method string) string {
	switch strings.ToUpper(method) {
	case "GET":
		return "bg-[var(--bgColor-success-muted)] text-[var(--fgColor-success)]"
	case "POST":
		return "bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
	case "PUT":
		return "bg-[var(--bgColor-attention-muted)] text-[var(--fgColor-attention)]"
	case "PATCH":
		return "bg-[var(--bgColor-done-muted)] text-[var(--fgColor-done)]"
	case "DELETE":
		return "bg-[var(--bgColor-danger-muted)] text-[var(--fgColor-danger)]"
	default:
		return "bg-[var(--bgColor-muted)] text-[var(--fgColor-default)]"
	}
}

func enhanceCodeBlocks(htmlSource string) string {
	codeRE := regexp.MustCompile(`(?s)<pre([^>]*)>(?:<code([^>]*)>)?(.*?)(?:</code>)?</pre>`)
	index := 0

	return codeRE.ReplaceAllStringFunc(htmlSource, func(match string) string {
		parts := codeRE.FindStringSubmatch(match)
		if len(parts) != 4 {
			return match
		}

		preAttrs := parts[1]
		codeAttrs := parts[2]
		codeClass := attrValue(codeAttrs, "class")
		language := codeBlockLanguage(codeClass)
		if language == "TEXT" {
			language = codeBlockLanguage(attrValue(preAttrs, "class"))
		}
		if language == "TEXT" {
			if value := attrValue(preAttrs, "data-code-language"); value != "" {
				language = strings.ToUpper(value)
			}
		}
		codeID := fmt.Sprintf("site-codeblock-%d", index)
		index++

		var b strings.Builder
		b.WriteString(`<div data-site-codeblock class="overflow-hidden rounded-3xl border border-[var(--borderColor-default)] bg-[var(--bgColor-inset)] shadow-[0_16px_36px_color-mix(in_srgb,var(--fgColor-default)_10%,transparent)]">`)
		b.WriteString(`<div class="flex items-center justify-between gap-4 border-b border-[var(--borderColor-muted)] bg-[var(--bgColor-muted)] px-4 py-3">`)
		b.WriteString(`<span class="text-xs font-semibold uppercase tracking-[0.18em] text-[var(--fgColor-default)]">`)
		b.WriteString(html.EscapeString(language))
		b.WriteString(`</span>`)
		b.WriteString(`<button type="button" class="rounded-full border border-[var(--button-default-borderColor-rest)] bg-[var(--button-default-bgColor-rest)] px-3 py-1 text-xs font-semibold text-[var(--button-default-fgColor-rest)] transition hover:border-[var(--button-default-borderColor-hover)] hover:bg-[var(--button-default-bgColor-hover)]" data-site-copy="#`)
		b.WriteString(codeID)
		b.WriteString(`">Copy</button>`)
		b.WriteString(`</div>`)
		b.WriteString(`<pre`)
		if strings.TrimSpace(preAttrs) != "" {
			b.WriteString(preAttrs)
		}
		b.WriteString(`><code`)
		if strings.TrimSpace(codeAttrs) != "" {
			b.WriteString(codeAttrs)
		}
		if !hasAttr(codeAttrs, "id") {
			b.WriteString(` id="`)
			b.WriteString(codeID)
			b.WriteString(`"`)
		}
		b.WriteString(` data-code-language="`)
		b.WriteString(html.EscapeString(strings.ToLower(language)))
		b.WriteString(`">`)
		b.WriteString(parts[3])
		b.WriteString(`</code></pre></div>`)
		return b.String()
	})
}

func enhanceTables(htmlSource string) string {
	tableRE := regexp.MustCompile(`(?s)<table>.*?</table>`)
	return tableRE.ReplaceAllStringFunc(htmlSource, func(match string) string {
		return `<div class="my-8 overflow-x-auto">` + match + `</div>`
	})
}

func codeBlockLanguage(className string) string {
	for _, token := range strings.Fields(className) {
		if strings.HasPrefix(token, "language-") {
			value := strings.TrimPrefix(token, "language-")
			if value != "" {
				return strings.ToUpper(value)
			}
		}
	}
	return "TEXT"
}

func stripLeadingH1(htmlSource string) string {
	leading := regexp.MustCompile(`(?s)^\s*<h1 id="[^"]+">.*?</h1>\s*`)
	return leading.ReplaceAllString(htmlSource, "")
}

func stripHTML(source string) string {
	tagRE := regexp.MustCompile(`<[^>]+>`)
	return strings.TrimSpace(tagRE.ReplaceAllString(source, ""))
}
