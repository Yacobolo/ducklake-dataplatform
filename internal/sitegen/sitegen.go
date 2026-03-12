package sitegen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"html/template"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	ghtml "github.com/yuin/goldmark/renderer/html"
	"gopkg.in/yaml.v3"
)

type Builder struct {
	ContentDir   string
	ConfigPath   string
	NavPath      string
	TemplatesDir string
	AssetsDir    string
	OutDir       string
	BaseURL      string
}

type siteConfig struct {
	Title          string `yaml:"title"`
	Description    string `yaml:"description"`
	GitHubEditBase string `yaml:"github_edit_base"`
}

type navConfig struct {
	Primary []navLinkConfig  `yaml:"primary"`
	Docs    []navGroupConfig `yaml:"docs"`
	API     []navGroupConfig `yaml:"api"`
}

type navLinkConfig struct {
	Title string `yaml:"title"`
	Path  string `yaml:"path"`
}

type navGroupConfig struct {
	Title string           `yaml:"title"`
	Icon  string           `yaml:"icon"`
	Open  bool             `yaml:"open"`
	Items []navEntryConfig `yaml:"items"`
}

type navEntryConfig struct {
	Source  string           `yaml:"source"`
	Title   string           `yaml:"title"`
	Icon    string           `yaml:"icon"`
	Open    bool             `yaml:"open"`
	AutoDir string           `yaml:"autogen_dir"`
	Items   []navEntryConfig `yaml:"items"`
}

type docFrontMatter struct {
	Title       string   `yaml:"title"`
	Description string   `yaml:"description"`
	Keywords    []string `yaml:"keywords"`
}

type homeFrontMatter struct {
	Layout   string        `yaml:"layout"`
	Hero     homeHero      `yaml:"hero"`
	Features []homeFeature `yaml:"features"`
	Title    string        `yaml:"title"`
	Keywords []string      `yaml:"keywords"`
}

type homeHero struct {
	Name    string         `yaml:"name"`
	Text    string         `yaml:"text"`
	Tagline string         `yaml:"tagline"`
	Actions []homeHeroLink `yaml:"actions"`
}

type homeHeroLink struct {
	Theme string `yaml:"theme"`
	Text  string `yaml:"text"`
	Link  string `yaml:"link"`
}

type homeFeature struct {
	Title   string `yaml:"title"`
	Details string `yaml:"details"`
	Link    string `yaml:"link"`
}

type pageKind string

const (
	pageKindHome pageKind = "home"
	pageKindDocs pageKind = "docs"
	pageKindAPI  pageKind = "api"
)

type page struct {
	SourcePath   string
	RelPath      string
	URLPath      string
	MirrorPath   string
	Title        string
	Description  string
	Kind         pageKind
	Section      string
	BodyMarkdown string
	MirrorBody   string
	BodyHTML     template.HTML
	Headings     []heading
	Keywords     []string
	IsHome       bool
	Home         homeFrontMatter
}

type heading struct {
	Level int
	ID    string
	Title string
}

type navItem struct {
	Title  string
	Path   string
	Active bool
}

type navGroup struct {
	Title string
	Icon  string
	Nodes []navNode
	Open  bool
}

type navNode struct {
	Title       string
	Icon        string
	Path        string
	Active      bool
	Open        bool
	ForceOpen   bool
	Children    []navNode
	Method      string
	RoutePath   string
	Description string
}

type searchItem struct {
	Title       string `json:"title"`
	Path        string `json:"path"`
	Description string `json:"description"`
	Section     string `json:"section"`
	SearchText  string `json:"search_text"`
}

type pageTemplateData struct {
	Site          siteConfig
	Page          page
	MetaTitle     string
	BodyHTML      template.HTML
	TopNav        []navItem
	SidebarGroups []navGroup
	TOC           []heading
	Breadcrumbs   []navItem
	Prev          *navItem
	Next          *navItem
	Home          homeFrontMatter
	MirrorPath    string
}

type templateSet struct {
	base *template.Template
}

func (b Builder) Build() error {
	cfg, err := loadConfig[siteConfig](b.ConfigPath)
	if err != nil {
		return err
	}
	nav, err := loadConfig[navConfig](b.NavPath)
	if err != nil {
		return err
	}

	templates, err := loadTemplates(b.TemplatesDir)
	if err != nil {
		return err
	}

	pages, err := loadPages(b.ContentDir)
	if err != nil {
		return err
	}

	pageByRel := make(map[string]page, len(pages))
	for _, p := range pages {
		pageByRel[p.RelPath] = p
	}

	docsGroups, docsFlat, err := buildConfiguredGroups(nav.Docs, pageByRel, pages)
	if err != nil {
		return err
	}
	apiGroups, apiFlat, err := buildConfiguredGroups(nav.API, pageByRel, pages)
	if err != nil {
		return err
	}

	if err := os.RemoveAll(b.OutDir); err != nil {
		return fmt.Errorf("clean output: %w", err)
	}
	if err := os.MkdirAll(b.OutDir, 0o755); err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	if err := copySiteAssets(b.AssetsDir, b.OutDir); err != nil {
		return err
	}

	searchItems := make([]searchItem, 0, len(pages))
	for _, p := range pages {
		sidebar := docsGroups
		flat := docsFlat
		if p.Kind == pageKindAPI {
			sidebar = apiGroups
			flat = apiFlat
		}

		data := pageTemplateData{
			Site:          cfg,
			Page:          p,
			MetaTitle:     metaTitle(cfg.Title, p.Title, p.IsHome),
			TopNav:        buildTopNav(nav.Primary, p.URLPath),
			SidebarGroups: activateGroups(sidebar, p.URLPath),
			TOC:           tocForPage(p),
			Breadcrumbs:   buildBreadcrumbs(p),
			Prev:          findNeighbor(flat, p.URLPath, -1),
			Next:          findNeighbor(flat, p.URLPath, 1),
			BodyHTML:      p.BodyHTML,
			Home:          p.Home,
			MirrorPath:    p.MirrorPath,
		}

		if err := renderHTMLPage(templates, b.OutDir, p, data); err != nil {
			return err
		}
		if err := writeMirrorMarkdown(b.OutDir, p); err != nil {
			return err
		}

		searchItems = append(searchItems, searchItem{
			Title:       p.Title,
			Path:        p.URLPath,
			Description: p.Description,
			Section:     p.Section,
			SearchText:  strings.ToLower(strings.Join([]string{p.Title, p.Description, strings.Join(p.Keywords, " "), strings.Join(headingTitles(p.Headings), " "), plainText(p.MirrorBody)}, " ")),
		})
	}

	if err := writeSearchIndex(b.OutDir, searchItems); err != nil {
		return err
	}
	if err := writeLLMSTXT(b.OutDir, cfg, pages); err != nil {
		return err
	}
	if err := writeSitemap(b.OutDir, b.BaseURL, pages); err != nil {
		return err
	}

	return nil
}

func Serve(addr, outDir string) error {
	return http.ListenAndServe(addr, http.FileServer(http.Dir(outDir)))
}

func loadConfig[T any](path string) (T, error) {
	var cfg T
	body, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return cfg, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(body, &cfg); err != nil {
		return cfg, fmt.Errorf("decode config %s: %w", path, err)
	}
	return cfg, nil
}

func loadTemplates(root string) (templateSet, error) {
	tmpl, err := template.New("base").Funcs(template.FuncMap{
		"navIcon": navIconSVG,
	}).ParseFS(os.DirFS(root), "*.tmpl")
	if err != nil {
		return templateSet{}, fmt.Errorf("parse templates: %w", err)
	}
	return templateSet{base: tmpl}, nil
}

func loadPages(root string) ([]page, error) {
	pages := make([]page, 0)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}

		p, parseErr := parsePage(root, path)
		if parseErr != nil {
			return parseErr
		}
		pages = append(pages, p)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk site content: %w", err)
	}

	sort.Slice(pages, func(i, j int) bool {
		return pages[i].RelPath < pages[j].RelPath
	})
	return pages, nil
}

func parsePage(root, path string) (page, error) {
	source, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return page{}, fmt.Errorf("read page %q: %w", path, err)
	}
	frontMatter, body := splitFrontMatter(string(source))
	body = stripGeneratedComment(body)
	relPath, err := filepath.Rel(root, path)
	if err != nil {
		return page{}, fmt.Errorf("rel path %q: %w", path, err)
	}
	relPath = filepath.ToSlash(relPath)
	kind, urlPath := routeForRelPath(relPath)

	if relPath == "index.md" {
		var home homeFrontMatter
		if err := yaml.Unmarshal([]byte(frontMatter), &home); err != nil {
			return page{}, fmt.Errorf("decode home front matter: %w", err)
		}
		for i := range home.Hero.Actions {
			home.Hero.Actions[i].Link = rewriteLink(home.Hero.Actions[i].Link)
		}
		for i := range home.Features {
			home.Features[i].Link = rewriteLink(home.Features[i].Link)
		}
		description := home.Hero.Tagline
		if description == "" {
			description = firstParagraph(body)
		}
		return page{
			SourcePath:   path,
			RelPath:      relPath,
			URLPath:      "/",
			MirrorPath:   "index.md",
			Title:        home.Hero.Name,
			Description:  description,
			Kind:         pageKindHome,
			BodyMarkdown: body,
			MirrorBody:   body,
			BodyHTML:     template.HTML(""),
			Headings:     extractHeadings(body),
			Keywords:     home.Keywords,
			IsHome:       true,
			Home:         home,
		}, nil
	}

	var fm docFrontMatter
	if frontMatter != "" {
		if err := yaml.Unmarshal([]byte(frontMatter), &fm); err != nil {
			return page{}, fmt.Errorf("decode front matter %q: %w", path, err)
		}
	}

	body = rewriteContentLinks(body)
	htmlMarkdown, mirror, err := transformDirectives(body)
	if err != nil {
		return page{}, fmt.Errorf("transform directives %q: %w", path, err)
	}
	rendered, err := renderMarkdown(htmlMarkdown)
	if err != nil {
		return page{}, fmt.Errorf("render markdown %q: %w", path, err)
	}

	title := strings.TrimSpace(fm.Title)
	if title == "" {
		title = firstHeading(body)
	}
	description := strings.TrimSpace(fm.Description)
	if description == "" {
		description = firstParagraph(mirror)
	}

	rendered = addHeadingAnchors(rendered)
	rendered = stripLeadingH1(rendered)
	rendered = enhanceCodeBlocks(rendered)
	if kind == pageKindAPI {
		rendered = enhanceAPIHTML(rendered)
	}

	return page{
		SourcePath:   path,
		RelPath:      relPath,
		URLPath:      urlPath,
		MirrorPath:   strings.TrimSuffix(relPath, filepath.Ext(relPath)) + ".md",
		Title:        title,
		Description:  description,
		Kind:         kind,
		Section:      sectionForPage(relPath, kind),
		BodyMarkdown: body,
		MirrorBody:   mirror,
		BodyHTML:     template.HTML(rendered),
		Headings:     extractHeadings(mirror),
		Keywords:     fm.Keywords,
	}, nil
}

func renderHTMLPage(templates templateSet, outDir string, p page, data pageTemplateData) error {
	var buf bytes.Buffer
	if err := templates.base.ExecuteTemplate(&buf, "base", data); err != nil {
		return fmt.Errorf("execute template for %s: %w", p.RelPath, err)
	}

	targetDir := filepath.Join(outDir, strings.TrimPrefix(filepath.FromSlash(p.URLPath), string(filepath.Separator)))
	if p.URLPath == "/" {
		targetDir = outDir
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("mkdir page dir: %w", err)
	}
	return os.WriteFile(filepath.Join(targetDir, "index.html"), buf.Bytes(), 0o644)
}

func writeMirrorMarkdown(outDir string, p page) error {
	target := filepath.Join(outDir, "llms", filepath.FromSlash(p.MirrorPath))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("mkdir llms dir: %w", err)
	}

	var content strings.Builder
	content.WriteString("# ")
	content.WriteString(p.Title)
	content.WriteString("\n\n")
	if p.Description != "" {
		content.WriteString(p.Description)
		content.WriteString("\n\n")
	}
	if p.IsHome {
		for _, feature := range p.Home.Features {
			content.WriteString("## ")
			content.WriteString(feature.Title)
			content.WriteString("\n\n")
			content.WriteString(feature.Details)
			content.WriteString("\n\n")
		}
	} else {
		content.WriteString(strings.TrimSpace(p.MirrorBody))
		content.WriteString("\n")
	}

	return os.WriteFile(target, []byte(content.String()), 0o644)
}

func writeSearchIndex(outDir string, items []searchItem) error {
	payload, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal search index: %w", err)
	}
	return os.WriteFile(filepath.Join(outDir, "search-index.json"), append(payload, '\n'), 0o644)
}

func writeLLMSTXT(outDir string, cfg siteConfig, pages []page) error {
	var b strings.Builder
	b.WriteString("# ")
	b.WriteString(cfg.Title)
	b.WriteString("\n\n")
	b.WriteString(cfg.Description)
	b.WriteString("\n\n")
	b.WriteString("Primary machine-readable mirrors:\n")
	for _, p := range pages {
		b.WriteString("- ")
		b.WriteString(p.Title)
		b.WriteString(": /llms/")
		b.WriteString(p.MirrorPath)
		b.WriteString("\n")
	}
	return os.WriteFile(filepath.Join(outDir, "llms.txt"), []byte(b.String()), 0o644)
}

func writeSitemap(outDir, baseURL string, pages []page) error {
	if strings.TrimSpace(baseURL) == "" {
		return nil
	}
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` + "\n")
	for _, p := range pages {
		b.WriteString("  <url><loc>")
		b.WriteString(html.EscapeString(strings.TrimRight(baseURL, "/") + p.URLPath))
		b.WriteString("</loc></url>\n")
	}
	b.WriteString("</urlset>\n")
	return os.WriteFile(filepath.Join(outDir, "sitemap.xml"), []byte(b.String()), 0o644)
}

func copySiteAssets(assetsDir, outDir string) error {
	staticDir := filepath.Join(outDir, "_site")
	if err := os.MkdirAll(staticDir, 0o755); err != nil {
		return fmt.Errorf("mkdir static dir: %w", err)
	}
	files := []struct {
		Source string
		Target string
	}{
		{Source: filepath.Join(assetsDir, "generated", "site.css"), Target: filepath.Join(staticDir, "site.css")},
		{Source: filepath.Join(assetsDir, "js", "site.js"), Target: filepath.Join(staticDir, "site.js")},
		{Source: filepath.Join(assetsDir, "icons", "favicon.svg"), Target: filepath.Join(staticDir, "favicon.svg")},
	}
	for _, file := range files {
		if err := copyFile(file.Source, file.Target); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(source, target string) error {
	in, err := os.Open(filepath.Clean(source))
	if err != nil {
		return fmt.Errorf("open %s: %w", source, err)
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", target, err)
	}

	out, err := os.Create(filepath.Clean(target))
	if err != nil {
		return fmt.Errorf("create %s: %w", target, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy %s: %w", source, err)
	}
	return nil
}

func splitFrontMatter(content string) (string, string) {
	if !strings.HasPrefix(content, "---\n") {
		return "", content
	}
	rest := strings.TrimPrefix(content, "---\n")
	idx := strings.Index(rest, "\n---\n")
	if idx < 0 {
		return "", content
	}
	return rest[:idx], rest[idx+5:]
}

func stripGeneratedComment(source string) string {
	return strings.TrimSpace(strings.ReplaceAll(source, "<!-- Code generated by cmd/docsgen. DO NOT EDIT. -->", ""))
}

func renderMarkdown(source string) (string, error) {
	md := goldmark.New(
		goldmark.WithExtensions(extension.GFM, extension.DefinitionList),
		goldmark.WithParserOptions(parser.WithAutoHeadingID()),
		goldmark.WithRendererOptions(ghtml.WithUnsafe()),
	)
	var buf bytes.Buffer
	if err := md.Convert([]byte(source), &buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func transformDirectives(source string) (string, string, error) {
	lines := strings.Split(source, "\n")
	htmlOut, mirrorOut, next, err := parseDirectiveLines(lines, 0)
	if err != nil {
		return "", "", err
	}
	if next != len(lines) {
		return "", "", fmt.Errorf("unexpected directive parse stop")
	}
	return strings.Join(htmlOut, "\n"), strings.Join(mirrorOut, "\n"), nil
}

func parseDirectiveLines(lines []string, start int) ([]string, []string, int, error) {
	htmlOut := make([]string, 0)
	mirrorOut := make([]string, 0)

	for i := start; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == ":::" {
			return htmlOut, mirrorOut, i + 1, nil
		}
		if strings.HasPrefix(trimmed, ":::") {
			name, attrs := parseDirectiveHeader(strings.TrimPrefix(trimmed, ":::"))
			childHTMLLines, childMirrorLines, next, err := parseDirectiveLines(lines, i+1)
			if err != nil {
				return nil, nil, 0, err
			}
			childHTML, err := renderMarkdown(strings.Join(childHTMLLines, "\n"))
			if err != nil {
				return nil, nil, 0, err
			}
			renderedHTML, renderedMirror := renderDirective(name, attrs, childHTML, strings.Join(childMirrorLines, "\n"))
			htmlOut = append(htmlOut, renderedHTML)
			mirrorOut = append(mirrorOut, renderedMirror)
			i = next - 1
			continue
		}
		htmlOut = append(htmlOut, lines[i])
		mirrorOut = append(mirrorOut, lines[i])
	}

	return htmlOut, mirrorOut, len(lines), nil
}

func parseDirectiveHeader(header string) (string, map[string]string) {
	attrs := map[string]string{}
	header = strings.TrimSpace(header)
	if header == "" {
		return "", attrs
	}

	name := header
	if idx := strings.IndexFunc(header, unicode.IsSpace); idx >= 0 {
		name = header[:idx]
		attrSource := strings.TrimSpace(header[idx+1:])
		attrRE := regexp.MustCompile(`([A-Za-z0-9_-]+)=(".*?"|'.*?'|[^\s]+)`)
		for _, match := range attrRE.FindAllStringSubmatch(attrSource, -1) {
			if len(match) != 3 {
				continue
			}
			attrs[match[1]] = strings.Trim(match[2], `"'`)
		}
	}
	return name, attrs
}

func renderDirective(name string, attrs map[string]string, childHTML, childMirror string) (string, string) {
	switch name {
	case "callout":
		title := attrs["title"]
		if title == "" {
			title = strings.Title(attrs["kind"])
		}
		htmlBlock := `<div class="site-callout">`
		if title != "" {
			htmlBlock += `<div class="mb-2 text-sm font-semibold">` + html.EscapeString(title) + `</div>`
		}
		htmlBlock += childHTML + `</div>`
		mirror := ""
		if title != "" {
			mirror = "## " + title + "\n\n"
		}
		mirror += strings.TrimSpace(childMirror)
		return htmlBlock, mirror
	case "details":
		title := attrs["title"]
		if title == "" {
			title = "Details"
		}
		htmlBlock := `<details class="site-card"><summary class="cursor-pointer font-semibold">` + html.EscapeString(title) + `</summary><div class="mt-4">` + childHTML + `</div></details>`
		return htmlBlock, "## " + title + "\n\n" + strings.TrimSpace(childMirror)
	case "steps":
		htmlBlock := `<div class="site-card">` + childHTML + `</div>`
		return htmlBlock, strings.TrimSpace(childMirror)
	case "card":
		title := attrs["title"]
		href := attrs["href"]
		htmlBlock := `<div class="site-card">`
		if href != "" {
			htmlBlock += `<a class="no-underline" href="` + html.EscapeString(href) + `">`
		}
		if title != "" {
			htmlBlock += `<div class="text-lg font-semibold">` + html.EscapeString(title) + `</div>`
		}
		htmlBlock += childHTML
		if href != "" {
			htmlBlock += `</a>`
		}
		htmlBlock += `</div>`
		mirror := "## " + title + "\n\n" + strings.TrimSpace(childMirror)
		if href != "" {
			mirror += "\n\nLink: " + href
		}
		return htmlBlock, mirror
	case "card-group":
		return `<div class="site-card-grid">` + childHTML + `</div>`, strings.TrimSpace(childMirror)
	default:
		return childHTML, childMirror
	}
}

func routeForRelPath(relPath string) (pageKind, string) {
	if relPath == "index.md" {
		return pageKindHome, "/"
	}
	if strings.HasPrefix(relPath, "reference/generated/api/") {
		clean := strings.TrimPrefix(relPath, "reference/generated/api/")
		if clean == "index.md" {
			return pageKindAPI, "/api-reference/"
		}
		if strings.HasSuffix(clean, "/index.md") {
			return pageKindAPI, "/api-reference/" + strings.TrimSuffix(strings.TrimSuffix(clean, "index.md"), "/") + "/"
		}
		return pageKindAPI, "/api-reference/" + strings.TrimSuffix(clean, filepath.Ext(clean)) + "/"
	}

	if relPath == "start-here/index.md" {
		return pageKindDocs, "/docs/"
	}
	clean := strings.TrimSuffix(relPath, filepath.Ext(relPath))
	if strings.HasSuffix(clean, "/index") {
		clean = strings.TrimSuffix(clean, "/index")
	}
	return pageKindDocs, "/docs/" + clean + "/"
}

func sectionForPage(relPath string, kind pageKind) string {
	if kind == pageKindAPI {
		return "api-reference"
	}
	if relPath == "index.md" {
		return "home"
	}
	return strings.Split(relPath, "/")[0]
}

func buildConfiguredGroups(configs []navGroupConfig, pageByRel map[string]page, pages []page) ([]navGroup, []navItem, error) {
	groups := make([]navGroup, 0, len(configs))
	flat := make([]navItem, 0)

	for _, groupCfg := range configs {
		group := navGroup{Title: groupCfg.Title, Icon: groupCfg.Icon, Open: groupCfg.Open}
		nodes, nodeFlat, err := buildNavNodes(groupCfg.Items, pageByRel, pages)
		if err != nil {
			return nil, nil, err
		}
		group.Nodes = nodes
		flat = append(flat, nodeFlat...)
		groups = append(groups, group)
	}

	return groups, flat, nil
}

func buildNavNodes(configs []navEntryConfig, pageByRel map[string]page, pages []page) ([]navNode, []navItem, error) {
	nodes := make([]navNode, 0)
	flat := make([]navItem, 0)

	for _, cfg := range configs {
		switch {
		case cfg.Source != "":
			p, ok := pageByRel[cfg.Source]
			if !ok {
				return nil, nil, fmt.Errorf("navigation source not found: %s", cfg.Source)
			}
			title := p.Title
			if strings.TrimSpace(cfg.Title) != "" {
				title = cfg.Title
			}
			nodes = append(nodes, navNode{Title: title, Icon: cfg.Icon, Path: p.URLPath, ForceOpen: cfg.Open})
			flat = append(flat, navItem{Title: title, Path: p.URLPath})
		case cfg.AutoDir != "":
			children := pagesInDir(pages, cfg.AutoDir)
			if len(children) == 0 {
				continue
			}
			node := navNode{Title: cfg.Title, Icon: cfg.Icon, ForceOpen: cfg.Open}
			for _, p := range children {
				if p.Kind == pageKindAPI && strings.HasPrefix(p.RelPath, "reference/generated/api/endpoints/") {
					node.Children = append(node.Children, apiEndpointNavNode(p))
				} else {
					node.Children = append(node.Children, navNode{Title: p.Title, Path: p.URLPath})
				}
				flat = append(flat, navItem{Title: p.Title, Path: p.URLPath})
			}
			nodes = append(nodes, node)
		case len(cfg.Items) > 0:
			children, childFlat, err := buildNavNodes(cfg.Items, pageByRel, pages)
			if err != nil {
				return nil, nil, err
			}
			nodes = append(nodes, navNode{Title: cfg.Title, Icon: cfg.Icon, ForceOpen: cfg.Open, Children: children})
			flat = append(flat, childFlat...)
		}
	}

	return nodes, flat, nil
}

func pagesInDir(pages []page, dir string) []page {
	matches := make([]page, 0)
	prefix := strings.TrimSuffix(dir, "/") + "/"
	for _, p := range pages {
		if strings.HasPrefix(p.RelPath, prefix) {
			rel := strings.TrimPrefix(p.RelPath, prefix)
			if strings.Contains(rel, "/") {
				continue
			}
			matches = append(matches, p)
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Title < matches[j].Title
	})
	return matches
}

func buildTopNav(configs []navLinkConfig, currentPath string) []navItem {
	items := make([]navItem, 0, len(configs))
	for _, cfg := range configs {
		active := currentPath == cfg.Path || strings.HasPrefix(currentPath, cfg.Path)
		items = append(items, navItem{Title: cfg.Title, Path: cfg.Path, Active: active})
	}
	return items
}

func activateGroups(groups []navGroup, currentPath string) []navGroup {
	out := make([]navGroup, 0, len(groups))
	for _, group := range groups {
		next := navGroup{Title: group.Title, Icon: group.Icon, Open: group.Open}
		next.Nodes, next.Open = activateNodes(group.Nodes, currentPath)
		next.Open = next.Open || group.Open
		out = append(out, next)
	}
	return out
}

func activateNodes(nodes []navNode, currentPath string) ([]navNode, bool) {
	out := make([]navNode, 0, len(nodes))
	anyOpen := false
	for _, node := range nodes {
		next := node
		if len(node.Children) == 0 {
			next.Active = node.Path == currentPath
			next.Open = next.Active || node.ForceOpen
		} else {
			children, open := activateNodes(node.Children, currentPath)
			next.Children = children
			next.Active = node.Path == currentPath
			next.Open = open || next.Active || node.ForceOpen
		}
		if next.Open || next.Active {
			anyOpen = true
		}
		out = append(out, next)
	}
	return out, anyOpen
}

func apiEndpointNavNode(p page) navNode {
	children := []navNode{
		{Title: "Overview", Path: p.URLPath},
	}
	children = append(children, apiOperationNodes(p)...)
	return navNode{
		Title:       trimAPINavTitle(p.Title),
		Icon:        "folder",
		Path:        p.URLPath,
		ForceOpen:   true,
		Children:    children,
		Description: p.Description,
	}
}

func apiOperationNodes(p page) []navNode {
	opRE := regexp.MustCompile("(?m)^## `([A-Z]+) ([^`]+)`\\n\\n([^\\n]+)")
	matches := opRE.FindAllStringSubmatch(p.BodyMarkdown, -1)
	nodes := make([]navNode, 0, len(matches))
	for _, match := range matches {
		if len(match) != 4 {
			continue
		}
		method := strings.TrimSpace(match[1])
		routePath := strings.TrimSpace(match[2])
		description := strings.TrimSpace(match[3])
		title := routePath
		if description == "" {
			title = method + " " + routePath
		}
		nodes = append(nodes, navNode{
			Title:       title,
			Path:        p.URLPath + "#" + slug(method+" "+routePath),
			Method:      method,
			RoutePath:   routePath,
			Description: description,
		})
	}
	return nodes
}

func trimAPINavTitle(title string) string {
	title = strings.TrimSpace(title)
	title = strings.TrimSuffix(title, " Endpoints")
	title = strings.TrimSuffix(title, " Endpoint")
	return strings.TrimSpace(title)
}

func navIconSVG(name string) template.HTML {
	switch strings.TrimSpace(strings.ToLower(name)) {
	case "rocket":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 16.5c-1.5 1.5-1.5 4.5-1.5 4.5s3 0 4.5-1.5c.84-.84 1.14-2.06.9-3.18L7.68 15.6c-1.12-.24-2.34.06-3.18.9Z"></path><path d="m12 15-3-3a33.76 33.76 0 0 1 6.01-8.29 2.18 2.18 0 0 1 3 0 2.18 2.18 0 0 1 0 3A33.76 33.76 0 0 1 12 15Z"></path><path d="M9 12H4a2 2 0 0 0-2 2v1"></path><path d="M12 9V4a2 2 0 0 1 2-2h1"></path></svg>`)
	case "book-open":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 7v14"></path><path d="M3 18V6a2 2 0 0 1 2-2h7v16H5a2 2 0 0 0-2 2"></path><path d="M21 18V6a2 2 0 0 0-2-2h-7v16h7a2 2 0 0 1 2 2"></path></svg>`)
	case "blocks":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="1"></rect><rect x="14" y="3" width="7" height="7" rx="1"></rect><rect x="14" y="14" width="7" height="7" rx="1"></rect><rect x="3" y="14" width="7" height="7" rx="1"></rect></svg>`)
	case "wrench":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a4 4 0 0 0 5 5l-8.4 8.4a2 2 0 1 1-2.8-2.8l8.4-8.4a4 4 0 0 0-5-5l3 3-3.5 3.5-3-3Z"></path></svg>`)
	case "library":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m16 6 4 14"></path><path d="M12 6v14"></path><path d="M8 8v12"></path><path d="M4 4v16"></path></svg>`)
	case "folders":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 19a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.93l-.81-1.21A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2Z"></path><path d="M2 10h20"></path></svg>`)
	case "folder":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.93l-.81-1.21A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z"></path></svg>`)
	case "route":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="6" cy="19" r="2"></circle><circle cx="18" cy="5" r="2"></circle><path d="M12 19h4a2 2 0 0 0 2-2V7"></path><path d="M6 17V9a2 2 0 0 1 2-2h8"></path></svg>`)
	case "database":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"></ellipse><path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5"></path><path d="M3 12c0 1.66 4.03 3 9 3s9-1.34 9-3"></path></svg>`)
	case "file-json":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8Z"></path><path d="M14 2v6h6"></path><path d="M10 12H8"></path><path d="M16 12h-2"></path><path d="M10 18H8"></path><path d="M16 18h-2"></path></svg>`)
	default:
		return ""
	}
}

func buildBreadcrumbs(p page) []navItem {
	switch p.Kind {
	case pageKindAPI:
		return []navItem{
			{Title: "Home", Path: "/"},
			{Title: "API Reference", Path: "/api-reference/"},
		}
	case pageKindDocs:
		return []navItem{
			{Title: "Home", Path: "/"},
			{Title: "Docs", Path: "/docs/"},
		}
	default:
		return []navItem{{Title: "Home", Path: "/"}}
	}
}

func tocForPage(p page) []heading {
	if p.Kind != pageKindDocs {
		return nil
	}
	filtered := make([]heading, 0)
	for _, item := range p.Headings {
		if item.Level >= 2 && item.Level <= 3 {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func findNeighbor(items []navItem, path string, offset int) *navItem {
	for i := range items {
		if items[i].Path != path {
			continue
		}
		j := i + offset
		if j < 0 || j >= len(items) {
			return nil
		}
		item := items[j]
		return &item
	}
	return nil
}

func metaTitle(siteTitle, pageTitle string, isHome bool) string {
	if isHome {
		return siteTitle
	}
	return pageTitle + " | " + siteTitle
}

func firstHeading(source string) string {
	for _, line := range strings.Split(source, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "# ") {
			return strings.TrimSpace(strings.TrimPrefix(trimmed, "# "))
		}
	}
	return ""
}

func firstParagraph(source string) string {
	var lines []string
	for _, line := range strings.Split(source, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			if len(lines) > 0 {
				break
			}
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		lines = append(lines, trimmed)
	}
	return strings.Join(lines, " ")
}

func extractHeadings(source string) []heading {
	headings := make([]heading, 0)
	for _, line := range strings.Split(source, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "#") {
			continue
		}
		level := 0
		for _, r := range trimmed {
			if r == '#' {
				level++
				continue
			}
			break
		}
		title := strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
		if level == 0 || title == "" {
			continue
		}
		headings = append(headings, heading{Level: level, Title: title, ID: slug(title)})
	}
	return headings
}

func headingTitles(items []heading) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.Title)
	}
	return out
}

func plainText(source string) string {
	replacer := strings.NewReplacer("`", " ", "*", " ", "#", " ", "[", " ", "]", " ", "(", " ", ")", " ")
	clean := replacer.Replace(source)
	return strings.Join(strings.Fields(clean), " ")
}

func addHeadingAnchors(htmlSource string) string {
	headingRE := regexp.MustCompile(`<h([1-6])>(.*?)</h[1-6]>`)
	return headingRE.ReplaceAllStringFunc(htmlSource, func(match string) string {
		parts := headingRE.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		title := stripHTML(parts[2])
		return `<h` + parts[1] + ` id="` + slug(title) + `">` + parts[2] + `</h` + parts[1] + `>`
	})
}

func enhanceAPIHTML(htmlSource string) string {
	opRE := regexp.MustCompile(`<h2 id="([^"]+)"><code>(GET|POST|PUT|PATCH|DELETE) ([^<]+)</code></h2>`)
	return opRE.ReplaceAllStringFunc(htmlSource, func(match string) string {
		parts := opRE.FindStringSubmatch(match)
		if len(parts) != 4 {
			return match
		}
		method := parts[2]
		routePath := parts[3]
		return `<h2 id="` + parts[1] + `"><span class="api-method" data-api-method="` + method + `">` + method + `</span><span class="api-path">` + routePath + `</span></h2>`
	})
}

func enhanceCodeBlocks(htmlSource string) string {
	codeRE := regexp.MustCompile(`(?s)<pre><code(?: class="([^"]*)")?>(.*?)</code></pre>`)
	index := 0

	return codeRE.ReplaceAllStringFunc(htmlSource, func(match string) string {
		parts := codeRE.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}

		language := codeBlockLanguage(parts[1])
		codeID := fmt.Sprintf("site-codeblock-%d", index)
		index++

		var b strings.Builder
		b.WriteString(`<div class="site-codeblock">`)
		b.WriteString(`<div class="site-codeblock-bar">`)
		b.WriteString(`<span class="site-codeblock-lang">`)
		b.WriteString(html.EscapeString(language))
		b.WriteString(`</span>`)
		b.WriteString(`<button type="button" class="site-codeblock-copy" data-site-copy="#`)
		b.WriteString(codeID)
		b.WriteString(`">Copy</button>`)
		b.WriteString(`</div>`)
		b.WriteString(`<pre><code`)
		if parts[1] != "" {
			b.WriteString(` class="`)
			b.WriteString(html.EscapeString(parts[1]))
			b.WriteString(`"`)
		}
		b.WriteString(` id="`)
		b.WriteString(codeID)
		b.WriteString(`">`)
		b.WriteString(parts[2])
		b.WriteString(`</code></pre></div>`)
		return b.String()
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

func rewriteContentLinks(source string) string {
	replacements := []struct {
		old string
		new string
	}{
		{old: "](/start-here/", new: "](/docs/start-here/"},
		{old: "](/how-to/", new: "](/docs/how-to/"},
		{old: "](/core-concepts/", new: "](/docs/core-concepts/"},
		{old: "](/operations/", new: "](/docs/operations/"},
		{old: "](/reference/generated/api/", new: "](/api-reference/"},
		{old: "](/reference/generated/declarative/", new: "](/docs/reference/generated/declarative/"},
		{old: "](/reference/", new: "](/docs/reference/"},
		{old: "](/browser-local-compute", new: "](/docs/browser-local-compute/"},
		{old: "](/architecture-governance", new: "](/docs/architecture-governance/"},
		{old: "\"/start-here/", new: "\"/docs/start-here/"},
		{old: "\"/how-to/", new: "\"/docs/how-to/"},
		{old: "\"/core-concepts/", new: "\"/docs/core-concepts/"},
		{old: "\"/operations/", new: "\"/docs/operations/"},
		{old: "\"/reference/generated/api/", new: "\"/api-reference/"},
		{old: "\"/reference/generated/declarative/", new: "\"/docs/reference/generated/declarative/"},
		{old: "\"/reference/", new: "\"/docs/reference/"},
		{old: "\"/browser-local-compute", new: "\"/docs/browser-local-compute/"},
		{old: "\"/architecture-governance", new: "\"/docs/architecture-governance/"},
	}
	for _, replacement := range replacements {
		source = strings.ReplaceAll(source, replacement.old, replacement.new)
	}
	return source
}

func rewriteLink(link string) string {
	return strings.TrimSuffix(strings.TrimPrefix(rewriteContentLinks(`"`+link+`"`), `"`), `"`)
}

func slug(source string) string {
	var out []rune
	lastDash := false
	for _, r := range strings.ToLower(source) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			out = append(out, r)
			lastDash = false
			continue
		}
		if lastDash {
			continue
		}
		out = append(out, '-')
		lastDash = true
	}
	return strings.Trim(string(out), "-")
}
