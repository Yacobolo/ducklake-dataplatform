// Package sitegen builds the static public site from markdown content and templates.
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
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	chromahtml "github.com/alecthomas/chroma/v2/formatters/html"
	"github.com/yuin/goldmark"
	highlighting "github.com/yuin/goldmark-highlighting/v2"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	ghtml "github.com/yuin/goldmark/renderer/html"
	"gopkg.in/yaml.v3"
)

// Builder assembles the static site from content, config, templates, and assets.
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
	Title    string           `yaml:"title"`
	Icon     string           `yaml:"icon"`
	Expanded bool             `yaml:"expanded"`
	Items    []navEntryConfig `yaml:"items"`
}

type navEntryConfig struct {
	Source   string           `yaml:"source"`
	Title    string           `yaml:"title"`
	Icon     string           `yaml:"icon"`
	Expanded bool             `yaml:"expanded"`
	AutoDir  string           `yaml:"autogen_dir"`
	Items    []navEntryConfig `yaml:"items"`
}

type docFrontMatter struct {
	Title       string   `yaml:"title"`
	Description string   `yaml:"description"`
	Keywords    []string `yaml:"keywords"`
}

type homeFrontMatter struct {
	Layout   string      `yaml:"layout"`
	Hero     homeHero    `yaml:"hero"`
	Pillars  homeSection `yaml:"pillars"`
	Title    string      `yaml:"title"`
	Keywords []string    `yaml:"keywords"`
}

type homeHero struct {
	Name          string             `yaml:"name"`
	Eyebrow       string             `yaml:"eyebrow"`
	Text          string             `yaml:"text"`
	Headline      string             `yaml:"headline"`
	Tagline       string             `yaml:"tagline"`
	SnapshotTitle string             `yaml:"snapshot_title"`
	SnapshotNote  string             `yaml:"snapshot_note"`
	Actions       []homeHeroLink     `yaml:"actions"`
	Proofs        []homeHeroProof    `yaml:"proofs"`
	Snapshot      []homeHeroSnapshot `yaml:"snapshot"`
}

type homeHeroLink struct {
	Theme string `yaml:"theme"`
	Text  string `yaml:"text"`
	Link  string `yaml:"link"`
}

type homeHeroProof struct {
	Icon string `yaml:"icon"`
	Text string `yaml:"text"`
}

type homeHeroSnapshot struct {
	Icon  string `yaml:"icon"`
	Label string `yaml:"label"`
	Value string `yaml:"value"`
}

type homeSection struct {
	Eyebrow string        `yaml:"eyebrow"`
	Title   string        `yaml:"title"`
	Details string        `yaml:"details"`
	Items   []homeFeature `yaml:"items"`
}

type homeFeature struct {
	Label   string `yaml:"label"`
	Title   string `yaml:"title"`
	Details string `yaml:"details"`
	Link    string `yaml:"link"`
	Icon    string `yaml:"icon"`
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
	Level  int
	ID     string
	Title  string
	Method string
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
	SiteRoot      string
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

// Build renders the full static site into the configured output directory.
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
	if err := os.MkdirAll(b.OutDir, 0o750); err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	if err := copySiteAssets(b.AssetsDir, b.OutDir); err != nil {
		return err
	}
	siteRoot := normalizeSiteRoot(b.BaseURL)

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
			SiteRoot:      siteRoot,
			Page:          p,
			MetaTitle:     metaTitle(cfg.Title, p.Title, p.IsHome),
			TopNav:        buildTopNav(nav.Primary, p.URLPath),
			SidebarGroups: prefixNavGroups(activateGroups(sidebar, p.URLPath), siteRoot),
			TOC:           tocForPage(p),
			Breadcrumbs:   buildBreadcrumbs(p),
			Prev:          findNeighbor(flat, p.URLPath, -1),
			Next:          findNeighbor(flat, p.URLPath, 1),
			// #nosec G203 -- BodyHTML is generated from repository-owned markdown, then only URL-prefixed here.
			BodyHTML:   template.HTML(prefixSiteRootInHTML(string(p.BodyHTML), siteRoot)),
			Home:       p.Home,
			MirrorPath: p.MirrorPath,
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
	if err := writeLLMSTXT(b.OutDir, cfg, pages, siteRoot); err != nil {
		return err
	}
	if err := writeSitemap(b.OutDir, b.BaseURL, pages); err != nil {
		return err
	}

	return nil
}

// Serve exposes the generated site locally with conservative HTTP timeouts.
func Serve(addr, outDir string) error {
	server := &http.Server{
		Addr:              addr,
		Handler:           http.FileServer(http.Dir(outDir)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return server.ListenAndServe()
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
	paths := make([]string, 0, 16)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || filepath.Ext(path) != ".tmpl" {
			return nil
		}
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("rel template path %s: %w", path, err)
		}
		paths = append(paths, filepath.ToSlash(relPath))
		return nil
	})
	if err != nil {
		return templateSet{}, fmt.Errorf("walk templates: %w", err)
	}
	sort.Strings(paths)

	tmpl, err := template.New("base").Funcs(template.FuncMap{
		"cond":         tmplCond,
		"dict":         tmplDict,
		"navIcon":      navIconSVG,
		"sectionLabel": pageSectionLabel,
		"siteURL":      joinSiteURL,
	}).ParseFS(os.DirFS(root), paths...)
	if err != nil {
		return templateSet{}, fmt.Errorf("parse templates: %w", err)
	}
	return templateSet{base: tmpl}, nil
}

func tmplCond(condition bool, whenTrue, whenFalse any) any {
	if condition {
		return whenTrue
	}
	return whenFalse
}

func tmplDict(values ...any) (map[string]any, error) {
	if len(values)%2 != 0 {
		return nil, fmt.Errorf("dict expects an even number of arguments")
	}
	out := make(map[string]any, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			return nil, fmt.Errorf("dict key at position %d must be a string, got %T", i, values[i])
		}
		out[key] = values[i+1]
	}
	return out, nil
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
		if strings.TrimSpace(home.Hero.Headline) == "" {
			home.Hero.Headline = strings.TrimSpace(home.Hero.Text)
		}
		if strings.TrimSpace(home.Hero.Name) == "" {
			home.Hero.Name = strings.TrimSpace(home.Hero.Headline)
		}
		for i := range home.Hero.Actions {
			home.Hero.Actions[i].Link = rewriteLink(home.Hero.Actions[i].Link)
		}
		for i := range home.Pillars.Items {
			home.Pillars.Items[i].Link = rewriteLink(home.Pillars.Items[i].Link)
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
	rendered = enhanceTables(rendered)
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
		// #nosec G203 -- rendered markdown is limited to repository-owned docs and generated content.
		BodyHTML: template.HTML(rendered),
		Headings: extractHeadings(mirror),
		Keywords: fm.Keywords,
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
	if err := os.MkdirAll(targetDir, 0o750); err != nil {
		return fmt.Errorf("mkdir page dir: %w", err)
	}
	return os.WriteFile(filepath.Join(targetDir, "index.html"), buf.Bytes(), 0o600)
}

func writeMirrorMarkdown(outDir string, p page) error {
	target := filepath.Join(outDir, "llms", filepath.FromSlash(p.MirrorPath))
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return fmt.Errorf("mkdir llms dir: %w", err)
	}

	var content strings.Builder
	content.WriteString("# ")
	content.WriteString(p.Title)
	content.WriteString("\n\n")
	if p.IsHome {
		writeHomeMirror(&content, p.Home)
	} else {
		if p.Description != "" {
			content.WriteString(p.Description)
			content.WriteString("\n\n")
		}
		content.WriteString(strings.TrimSpace(p.MirrorBody))
		content.WriteString("\n")
	}

	return os.WriteFile(target, []byte(content.String()), 0o600)
}

func writeHomeMirror(content *strings.Builder, home homeFrontMatter) {
	if headline := strings.TrimSpace(home.Hero.Headline); headline != "" {
		content.WriteString(headline)
		content.WriteString("\n\n")
	}
	if tagline := strings.TrimSpace(home.Hero.Tagline); tagline != "" {
		content.WriteString(tagline)
		content.WriteString("\n\n")
	}

	writeHomeMirrorSection(content, home.Pillars)
}

func writeHomeMirrorSection(content *strings.Builder, section homeSection) {
	if strings.TrimSpace(section.Title) == "" {
		return
	}
	content.WriteString("## ")
	content.WriteString(section.Title)
	content.WriteString("\n\n")
	if details := strings.TrimSpace(section.Details); details != "" {
		content.WriteString(details)
		content.WriteString("\n\n")
	}
	for _, item := range section.Items {
		title := strings.TrimSpace(item.Title)
		if title == "" {
			continue
		}
		content.WriteString("### ")
		content.WriteString(title)
		content.WriteString("\n\n")
		if details := strings.TrimSpace(item.Details); details != "" {
			content.WriteString(details)
			content.WriteString("\n\n")
		}
	}
}

func writeSearchIndex(outDir string, items []searchItem) error {
	payload, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal search index: %w", err)
	}
	return os.WriteFile(filepath.Join(outDir, "search-index.json"), append(payload, '\n'), 0o600)
}

func writeLLMSTXT(outDir string, cfg siteConfig, pages []page, siteRoot string) error {
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
		b.WriteString(": ")
		b.WriteString(joinSiteURL(siteRoot, "/llms/"+p.MirrorPath))
		b.WriteString("\n")
	}
	return os.WriteFile(filepath.Join(outDir, "llms.txt"), []byte(b.String()), 0o600)
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
	return os.WriteFile(filepath.Join(outDir, "sitemap.xml"), []byte(b.String()), 0o600)
}

func copySiteAssets(assetsDir, outDir string) error {
	staticDir := filepath.Join(outDir, "_site")
	if err := os.MkdirAll(staticDir, 0o750); err != nil {
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
	if err := copyDir(filepath.Join(assetsDir, "diagrams"), filepath.Join(staticDir, "diagrams")); err != nil {
		return err
	}
	return nil
}

func copyFile(source, target string) error {
	in, err := os.Open(filepath.Clean(source))
	if err != nil {
		return fmt.Errorf("open %s: %w", source, err)
	}
	defer func() {
		_ = in.Close()
	}()

	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return fmt.Errorf("mkdir %s: %w", target, err)
	}

	out, err := os.OpenFile(filepath.Clean(target), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", target, err)
	}
	defer func() {
		_ = out.Close()
	}()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy %s: %w", source, err)
	}
	return nil
}

func copyDir(source, target string) error {
	info, err := os.Stat(filepath.Clean(source))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat %s: %w", source, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", source)
	}

	return filepath.WalkDir(source, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(source, path)
		if err != nil {
			return fmt.Errorf("rel path %s: %w", path, err)
		}
		return copyFile(path, filepath.Join(target, relPath))
	})
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

func normalizeSiteRoot(baseURL string) string {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return ""
	}
	if strings.HasPrefix(baseURL, "/") {
		return trimTrailingSlash(baseURL)
	}

	parsed, err := url.Parse(baseURL)
	if err != nil {
		return ""
	}
	return trimTrailingSlash(parsed.Path)
}

func trimTrailingSlash(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || path == "/" {
		return ""
	}
	return strings.TrimRight(path, "/")
}

func joinSiteURL(siteRoot, target string) string {
	target = strings.TrimSpace(target)
	if target == "" {
		if siteRoot == "" {
			return "/"
		}
		return siteRoot + "/"
	}
	if strings.HasPrefix(target, "#") {
		return target
	}
	lower := strings.ToLower(target)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") || strings.HasPrefix(lower, "mailto:") || strings.HasPrefix(lower, "tel:") {
		return target
	}
	if siteRoot == "" {
		if strings.HasPrefix(target, "/") {
			return target
		}
		return "/" + strings.TrimLeft(target, "/")
	}
	if strings.HasPrefix(target, "/") {
		return siteRoot + target
	}
	return siteRoot + "/" + strings.TrimLeft(target, "/")
}

func prefixSiteRootInHTML(source, siteRoot string) string {
	if siteRoot == "" {
		return source
	}
	attrRE := regexp.MustCompile(`\b(href|src)=(")(/[^"]*)"`)
	return attrRE.ReplaceAllStringFunc(source, func(match string) string {
		parts := attrRE.FindStringSubmatch(match)
		if len(parts) != 4 {
			return match
		}
		return parts[1] + `="` + joinSiteURL(siteRoot, parts[3]) + `"`
	})
}

func prefixNavGroups(groups []navGroup, siteRoot string) []navGroup {
	prefixed := make([]navGroup, len(groups))
	for i, group := range groups {
		prefixed[i] = group
		prefixed[i].Nodes = prefixNavNodes(group.Nodes, siteRoot)
	}
	return prefixed
}

func prefixNavNodes(nodes []navNode, siteRoot string) []navNode {
	prefixed := make([]navNode, len(nodes))
	for i, node := range nodes {
		prefixed[i] = node
		if node.Path != "" {
			prefixed[i].Path = joinSiteURL(siteRoot, node.Path)
		}
		if len(node.Children) > 0 {
			prefixed[i].Children = prefixNavNodes(node.Children, siteRoot)
		}
	}
	return prefixed
}

func hasAttr(attrs, name string) bool {
	pattern := regexp.MustCompile(`(?:^|\s)` + regexp.QuoteMeta(name) + `=`)
	return pattern.FindStringIndex(attrs) != nil
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
			title = directiveKindTitle(attrs["kind"])
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
	clean = strings.TrimSuffix(clean, "/index")
	return pageKindDocs, "/docs/" + clean + "/"
}

func directiveKindTitle(kind string) string {
	if kind == "" {
		return ""
	}
	parts := strings.FieldsFunc(kind, func(r rune) bool {
		return r == '-' || r == '_' || unicode.IsSpace(r)
	})
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		runes := []rune(strings.ToLower(parts[i]))
		runes[0] = unicode.ToUpper(runes[0])
		parts[i] = string(runes)
	}
	return strings.Join(parts, " ")
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

func pageSectionLabel(p page) string {
	if p.Kind == pageKindAPI {
		switch {
		case strings.Contains(p.RelPath, "reference/generated/api/endpoints/"):
			return "Endpoint Groups"
		case strings.Contains(p.RelPath, "reference/generated/api/schemas/"):
			return "Models"
		default:
			return "API Reference"
		}
	}

	switch p.Section {
	case "start-here":
		return "Getting Started"
	case "concepts":
		return "Concepts"
	case "build":
		return "Build"
	case "govern":
		return "Govern"
	case "operations":
		return "Operate"
	case "reference":
		return "Reference"
	default:
		if p.Title != "" {
			return p.Title
		}
		return "Documentation"
	}
}

func buildConfiguredGroups(configs []navGroupConfig, pageByRel map[string]page, pages []page) ([]navGroup, []navItem, error) {
	groups := make([]navGroup, 0, len(configs))
	flat := make([]navItem, 0)

	for _, groupCfg := range configs {
		group := navGroup{Title: groupCfg.Title, Icon: groupCfg.Icon, Open: groupCfg.Expanded}
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
			nodes = append(nodes, navNode{Title: title, Icon: cfg.Icon, Path: p.URLPath, ForceOpen: cfg.Expanded})
			flat = append(flat, navItem{Title: title, Path: p.URLPath})
		case cfg.AutoDir != "":
			children, childFlat := buildAutogenNavNodes(pages, cfg.AutoDir, cfg.Icon, cfg.Expanded)
			if len(children) == 0 {
				continue
			}
			node := navNode{Title: cfg.Title, Icon: cfg.Icon, ForceOpen: cfg.Expanded}
			node.Children = children
			flat = append(flat, childFlat...)
			if strings.TrimSpace(cfg.Title) == "" {
				nodes = append(nodes, node.Children...)
			} else {
				nodes = append(nodes, node)
			}
		case len(cfg.Items) > 0:
			children, childFlat, err := buildNavNodes(cfg.Items, pageByRel, pages)
			if err != nil {
				return nil, nil, err
			}
			if strings.TrimSpace(cfg.Title) == "" {
				nodes = append(nodes, children...)
			} else {
				nodes = append(nodes, navNode{Title: cfg.Title, Icon: cfg.Icon, ForceOpen: cfg.Expanded, Children: children})
			}
			flat = append(flat, childFlat...)
		}
	}

	return nodes, flat, nil
}

func buildAutogenNavNodes(pages []page, dir, icon string, forceOpen bool) ([]navNode, []navItem) {
	entries := autogenEntriesInDir(pages, dir)
	nodes := make([]navNode, 0, len(entries))
	flat := make([]navItem, 0)
	for _, entry := range entries {
		if entry.Page != nil {
			node := navNodeForPage(*entry.Page, icon)
			node.ForceOpen = forceOpen
			nodes = append(nodes, node)
			flat = append(flat, navItem{Title: node.Title, Path: node.Path})
			continue
		}

		children, childFlat := buildAutogenNavNodes(pages, entry.Dir, icon, forceOpen)
		if entry.Index != nil {
			node := navNodeForPage(*entry.Index, icon)
			node.Children = children
			node.ForceOpen = forceOpen
			nodes = append(nodes, node)
			flat = append(flat, navItem{Title: node.Title, Path: node.Path})
		} else {
			nodes = append(nodes, navNode{
				Title:     humanizeNavSegment(filepath.Base(entry.Dir)),
				Icon:      icon,
				Children:  children,
				ForceOpen: forceOpen,
			})
		}
		flat = append(flat, childFlat...)
	}
	return nodes, flat
}

type autogenEntry struct {
	Page  *page
	Dir   string
	Index *page
	Title string
}

func autogenEntriesInDir(pages []page, dir string) []autogenEntry {
	entries := make([]autogenEntry, 0)
	prefix := strings.TrimSuffix(dir, "/") + "/"
	dirIndexes := make(map[string]page)
	dirSeen := make(map[string]struct{})
	for _, p := range pages {
		if strings.HasPrefix(p.RelPath, prefix) {
			rel := strings.TrimPrefix(p.RelPath, prefix)
			if rel == "" {
				continue
			}
			if rel == "index.md" {
				continue
			}
			if !strings.Contains(rel, "/") {
				pageCopy := p
				entries = append(entries, autogenEntry{Page: &pageCopy, Title: navTitleForPage(p)})
				continue
			}
			head, tail, _ := strings.Cut(rel, "/")
			if head == "" {
				continue
			}
			subdir := prefix + head
			if tail == "index.md" {
				dirIndexes[subdir] = p
			}
			dirSeen[subdir] = struct{}{}
		}
	}

	for subdir := range dirSeen {
		subdir := subdir
		var indexPtr *page
		if indexPage, ok := dirIndexes[subdir]; ok {
			indexCopy := indexPage
			indexPtr = &indexCopy
		}
		title := humanizeNavSegment(filepath.Base(subdir))
		if indexPtr != nil {
			title = navTitleForPage(*indexPtr)
		}
		entries = append(entries, autogenEntry{Dir: subdir, Index: indexPtr, Title: title})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Title < entries[j].Title
	})
	return entries
}

func navNodeForPage(p page, fallbackIcon string) navNode {
	if p.Kind == pageKindAPI && strings.HasPrefix(p.RelPath, "reference/generated/api/endpoints/") {
		node := apiEndpointNavNode(p)
		if node.Icon == "" {
			node.Icon = fallbackIcon
		}
		return node
	}
	return navNode{Title: p.Title, Icon: fallbackIcon, Path: p.URLPath}
}

func navTitleForPage(p page) string {
	if p.Kind == pageKindAPI && strings.HasPrefix(p.RelPath, "reference/generated/api/endpoints/") {
		return trimAPINavTitle(p.Title)
	}
	return p.Title
}

func humanizeNavSegment(value string) string {
	parts := strings.FieldsFunc(strings.TrimSpace(value), func(r rune) bool {
		return r == '-' || r == '_' || unicode.IsSpace(r)
	})
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		part := strings.ToLower(parts[i])
		runes := []rune(part)
		runes[0] = unicode.ToUpper(runes[0])
		parts[i] = string(runes)
	}
	return strings.Join(parts, " ")
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
	return navNode{
		Title:       trimAPINavTitle(p.Title),
		Icon:        apiEndpointIcon(p.RelPath),
		Path:        p.URLPath,
		Description: p.Description,
	}
}

func trimAPINavTitle(title string) string {
	title = strings.TrimSpace(title)
	title = strings.TrimSuffix(title, " Endpoints")
	title = strings.TrimSuffix(title, " Endpoint")
	return strings.TrimSpace(title)
}

func apiEndpointIcon(relPath string) string {
	name := strings.TrimSuffix(filepath.Base(relPath), filepath.Ext(relPath))
	switch name {
	case "assets":
		return "package"
	case "auth":
		return "shield"
	case "catalogs":
		return "boxes"
	case "compute":
		return "cpu"
	case "dashboards":
		return "layout"
	case "governance":
		return "badge-check"
	case "health":
		return "activity"
	case "identity":
		return "fingerprint"
	case "integrations":
		return "plug"
	case "lineage":
		return "git-branch"
	case "models":
		return "layers"
	case "notebooks":
		return "notebook"
	case "pipelines":
		return "workflow"
	case "queries":
		return "search-code"
	case "semantic-layer":
		return "network"
	case "storage":
		return "hard-drive"
	default:
		return "folder"
	}
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
	case "package":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m7.5 4.27 9 5.15"></path><path d="M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z"></path><path d="m3.3 7 8.7 5 8.7-5"></path><path d="M12 22V12"></path></svg>`)
	case "shield":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10Z"></path></svg>`)
	case "boxes":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 0 0-1-1.73l-6-3.43a2 2 0 0 0-2 0L6 6.27A2 2 0 0 0 5 8v8a2 2 0 0 0 1 1.73l6 3.43a2 2 0 0 0 2 0l6-3.43A2 2 0 0 0 21 16Z"></path><path d="M3.3 7 12 12l8.7-5"></path><path d="M12 22V12"></path><path d="m7.5 4.27 9 5.15"></path></svg>`)
	case "cpu":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="2"></rect><rect x="9" y="9" width="6" height="6"></rect><path d="M9 1v3"></path><path d="M15 1v3"></path><path d="M9 20v3"></path><path d="M15 20v3"></path><path d="M20 9h3"></path><path d="M20 14h3"></path><path d="M1 9h3"></path><path d="M1 14h3"></path></svg>`)
	case "layout":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2"></rect><path d="M3 9h18"></path><path d="M9 21V9"></path></svg>`)
	case "badge-check":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16.3 8.7a4 4 0 1 0-8.6 0A4 4 0 0 0 12 16a4 4 0 0 0 4.3-7.3Z"></path><path d="m9 12 2 2 4-4"></path><path d="m8.5 2.5 1 2.1"></path><path d="m14.5 2.5-1 2.1"></path><path d="m2.5 8.5 2.1 1"></path><path d="m19.4 9.5 2.1-1"></path><path d="m2.5 15.5 2.1-1"></path><path d="m19.4 14.5 2.1 1"></path><path d="m8.5 21.5 1-2.1"></path><path d="m14.5 21.5-1-2.1"></path></svg>`)
	case "activity":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 12h-4l-3 9L9 3l-3 9H2"></path></svg>`)
	case "fingerprint":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 11a4 4 0 0 1 4 4v2"></path><path d="M8 15a4 4 0 0 1 8 0v1"></path><path d="M12 3a9 9 0 0 0-9 9v1"></path><path d="M21 12a9 9 0 0 0-9-9"></path><path d="M3 17a6 6 0 0 0 6 6"></path><path d="M15 23a6 6 0 0 0 6-6v-1"></path></svg>`)
	case "plug":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22v-5"></path><path d="M9 8V2"></path><path d="M15 8V2"></path><path d="M18 8H6v5a6 6 0 0 0 12 0Z"></path></svg>`)
	case "git-branch":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="6" cy="6" r="3"></circle><path d="M6 9v12"></path><circle cx="18" cy="18" r="3"></circle><path d="M18 9a3 3 0 0 0-3-3H9"></path></svg>`)
	case "layers":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m12 2 9 5-9 5-9-5 9-5Z"></path><path d="m3 12 9 5 9-5"></path><path d="m3 17 9 5 9-5"></path></svg>`)
	case "notebook":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 2h8"></path><path d="M8 6h8"></path><path d="M8 10h8"></path><rect x="4" y="2" width="16" height="20" rx="2"></rect><path d="M8 14h5"></path></svg>`)
	case "workflow":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="6" height="6" rx="1"></rect><rect x="15" y="15" width="6" height="6" rx="1"></rect><rect x="15" y="3" width="6" height="6" rx="1"></rect><path d="M9 6h6"></path><path d="M18 9v6"></path><path d="M9 6v12h6"></path></svg>`)
	case "search-code":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="7"></circle><path d="m21 21-4.3-4.3"></path><path d="m10 8-3 3 3 3"></path><path d="m12 14 3-3-3-3"></path></svg>`)
	case "network":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="5" r="2"></circle><circle cx="5" cy="19" r="2"></circle><circle cx="19" cy="19" r="2"></circle><path d="M12 7v4"></path><path d="M12 11 6.5 17"></path><path d="M12 11 17.5 17"></path></svg>`)
	case "hard-drive":
		return template.HTML(`<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="22" y1="12" x2="2" y2="12"></line><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11Z"></path><line x1="6" y1="16" x2="6.01" y2="16"></line><line x1="10" y1="16" x2="10.01" y2="16"></line></svg>`)
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
	if p.Kind == pageKindHome {
		return nil
	}
	if p.Kind == pageKindAPI {
		return apiTOCForPage(p)
	}
	filtered := make([]heading, 0)
	for _, item := range p.Headings {
		if item.Level == 2 {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func apiTOCForPage(p page) []heading {
	opRE := regexp.MustCompile("(?m)^## `([A-Z]+) ([^`]+)`\\n\\n([^\\n]+)")
	matches := opRE.FindAllStringSubmatch(p.BodyMarkdown, -1)
	headings := make([]heading, 0, len(matches))
	for _, match := range matches {
		if len(match) != 4 {
			continue
		}
		method := strings.TrimSpace(match[1])
		routePath := strings.TrimSpace(match[2])
		title := strings.TrimSpace(match[3])
		if title == "" {
			title = routePath
		}
		headings = append(headings, heading{
			Level:  2,
			ID:     slug(method + " " + routePath),
			Title:  title,
			Method: method,
		})
	}
	if len(headings) > 0 {
		return headings
	}
	return p.Headings
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

	var out strings.Builder
	out.WriteString(`<section class="api-operation">`)
	out.WriteString(`<header class="api-operation-header">`)
	out.WriteString(`<h2 id="` + sectionID + `" class="api-operation-title"><span class="api-operation-title-text">` + titleHTML + `</span><span class="api-method" data-api-method="` + method + `">` + method + `</span></h2>`)
	out.WriteString(`<p class="api-operation-route"><code>` + html.EscapeString(routePath) + `</code></p>`)
	if descriptionHTML != "" {
		out.WriteString(`<p class="api-operation-description">` + descriptionHTML + `</p>`)
	}
	if operationID != "" {
		out.WriteString(`<div class="api-operation-meta"><span class="api-operation-meta-label">Operation ID</span><code>` + html.EscapeString(operationID) + `</code></div>`)
	}
	out.WriteString(`</header>`)
	if rest != "" {
		out.WriteString(rest)
	}
	out.WriteString(`</section>`)
	return out.String()
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
		b.WriteString(`<div class="site-codeblock">`)
		b.WriteString(`<div class="site-codeblock-bar">`)
		b.WriteString(`<span class="site-codeblock-lang">`)
		b.WriteString(html.EscapeString(language))
		b.WriteString(`</span>`)
		b.WriteString(`<button type="button" class="site-codeblock-copy" data-site-copy="#`)
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
		return `<div class="site-table-wrap">` + match + `</div>`
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
