package sitegen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

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

	pages, err := loadPages(b.ContentDir, templates)
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
			BodyHTML:      prefixHTML(p.BodyHTML, siteRoot),
			Home:          p.Home,
			HomeView:      buildHomeView(p.Home),
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
