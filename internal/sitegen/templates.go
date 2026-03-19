package sitegen

import (
	"bytes"
	"fmt"
	"html/template"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

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

func executeNamedTemplate(tmpl *template.Template, name string, data any) (string, error) {
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		return "", err
	}
	return buf.String(), nil
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

func prefixHTML(source template.HTML, siteRoot string) template.HTML {
	// #nosec G203 -- source is already trusted repository-owned rendered HTML and only has internal URLs prefixed here.
	return template.HTML(prefixSiteRootInHTML(string(source), siteRoot))
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
