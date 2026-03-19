package sitegen

import (
	"fmt"
	"html/template"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

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

var _ = template.HTML("")
