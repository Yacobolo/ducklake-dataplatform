package ui

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

type navItem struct {
	Label string
	Href  string
	Key   string
	Icon  string
}

type workspaceAsideTab struct {
	ID         string
	Label      string
	Icon       string
	Count      string
	PanelClass string
	Content    Node
}

type catalogExplorerObjectItem struct {
	Name   string
	URL    string
	Icon   string
	Active bool
}

type catalogExplorerSchemaItem struct {
	Name      string
	URL       string
	Active    bool
	Open      bool
	Objects   []catalogExplorerObjectItem
	EmptyText string
}

type catalogExplorerCatalogItem struct {
	Name      string
	URL       string
	Active    bool
	Open      bool
	Schemas   []catalogExplorerSchemaItem
	EmptyText string
}

type catalogExplorerPanelData struct {
	Title             string
	FilterPlaceholder string
	Catalogs          []catalogExplorerCatalogItem
	NewCatalogURL     string
	EmptyCatalogsText string
}

var navItems = []navItem{
	{Label: "Overview", Href: "/ui", Key: "home", Icon: "house"},
	{Label: "SQL Editor", Href: "/ui/sql", Key: "sql", Icon: "square-terminal"},
	{Label: "Catalogs", Href: "/ui/catalogs", Key: "catalogs", Icon: "database"},
	{Label: "Pipelines", Href: "/ui/pipelines", Key: "pipelines", Icon: "workflow"},
	{Label: "Notebooks", Href: "/ui/notebooks", Key: "notebooks", Icon: "notebook-text"},
	{Label: "Macros", Href: "/ui/macros", Key: "macros", Icon: "braces"},
	{Label: "Models", Href: "/ui/models", Key: "models", Icon: "boxes"},
}

func appPage(title, active string, principal domain.ContextPrincipal, body ...Node) Node {
	nav := make([]Node, 0, len(navItems))
	for _, item := range navItems {
		className := "app-nav-link Link--secondary d-flex flex-items-center"
		currentAttr := Node(nil)
		if item.Key == active {
			className += " active"
			currentAttr = Attr("aria-current", "page")
		}
		nav = append(nav, A(
			Href(item.Href),
			Class(className),
			currentAttr,
			I(Class("nav-icon"), Attr("data-lucide", item.Icon), Attr("aria-hidden", "true")),
			Span(Class("app-nav-text"), Text(item.Label)),
		))
	}

	principalLabel := principal.Name
	if principalLabel == "" {
		principalLabel = "unknown"
	}

	mainClass := "app-main"
	contentClass := "content"
	if active == "sql" {
		mainClass += " app-main-sql"
		contentClass += " content-sql"
	}

	return HTML(
		Lang("en"),
		Attr("data-color-mode", "auto"),
		Attr("data-light-theme", "light"),
		Attr("data-dark-theme", "dark"),
		Head(
			Meta(Charset("utf-8")),
			Meta(Name("viewport"), Content("width=device-width, initial-scale=1")),
			TitleEl(Text(title+" | Duck UI")),
			Link(Rel("icon"), Href("data:,")),
			Script(Raw(themeInitScript)),
			Link(Rel("preconnect"), Href("https://fonts.googleapis.com")),
			Link(Rel("preconnect"), Href("https://fonts.gstatic.com"), Attr("crossorigin", "")),
			Link(Rel("stylesheet"), Href("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
			Link(Rel("stylesheet"), Href(uiStylesheetHref())),
			Script(Src("https://unpkg.com/lucide@latest/dist/umd/lucide.min.js")),
			Script(
				Type("module"),
				Src("https://cdn.jsdelivr.net/gh/starfederation/datastar@1.0.0-RC.7/bundles/datastar.js"),
			),
		),
		Body(
			Class("app-frame"),
			A(Href("#main-content"), Class("skip-link"), Text("Skip to content")),
			Main(Class("app-shell"),
				Header(
					Class("app-header"),
					Div(
						Class("app-header-brand"),
						Button(
							Type("button"),
							ID("nav-toggle"),
							Class("btn btn-sm btn-icon app-header-menu"),
							Attr("aria-label", "Toggle navigation"),
							Attr("aria-controls", "app-sidebar"),
							Attr("aria-expanded", "false"),
							I(Class("btn-icon-glyph"), Attr("data-lucide", "menu"), Attr("aria-hidden", "true")),
							Span(Class("sr-only"), Text("Toggle navigation")),
						),
						Strong(Text("Duck Platform")),
					),
					Div(
						Class("app-header-meta"),
						Button(
							Type("button"),
							ID("sidebar-toggle"),
							Class("btn btn-sm btn-icon"),
							Attr("aria-label", "Toggle compact sidebar"),
							Title("Toggle compact sidebar"),
							I(Class("btn-icon-glyph"), Attr("data-lucide", "panel-left"), Attr("aria-hidden", "true")),
							Span(Class("sr-only"), Text("Toggle compact sidebar")),
						),
						P(Class("color-fg-muted text-small mb-0"), Text("Signed in as "+principalLabel)),
						Button(
							Type("button"),
							ID("theme-toggle"),
							Class("btn btn-sm btn-icon"),
							Title("Toggle theme"),
							Attr("aria-label", "Toggle theme"),
							Span(ID("theme-icon-sun"), I(Class("btn-icon-glyph"), Attr("data-lucide", "sun"), Attr("aria-hidden", "true"))),
							Span(ID("theme-icon-moon"), Class("is-hidden"), I(Class("btn-icon-glyph"), Attr("data-lucide", "moon"), Attr("aria-hidden", "true"))),
							Span(Class("sr-only"), Text("Toggle theme")),
						),
						Form(
							Method("post"),
							Action("/ui/logout"),
							Button(Type("submit"), Class("btn btn-sm"), Text("Sign out")),
						),
					),
				),
				Div(
					Class("app-body"),
					Aside(
						Class("app-sidebar"),
						ID("app-sidebar"),
						Nav(Class("app-nav"), Group(nav)),
					),
					Section(
						Class(mainClass),
						ID("main-content"),
						Attr("tabindex", "-1"),
						H1(Class("sr-only"), Text(title)),
						Div(Class("app-main-content "+contentClass), Group(body)),
					),
				),
				Div(Class("app-overlay"), ID("app-overlay"), Attr("aria-hidden", "true")),
			),
			Script(Raw(themeBehaviorScript)),
			Script(Raw(shellBehaviorScript)),
			Script(Raw("if (window.lucide) { window.lucide.createIcons(); } document.addEventListener('click', function(e){ var t=e.target; if(!(t instanceof Element)){return;} document.querySelectorAll('details.dropdown[open]').forEach(function(d){ if(!d.contains(t)){ d.removeAttribute('open'); }}); });")),
		),
	)
}

func errorPage(title, message string) Node {
	return HTML(
		Lang("en"),
		Attr("data-color-mode", "auto"),
		Attr("data-light-theme", "light"),
		Attr("data-dark-theme", "dark"),
		Head(
			Meta(Charset("utf-8")),
			Meta(Name("viewport"), Content("width=device-width, initial-scale=1")),
			TitleEl(Text(title+" | Duck UI")),
			Link(Rel("icon"), Href("data:,")),
			Script(Raw(themeInitScript)),
			Link(Rel("preconnect"), Href("https://fonts.googleapis.com")),
			Link(Rel("preconnect"), Href("https://fonts.gstatic.com"), Attr("crossorigin", "")),
			Link(Rel("stylesheet"), Href("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
			Link(Rel("stylesheet"), Href(uiStylesheetHref())),
			Script(Src("https://unpkg.com/lucide@latest/dist/umd/lucide.min.js")),
		),
		Body(
			Main(
				Class("layout"),
				H1(Class("page-title"), Text(title)),
				P(Text(message)),
				P(A(Href("/ui"), Text("Back to overview"))),
			),
			Script(Raw(themeBehaviorScript)),
			Script(Raw("if (window.lucide) { window.lucide.createIcons(); }")),
		),
	)
}

func workspaceLayout(className string, aside Node, main ...Node) Node {
	classes := "workspace-layout"
	if strings.TrimSpace(className) != "" {
		classes += " " + strings.TrimSpace(className)
	}

	return Div(
		Class(classes),
		Attr("data-workspace-layout", "true"),
		aside,
		Section(Class("workspace-main"), Group(main)),
	)
}

func workspaceAside(storageKey, className string, tabs []workspaceAsideTab, defaultTab string) Node {
	classes := "workspace-aside"
	if strings.TrimSpace(className) != "" {
		classes += " " + strings.TrimSpace(className)
	}

	if len(tabs) == 0 {
		return Aside(Class(classes))
	}

	activeTab := strings.TrimSpace(defaultTab)
	if activeTab == "" {
		activeTab = tabs[0].ID
	}
	if !workspaceTabExists(tabs, activeTab) {
		activeTab = tabs[0].ID
	}

	tabButtons := make([]Node, 0, len(tabs))
	tabPanels := make([]Node, 0, len(tabs))
	for i := range tabs {
		tab := tabs[i]
		tabID := "workspace-tab-" + tab.ID
		panelID := "workspace-panel-" + tab.ID

		tabClass := "workspace-aside-tab"
		panelClass := "workspace-aside-panel"
		selected := "false"
		if tab.ID == activeTab {
			tabClass += " is-active"
			panelClass += " is-active"
			selected = "true"
		}
		if strings.TrimSpace(tab.PanelClass) != "" {
			panelClass += " " + strings.TrimSpace(tab.PanelClass)
		}

		countNode := Node(nil)
		if strings.TrimSpace(tab.Count) != "" {
			countNode = Span(Class("workspace-aside-tab-count"), Text(tab.Count))
		}

		tabButtons = append(tabButtons,
			Button(
				Type("button"),
				Class(tabClass),
				ID(tabID),
				Title(tab.Label),
				Attr("aria-label", tab.Label),
				Attr("role", "tab"),
				Attr("aria-selected", selected),
				Attr("aria-controls", panelID),
				Attr("data-workspace-aside-tab", tab.ID),
				I(Class("workspace-aside-tab-icon"), Attr("data-lucide", tab.Icon), Attr("aria-hidden", "true")),
				Span(Class("workspace-aside-tab-label"), Text(tab.Label)),
				countNode,
			),
		)

		tabPanels = append(tabPanels,
			Section(
				Class(panelClass),
				ID(panelID),
				Attr("role", "tabpanel"),
				Attr("aria-labelledby", tabID),
				Attr("data-workspace-aside-panel", tab.ID),
				tab.Content,
			),
		)
	}

	storageAttr := Node(nil)
	if strings.TrimSpace(storageKey) != "" {
		storageAttr = Attr("data-workspace-aside-storage", strings.TrimSpace(storageKey))
	}

	collapseButton := Button(
		Type("button"),
		Class("workspace-aside-toggle btn btn-sm btn-icon"),
		Attr("data-workspace-aside-toggle", "true"),
		Attr("aria-label", "Collapse sidebar"),
		Attr("aria-expanded", "true"),
		Title("Collapse sidebar"),
		I(Class("btn-icon-glyph"), Attr("data-lucide", "panel-left-close"), Attr("aria-hidden", "true")),
		Span(Class("sr-only"), Text("Collapse sidebar")),
	)

	return Aside(
		Class(classes),
		Attr("data-workspace-aside", "true"),
		Attr("data-workspace-aside-default", activeTab),
		storageAttr,
		Div(
			Class("workspace-aside-shell"),
			Div(
				Class("workspace-aside-head"),
				Div(Class("workspace-aside-tabs"), Attr("role", "tablist"), Group(tabButtons)),
				collapseButton,
			),
			Div(Class("workspace-aside-panels"), Group(tabPanels)),
		),
	)
}

func workspaceTabExists(tabs []workspaceAsideTab, tabID string) bool {
	for i := range tabs {
		if tabs[i].ID == tabID {
			return true
		}
	}
	return false
}

func catalogExplorerPanel(d catalogExplorerPanelData) Node {
	catalogNodes := make([]Node, 0, len(d.Catalogs))
	for i := range d.Catalogs {
		catalog := d.Catalogs[i]
		catalogClass := "catalog-tree-catalog-link"
		if catalog.Active {
			catalogClass += " active"
		}

		schemaNodes := make([]Node, 0, len(catalog.Schemas))
		for j := range catalog.Schemas {
			schema := catalog.Schemas[j]
			schemaClass := "catalog-tree-schema-link"
			if schema.Active {
				schemaClass += " active"
			}

			openAttr := Node(nil)
			if schema.Open {
				openAttr = Attr("open", "")
			}

			objectNodes := make([]Node, 0, len(schema.Objects))
			for k := range schema.Objects {
				obj := schema.Objects[k]
				leafClass := "catalog-tree-leaf"
				if obj.Active {
					leafClass += " active"
				}
				icon := strings.TrimSpace(obj.Icon)
				if icon == "" {
					icon = "table"
				}
				objectNodes = append(objectNodes,
					Li(
						A(Href(obj.URL), Class(leafClass), I(Class("nav-icon"), Attr("data-lucide", icon), Attr("aria-hidden", "true")), Span(Text(obj.Name))),
					),
				)
			}

			objectSection := Node(P(Class("catalog-tree-empty"), Text(fallbackString(schema.EmptyText, "No objects in this schema."))))
			if len(objectNodes) > 0 {
				objectSection = Ul(Class("catalog-tree-list"), Group(objectNodes))
			}

			schemaFilter := schema.Name + " " + catalogExplorerNames(schema.Objects)
			schemaNodes = append(schemaNodes,
				Li(
					Class("catalog-tree-node"),
					data.Show(containsExpr(schemaFilter)),
					Details(
						Class("details-reset catalog-tree-disclosure"),
						openAttr,
						Summary(
							Class("catalog-tree-summary"),
							Div(Class("catalog-tree-summary-main"),
								I(Class("nav-icon catalog-tree-caret"), Attr("data-lucide", "chevron-right"), Attr("aria-hidden", "true")),
								A(Href(schema.URL), Class(schemaClass), I(Class("nav-icon"), Attr("data-lucide", "folder"), Attr("aria-hidden", "true")), Span(Text(schema.Name))),
							),
						),
						Div(Class("catalog-tree-children"), objectSection),
					),
				),
			)
		}

		childrenNode := Node(P(Class("catalog-tree-empty"), Text(fallbackString(catalog.EmptyText, "No schemas in this catalog."))))
		if len(schemaNodes) > 0 {
			childrenNode = Ul(Class("catalog-tree-catalog-children"), Group(schemaNodes))
		}

		showValue := catalog.Name + " " + catalogExplorerNamesFromSchemas(catalog.Schemas)
		catalogItem := Node(
			Li(
				Class("catalog-tree-catalog-node"),
				data.Show(containsExpr(showValue)),
				A(Href(catalog.URL), Class(catalogClass), I(Class("nav-icon"), Attr("data-lucide", "database"), Attr("aria-hidden", "true")), Span(Text(catalog.Name))),
			),
		)

		if catalog.Open || len(schemaNodes) > 0 {
			openAttr := Node(nil)
			if catalog.Open {
				openAttr = Attr("open", "")
			}
			catalogItem = Li(
				Class("catalog-tree-catalog-node"),
				data.Show(containsExpr(showValue)),
				Details(
					Class("details-reset catalog-tree-disclosure catalog-tree-catalog-disclosure"),
					openAttr,
					Summary(
						Class("catalog-tree-summary"),
						Div(Class("catalog-tree-summary-main"),
							I(Class("nav-icon catalog-tree-caret"), Attr("data-lucide", "chevron-right"), Attr("aria-hidden", "true")),
							A(Href(catalog.URL), Class(catalogClass), I(Class("nav-icon"), Attr("data-lucide", "database"), Attr("aria-hidden", "true")), Span(Text(catalog.Name))),
						),
					),
					childrenNode,
				),
			)
		}

		catalogNodes = append(catalogNodes, catalogItem)
	}

	body := Node(P(Class("catalog-tree-empty"), Text(fallbackString(d.EmptyCatalogsText, "No catalogs found."))))
	if len(catalogNodes) > 0 {
		body = Ul(Class("catalog-tree-root"), Group(catalogNodes))
	}

	newCatalogButton := Node(nil)
	if strings.TrimSpace(d.NewCatalogURL) != "" {
		newCatalogButton = A(Href(d.NewCatalogURL), Class("btn btn-sm btn-icon"), Title("New catalog"), Attr("aria-label", "New catalog"), I(Class("btn-icon-glyph"), Attr("data-lucide", "plus"), Attr("aria-hidden", "true")), Span(Class("sr-only"), Text("New catalog")))
	}

	filterNode := Node(nil)
	if strings.TrimSpace(d.FilterPlaceholder) != "" {
		filterNode = Div(Class("catalog-rail-search"),
			I(Class("nav-icon"), Attr("data-lucide", "search"), Attr("aria-hidden", "true")),
			Label(Class("sr-only"), Text("Filter catalog explorer")),
			Input(Type("search"), Class("form-control"), Placeholder(d.FilterPlaceholder), data.Bind("q"), AutoComplete("off")),
		)
	}

	return Div(
		Class("catalog-rail"),
		Div(Class("catalog-rail-head"),
			Div(Class("catalog-rail-head-row"),
				P(Class("catalog-rail-kicker"), Text(fallbackString(d.Title, "Catalog Explorer"))),
				newCatalogButton,
			),
			filterNode,
		),
		body,
	)
}

func catalogExplorerNames(items []catalogExplorerObjectItem) string {
	names := make([]string, 0, len(items))
	for i := range items {
		names = append(names, items[i].Name)
	}
	return strings.Join(names, " ")
}

func catalogExplorerNamesFromSchemas(items []catalogExplorerSchemaItem) string {
	names := make([]string, 0, len(items))
	for i := range items {
		names = append(names, items[i].Name)
	}
	return strings.Join(names, " ")
}

func fallbackString(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func formatTime(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return ts.Format(time.RFC3339)
}

func formatTimePtr(ts *time.Time) string {
	if ts == nil || ts.IsZero() {
		return "-"
	}
	return ts.Format(time.RFC3339)
}

func stringPtr(v *string) string {
	if v == nil || strings.TrimSpace(*v) == "" {
		return "-"
	}
	return *v
}

func mapJSON(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for i := range keys {
		k := keys[i]
		parts = append(parts, fmt.Sprintf("%s=%s", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

func containsExpr(value string) string {
	lower := strings.ToLower(value)
	return "$q === '' || " + strconv.Quote(lower) + ".includes($q.toLowerCase())"
}

func paginationCard(basePath string, page domain.PageRequest, total int64) Node {
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	if nextToken == "" {
		return Div(Class(cardClass()), P(Class(mutedClass()), Text(fmt.Sprintf("Showing %d of %d entries.", min(page.Limit(), int(total)), total))))
	}
	url := fmt.Sprintf("%s?max_results=%d&page_token=%s", basePath, page.Limit(), nextToken)
	return Div(
		Class(cardClass()),
		P(Class(mutedClass()), Text(fmt.Sprintf("Showing up to %d of %d entries.", page.Limit(), total))),
		A(Href(url), Text("Next page ->")),
	)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func cardClass(extra ...string) string {
	parts := []string{"Box", "p-3", "mb-3", "card"}
	parts = append(parts, extra...)
	return strings.Join(parts, " ")
}

func mutedClass() string {
	return "color-fg-muted text-small"
}

func primaryButtonClass() string {
	return "btn btn-primary"
}

func secondaryButtonClass() string {
	return "btn"
}

func quickFilterCard(placeholder string, extraControls ...Node) Node {
	controls := []Node{
		Div(
			Class("d-flex flex-items-center gap-2 flex-1"),
			Label(Class("sr-only"), Text("Quick filter")),
			Input(Type("search"), Class("form-control"), Placeholder(placeholder), data.Bind("q"), AutoComplete("off")),
		),
	}
	controls = append(controls, extraControls...)
	return Div(
		Class(cardClass("toolbar")),
		data.Signals(map[string]any{"q": ""}),
		Div(Class("d-flex flex-wrap flex-items-center gap-2"), Group(controls)),
	)
}

func pageToolbar(newHref, newLabel string) Node {
	return Div(
		Class(cardClass("toolbar")),
		Div(
			Class("d-flex flex-justify-between flex-items-center flex-wrap gap-2"),
			P(Class("color-fg-muted text-small mb-0"), Text("Browse and manage resources.")),
			A(Href(newHref), Class(primaryButtonClass()), Text(newLabel)),
		),
	)
}

func emptyStateCard(message, ctaLabel, ctaHref string) Node {
	cta := Node(nil)
	if ctaLabel != "" && ctaHref != "" {
		cta = A(Href(ctaHref), Class(primaryButtonClass()), Text(ctaLabel))
	}
	return Div(
		Class(cardClass("blankslate")),
		P(Class("color-fg-muted mb-2"), Text(message)),
		cta,
	)
}

func statusLabel(text, tone string) Node {
	className := "Label"
	if tone != "" {
		className += " Label--" + tone
	}
	return Span(Class(className), Text(text))
}

func actionMenu(label string, items ...Node) Node {
	summaryClass := "btn btn-sm"
	summaryContent := Node(Text(label))
	if label == "More" || label == "Actions" {
		summaryClass = "btn btn-sm btn-icon"
		summaryContent = Group([]Node{
			I(Class("btn-icon-glyph"), Attr("data-lucide", "ellipsis"), Attr("aria-hidden", "true")),
			Span(Class("sr-only"), Text(label)),
		})
	}

	return Details(
		Class("dropdown details-reset details-overlay d-inline-block"),
		Summary(Class(summaryClass), Title(label), Attr("aria-label", label), summaryContent),
		Div(
			Class("dropdown-menu dropdown-menu-sw"),
			Group(items),
		),
	)
}

func actionMenuLink(href, label string) Node {
	icon := actionIconForLabel(label)
	return A(
		Href(href),
		Class("dropdown-item"),
		I(Class("dropdown-item-icon"), Attr("data-lucide", icon), Attr("aria-hidden", "true")),
		Span(Text(label)),
	)
}

func actionMenuPost(action, label string, csrfField func() Node, danger bool) Node {
	btnClass := "dropdown-item"
	if danger {
		btnClass += " dropdown-item-danger color-fg-danger"
	}
	icon := actionIconForLabel(label)
	button := Form(
		Method("post"),
		Action(action),
		csrfField(),
		Button(
			Type("submit"),
			Class(btnClass),
			I(Class("dropdown-item-icon"), Attr("data-lucide", icon), Attr("aria-hidden", "true")),
			Span(Text(label)),
		),
	)
	if danger {
		return Group([]Node{
			Div(Class("dropdown-divider")),
			button,
		})
	}
	return button
}

func actionIconForLabel(label string) string {
	lower := strings.ToLower(strings.TrimSpace(label))
	switch {
	case strings.Contains(lower, "delete"):
		return "trash-2"
	case strings.Contains(lower, "cancel"):
		return "x-circle"
	case strings.Contains(lower, "edit"):
		return "pencil"
	case strings.Contains(lower, "open"):
		return "square-arrow-out-up-right"
	case strings.Contains(lower, "move"):
		return "move-vertical"
	case strings.Contains(lower, "insert") || strings.Contains(lower, "add"):
		return "plus"
	default:
		return "circle"
	}
}
