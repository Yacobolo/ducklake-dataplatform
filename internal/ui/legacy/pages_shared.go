package legacy

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"
	"duck-demo/internal/ui/core"

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
	{Label: "Products", Href: "/ui/products", Key: "products", Icon: "package-open"},
	{Label: "Components", Href: "/ui/components", Key: "components", Icon: "layout-grid"},
	{Label: "Catalogs", Href: "/ui/catalogs", Key: "catalogs", Icon: "database"},
	{Label: "Security", Href: "/ui/security", Key: "security", Icon: "shield"},
	{Label: "Storage", Href: "/ui/storage", Key: "storage", Icon: "hard-drive"},
	{Label: "Compute", Href: "/ui/compute", Key: "compute", Icon: "server"},
	{Label: "Governance", Href: "/ui/governance", Key: "governance", Icon: "scan-search"},
	{Label: "Runtime Assets", Href: "/ui/assets", Key: "assets", Icon: "git-fork"},
	{Label: "Notebooks", Href: "/ui/notebooks", Key: "notebooks", Icon: "notebook-text"},
	{Label: "Dashboards", Href: "/ui/dashboards", Key: "dashboards", Icon: "chart-column"},
	{Label: "Macros", Href: "/ui/macros", Key: "macros", Icon: "braces"},
	{Label: "Models", Href: "/ui/models", Key: "models", Icon: "boxes"},
	{Label: "Semantic", Href: "/ui/semantic", Key: "semantic", Icon: "waypoints"},
}

func appPage(title, active string, principal domain.ContextPrincipal, body ...Node) Node {
	nav := make([]Node, 0, len(navItems))
	for _, item := range navItems {
		className := classNames(
			"app-nav-link",
			"group flex items-center gap-3 rounded-xl px-3 py-2 text-sm font-medium text-[var(--fgColor-muted)] transition-colors hover:bg-[var(--bgColor-muted)] hover:text-[var(--fgColor-default)] [.app-shell.sidebar-compact_&]:justify-center [.app-shell.sidebar-compact_&]:px-1 max-md:[.app-shell.sidebar-compact_&]:justify-start max-md:[.app-shell.sidebar-compact_&]:px-3",
		)
		currentAttr := Node(nil)
		if item.Key == active {
			className = classNames(className, "active bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)] shadow-[inset_3px_0_0_var(--borderColor-accent-emphasis)]")
			currentAttr = Attr("aria-current", "page")
		}
		nav = append(nav, A(
			Href(item.Href),
			Class(className),
			currentAttr,
			I(Class(navIconClass()), Attr("data-lucide", item.Icon), Attr("aria-hidden", "true")),
			Span(Class("app-nav-text [.app-shell.sidebar-compact_&]:hidden max-md:[.app-shell.sidebar-compact_&]:inline"), Text(item.Label)),
		))
	}

	principalLabel := principal.Name
	if principalLabel == "" {
		principalLabel = "unknown"
	}

	mainClass := "app-main"
	contentClass := "content"

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
			Class("app-frame min-h-screen h-screen overflow-hidden bg-[var(--bgColor-default)] text-[var(--fgColor-default)]"),
			A(Href("#main-content"), Class("skip-link sr-only focus:not-sr-only focus:absolute focus:left-0 focus:top-0 focus:z-20 focus:rounded-md focus:bg-[var(--bgColor-default)] focus:px-4 focus:py-2 focus:text-[var(--fgColor-accent)] focus:shadow-[var(--shadow-floating-small)]"), Text("Skip to content")),
			Main(Class("app-shell flex h-full flex-col"),
				Header(
					Class("app-header flex items-center justify-between gap-4 border-b border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-4 py-3 shadow-[var(--shadow-resting-xsmall)] max-md:flex-wrap"),
					Div(
						Class("app-header-brand inline-flex items-center gap-2"),
						Button(
							Type("button"),
							ID("nav-toggle"),
							Class(classNames(iconButtonClass("small"), "app-header-menu hidden max-md:inline-flex")),
							Attr("aria-label", "Toggle navigation"),
							Attr("aria-controls", "app-sidebar"),
							Attr("aria-expanded", "false"),
							I(Class(iconGlyphClass()), Attr("data-lucide", "menu"), Attr("aria-hidden", "true")),
							Span(Class("sr-only"), Text("Toggle navigation")),
						),
						Strong(Class("block text-[0.8125rem] font-semibold uppercase tracking-[0.04em]"), Text("Duck Platform")),
					),
					Div(
						Class("app-header-meta inline-flex items-center gap-2 max-md:w-full max-md:justify-between"),
						Button(
							Type("button"),
							ID("sidebar-toggle"),
							Class(classNames(iconButtonClass("small"), "max-md:hidden")),
							Attr("aria-label", "Toggle compact sidebar"),
							Title("Toggle compact sidebar"),
							I(Class(iconGlyphClass()), Attr("data-lucide", "panel-left"), Attr("aria-hidden", "true")),
							Span(Class("sr-only"), Text("Toggle compact sidebar")),
						),
						P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Signed in as "+principalLabel)),
						Button(
							Type("button"),
							ID("theme-toggle"),
							Class(iconButtonClass("small")),
							Title("Toggle theme"),
							Attr("aria-label", "Toggle theme"),
							Span(ID("theme-icon-sun"), I(Class(iconGlyphClass()), Attr("data-lucide", "sun"), Attr("aria-hidden", "true"))),
							Span(ID("theme-icon-moon"), Class("hidden"), I(Class(iconGlyphClass()), Attr("data-lucide", "moon"), Attr("aria-hidden", "true"))),
							Span(Class("sr-only"), Text("Toggle theme")),
						),
						Form(
							Method("post"),
							Action("/ui/logout"),
							Button(Type("submit"), Class(secondaryButtonClass("small")), Text("Sign out")),
						),
					),
				),
				Div(
					Class("app-body grid min-h-0 flex-1 overflow-hidden [grid-template-columns:var(--size-sidebar-width)_minmax(0,1fr)] max-md:grid-cols-1"),
					Aside(
						Class("app-sidebar relative h-full overflow-y-auto border-r border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-2 py-4 shadow-[var(--shadow-resting-xsmall)] transition-transform duration-100 ease-out [.app-shell.sidebar-compact_&]:px-1 max-md:fixed max-md:inset-y-0 max-md:left-0 max-md:z-20 max-md:h-screen max-md:w-[min(var(--size-sidebar-width),calc(var(--overlay-width-small)+var(--space-5)))] max-md:-translate-x-full max-md:[.app-shell.nav-open_&]:translate-x-0"),
						ID("app-sidebar"),
						Nav(Class("app-nav grid gap-1 max-md:flex max-md:flex-wrap max-md:gap-2"), Group(nav)),
					),
					Section(
						Class(classNames(mainClass, "flex min-h-0 flex-col overflow-auto bg-[var(--bgColor-default)] px-[clamp(var(--space-3),3vw,var(--space-5))] py-4 max-md:px-3 max-md:py-3")),
						ID("main-content"),
						Attr("tabindex", "-1"),
						H1(Class("sr-only"), Text(title)),
						Div(Class(classNames("app-main-content", contentClass, "flex min-h-0 w-full flex-1 flex-col")), Group(body)),
					),
				),
				Div(Class("app-overlay pointer-events-none fixed inset-0 z-10 hidden bg-[var(--overlay-backdrop-bgColor)] opacity-0 transition-opacity duration-100 ease-out [.app-shell.nav-open_&]:opacity-100 [.app-shell.nav-open_&]:pointer-events-auto max-md:block"), ID("app-overlay"), Attr("aria-hidden", "true")),
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
				Class("mx-auto max-w-[var(--size-layout-max-width)] px-6 py-8"),
				H1(Class("m-0 text-[calc(var(--text-title-size-medium)+var(--borderWidth-thick))] font-semibold leading-[var(--text-title-lineHeight-medium)]"), Text(title)),
				P(Text(message)),
				P(A(Href("/ui"), Text("Back to overview"))),
			),
			Script(Raw(themeBehaviorScript)),
			Script(Raw("if (window.lucide) { window.lucide.createIcons(); }")),
		),
	)
}

func workspaceLayout(className string, aside Node, main ...Node) Node {
	return core.WorkspaceLayout(className, aside, main...)
}

func workspaceAside(storageKey, className string, tabs []workspaceAsideTab, defaultTab string) Node {
	coreTabs := make([]core.WorkspaceAsideTab, 0, len(tabs))
	for i := range tabs {
		tab := tabs[i]
		coreTabs = append(coreTabs, core.WorkspaceAsideTab{
			ID:         tab.ID,
			Label:      tab.Label,
			Icon:       tab.Icon,
			Count:      tab.Count,
			PanelClass: tab.PanelClass,
			Content:    tab.Content,
		})
	}
	return core.WorkspaceAside(storageKey, className, coreTabs, defaultTab)
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
	coreCatalogs := make([]core.CatalogExplorerCatalogItem, 0, len(d.Catalogs))
	for i := range d.Catalogs {
		catalog := d.Catalogs[i]
		coreSchemas := make([]core.CatalogExplorerSchemaItem, 0, len(catalog.Schemas))
		for j := range catalog.Schemas {
			schema := catalog.Schemas[j]
			coreObjects := make([]core.CatalogExplorerObjectItem, 0, len(schema.Objects))
			for k := range schema.Objects {
				obj := schema.Objects[k]
				coreObjects = append(coreObjects, core.CatalogExplorerObjectItem{
					Name:   obj.Name,
					URL:    obj.URL,
					Icon:   obj.Icon,
					Active: obj.Active,
				})
			}
			coreSchemas = append(coreSchemas, core.CatalogExplorerSchemaItem{
				Name:      schema.Name,
				URL:       schema.URL,
				Active:    schema.Active,
				Open:      schema.Open,
				Objects:   coreObjects,
				EmptyText: schema.EmptyText,
			})
		}
		coreCatalogs = append(coreCatalogs, core.CatalogExplorerCatalogItem{
			Name:      catalog.Name,
			URL:       catalog.URL,
			Active:    catalog.Active,
			Open:      catalog.Open,
			Schemas:   coreSchemas,
			EmptyText: catalog.EmptyText,
		})
	}
	return core.CatalogExplorerPanel(core.CatalogExplorerPanelData{
		Title:             d.Title,
		FilterPlaceholder: d.FilterPlaceholder,
		Catalogs:          coreCatalogs,
		NewCatalogURL:     d.NewCatalogURL,
		EmptyCatalogsText: d.EmptyCatalogsText,
	})
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
	shown := min(page.Limit(), int(total))
	summary := fmt.Sprintf("Showing %d of %d entries.", shown, total)
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	if nextToken == "" {
		return Div(
			Class(cardClass()),
			Div(
				Class("flex items-center justify-between gap-3 max-sm:flex-col max-sm:items-start"),
				Div(
					Class("flex min-w-0 flex-col gap-1"),
					P(Class("m-0 text-sm font-semibold text-[var(--fgColor-default)]"), Text("Pagination")),
					P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary)),
				),
				Span(Class(classNames(secondaryButtonClass("small"), "pointer-events-none opacity-60")), Attr("aria-disabled", "true"), Text("Next")),
			),
		)
	}
	url := fmt.Sprintf("%s?max_results=%d&page_token=%s", basePath, page.Limit(), nextToken)
	return Div(
		Class(cardClass()),
		Div(
			Class("flex items-center justify-between gap-3 max-sm:flex-col max-sm:items-start"),
			Div(
				Class("flex min-w-0 flex-col gap-1"),
				P(Class("m-0 text-sm font-semibold text-[var(--fgColor-default)]"), Text("Pagination")),
				P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(summary)),
			),
			A(Href(url), Class(secondaryButtonClass("small")), Text("Next page")),
		),
	)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func cardClass(extra ...string) string {
	return core.CardClass(extra...)
}

func mutedClass() string {
	return core.MutedClass()
}

func buttonBaseClass(size string) string {
	sizeClasses := "min-h-10 px-4 py-2 text-sm"
	switch strings.TrimSpace(size) {
	case "small":
		sizeClasses = "min-h-8 px-3 py-1.5 text-xs"
	}
	return classNames(
		"inline-flex items-center justify-center gap-2 rounded-lg border font-medium transition-colors duration-100 ease-out focus-visible:outline-2 focus-visible:outline-offset-0 focus-visible:outline-[var(--focus-outlineColor)] disabled:cursor-not-allowed disabled:border-[var(--control-borderColor-disabled)] disabled:bg-[var(--button-default-bgColor-disabled)] disabled:text-[var(--fgColor-disabled)]",
		"border-[var(--button-default-borderColor-rest)] bg-[var(--button-default-bgColor-rest)] text-[var(--button-default-fgColor-rest)] hover:bg-[var(--button-default-bgColor-hover)] active:bg-[var(--button-default-bgColor-active)]",
		sizeClasses,
	)
}

func primaryButtonClass(size ...string) string {
	return core.PrimaryButtonClass(size...)
}

func secondaryButtonClass(size ...string) string {
	buttonSize := ""
	if len(size) > 0 {
		buttonSize = size[0]
	}
	return buttonBaseClass(buttonSize)
}

func dangerButtonClass(size ...string) string {
	return core.DangerButtonClass(size...)
}

func iconButtonClass(size string) string {
	sizeClasses := "h-10 w-10"
	if strings.TrimSpace(size) == "small" {
		sizeClasses = "h-8 w-8"
	}
	return classNames(secondaryButtonClass(size), sizeClasses, "p-0")
}

func iconGlyphClass() string {
	return "h-4 w-4"
}

func navIconClass(extra ...string) string {
	return classNames("h-4 w-4 shrink-0", strings.Join(extra, " "))
}

func detailsClass(extra ...string) string {
	return classNames("relative inline-block", strings.Join(extra, " "))
}

func detailsSummaryClass(extra ...string) string {
	return classNames("list-none [&::-webkit-details-marker]:hidden", strings.Join(extra, " "))
}

func dropdownMenuClass(extra ...string) string {
	return classNames("absolute right-0 top-full z-20 mt-1 min-w-[var(--overlay-width-xsmall)] rounded-xl border border-[var(--overlay-borderColor)] bg-[var(--overlay-bgColor)] p-1 shadow-[var(--shadow-floating-small)]", strings.Join(extra, " "))
}

func dropdownItemClass(extra ...string) string {
	return classNames("flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[0.8125rem] no-underline hover:bg-[var(--control-bgColor-hover)]", strings.Join(extra, " "))
}

func stackFormClass(extra ...string) string {
	return classNames("[&>:not(label):not(.form-actions)]:mb-3 [&>:last-child]:mb-0", strings.Join(extra, " "))
}

func formActionsClass(extra ...string) string {
	return classNames("form-actions mt-2", strings.Join(extra, " "))
}

func errorTextClass(extra ...string) string {
	return core.ErrorTextClass(extra...)
}

func formControlClass(extra ...string) string {
	return core.FormControlClass(extra...)
}

func formSelectClass(extra ...string) string {
	return core.FormSelectClass(extra...)
}

func tableWrapClass(extra ...string) string {
	return core.TableWrapClass(extra...)
}

func sectionTitleClass() string {
	return "m-0 text-lg font-semibold text-[var(--fgColor-default)]"
}

func sectionCopyClass() string {
	return "m-0 text-sm text-[var(--fgColor-muted)]"
}

func subtleLinkClass() string {
	return "text-[var(--fgColor-muted)] no-underline hover:text-[var(--fgColor-default)]"
}

func dataTableClass(extra ...string) string {
	return classNames("min-w-full border-collapse overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] [&_tbody_tr:hover]:bg-[var(--control-bgColor-hover)] [&_td]:border-b [&_td]:border-[var(--borderColor-default)] [&_td]:px-4 [&_td]:py-3 [&_td]:align-top [&_td]:text-[0.8125rem] [&_th]:sticky [&_th]:top-0 [&_th]:z-[1] [&_th]:border-b [&_th]:border-[var(--borderColor-default)] [&_th]:bg-[var(--bgColor-muted)] [&_th]:px-4 [&_th]:py-3 [&_th]:text-left [&_th]:text-[0.8125rem] [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.02em] [&_th]:text-[var(--fgColor-muted)]", strings.Join(extra, " "))
}

func buttonRowClass(extra ...string) string {
	return core.ButtonRowClass(extra...)
}

func sqlResultCardClass(extra ...string) string {
	return classNames("rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)]", strings.Join(extra, " "))
}

func sqlResultsTitleClass() string {
	return "m-0 text-lg font-semibold text-[var(--fgColor-default)]"
}

func sqlComputeMetaGridClass() string {
	return "grid gap-3 sm:grid-cols-2 xl:grid-cols-3"
}

func sqlComputeMetaItemClass() string {
	return "flex flex-col gap-1 rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-3 py-3"
}

func sqlComputeMetaLabelClass() string {
	return "m-0 text-[11px] font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"
}

func assetDetailShellClass() string {
	return "flex flex-col gap-4"
}

func assetShellClass() string {
	return assetDetailShellClass()
}

func assetHeroClass() string {
	return "grid gap-4 rounded-2xl border border-[var(--borderColor-default)] bg-[linear-gradient(135deg,var(--bgColor-muted)_0%,var(--bgColor-default)_65%)] p-5 shadow-[var(--shadow-resting-small)] lg:grid-cols-[minmax(0,1.5fr)_minmax(16rem,0.8fr)]"
}

func assetHeroCopyClass() string {
	return "flex min-w-0 flex-col gap-3"
}

func assetHeroMetaClass() string {
	return "grid gap-2 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4"
}

func assetKickerClass() string {
	return "m-0 text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)]"
}

func assetTitleRowClass() string {
	return "flex flex-wrap items-center gap-3"
}

func assetTitleClass() string {
	return "m-0 text-3xl font-semibold leading-tight text-[var(--fgColor-default)]"
}

func assetDescriptionClass() string {
	return "m-0 max-w-3xl text-sm leading-6 text-[var(--fgColor-muted)]"
}

func assetBadgeRowClass() string {
	return "flex flex-wrap items-center gap-2"
}

func assetHeroActionsClass() string {
	return "flex flex-wrap items-center gap-3"
}

func assetMetricsGridClass() string {
	return "grid gap-3 sm:grid-cols-2 xl:grid-cols-4"
}

func assetMetricCardClass() string {
	return "flex flex-col gap-1 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4 shadow-[var(--shadow-resting-xsmall)]"
}

func assetMetricLabelClass() string {
	return "m-0 text-xs font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"
}

func assetMetricValueClass() string {
	return "m-0 text-2xl font-semibold text-[var(--fgColor-default)]"
}

func assetMetricHintClass() string {
	return "m-0 text-xs text-[var(--fgColor-muted)]"
}

func assetTypeBandClass() string {
	return cardClass("flex flex-col gap-4")
}

func assetTypeListClass() string {
	return "flex flex-wrap gap-3"
}

func assetTypeChipClass() string {
	return "inline-flex items-center gap-2 rounded-full border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-3 py-2"
}

func assetTypeCountClass() string {
	return "text-xs font-semibold text-[var(--fgColor-default)]"
}

func assetShowcaseSectionClass() string {
	return cardClass("flex flex-col gap-4")
}

func assetShowcaseGridClass() string {
	return "grid gap-3 lg:grid-cols-2 2xl:grid-cols-3"
}

func assetShowcaseCardClass() string {
	return "flex h-full flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4 text-inherit no-underline shadow-[var(--shadow-resting-xsmall)] transition-colors hover:border-[var(--borderColor-accent-muted)] hover:bg-[var(--control-bgColor-hover)]"
}

func assetShowcaseHeadClass() string {
	return "flex items-start justify-between gap-3"
}

func assetShowcaseKeyClass() string {
	return "m-0 text-base font-semibold text-[var(--fgColor-default)]"
}

func assetShowcaseOwnerClass() string {
	return "mt-1 mb-0 text-xs text-[var(--fgColor-muted)]"
}

func assetShowcaseDescriptionClass() string {
	return "m-0 text-sm leading-6 text-[var(--fgColor-muted)]"
}

func assetShowcaseFootClass() string {
	return "mt-auto flex items-center justify-between gap-3 pt-2 text-xs"
}

func assetShowcaseUpdatedClass() string {
	return "text-[var(--fgColor-muted)]"
}

func assetShowcaseLinkClass() string {
	return "font-medium text-[var(--fgColor-accent)]"
}

func assetTableSubtitleClass() string {
	return "mt-1 mb-0 text-xs leading-5 text-[var(--fgColor-muted)]"
}

func assetBadgeStackClass() string {
	return "flex flex-wrap gap-2"
}

func assetDetailLayoutClass() string {
	return "grid gap-4 xl:grid-cols-[minmax(0,1.7fr)_minmax(18rem,0.8fr)]"
}

func assetDetailMainClass() string {
	return "flex min-w-0 flex-col gap-4"
}

func assetDetailRailClass() string {
	return "flex min-w-0 flex-col gap-4"
}

func assetSectionClass() string {
	return "flex flex-col gap-4"
}

func assetSectionHeadClass() string {
	return "flex flex-wrap items-start justify-between gap-2"
}

func assetSubGridClass() string {
	return "grid gap-4 lg:grid-cols-2"
}

func assetPanelClass(extra ...string) string {
	return classNames("flex min-w-0 flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4", strings.Join(extra, " "))
}

func assetListClass(extra ...string) string {
	return classNames("grid gap-2", strings.Join(extra, " "))
}

func assetListItemClass() string {
	return "rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-2 text-sm"
}

func assetMetaRowClass() string {
	return "grid gap-1 border-b border-[var(--borderColor-default)] pb-2 last:border-b-0 last:pb-0"
}

func assetMetaLabelClass() string {
	return "text-[11px] font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"
}

func assetMetaValueClass() string {
	return "text-sm text-[var(--fgColor-default)]"
}

func assetFactListClass() string {
	return "grid gap-2"
}

func assetFactRowClass() string {
	return "flex items-start justify-between gap-3 rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-2"
}

func assetFactLabelClass() string {
	return "text-xs font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"
}

func assetFactValueClass() string {
	return "text-sm text-right text-[var(--fgColor-default)]"
}

func catalogSectionClass(extra ...string) string {
	return classNames("flex flex-col gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] p-4", strings.Join(extra, " "))
}

func catalogMetaListClass(extra ...string) string {
	return classNames("grid gap-3 sm:grid-cols-2", strings.Join(extra, " "))
}

func catalogMetaRowClass() string {
	return "grid gap-1 rounded-lg border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] px-3 py-3"
}

func catalogMetaLabelClass() string {
	return "text-[11px] font-semibold uppercase tracking-[0.04em] text-[var(--fgColor-muted)]"
}

func catalogMetaValueClass() string {
	return "m-0 text-sm text-[var(--fgColor-default)]"
}

func catalogTabsClass() string {
	return "flex flex-wrap gap-2 border-b border-[var(--borderColor-default)] pb-3"
}

func catalogTabClass(active bool) string {
	base := "rounded-lg px-3 py-2 text-sm font-medium no-underline transition-colors"
	if active {
		return classNames(base, "bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]")
	}
	return classNames(base, "text-[var(--fgColor-muted)] hover:bg-[var(--bgColor-default)] hover:text-[var(--fgColor-default)]")
}

func catalogHistoryFilterClass(active bool) string {
	base := "inline-flex min-h-9 items-center rounded-full border px-3 text-sm no-underline transition-colors"
	if active {
		return classNames(base, "border-[var(--borderColor-accent-emphasis)] bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]")
	}
	return classNames(base, "border-[var(--borderColor-muted)] text-[var(--fgColor-muted)] hover:text-[var(--fgColor-default)]")
}

func catalogBreadcrumbClass() string {
	return "m-0"
}

func catalogBreadcrumbListClass() string {
	return "flex max-w-full min-w-0 flex-wrap items-center gap-1 text-xs text-[var(--fgColor-muted)]"
}

func catalogBreadcrumbItemClass() string {
	return "inline-flex min-w-0 items-center gap-1"
}

func catalogBreadcrumbLabelClass(current bool) string {
	base := "inline-block max-w-[min(32ch,40vw)] overflow-hidden text-ellipsis whitespace-nowrap align-bottom"
	if current {
		return classNames(base, "font-semibold text-[var(--fgColor-default)]")
	}
	return base
}

func catalogOverviewToolbarClass() string {
	return "flex flex-wrap items-center justify-between gap-3"
}

func quickFilterCard(placeholder string, extraControls ...Node) Node {
	return quickFilterCardWithValue(placeholder, "", extraControls...)
}

func quickFilterCardWithValue(placeholder, initialValue string, extraControls ...Node) Node {
	controls := []Node{
		Div(
			Class("flex min-w-[min(20rem,100%)] flex-1 flex-col gap-1"),
			Label(Class("sr-only"), Text("Quick filter")),
			Input(Type("search"), Class(formControlClass()), Name("q"), Placeholder(placeholder), data.Bind("q"), AutoComplete("off"), Attr("data-quick-filter-input", "true")),
		),
	}
	controls = append(controls, extraControls...)
	syncScript := `(function(){
  var input=document.querySelector('[data-quick-filter-input="true"]');
  if(!(input instanceof HTMLInputElement)){ return; }

  function syncURL(value){
    var url=new URL(window.location.href);
    if(value){
      url.searchParams.set('q', value);
    } else {
      url.searchParams.delete('q');
    }
    url.searchParams.delete('page_token');
    var next=url.pathname;
    var query=url.searchParams.toString();
    if(query){ next+='?'+query; }
    if(next!==window.location.pathname+window.location.search){
      window.history.replaceState({}, '', next);
    }
  }

  input.addEventListener('input', function(){
    syncURL(input.value.trim());
  });
})();`

	return Div(
		Class(cardClass()),
		data.Signals(map[string]any{"q": initialValue}),
		Div(Class("flex flex-wrap items-center gap-3"), Group(controls)),
		Script(Raw(syncScript)),
	)
}

func pageToolbar(newHref, newLabel string) Node {
	return Div(
		Class(cardClass()),
		Div(
			Class("flex flex-wrap items-center justify-between gap-3"),
			Div(
				Class("flex min-w-0 flex-col gap-1"),
				Span(Class(labelClass("")), Text("Workspace")),
				P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Browse and manage resources.")),
			),
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
		Class(cardClass("text-center")),
		Div(
			Class("mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-[var(--bgColor-muted)] text-[var(--fgColor-accent)]"),
			I(Class(navIconClass()), Attr("data-lucide", "inbox"), Attr("aria-hidden", "true")),
		),
		Div(
			Class("flex flex-col items-center gap-2 text-center"),
			P(Class("m-0 text-lg font-semibold"), Text("No results yet")),
			P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(message)),
			cta,
		),
	)
}

func labelClass(tone string) string {
	base := "inline-flex items-center rounded-full border border-transparent px-2 py-0.5 text-xs font-medium"
	switch tone {
	case "accent":
		return classNames(base, "bg-[var(--label-blue-bgColor-rest)] text-[var(--label-blue-fgColor-rest)]")
	case "attention":
		return classNames(base, "bg-[var(--label-yellow-bgColor-rest)] text-[var(--label-yellow-fgColor-rest)]")
	case "success":
		return classNames(base, "bg-[var(--label-green-bgColor-rest)] text-[var(--label-green-fgColor-rest)]")
	case "severe":
		return classNames(base, "bg-[var(--label-orange-bgColor-rest)] text-[var(--label-orange-fgColor-rest)]")
	default:
		return classNames(base, "bg-[var(--label-gray-bgColor-rest)] text-[var(--label-gray-fgColor-rest)]")
	}
}

func statusLabel(text, tone string) Node {
	return Span(Class(labelClass(tone)), Text(text))
}

func actionMenu(label string, items ...Node) Node {
	summaryClass := secondaryButtonClass("small")
	summaryContent := Node(Text(label))
	if label == "More" || label == "Actions" {
		summaryClass = iconButtonClass("small")
		summaryContent = Group([]Node{
			I(Class(iconGlyphClass()), Attr("data-lucide", "ellipsis"), Attr("aria-hidden", "true")),
			Span(Class("sr-only"), Text(label)),
		})
	}

	return Details(
		Class(detailsClass()),
		Summary(Class(detailsSummaryClass(summaryClass)), Title(label), Attr("aria-label", label), summaryContent),
		Div(
			Class(dropdownMenuClass()),
			Group(items),
		),
	)
}

func actionMenuLink(href, label string) Node {
	icon := actionIconForLabel(label)
	return A(
		Href(href),
		Class(dropdownItemClass("text-[var(--fgColor-default)]")),
		I(Class(navIconClass()), Attr("data-lucide", icon), Attr("aria-hidden", "true")),
		Span(Text(label)),
	)
}

func actionMenuPost(action, label string, csrfField func() Node, danger bool) Node {
	btnClass := dropdownItemClass()
	if danger {
		btnClass += " text-[var(--fgColor-danger)] hover:bg-[var(--bgColor-danger-muted)]"
	} else {
		btnClass += " text-[var(--fgColor-default)]"
	}
	icon := actionIconForLabel(label)
	button := Form(
		Method("post"),
		Action(action),
		csrfField(),
		Button(
			Type("submit"),
			Class(btnClass),
			I(Class(navIconClass()), Attr("data-lucide", icon), Attr("aria-hidden", "true")),
			Span(Text(label)),
		),
	)
	if danger {
		return Group([]Node{
			Div(Class("dropdown-divider my-1 border-t border-[var(--borderColor-muted)]")),
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
