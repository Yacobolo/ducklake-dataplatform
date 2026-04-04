package core

import (
	"os"
	"strings"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type navItem struct {
	Label string
	Href  string
	Key   string
	Icon  string
	Kids  []navItem
}

type navGroup struct {
	Label string
	Items []navItem
}

var navGroups = []navGroup{
	{
		Label: "Discover",
		Items: []navItem{
			{Label: "Overview", Href: "/ui", Key: "home", Icon: "house"},
			{Label: "Explore", Href: "/ui/explore", Key: "explore", Icon: "compass"},
			{Label: "Products", Href: "/ui/products", Key: "products", Icon: "package-open"},
			{Label: "Catalogs", Href: "/ui/catalogs", Key: "catalogs", Icon: "database"},
			{Label: "Runtime Assets", Href: "/ui/assets", Key: "assets", Icon: "git-fork"},
			{Label: "Dashboards", Href: "/ui/dashboards", Key: "dashboards", Icon: "chart-column"},
		},
	},
	{
		Label: "Build",
		Items: []navItem{
			{
				Label: "Models",
				Href:  "/ui/models",
				Key:   "models",
				Icon:  "boxes",
				Kids: []navItem{
					{Label: "Macros", Href: "/ui/macros", Key: "macros", Icon: "braces"},
				},
			},
			{Label: "Semantic", Href: "/ui/semantic", Key: "semantic", Icon: "waypoints"},
		},
	},
	{
		Label: "Operate",
		Items: []navItem{
			{Label: "Security", Href: "/ui/security", Key: "security", Icon: "shield"},
			{Label: "Governance", Href: "/ui/governance", Key: "governance", Icon: "scan-search"},
			{Label: "Storage", Href: "/ui/storage", Key: "storage", Icon: "hard-drive"},
			{Label: "Compute", Href: "/ui/compute", Key: "compute", Icon: "server"},
		},
	},
	{
		Label: "Internal",
		Items: []navItem{
			{Label: "Components", Href: "/ui/components", Key: "components", Icon: "layout-grid"},
		},
	},
}

func AppPage(title, active string, principal domain.ContextPrincipal, body ...Node) Node {
	devInspector := shouldShowDatastarInspector()
	nav := make([]Node, 0, len(navGroups))
	for _, group := range navGroups {
		items := make([]Node, 0, len(group.Items))
		for _, item := range group.Items {
			itemActive := navItemActive(item, active)
			className := ClassNames(
				"app-nav-link",
				"group flex items-center gap-3 rounded-xl px-3 py-2 text-sm font-medium text-[var(--fgColor-muted)] transition-colors hover:bg-[var(--bgColor-muted)] hover:text-[var(--fgColor-default)] [.app-shell.sidebar-compact_&]:justify-center [.app-shell.sidebar-compact_&]:px-1 max-md:[.app-shell.sidebar-compact_&]:justify-start max-md:[.app-shell.sidebar-compact_&]:px-3",
			)
			currentAttr := Node(nil)
			if item.Key == active {
				className = ClassNames(className, "active bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)] shadow-[inset_3px_0_0_var(--borderColor-accent-emphasis)]")
				currentAttr = Attr("aria-current", "page")
			} else if itemActive {
				className = ClassNames(className, "text-[var(--fgColor-default)]")
			}

			children := Node(nil)
			if len(item.Kids) > 0 && itemActive {
				kids := make([]Node, 0, len(item.Kids))
				for _, kid := range item.Kids {
					kidClass := "ml-9 inline-flex items-center gap-2 rounded-lg px-3 py-1.5 text-xs font-medium text-[var(--fgColor-muted)] transition-colors hover:bg-[var(--bgColor-muted)] hover:text-[var(--fgColor-default)] [.app-shell.sidebar-compact_&]:hidden max-md:[.app-shell.sidebar-compact_&]:inline-flex"
					kidCurrent := Node(nil)
					if kid.Key == active {
						kidClass = ClassNames(kidClass, "bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]")
						kidCurrent = Attr("aria-current", "page")
					}
					kids = append(kids, A(Href(kid.Href), Class(kidClass), kidCurrent, Text(kid.Label)))
				}
				children = Div(Class("grid gap-1"), Group(kids))
			}

			items = append(items, Div(
				Class("grid gap-1"),
				A(
					Href(item.Href),
					Class(className),
					currentAttr,
					Icon(item.Icon, Class(NavIconClass())),
					Span(Class("app-nav-text [.app-shell.sidebar-compact_&]:hidden max-md:[.app-shell.sidebar-compact_&]:inline"), Text(item.Label)),
				),
				children,
			))
		}
		nav = append(nav, Section(
			Class("grid gap-2 border-b border-[var(--borderColor-default)] pb-3 last:border-b-0 last:pb-0"),
			P(Class("px-3 text-[11px] font-semibold uppercase tracking-[0.08em] text-[var(--fgColor-muted)] [.app-shell.sidebar-compact_&]:hidden max-md:[.app-shell.sidebar-compact_&]:block"), Text(group.Label)),
			Div(Class("grid gap-1"), Group(items)),
		))
	}

	principalLabel := principal.Name
	if principalLabel == "" {
		principalLabel = "unknown"
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
			Script(Raw(ThemeInitScript)),
			Link(Rel("stylesheet"), Href(UIStylesheetHref())),
			Script(Type("module"), Src(UIScriptHref("datastar.js"))),
			func() Node {
				if !devInspector {
					return nil
				}
				return Script(Type("module"), Src(UIScriptHref("datastar-inspector.js")))
			}(),
		),
		Body(
			Class("app-frame min-h-screen h-screen overflow-hidden bg-[var(--bgColor-default)] text-[var(--fgColor-default)]"),
			A(Href("#main-content"), Class("skip-link sr-only focus:not-sr-only focus:absolute focus:left-0 focus:top-0 focus:z-20 focus:rounded-md focus:bg-[var(--bgColor-default)] focus:px-4 focus:py-2 focus:text-[var(--fgColor-accent)] focus:shadow-md"), Text("Skip to content")),
			Main(Class("app-shell flex h-full flex-col"),
				Header(
					Class("app-header flex items-center justify-between gap-4 border-b border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-4 py-3 shadow-xs max-md:flex-wrap"),
					Div(
						Class("app-header-brand inline-flex items-center gap-2"),
						Button(Type("button"), ID("nav-toggle"), Class(ClassNames(iconButtonClass("small"), "app-header-menu hidden max-md:inline-flex")), Attr("aria-label", "Toggle navigation"), Attr("aria-controls", "app-sidebar"), Attr("aria-expanded", "false"), Icon("menu", Class(IconGlyphClass())), Span(Class("sr-only"), Text("Toggle navigation"))),
						Strong(Class("block text-[0.8125rem] font-semibold uppercase tracking-[0.04em]"), Text("Duck Platform")),
					),
					Div(
						Class("app-header-meta inline-flex items-center gap-2 max-md:w-full max-md:justify-between"),
						Button(Type("button"), ID("sidebar-toggle"), Class(ClassNames(iconButtonClass("small"), "max-md:hidden")), Attr("aria-label", "Toggle compact sidebar"), Title("Toggle compact sidebar"), Icon("panel-left", Class(IconGlyphClass())), Span(Class("sr-only"), Text("Toggle compact sidebar"))),
						P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Signed in as "+principalLabel)),
						Button(Type("button"), ID("theme-toggle"), Class(iconButtonClass("small")), Title("Toggle theme"), Attr("aria-label", "Toggle theme"), Span(ID("theme-icon-sun"), Icon("sun", Class(IconGlyphClass()))), Span(ID("theme-icon-moon"), Class("hidden"), Icon("moon", Class(IconGlyphClass()))), Span(Class("sr-only"), Text("Toggle theme"))),
						Form(Method("post"), Action("/ui/logout"), Button(Type("submit"), Class(secondaryButtonClass("small")), Text("Sign out"))),
					),
				),
				Div(
					Class("app-body grid min-h-0 flex-1 overflow-hidden [grid-template-columns:18rem_minmax(0,1fr)] max-md:grid-cols-1"),
					Aside(Class("app-sidebar relative h-full overflow-y-auto border-r border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-2 py-4 shadow-xs transition-transform duration-100 ease-out [.app-shell.sidebar-compact_&]:px-1 max-md:fixed max-md:inset-y-0 max-md:left-0 max-md:z-20 max-md:h-screen max-md:w-72 max-md:-translate-x-full max-md:[.app-shell.nav-open_&]:translate-x-0"), ID("app-sidebar"), Nav(Class("app-nav grid gap-4"), Group(nav))),
					MainContentSection(title, Group(body)),
				),
				Div(Class("app-overlay pointer-events-none fixed inset-0 z-10 hidden bg-black/40 opacity-0 transition-opacity duration-100 ease-out [.app-shell.nav-open_&]:opacity-100 [.app-shell.nav-open_&]:pointer-events-auto max-md:block"), ID("app-overlay"), Attr("aria-hidden", "true")),
			),
			Script(Raw(ThemeBehaviorScript)),
			Script(Raw(ShellBehaviorScript)),
			Script(Raw("document.addEventListener('click', function(e){ var t=e.target; if(!(t instanceof Element)){return;} document.querySelectorAll('details.dropdown[open]').forEach(function(d){ if(!d.contains(t)){ d.removeAttribute('open'); }}); });")),
			func() Node {
				if !devInspector {
					return nil
				}
				return El("datastar-inspector")
			}(),
		),
	)
}

func MainContentSection(title string, body ...Node) Node {
	return Section(
		Class(ClassNames("app-main", "flex min-h-0 flex-col overflow-auto bg-[var(--bgColor-default)] px-[clamp(0.75rem,3vw,1.5rem)] py-5 max-md:px-3 max-md:py-4")),
		ID("main-content"),
		Attr("tabindex", "-1"),
		H1(Class("sr-only"), Text(title)),
		Div(Class(ClassNames("app-main-content", "content", "mx-auto flex min-h-0 w-full max-w-7xl flex-1 flex-col gap-6 max-md:gap-5")), Group(body)),
	)
}

func shouldShowDatastarInspector() bool {
	env := strings.TrimSpace(strings.ToLower(os.Getenv("ENV")))
	if env == "development" {
		return true
	}
	bypass := strings.TrimSpace(strings.ToLower(os.Getenv("AUTH_UI_DEV_BYPASS")))
	return bypass == "1" || bypass == "true" || bypass == "yes" || bypass == "on"
}

func navItemActive(item navItem, active string) bool {
	if item.Key == active {
		return true
	}
	for _, kid := range item.Kids {
		if kid.Key == active {
			return true
		}
	}
	return false
}
