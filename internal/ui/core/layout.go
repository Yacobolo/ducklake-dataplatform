package core

import (
	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type navItem struct {
	Label string
	Href  string
	Key   string
	Icon  string
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

func AppPage(title, active string, principal domain.ContextPrincipal, body ...Node) Node {
	nav := make([]Node, 0, len(navItems))
	for _, item := range navItems {
		className := ClassNames(
			"app-nav-link",
			"group flex items-center gap-3 rounded-xl px-3 py-2 text-sm font-medium text-[var(--fgColor-muted)] transition-colors hover:bg-[var(--bgColor-muted)] hover:text-[var(--fgColor-default)] [.app-shell.sidebar-compact_&]:justify-center [.app-shell.sidebar-compact_&]:px-1 max-md:[.app-shell.sidebar-compact_&]:justify-start max-md:[.app-shell.sidebar-compact_&]:px-3",
		)
		currentAttr := Node(nil)
		if item.Key == active {
			className = ClassNames(className, "active bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)] shadow-[inset_3px_0_0_var(--borderColor-accent-emphasis)]")
			currentAttr = Attr("aria-current", "page")
		}
		nav = append(nav, A(
			Href(item.Href),
			Class(className),
			currentAttr,
			I(Class(NavIconClass()), Attr("data-lucide", item.Icon), Attr("aria-hidden", "true")),
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
			Script(Raw(ThemeInitScript)),
			Link(Rel("preconnect"), Href("https://fonts.googleapis.com")),
			Link(Rel("preconnect"), Href("https://fonts.gstatic.com"), Attr("crossorigin", "")),
			Link(Rel("stylesheet"), Href("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
			Link(Rel("stylesheet"), Href(UIStylesheetHref())),
			Script(Src("https://unpkg.com/lucide@latest/dist/umd/lucide.min.js")),
			Script(Type("module"), Src("https://cdn.jsdelivr.net/gh/starfederation/datastar@1.0.0-RC.7/bundles/datastar.js")),
		),
		Body(
			Class("app-frame min-h-screen h-screen overflow-hidden bg-[var(--bgColor-default)] text-[var(--fgColor-default)]"),
			A(Href("#main-content"), Class("skip-link sr-only focus:not-sr-only focus:absolute focus:left-0 focus:top-0 focus:z-20 focus:rounded-md focus:bg-[var(--bgColor-default)] focus:px-4 focus:py-2 focus:text-[var(--fgColor-accent)] focus:shadow-[var(--shadow-floating-small)]"), Text("Skip to content")),
			Main(Class("app-shell flex h-full flex-col"),
				Header(
					Class("app-header flex items-center justify-between gap-4 border-b border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-4 py-3 shadow-[var(--shadow-resting-xsmall)] max-md:flex-wrap"),
					Div(
						Class("app-header-brand inline-flex items-center gap-2"),
						Button(Type("button"), ID("nav-toggle"), Class(ClassNames(IconButtonClass("small"), "app-header-menu hidden max-md:inline-flex")), Attr("aria-label", "Toggle navigation"), Attr("aria-controls", "app-sidebar"), Attr("aria-expanded", "false"), I(Class(IconGlyphClass()), Attr("data-lucide", "menu"), Attr("aria-hidden", "true")), Span(Class("sr-only"), Text("Toggle navigation"))),
						Strong(Class("block text-[0.8125rem] font-semibold uppercase tracking-[0.04em]"), Text("Duck Platform")),
					),
					Div(
						Class("app-header-meta inline-flex items-center gap-2 max-md:w-full max-md:justify-between"),
						Button(Type("button"), ID("sidebar-toggle"), Class(ClassNames(IconButtonClass("small"), "max-md:hidden")), Attr("aria-label", "Toggle compact sidebar"), Title("Toggle compact sidebar"), I(Class(IconGlyphClass()), Attr("data-lucide", "panel-left"), Attr("aria-hidden", "true")), Span(Class("sr-only"), Text("Toggle compact sidebar"))),
						P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Signed in as "+principalLabel)),
						Button(Type("button"), ID("theme-toggle"), Class(IconButtonClass("small")), Title("Toggle theme"), Attr("aria-label", "Toggle theme"), Span(ID("theme-icon-sun"), I(Class(IconGlyphClass()), Attr("data-lucide", "sun"), Attr("aria-hidden", "true"))), Span(ID("theme-icon-moon"), Class("hidden"), I(Class(IconGlyphClass()), Attr("data-lucide", "moon"), Attr("aria-hidden", "true"))), Span(Class("sr-only"), Text("Toggle theme"))),
						Form(Method("post"), Action("/ui/logout"), Button(Type("submit"), Class(SecondaryButtonClass("small")), Text("Sign out"))),
					),
				),
				Div(
					Class("app-body grid min-h-0 flex-1 overflow-hidden [grid-template-columns:var(--size-sidebar-width)_minmax(0,1fr)] max-md:grid-cols-1"),
					Aside(Class("app-sidebar relative h-full overflow-y-auto border-r border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-2 py-4 shadow-[var(--shadow-resting-xsmall)] transition-transform duration-100 ease-out [.app-shell.sidebar-compact_&]:px-1 max-md:fixed max-md:inset-y-0 max-md:left-0 max-md:z-20 max-md:h-screen max-md:w-[min(var(--size-sidebar-width),calc(var(--overlay-width-small)+var(--space-5)))] max-md:-translate-x-full max-md:[.app-shell.nav-open_&]:translate-x-0"), ID("app-sidebar"), Nav(Class("app-nav grid gap-1 max-md:flex max-md:flex-wrap max-md:gap-2"), Group(nav))),
					Section(Class(ClassNames(mainClass, "flex min-h-0 flex-col overflow-auto bg-[var(--bgColor-default)] px-[clamp(var(--space-3),3vw,var(--space-5))] py-4 max-md:px-3 max-md:py-3")), ID("main-content"), Attr("tabindex", "-1"), H1(Class("sr-only"), Text(title)), Div(Class(ClassNames("app-main-content", contentClass, "flex min-h-0 w-full flex-1 flex-col")), Group(body))),
				),
				Div(Class("app-overlay pointer-events-none fixed inset-0 z-10 hidden bg-[var(--overlay-backdrop-bgColor)] opacity-0 transition-opacity duration-100 ease-out [.app-shell.nav-open_&]:opacity-100 [.app-shell.nav-open_&]:pointer-events-auto max-md:block"), ID("app-overlay"), Attr("aria-hidden", "true")),
			),
			Script(Raw(ThemeBehaviorScript)),
			Script(Raw(ShellBehaviorScript)),
			Script(Raw("if (window.lucide) { window.lucide.createIcons(); } document.addEventListener('click', function(e){ var t=e.target; if(!(t instanceof Element)){return;} document.querySelectorAll('details.dropdown[open]').forEach(function(d){ if(!d.contains(t)){ d.removeAttribute('open'); }}); });")),
		),
	)
}
