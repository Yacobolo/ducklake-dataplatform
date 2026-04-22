package core

import (
	"os"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"

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

type ApplicationLayoutSlots struct {
	Header              Node
	PrimaryRail         Node
	SecondaryAside      Node
	Main                Node
	TertiaryAside       Node
	Footer              Node
	CenterAttributes    []Node
	ShellClass          string
	CenterClass         string
	PrimaryRailClass    string
	SecondaryAsideClass string
	MainClass           string
	TertiaryAsideClass  string
	FooterClass         string
}

var navGroups = []navGroup{
	{
		Label: "Discover",
		Items: []navItem{
			{Label: "Overview", Href: "/ui", Key: "home", Icon: "house"},
			{Label: "Explore", Href: "/ui/explore", Key: "explore", Icon: "compass"},
			{Label: "Catalogs", Href: "/ui/catalogs", Key: "catalogs", Icon: "database"},
			{Label: "Runtime Assets", Href: "/ui/assets", Key: "assets", Icon: "git-fork"},
			{Label: "Dashboards", Href: "/ui/dashboards", Key: "dashboards", Icon: "chart-column"},
		},
	},
	{
		Label: "Build",
		Items: []navItem{
			{Label: "Projects", Href: "/ui/projects", Key: "projects", Icon: "folder-git-2"},
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
	return appPage(title, active, principal, "mx-auto flex min-h-0 w-full max-w-7xl flex-1 flex-col gap-6 max-md:gap-5", body...)
}

func AppPageFullWidth(title, active string, principal domain.ContextPrincipal, body ...Node) Node {
	return appPage(title, active, principal, "flex min-h-0 w-full flex-1 flex-col gap-6 max-md:gap-5", body...)
}

func ApplicationPage(title, active string, principal domain.ContextPrincipal, slots ApplicationLayoutSlots) Node {
	devInspector := shouldShowDatastarInspector()
	if slots.Header == nil {
		slots.Header = defaultAppHeader(principal, false)
	}
	if slots.PrimaryRail == nil {
		slots.PrimaryRail = appPrimaryRail(active)
	}
	return appDocument(title, devInspector, applicationShell(slots))
}

func ApplicationLayout(slots ApplicationLayoutSlots) Node {
	return applicationShell(slots)
}

func appPage(title, active string, principal domain.ContextPrincipal, mainContentClass string, body ...Node) Node {
	devInspector := shouldShowDatastarInspector()
	return appDocument(title, devInspector, legacyAppShell(title, active, principal, mainContentClass, body...))
}

func appDocument(title string, devInspector bool, body ...Node) Node {
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
			StyleEl(Text(uiGlobalStyles())),
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
			Group(body),
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

func legacyAppShell(title, active string, principal domain.ContextPrincipal, mainContentClass string, body ...Node) Node {
	return Main(
		Class("app-shell flex h-full flex-col"),
		defaultAppHeader(principal, true),
		Div(
			Class("app-body grid min-h-0 flex-1 overflow-hidden [grid-template-columns:18rem_minmax(0,1fr)] max-md:grid-cols-1"),
			Aside(
				Class("app-sidebar relative h-full overflow-y-auto border-r border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-2 py-4 shadow-xs transition-transform duration-100 ease-out [.app-shell.sidebar-compact_&]:px-1 max-md:fixed max-md:inset-y-0 max-md:left-0 max-md:z-20 max-md:h-screen max-md:w-72 max-md:-translate-x-full max-md:[.app-shell.nav-open_&]:translate-x-0"),
				ID("app-sidebar"),
				appNavigation(active, "app-nav grid gap-4"),
			),
			MainContentSection(title, mainContentClass, Group(body)),
		),
		appOverlay(),
	)
}

func applicationShell(slots ApplicationLayoutSlots) Node {
	centerClass := "app-layout-center"
	if slots.TertiaryAside != nil {
		centerClass = ClassNames(centerClass, "has-tertiary")
	}
	centerClass = ClassNames(centerClass, strings.TrimSpace(slots.CenterClass))

	footerContent := slots.Footer
	if footerContent == nil {
		footerContent = Span(Class("sr-only"), Text("Application status"))
	}

	return Main(
		Class(ClassNames("app-shell app-layout-shell sidebar-compact", strings.TrimSpace(slots.ShellClass))),
		Attr("data-shell-compact-locked", "true"),
		slots.Header,
		Div(
			Class(centerClass),
			Attr("data-workspace-layout", "true"),
			Group(slots.CenterAttributes),
			Aside(
				Class(ClassNames("app-primary-rail", strings.TrimSpace(slots.PrimaryRailClass))),
				Attr("aria-label", "Primary navigation"),
				slots.PrimaryRail,
			),
			Aside(
				Class(ClassNames("app-secondary-aside", strings.TrimSpace(slots.SecondaryAsideClass))),
				ID("app-sidebar"),
				Attr("aria-label", "Secondary sidebar"),
				Div(Class("app-secondary-aside-inner"), slots.SecondaryAside),
			),
			Div(
				Class(ClassNames("app-layout-main-region", strings.TrimSpace(slots.MainClass))),
				slots.Main,
			),
			func() Node {
				if slots.TertiaryAside == nil {
					return nil
				}
				return Aside(
					Class(ClassNames("app-tertiary-aside", strings.TrimSpace(slots.TertiaryAsideClass))),
					Attr("aria-label", "Tertiary sidebar"),
					slots.TertiaryAside,
				)
			}(),
		),
		Footer(
			Class(ClassNames("app-layout-footer", strings.TrimSpace(slots.FooterClass))),
			footerContent,
		),
		appOverlay(),
	)
}

func MainContentSection(title string, contentClass string, body ...Node) Node {
	return mainContentSection(title, contentClass, true, body...)
}

func ApplicationMainContentSection(title string, contentClass string, body ...Node) Node {
	return mainContentSection(title, contentClass, false, body...)
}

func mainContentSection(title string, contentClass string, includeLegacyContentClass bool, body ...Node) Node {
	if strings.TrimSpace(contentClass) == "" {
		contentClass = "mx-auto flex min-h-0 w-full max-w-7xl flex-1 flex-col gap-6 max-md:gap-5"
	}
	mainContentClass := ClassNames("app-main-content", contentClass)
	if includeLegacyContentClass {
		mainContentClass = ClassNames(mainContentClass, "content")
	}
	return Section(
		Class(ClassNames("app-main", "flex min-h-0 flex-col overflow-auto bg-[var(--bgColor-default)] px-[clamp(0.75rem,3vw,1.5rem)] py-5 max-md:px-3 max-md:py-4")),
		ID("main-content"),
		Attr("tabindex", "-1"),
		H1(Class("sr-only"), Text(title)),
		Div(Class(mainContentClass), Group(body)),
	)
}

func defaultAppHeader(principal domain.ContextPrincipal, showCompactToggle bool) Node {
	principalLabel := principal.Name
	if principalLabel == "" {
		principalLabel = "unknown"
	}

	compactToggle := Node(nil)
	if showCompactToggle {
		compactToggle = Button(
			Type("button"),
			ID("sidebar-toggle"),
			Class(ClassNames(iconButtonClass("small"), "max-md:hidden")),
			Attr("aria-label", "Toggle compact sidebar"),
			Title("Toggle compact sidebar"),
			Icon("panel-left", Class(IconGlyphClass())),
			Span(Class("sr-only"), Text("Toggle compact sidebar")),
		)
	}

	return Header(
		Class("app-header flex items-center justify-between gap-4 border-b border-[var(--borderColor-default)] bg-[var(--bgColor-muted)] px-4 py-3 shadow-xs max-md:flex-wrap"),
		Div(
			Class("app-header-brand inline-flex items-center gap-2"),
			Button(
				Type("button"),
				ID("nav-toggle"),
				Class(ClassNames(iconButtonClass("small"), "app-header-menu hidden max-md:inline-flex")),
				Attr("aria-label", "Toggle navigation"),
				Attr("aria-controls", "app-sidebar"),
				Attr("aria-expanded", "false"),
				Icon("menu", Class(IconGlyphClass())),
				Span(Class("sr-only"), Text("Toggle navigation")),
			),
			Strong(Class("block text-[0.8125rem] font-semibold uppercase tracking-[0.04em]"), Text("Duck Platform")),
		),
		Div(
			Class("app-header-meta inline-flex items-center gap-2 max-md:w-full max-md:justify-between"),
			compactToggle,
			P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Signed in as "+principalLabel)),
			Button(
				Type("button"),
				ID("theme-toggle"),
				Class(iconButtonClass("small")),
				Title("Toggle theme"),
				Attr("aria-label", "Toggle theme"),
				Span(ID("theme-icon-sun"), Icon("sun", Class(IconGlyphClass()))),
				Span(ID("theme-icon-moon"), Class("hidden"), Icon("moon", Class(IconGlyphClass()))),
				Span(Class("sr-only"), Text("Toggle theme")),
			),
			Form(Method("post"), Action("/ui/logout"), Button(Type("submit"), Class(secondaryButtonClass("small")), Text("Sign out"))),
		),
	)
}

func appPrimaryRail(active string) Node {
	return appNavigation(active, "app-nav app-primary-rail-nav grid gap-4")
}

func appNavigation(active, className string) Node {
	nav := make([]Node, 0, len(navGroups))
	for _, group := range navGroups {
		items := make([]Node, 0, len(group.Items))
		for _, item := range group.Items {
			itemActive := navItemActive(item, active)
			linkClass := ClassNames(
				"app-nav-link",
				"group flex items-center gap-3 rounded-xl px-3 py-2 text-sm font-medium text-[var(--fgColor-muted)] transition-colors hover:bg-[var(--bgColor-muted)] hover:text-[var(--fgColor-default)] [.app-shell.sidebar-compact_&]:justify-center [.app-shell.sidebar-compact_&]:px-1 max-md:[.app-shell.sidebar-compact_&]:justify-start max-md:[.app-shell.sidebar-compact_&]:px-3",
			)
			currentAttr := Node(nil)
			if item.Key == active {
				linkClass = ClassNames(linkClass, "active bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)] shadow-[inset_3px_0_0_var(--borderColor-accent-emphasis)]")
				currentAttr = Attr("aria-current", "page")
			} else if itemActive {
				linkClass = ClassNames(linkClass, "text-[var(--fgColor-default)]")
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
					Class(linkClass),
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
	return Nav(Class(className), Group(nav))
}

func appOverlay() Node {
	return Div(
		Class("app-overlay pointer-events-none fixed inset-0 z-10 hidden bg-black/40 opacity-0 transition-opacity duration-100 ease-out [.app-shell.nav-open_&]:opacity-100 [.app-shell.nav-open_&]:pointer-events-auto max-md:block"),
		ID("app-overlay"),
		Attr("aria-hidden", "true"),
	)
}

func uiGlobalStyles() string {
	return `
.ui-icon-action-trigger {
  color: var(--fgColor-muted);
  cursor: pointer;
}

.ui-icon-action-trigger:hover,
.ui-icon-action-trigger:focus-visible {
  color: var(--fgColor-accent);
}

.ui-icon-action-trigger:disabled {
  color: var(--fgColor-muted);
  opacity: 0.55;
  cursor: not-allowed;
}

.app-layout-shell {
  display: flex;
  height: 100%;
  min-height: 0;
  flex-direction: column;
  overflow: hidden;
}

.app-layout-center {
  display: grid;
  min-height: 0;
  flex: 1 1 auto;
  overflow: hidden;
  grid-template-columns: 3.75rem 22rem minmax(0, 1fr);
}

.app-layout-center.has-tertiary {
  grid-template-columns: 3.75rem 22rem minmax(0, 1fr) 18rem;
}

.app-primary-rail {
  min-width: 0;
  overflow-y: auto;
  border-right: 1px solid var(--borderColor-default);
  background: var(--bgColor-muted);
  padding: 0.75rem 0.35rem;
  box-shadow: var(--shadow-xs, 0 1px 2px rgba(15, 23, 42, 0.08));
}

.app-primary-rail-nav {
  align-content: start;
}

.app-secondary-aside {
  min-width: 0;
  min-height: 0;
  overflow-y: auto;
  border-right: 1px solid var(--borderColor-default);
  background: var(--bgColor-default);
  padding: 1rem 0.75rem;
}

.app-secondary-aside-inner {
  min-height: 100%;
}

.app-layout-main-region {
  display: flex;
  justify-content: center;
  min-width: 0;
  min-height: 0;
  overflow: hidden;
  background: var(--bgColor-default);
  padding-inline: clamp(1rem, 2.5vw, 3rem);
}

.app-layout-main-region .app-main {
  width: 100%;
  max-width: 180rem;
}

.app-tertiary-aside {
  min-width: 0;
  min-height: 0;
  overflow-y: auto;
  border-left: 1px solid var(--borderColor-default);
  background: var(--bgColor-muted);
  padding: 1rem 0.75rem;
}

.app-layout-footer {
  display: flex;
  min-height: 2rem;
  flex-shrink: 0;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  border-top: 1px solid var(--borderColor-default);
  background: var(--bgColor-muted);
  padding: 0 0.875rem;
  font-size: 0.75rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--fgColor-muted);
}

@media (max-width: 80rem) {
  .app-layout-center.has-tertiary {
    grid-template-columns: 3.75rem 22rem minmax(0, 1fr);
  }

  .app-tertiary-aside {
    display: none;
  }
}

@media (max-width: 48rem) {
  .app-layout-center,
  .app-layout-center.has-tertiary {
    position: relative;
    display: block;
  }

  .app-primary-rail {
    display: none;
  }

  .app-secondary-aside {
    position: fixed;
    inset: 0 auto 0 0;
    z-index: 20;
    width: min(20rem, calc(100vw - 1rem));
    max-width: calc(100vw - 1rem);
    transform: translateX(-100%);
    transition: transform 100ms ease-out;
    box-shadow: 0 24px 48px rgba(15, 23, 42, 0.24);
  }

  .app-shell.nav-open .app-secondary-aside {
    transform: translateX(0);
  }

  .app-layout-main-region {
    height: 100%;
    padding-inline: 0;
  }

  .app-layout-footer {
    min-height: 2.25rem;
    padding: 0 0.75rem;
  }
}
` + dataTableGlobalStyles()
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
