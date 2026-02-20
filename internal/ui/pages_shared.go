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
		if item.Key == active {
			className += " active"
		}
		nav = append(nav, A(
			Href(item.Href),
			Class(className),
			I(Class("nav-icon"), Attr("data-lucide", item.Icon), Attr("aria-hidden", "true")),
			Span(Text(item.Label)),
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
			Link(Rel("preconnect"), Href("https://fonts.googleapis.com")),
			Link(Rel("preconnect"), Href("https://fonts.gstatic.com"), Attr("crossorigin", "")),
			Link(Rel("stylesheet"), Href("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
			Link(Rel("stylesheet"), Href("/ui/static/app.css")),
			Script(Src("https://unpkg.com/lucide@latest/dist/umd/lucide.min.js")),
			Script(
				Type("module"),
				Src("https://cdn.jsdelivr.net/gh/starfederation/datastar@1.0.0-RC.7/bundles/datastar.js"),
			),
		),
		Body(
			Main(Class("app-shell"),
				Aside(
					Class("app-sidebar"),
					Div(
						Class("brand"),
						Strong(Text("Duck Platform")),
						P(Class("color-fg-muted text-small mb-0"), Text("Metadata browser and editor")),
					),
					Nav(Class("app-nav"), Group(nav)),
				),
				Section(
					Class("app-main"),
					Div(
						Class("topbar"),
						Div(
							H1(Class("page-title"), Text(title)),
						),
						Div(
							P(Class("color-fg-muted text-small mb-2"), Text("Signed in as "+principalLabel)),
							Form(
								Method("post"),
								Action("/ui/logout"),
								Button(Type("submit"), Class("btn btn-sm"), Text("Sign out")),
							),
						),
					),
					Div(Class("content"), Group(body)),
				),
			),
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
			Link(Rel("preconnect"), Href("https://fonts.googleapis.com")),
			Link(Rel("preconnect"), Href("https://fonts.gstatic.com"), Attr("crossorigin", "")),
			Link(Rel("stylesheet"), Href("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
			Link(Rel("stylesheet"), Href("/ui/static/app.css")),
			Script(Src("https://unpkg.com/lucide@latest/dist/umd/lucide.min.js")),
		),
		Body(
			Main(
				Class("layout"),
				H1(Class("page-title"), Text(title)),
				P(Text(message)),
				P(A(Href("/ui"), Text("Back to overview"))),
			),
			Script(Raw("if (window.lucide) { window.lucide.createIcons(); }")),
		),
	)
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
