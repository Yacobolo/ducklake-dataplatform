package ui

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	html "maragu.dev/gomponents/html"
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

func appPage(title, active string, principal domain.ContextPrincipal, body ...gomponents.Node) gomponents.Node {
	nav := make([]gomponents.Node, 0, len(navItems))
	for _, item := range navItems {
		className := "app-nav-link Link--secondary d-flex flex-items-center"
		if item.Key == active {
			className += " active"
		}
		nav = append(nav, html.A(
			html.Href(item.Href),
			html.Class(className),
			html.I(html.Class("nav-icon"), gomponents.Attr("data-lucide", item.Icon), gomponents.Attr("aria-hidden", "true")),
			html.Span(gomponents.Text(item.Label)),
		))
	}

	principalLabel := principal.Name
	if principalLabel == "" {
		principalLabel = "unknown"
	}

	return html.HTML(
		html.Lang("en"),
		html.Head(
			html.Meta(html.Charset("utf-8")),
			html.Meta(html.Name("viewport"), html.Content("width=device-width, initial-scale=1")),
			html.TitleEl(gomponents.Text(title+" | Duck UI")),
			html.Link(html.Rel("preconnect"), html.Href("https://fonts.googleapis.com")),
			html.Link(html.Rel("preconnect"), html.Href("https://fonts.gstatic.com"), gomponents.Attr("crossorigin", "")),
			html.Link(html.Rel("stylesheet"), html.Href("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
			html.Link(html.Rel("stylesheet"), html.Href("https://cdn.jsdelivr.net/npm/@primer/css@22.1.0/dist/primer.min.css")),
			html.Link(html.Rel("stylesheet"), html.Href("/ui/static/app.css")),
			html.Script(html.Src("https://unpkg.com/lucide@latest/dist/umd/lucide.min.js")),
			html.Script(
				html.Type("module"),
				html.Src("https://cdn.jsdelivr.net/gh/starfederation/datastar@1.0.0-RC.7/bundles/datastar.js"),
			),
		),
		html.Body(
			html.Main(html.Class("app-shell"),
				html.Aside(
					html.Class("app-sidebar"),
					html.Div(
						html.Class("brand"),
						html.Strong(gomponents.Text("Duck Platform")),
						html.P(html.Class("color-fg-muted text-small mb-0"), gomponents.Text("Metadata browser and editor")),
					),
					html.Nav(html.Class("app-nav"), gomponents.Group(nav)),
				),
				html.Section(
					html.Class("app-main"),
					html.Div(
						html.Class("topbar"),
						html.Div(
							html.H1(html.Class("page-title"), gomponents.Text(title)),
						),
						html.Div(
							html.P(html.Class("color-fg-muted text-small mb-2"), gomponents.Text("Signed in as "+principalLabel)),
							html.Form(
								html.Method("post"),
								html.Action("/ui/logout"),
								html.Button(html.Type("submit"), html.Class("btn btn-sm"), gomponents.Text("Sign out")),
							),
						),
					),
					html.Div(html.Class("content"), gomponents.Group(body)),
				),
			),
			html.Script(gomponents.Text("if (window.lucide) { window.lucide.createIcons(); }")),
		),
	)
}

func errorPage(title, message string) gomponents.Node {
	return html.HTML(
		html.Lang("en"),
		html.Head(
			html.Meta(html.Charset("utf-8")),
			html.Meta(html.Name("viewport"), html.Content("width=device-width, initial-scale=1")),
			html.TitleEl(gomponents.Text(title+" | Duck UI")),
			html.Link(html.Rel("preconnect"), html.Href("https://fonts.googleapis.com")),
			html.Link(html.Rel("preconnect"), html.Href("https://fonts.gstatic.com"), gomponents.Attr("crossorigin", "")),
			html.Link(html.Rel("stylesheet"), html.Href("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap")),
			html.Link(html.Rel("stylesheet"), html.Href("https://cdn.jsdelivr.net/npm/@primer/css@22.1.0/dist/primer.min.css")),
			html.Link(html.Rel("stylesheet"), html.Href("/ui/static/app.css")),
			html.Script(html.Src("https://unpkg.com/lucide@latest/dist/umd/lucide.min.js")),
		),
		html.Body(
			html.Main(
				html.Class("layout"),
				html.H1(html.Class("page-title"), gomponents.Text(title)),
				html.P(gomponents.Text(message)),
				html.P(html.A(html.Href("/ui"), gomponents.Text("Back to overview"))),
			),
			html.Script(gomponents.Text("if (window.lucide) { window.lucide.createIcons(); }")),
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

func paginationCard(basePath string, page domain.PageRequest, total int64) gomponents.Node {
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	if nextToken == "" {
		return html.Div(html.Class(cardClass()), html.P(html.Class(mutedClass()), gomponents.Text(fmt.Sprintf("Showing %d of %d entries.", min(page.Limit(), int(total)), total))))
	}
	url := fmt.Sprintf("%s?max_results=%d&page_token=%s", basePath, page.Limit(), nextToken)
	return html.Div(
		html.Class(cardClass()),
		html.P(html.Class(mutedClass()), gomponents.Text(fmt.Sprintf("Showing up to %d of %d entries.", page.Limit(), total))),
		html.A(html.Href(url), gomponents.Text("Next page ->")),
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
