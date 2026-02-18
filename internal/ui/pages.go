package ui

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"duck-demo/internal/domain"

	gomponents "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	html "maragu.dev/gomponents/html"
)

type navItem struct {
	Label string
	Href  string
	Key   string
}

var navItems = []navItem{
	{Label: "Overview", Href: "/ui", Key: "home"},
	{Label: "Catalogs", Href: "/ui/catalogs", Key: "catalogs"},
	{Label: "Pipelines", Href: "/ui/pipelines", Key: "pipelines"},
	{Label: "Notebooks", Href: "/ui/notebooks", Key: "notebooks"},
	{Label: "Macros", Href: "/ui/macros", Key: "macros"},
	{Label: "Models", Href: "/ui/models", Key: "models"},
}

func appPage(title, active string, principal domain.ContextPrincipal, body ...gomponents.Node) gomponents.Node {
	nav := make([]gomponents.Node, 0, len(navItems))
	for _, item := range navItems {
		className := ""
		if item.Key == active {
			className = "active"
		}
		nav = append(nav, html.A(html.Href(item.Href), html.Class(className), gomponents.Text(item.Label)))
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
			html.Link(html.Rel("stylesheet"), html.Href("/ui/static/app.css")),
			html.Script(
				html.Type("module"),
				html.Src("https://cdn.jsdelivr.net/gh/starfederation/datastar@1.0.0-RC.6/bundles/datastar.js"),
			),
		),
		html.Body(
			html.Main(
				html.Class("layout"),
				html.Div(
					html.Class("topbar"),
					html.Div(
						html.Strong(gomponents.Text("Duck Platform UI")),
						html.P(html.Class("muted"), gomponents.Text("Read-only metadata browser")),
					),
					html.Div(
						html.P(html.Class("muted"), gomponents.Text("Signed in as "+principalLabel)),
						html.Form(
							html.Method("post"),
							html.Action("/ui/logout"),
							html.Button(html.Type("submit"), html.Class("secondary"), gomponents.Text("Sign out")),
						),
					),
				),
				html.Nav(html.Class("nav"), gomponents.Group(nav)),
				html.H1(html.Class("page-title"), gomponents.Text(title)),
				gomponents.Group(body),
			),
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
			html.Link(html.Rel("stylesheet"), html.Href("/ui/static/app.css")),
		),
		html.Body(
			html.Main(
				html.Class("layout"),
				html.H1(html.Class("page-title"), gomponents.Text(title)),
				html.P(gomponents.Text(message)),
				html.P(html.A(html.Href("/ui"), gomponents.Text("Back to overview"))),
			),
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
	parts := make([]string, 0, len(m))
	for k, v := range m {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
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
		return html.Div(html.Class("card"), html.P(html.Class("muted"), gomponents.Text(fmt.Sprintf("Showing %d of %d entries.", min(page.Limit(), int(total)), total))))
	}
	url := fmt.Sprintf("%s?max_results=%d&page_token=%s", basePath, page.Limit(), nextToken)
	return html.Div(
		html.Class("card"),
		html.P(html.Class("muted"), gomponents.Text(fmt.Sprintf("Showing up to %d of %d entries.", page.Limit(), total))),
		html.A(html.Href(url), gomponents.Text("Next page ->")),
	)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func filterInput(placeholder string) gomponents.Node {
	return html.Div(
		html.Class("card"),
		data.Signals(map[string]any{"q": ""}),
		html.Label(gomponents.Text("Quick filter")),
		html.Input(html.Type("text"), html.Placeholder(placeholder), data.Bind("q")),
	)
}
