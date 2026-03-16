package ui

import (
	"strings"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func segmentedTabs(items []segmentedTabItem) Node {
	nodes := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		className := "inline-flex min-h-9 items-center justify-center rounded-lg px-3 py-2 text-sm font-medium text-[var(--fgColor-muted)] transition-colors hover:text-[var(--fgColor-default)]"
		if item.Active {
			className += " bg-[var(--bgColor-default)] text-[var(--fgColor-default)] shadow-[var(--shadow-resting-xsmall)]"
		}
		nodes = append(nodes, Button(Type("button"), Class(className), Attr("aria-pressed", boolToString(item.Active)), Text(item.Label)))
	}

	return Div(Class("inline-flex flex-wrap gap-1 rounded-xl bg-[var(--bgColor-muted)] p-1"), Group(nodes))
}

func breadcrumbs(items []breadcrumbItem) Node {
	if len(items) == 0 {
		return Node(nil)
	}

	nodes := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		linkClass := "rounded-md px-2 py-1 text-xs text-[var(--fgColor-accent)] no-underline hover:bg-[var(--bgColor-muted)]"
		ariaCurrent := Node(nil)
		if item.Active {
			linkClass += " font-semibold text-[var(--fgColor-default)]"
			ariaCurrent = Attr("aria-current", "page")
		}
		nodes = append(nodes,
			Li(
				Class("inline-flex items-center gap-1"),
				A(Href(fallbackString(item.Href, "#")), Class(linkClass), ariaCurrent, Text(item.Label)),
			),
		)
	}

	return Nav(
		Attr("aria-label", "Breadcrumb"),
		Ol(Class("flex flex-wrap items-center gap-1 text-xs text-[var(--fgColor-muted)]"), Group(nodes)),
	)
}

func treeView(items []treeViewItem) Node {
	if len(items) == 0 {
		return P(Class("text-sm text-[var(--fgColor-muted)]"), Text("No items"))
	}

	nodes := make([]Node, 0, len(items))
	for i := range items {
		nodes = append(nodes, treeViewNode(items[i]))
	}

	return Ul(Class("grid gap-1"), Group(nodes))
}

func treeViewNode(item treeViewItem) Node {
	icon := strings.TrimSpace(item.Icon)
	if icon == "" {
		icon = "circle"
	}

	linkClass := "inline-flex min-h-9 items-center gap-2 rounded-lg px-3 py-2 text-sm text-[var(--fgColor-default)] no-underline hover:bg-[var(--bgColor-muted)]"
	if item.Active {
		linkClass += " bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
	}

	link := A(
		Href(fallbackString(item.Href, "#")),
		Class(linkClass),
		I(Class(navIconClass()), Attr("data-lucide", icon), Attr("aria-hidden", "true")),
		Span(Text(item.Label)),
	)

	if len(item.Children) == 0 {
		return Li(link)
	}

	childNodes := make([]Node, 0, len(item.Children))
	for i := range item.Children {
		childNodes = append(childNodes, treeViewNode(item.Children[i]))
	}

	openAttr := Node(nil)
	if item.Open {
		openAttr = Attr("open", "")
	}

	return Li(
		Details(
			Class("group"),
			openAttr,
			Summary(
				Class(detailsSummaryClass("flex items-center gap-2")),
				I(Class(navIconClass("transition-transform group-open:rotate-90")), Attr("data-lucide", "chevron-right"), Attr("aria-hidden", "true")),
				link,
			),
			Ul(Class("ml-6 mt-1 grid gap-1 border-l border-[var(--borderColor-muted)] pl-2"), Group(childNodes)),
		),
	)
}
