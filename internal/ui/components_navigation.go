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
		className := "SegmentedControl-item"
		if item.Active {
			className += " is-active"
		}
		nodes = append(nodes, Button(Type("button"), Class(className), Attr("aria-pressed", boolToString(item.Active)), Text(item.Label)))
	}

	return Div(Class("SegmentedControl"), Group(nodes))
}

func breadcrumbs(items []breadcrumbItem) Node {
	if len(items) == 0 {
		return Node(nil)
	}

	nodes := make([]Node, 0, len(items))
	for i := range items {
		item := items[i]
		linkClass := "Breadcrumbs-link"
		ariaCurrent := Node(nil)
		if item.Active {
			linkClass += " is-active"
			ariaCurrent = Attr("aria-current", "page")
		}
		nodes = append(nodes,
			Li(
				Class("Breadcrumbs-item"),
				A(Href(fallbackString(item.Href, "#")), Class(linkClass), ariaCurrent, Text(item.Label)),
			),
		)
	}

	return Nav(
		Class("Breadcrumbs"),
		Attr("aria-label", "Breadcrumb"),
		Ol(Class("Breadcrumbs-list"), Group(nodes)),
	)
}

func treeView(items []treeViewItem) Node {
	if len(items) == 0 {
		return P(Class("TreeView-empty"), Text("No items"))
	}

	nodes := make([]Node, 0, len(items))
	for i := range items {
		nodes = append(nodes, treeViewNode(items[i]))
	}

	return Ul(Class("TreeView-root"), Group(nodes))
}

func treeViewNode(item treeViewItem) Node {
	icon := strings.TrimSpace(item.Icon)
	if icon == "" {
		icon = "circle"
	}

	linkClass := "TreeView-link"
	if item.Active {
		linkClass += " active"
	}

	link := A(
		Href(fallbackString(item.Href, "#")),
		Class(linkClass),
		I(Class("nav-icon"), Attr("data-lucide", icon), Attr("aria-hidden", "true")),
		Span(Text(item.Label)),
	)

	if len(item.Children) == 0 {
		return Li(Class("TreeView-node"), link)
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
		Class("TreeView-node"),
		Details(
			Class("details-reset TreeView-disclosure"),
			openAttr,
			Summary(
				Class("TreeView-summary"),
				I(Class("nav-icon TreeView-caret"), Attr("data-lucide", "chevron-right"), Attr("aria-hidden", "true")),
				link,
			),
			Ul(Class("TreeView-children"), Group(childNodes)),
		),
	)
}
