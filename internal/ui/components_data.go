package ui

import (
	"strings"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

func metricCard(label, value, meta, tone string) Node {
	classes := "metric-card"
	trimmedTone := strings.TrimSpace(tone)
	if trimmedTone != "" {
		classes += " metric-card-" + trimmedTone
	}

	metaNode := Node(nil)
	if strings.TrimSpace(meta) != "" {
		metaNode = P(Class(mutedClass()), Text(meta))
	}

	return Div(
		Class(classes),
		P(Class("metric-label"), Text(label)),
		P(Class("metric-value"), Text(value)),
		metaNode,
	)
}

func actionBar() Node {
	return Div(
		Class("ActionBar"),
		data.Signals(map[string]any{"q": "", "sort": "updated"}),
		Div(
			Class("ActionBar-search"),
			Label(For("component-search"), Text("Search")),
			Input(ID("component-search"), Type("search"), Class("form-control"), Placeholder("Search components"), AutoComplete("off"), data.Bind("q")),
		),
		Div(
			Class("ActionBar-controls"),
			Label(For("component-sort"), Text("Sort")),
			Select(
				ID("component-sort"),
				Class("form-select"),
				data.Bind("sort"),
				Option(Value("updated"), Text("Recently updated")),
				Option(Value("name"), Text("Name")),
				Option(Value("category"), Text("Category")),
			),
		),
		Div(
			Class("ActionBar-actions"),
			Button(Type("button"), Class(primaryButtonClass()), Text("Create")),
			Button(Type("button"), Class(secondaryButtonClass()), Text("Export")),
		),
	)
}
