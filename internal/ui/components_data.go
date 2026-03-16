package ui

import (
	"strings"

	. "maragu.dev/gomponents"
	data "maragu.dev/gomponents-datastar"
	. "maragu.dev/gomponents/html"
)

func metricCard(label, value, meta, tone string) Node {
	classes := "relative overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-small)] before:absolute before:inset-y-0 before:left-0 before:w-1 before:bg-[var(--borderColor-accent-emphasis)] before:content-['']"
	trimmedTone := strings.TrimSpace(tone)
	if trimmedTone != "" {
		switch trimmedTone {
		case "success":
			classes += " bg-[linear-gradient(135deg,var(--bgColor-success-muted)_0%,var(--bgColor-default)_45%)] before:bg-[var(--borderColor-success-emphasis)]"
		case "attention":
			classes += " bg-[linear-gradient(135deg,var(--bgColor-attention-muted)_0%,var(--bgColor-default)_45%)] before:bg-[var(--borderColor-attention-emphasis)]"
		default:
			classes += " bg-[linear-gradient(135deg,var(--bgColor-accent-muted)_0%,var(--bgColor-default)_45%)]"
		}
	}

	metaNode := Node(nil)
	if strings.TrimSpace(meta) != "" {
		metaNode = P(Class(mutedClass()), Text(meta))
	}

	return Div(
		Class(classes),
		P(Class("m-0 text-xs font-semibold text-[var(--fgColor-default)]"), Text(label)),
		P(Class("my-1 text-3xl font-semibold leading-[var(--text-title-lineHeight-medium)] text-[var(--fgColor-default)]"), Text(value)),
		metaNode,
	)
}

func actionBar() Node {
	return Div(
		Class("grid gap-3 rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] p-4 shadow-[var(--shadow-resting-xsmall)] lg:grid-cols-[minmax(0,1fr)_minmax(16rem,20rem)_minmax(12rem,14rem)_auto] lg:items-end"),
		data.Signals(map[string]any{"q": "", "sort": "updated"}),
		Div(
			Class("flex flex-col gap-1"),
			Span(Class(labelClass("")), Text("Component Catalog")),
			P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text("Filter and sort reusable building blocks before composing screens.")),
		),
		Div(
			Class("flex flex-col gap-1"),
			Label(For("component-search"), Class("text-xs font-semibold text-[var(--fgColor-muted)]"), Text("Search")),
			Input(ID("component-search"), Type("search"), Class(formControlClass()), Placeholder("Search components"), AutoComplete("off"), data.Bind("q")),
		),
		Div(
			Class("flex flex-col gap-1"),
			Label(For("component-sort"), Class("text-xs font-semibold text-[var(--fgColor-muted)]"), Text("Sort")),
			Select(
				ID("component-sort"),
				Class(formSelectClass()),
				data.Bind("sort"),
				Option(Value("updated"), Text("Recently updated")),
				Option(Value("name"), Text("Name")),
				Option(Value("category"), Text("Category")),
			),
		),
		Div(
			Class("flex flex-wrap items-center gap-2"),
			Button(Type("button"), Class(secondaryButtonClass()), Text("Reset")),
			Button(Type("button"), Class(primaryButtonClass()), Text("Create component")),
		),
	)
}
