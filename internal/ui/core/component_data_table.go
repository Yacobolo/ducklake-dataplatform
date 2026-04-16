package core

import (
	"strings"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func TableContainer(extraClass string, nodes ...Node) Node {
	base := []Node{Class(tableWrapClass(extraClass))}
	return Div(append(base, Div(Class("overflow-x-auto"), Group(nodes)))...)
}

func DataTable(extraClass string, nodes ...Node) Node {
	base := []Node{Class(dataTableClass(extraClass))}
	return Table(append(base, nodes...)...)
}

func IconChip(iconName string, extraClass string, iconAttrs ...Node) Node {
	attrs := []Node{
		Class("h-5 w-5 shrink-0"),
		Attr("style", "stroke-width:1.75"),
	}
	attrs = append(attrs, iconAttrs...)
	return Span(
		Class(ClassNames("inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-lg", extraClass)),
		Icon(iconName, attrs...),
	)
}

func TablePrimaryLink(href, label string) Node {
	return A(
		Href(href),
		Class("font-semibold text-[var(--fgColor-accent)] no-underline visited:text-[var(--fgColor-accent)] hover:text-[var(--fgColor-accent)] hover:underline active:text-[var(--fgColor-accent)]"),
		Text(label),
	)
}

func TablePrimaryCell(icon Node, primary Node, secondary ...Node) Node {
	stackNodes := []Node{primary}
	stackNodes = append(stackNodes, secondary...)
	return Td(
		Div(
			Class("flex items-center gap-3"),
			icon,
			Div(Class("min-w-0 flex-1 grid gap-1"), Group(stackNodes)),
		),
	)
}

func TableMetaText(text string) Node {
	return Span(Class("text-sm text-[var(--fgColor-muted)]"), Text(TableValue(text)))
}

func TableSubtleCopy(text string) Node {
	return P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(TableValue(text)))
}

func TableValue(text string) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" || trimmed == "-" {
		return ""
	}
	return text
}

func TableActionHeader() Node {
	return Th(
		Scope("col"),
		Class("relative"),
		Span(Class("sr-only"), Text("Actions")),
	)
}

func TableActionCell(nodes ...Node) Node {
	return Td(
		Class("text-right"),
		Div(
			Class("flex items-center justify-end gap-2 [&_form]:m-0 [&_form]:inline-flex"),
			Group(nodes),
		),
	)
}

func TableIconActionLink(href, label, iconName, tone string) Node {
	return A(
		Href(href),
		Class(tableIconActionClass(tone)),
		Title(label),
		Attr("aria-label", label),
		tableIconActionContent(label, iconName),
	)
}

func TableIconActionPost(action, label, iconName, tone string, csrfField func() Node, fields ...Node) Node {
	formNodes := []Node{
		Method("post"),
		Action(action),
	}
	if csrfField != nil {
		formNodes = append(formNodes, csrfField())
	}
	formNodes = append(formNodes, fields...)
	formNodes = append(formNodes,
		Button(
			Type("submit"),
			Class(tableIconActionClass(tone)),
			Title(label),
			Attr("aria-label", label),
			tableIconActionContent(label, iconName),
		),
	)
	return Form(formNodes...)
}

func tableIconActionContent(label, iconName string) Node {
	return Group([]Node{
		Icon(iconName, Class("h-[18px] w-[18px]")),
		Span(Class("sr-only"), Text(label)),
	})
}

func dataTableGlobalStyles() string {
	return `
.ui-table-action {
  color: var(--fgColor-muted);
  cursor: pointer;
  text-decoration: none;
}

.ui-table-action:hover,
.ui-table-action:focus-visible {
  color: var(--fgColor-accent);
}

.ui-table-action--danger:hover,
.ui-table-action--danger:focus-visible {
  color: var(--fgColor-danger);
}

.ui-table-action:disabled,
.ui-table-action[aria-disabled="true"] {
  color: var(--fgColor-muted);
  opacity: 0.55;
  cursor: not-allowed;
}
`
}

func tableWrapClass(extra ...string) string {
	return ClassNames("overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[var(--bgColor-default)] shadow-xs", strings.Join(extra, " "))
}

func tableIconActionClass(tone string) string {
	base := "ui-table-action inline-flex h-8 w-8 items-center justify-center rounded-md p-1 transition-colors duration-150 ease-out focus-visible:outline-2 focus-visible:outline-offset-0"
	switch strings.TrimSpace(tone) {
	case "danger":
		return ClassNames(base, "ui-table-action--danger focus-visible:outline-[var(--borderColor-danger-emphasis)]")
	default:
		return ClassNames(base, "focus-visible:outline-[var(--borderColor-accent-emphasis)]")
	}
}

func dataTableClass(extra ...string) string {
	return ClassNames(
		"w-full border-collapse bg-[var(--bgColor-default)] text-left text-sm",
		"[&_thead]:border-b [&_thead]:border-[var(--borderColor-default)] [&_thead]:bg-[var(--bgColor-muted)]",
		"[&_tbody_tr]:border-b [&_tbody_tr]:border-[var(--borderColor-default)] [&_tbody_tr]:transition-colors [&_tbody_tr]:duration-150 [&_tbody_tr]:ease-out [&_tbody_tr:hover]:bg-[var(--bgColor-muted)] [&_tbody_tr:last-child]:border-b-0",
		"[&_td]:px-6 [&_td]:py-4 [&_td]:align-middle",
		"[&_th]:px-6 [&_th]:py-3 [&_th]:text-left [&_th]:text-xs [&_th]:font-bold [&_th]:uppercase [&_th]:tracking-[0.1em] [&_th]:text-[var(--fgColor-muted)]",
		strings.Join(extra, " "),
	)
}
