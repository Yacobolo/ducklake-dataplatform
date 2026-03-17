package core

import (
	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func Card(nodes ...Node) Node {
	return Div(append([]Node{Class(cardClass())}, nodes...)...)
}

func MutedText(nodes ...Node) Node {
	return P(append([]Node{Class(mutedClass())}, nodes...)...)
}

func PrimaryButton(size string, nodes ...Node) Node {
	return Button(append([]Node{Class(primaryButtonClass(size))}, nodes...)...)
}

func SecondaryButton(size string, nodes ...Node) Node {
	return Button(append([]Node{Class(secondaryButtonClass(size))}, nodes...)...)
}

func DangerButton(size string, nodes ...Node) Node {
	return Button(append([]Node{Class(dangerButtonClass(size))}, nodes...)...)
}

func IconButton(size string, nodes ...Node) Node {
	return Button(append([]Node{Class(iconButtonClass(size))}, nodes...)...)
}

func PrimaryLink(href, size string, nodes ...Node) Node {
	base := []Node{Href(href), Class(linkButtonClass(primaryButtonClass(size)))}
	return A(append(base, nodes...)...)
}

func SecondaryLink(href, size string, nodes ...Node) Node {
	base := []Node{Href(href), Class(linkButtonClass(secondaryButtonClass(size)))}
	return A(append(base, nodes...)...)
}

func DangerLink(href, size string, nodes ...Node) Node {
	base := []Node{Href(href), Class(linkButtonClass(dangerButtonClass(size)))}
	return A(append(base, nodes...)...)
}

func InputControl(extraClass string, nodes ...Node) Node {
	base := []Node{Class(formControlClass(extraClass))}
	return Input(append(base, nodes...)...)
}

func TextareaControl(extraClass string, nodes ...Node) Node {
	base := []Node{Class(formControlClass(extraClass))}
	return Textarea(append(base, nodes...)...)
}

func SelectControl(extraClass string, nodes ...Node) Node {
	base := []Node{Class(formControlClass(extraClass))}
	return Select(append(base, nodes...)...)
}

func TableContainer(extraClass string, nodes ...Node) Node {
	base := []Node{Class(tableWrapClass(extraClass))}
	return Div(append(base, nodes...)...)
}

func DataTable(extraClass string, nodes ...Node) Node {
	base := []Node{Class(dataTableClass(extraClass))}
	return Table(append(base, nodes...)...)
}

func Badge(text, tone string) Node {
	return Span(Class(labelClass(tone)), Text(text))
}

func ActionMenu(label string, items ...Node) Node {
	summaryClass := "list-none [&::-webkit-details-marker]:hidden inline-flex min-h-8 items-center justify-center rounded-lg border border-border bg-background px-3 text-sm font-medium text-foreground shadow-xs hover:bg-surface-muted"
	summaryContent := Node(Text(label))
	if label == "More" || label == "Actions" {
		summaryClass = "list-none [&::-webkit-details-marker]:hidden inline-flex min-h-8 min-w-8 items-center justify-center rounded-lg border border-border bg-background px-2 text-foreground shadow-xs hover:bg-surface-muted"
		summaryContent = Group([]Node{
			I(Class(IconGlyphClass()), Attr("data-lucide", "ellipsis"), Attr("aria-hidden", "true")),
			Span(Class("sr-only"), Text(label)),
		})
	}
	return Details(
		Class(DetailsClass()),
		Summary(Class(summaryClass), Title(label), Attr("aria-label", label), summaryContent),
		Div(Class(DropdownMenuClass()), Group(items)),
	)
}

func ActionMenuLink(href, label string) Node {
	return A(Href(href), Class(DropdownItemClass("text-foreground")), Span(Text(label)))
}

func ActionMenuPost(action, label string, csrfField func() Node, danger bool) Node {
	btnClass := DropdownItemClass()
	if danger {
		btnClass += " text-danger-text hover:bg-danger-muted"
	} else {
		btnClass += " text-foreground"
	}
	button := Form(
		Method("post"),
		Action(action),
		csrfField(),
		Button(Type("submit"), Class(btnClass), Span(Text(label))),
	)
	if danger {
		return Group([]Node{
			Div(Class("dropdown-divider my-1 border-t border-border-muted")),
			button,
		})
	}
	return button
}

func ButtonGroup(extraClass string, nodes ...Node) Node {
	base := []Node{Class(buttonRowClass(extraClass))}
	return Div(append(base, nodes...)...)
}

func EmptyState(iconName, title, message string, action Node) Node {
	icon := Node(nil)
	if iconName != "" {
		icon = Div(
			Class("mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-surface-muted text-accent"),
			I(Class(NavIconClass()), Attr("data-lucide", iconName), Attr("aria-hidden", "true")),
		)
	}
	return Div(
		Class(cardClass("text-center")),
		icon,
		Div(
			Class("flex flex-col items-center gap-2 text-center"),
			P(Class("m-0 text-lg font-semibold"), Text(title)),
			P(Class("m-0 text-sm text-muted"), Text(message)),
			action,
		),
	)
}

func MetaItem(label, value string) Node {
	return Div(
		Class("grid gap-1 border-b border-border pb-2 last:border-b-0 last:pb-0"),
		Span(Class("text-[11px] font-semibold uppercase tracking-[0.04em] text-muted"), Text(label)),
		Span(Class("text-sm text-foreground"), Text(value)),
	)
}

func MetricCard(label, value, hint string) Node {
	return Div(
		Class("flex flex-col gap-1 rounded-xl border border-border bg-surface-muted p-4 shadow-xs"),
		P(Class("m-0 text-xs font-semibold uppercase tracking-[0.04em] text-muted"), Text(label)),
		P(Class("m-0 text-2xl font-semibold text-foreground"), Text(value)),
		P(Class("m-0 text-xs text-muted"), Text(hint)),
	)
}

func FactList(items [][2]string) Node {
	rows := make([]Node, 0, len(items))
	for i := range items {
		rows = append(rows, Div(
			Class("flex items-start justify-between gap-3 rounded-lg border border-border bg-background px-3 py-2"),
			Span(Class("text-xs font-semibold uppercase tracking-[0.04em] text-muted"), Text(items[i][0])),
			Span(Class("text-sm text-right text-foreground"), Text(items[i][1])),
		))
	}
	return Div(Class("grid gap-2"), Group(rows))
}

func SubtleLink(href string, nodes ...Node) Node {
	base := []Node{
		Href(href),
		Class("text-muted no-underline hover:text-foreground"),
	}
	return A(append(base, nodes...)...)
}

func DetailShell(nodes ...Node) Node {
	return Div(append([]Node{Class("flex flex-col gap-4")}, nodes...)...)
}

func DetailHero(nodes ...Node) Node {
	return Div(append([]Node{Class("grid gap-4 rounded-2xl border border-border bg-[linear-gradient(135deg,var(--color-surface-muted)_0%,var(--color-background)_65%)] p-5 shadow-sm lg:grid-cols-[minmax(0,1.5fr)_minmax(16rem,0.8fr)]")}, nodes...)...)
}

func DetailHeroCopy(nodes ...Node) Node {
	return Div(append([]Node{Class("flex min-w-0 flex-col gap-3")}, nodes...)...)
}

func DetailHeroMeta(nodes ...Node) Node {
	return Div(append([]Node{Class("grid gap-2 rounded-xl border border-border bg-background p-4")}, nodes...)...)
}

func Kicker(text string) Node {
	return P(Class("m-0 text-[11px] font-semibold uppercase tracking-[0.08em] text-muted"), Text(text))
}

func DetailTitleRow(nodes ...Node) Node {
	return Div(append([]Node{Class("flex flex-wrap items-center gap-3")}, nodes...)...)
}

func DetailTitle(text string) Node {
	return H2(Class("m-0 text-3xl font-semibold leading-tight text-foreground"), Text(text))
}

func DetailDescription(text string) Node {
	return P(Class("m-0 max-w-3xl text-sm leading-6 text-muted"), Text(text))
}

func BadgeRow(nodes ...Node) Node {
	return Div(append([]Node{Class("flex flex-wrap items-center gap-2")}, nodes...)...)
}

func MetricsGrid(nodes ...Node) Node {
	return Div(append([]Node{Class("grid gap-3 sm:grid-cols-2 xl:grid-cols-4")}, nodes...)...)
}

func DetailLayout(nodes ...Node) Node {
	return Div(append([]Node{Class("grid gap-4 xl:grid-cols-[minmax(0,1.7fr)_minmax(18rem,0.8fr)]")}, nodes...)...)
}

func DetailMain(nodes ...Node) Node {
	return Div(append([]Node{Class("flex min-w-0 flex-col gap-4")}, nodes...)...)
}

func DetailRail(nodes ...Node) Node {
	return Div(append([]Node{Class("flex min-w-0 flex-col gap-4")}, nodes...)...)
}

func ItemList(extraClass string, nodes ...Node) Node {
	className := "grid gap-2"
	if extraClass != "" {
		className += " " + extraClass
	}
	return Ul(append([]Node{Class(className)}, nodes...)...)
}

func ItemListEntry(nodes ...Node) Node {
	return Li(append([]Node{Class("rounded-lg border border-border bg-background px-3 py-2 text-sm")}, nodes...)...)
}
