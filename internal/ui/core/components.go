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
	base := []Node{Href(href), Class(primaryButtonClass(size))}
	return A(append(base, nodes...)...)
}

func SecondaryLink(href, size string, nodes ...Node) Node {
	base := []Node{Href(href), Class(secondaryButtonClass(size))}
	return A(append(base, nodes...)...)
}

func DangerLink(href, size string, nodes ...Node) Node {
	base := []Node{Href(href), Class(dangerButtonClass(size))}
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

func ButtonGroup(extraClass string, nodes ...Node) Node {
	base := []Node{Class(buttonRowClass(extraClass))}
	return Div(append(base, nodes...)...)
}

func EmptyState(iconName, title, message string, action Node) Node {
	icon := Node(nil)
	if iconName != "" {
		icon = Div(
			Class("mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-[var(--bgColor-muted)] text-[var(--fgColor-accent)]"),
			I(Class(NavIconClass()), Attr("data-lucide", iconName), Attr("aria-hidden", "true")),
		)
	}
	return Div(
		Class(cardClass("text-center")),
		icon,
		Div(
			Class("flex flex-col items-center gap-2 text-center"),
			P(Class("m-0 text-lg font-semibold"), Text(title)),
			P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(message)),
			action,
		),
	)
}
